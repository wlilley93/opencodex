import { createHash } from "node:crypto";
import { formatErrorResponse } from "../bridge";
import {
  liveVoiceProviderEndpointError,
  resolveLiveVoiceHeaders,
  type LiveVoiceTargetResolution,
} from "../config/live-voice";
import { selectVoiceBackend } from "../config/voice-target";
import { MAIN_CODEX_ACCOUNT_ID } from "../codex/account-id";
import { cancelBodyOnAbort, clearableDeadline } from "../lib/abort";
import type { AdmissionLease } from "../lib/admission";
import { captureExplicitOpenAiCallerAuth } from "../providers/openai-sidecar";
import type { OcxConfig } from "../types";
import type { AudioClient } from "./audio-client";
import type { DataPlaneAdmission } from "./auth-cors";
import { dictationConversationIds, finishAudioUpstream, latestDictationModelFromEntries, type AudioSocketTarget } from "./audio-dictation";
import { LIVE_AUDIO_MODEL, resolveAudioUpstream, type AudioUpstream } from "./audio-upstream";
import { getRequestLogEntries } from "./request-log";
import { registerTurn, unregisterTurn } from "./lifecycle";
import {
  backendJsonBodyFromApiMultipart, buildLiveSidebandUpstreamWsUrl, forwardLiveUrl, keyedLiveUrl,
  LIVE_CLIENT_PROTOCOL_HEADERS, LIVE_REQUEST_MAX_BYTES, LIVE_RESPONSE_MAX_BYTES, liveSidebandModel,
  readBodyCapped,
  type LiveSidebandTarget,
} from "./live";
import { EXTERNAL_CALL_PREFIX, LIVE_CALL_TTL_MS, LiveCallBindings, upstreamLiveCallId } from "./live-call-bindings";
import type { RequestLogContext } from "./request-log";

function protocolHeaders(client: AudioClient, relay: AudioUpstream, frameless: boolean): Headers {
  const headers = new Headers();
  for (const name of LIVE_CLIENT_PROTOCOL_HEADERS) {
    const value = client.headers.get(name);
    if (value) headers.set(name, value);
  }
  for (const [name, value] of Object.entries(relay.headers)) headers.set(name, value);
  if (frameless && !headers.has("openai-alpha")) headers.set("openai-alpha", "quicksilver=v2");
  return headers;
}

/**
 * Pick the live voice backend for one active model: exact `byModel` entry, then `liveVoice.provider`,
 * then the reserved `"openai"` (the historical realtime relay). Resolution is the shared config
 * helper, so the runtime reports the same unknown/registry-managed rejection the write boundary does.
 */
export function selectLiveVoiceBackend(config: OcxConfig, modelId: string | undefined): LiveVoiceTargetResolution {
  return selectVoiceBackend(config.providers, config.liveVoice, modelId, "live");
}

/**
 * Resolve the thread's active model from the request log, exactly as dictation does: the most
 * recent logged request whose `conversationId` digests to one of the upgrade's thread identities.
 */
export function resolveLiveVoiceActiveModelId(headers: Headers): string | undefined {
  return latestDictationModelFromEntries(getRequestLogEntries(), dictationConversationIds(headers));
}

async function parseExternalOffer(req: Request, signal: AbortSignal): Promise<{ sdp: string; session?: Record<string, unknown> } | Response> {
  const upload = clearableDeadline(30_000, signal);
  try {
    const body = await readBodyCapped(req.body, LIVE_REQUEST_MAX_BYTES, () => "Live offer too large", upload.signal);
    if (body instanceof Response) return formatErrorResponse(413, "invalid_request_error", "Live offer exceeds 16 MiB");
    const type = req.headers.get("content-type") ?? "";
    let payload: unknown;
    if (type.toLowerCase().includes("multipart/form-data")) {
      const converted = await backendJsonBodyFromApiMultipart(body, type);
      if (converted instanceof Response) return converted;
      payload = JSON.parse(new TextDecoder().decode(converted.body));
    } else if (type.toLowerCase().includes("application/json")) {
      payload = JSON.parse(new TextDecoder().decode(body));
    } else if (type.toLowerCase().includes("application/sdp")) {
      payload = { sdp: new TextDecoder().decode(body) };
    }
    if (!payload || typeof payload !== "object" || !("sdp" in payload) || typeof payload.sdp !== "string" || !payload.sdp.trim()) {
      return formatErrorResponse(400, "invalid_request_error", "Live call requires a nonempty SDP offer");
    }
    const session = "session" in payload ? payload.session : undefined;
    if (session !== undefined && (!session || typeof session !== "object" || Array.isArray(session))) {
      return formatErrorResponse(400, "invalid_request_error", "Live session must be an object");
    }
    return { sdp: payload.sdp, ...(session ? { session: session as Record<string, unknown> } : {}) };
  } catch {
    if (signal.aborted) throw signal.reason;
    return formatErrorResponse(upload.didExpire() ? 408 : 400, "invalid_request_error", upload.didExpire() ? "Live offer upload timed out" : "Malformed live offer");
  } finally { upload.clear(); }
}

/**
 * Create a call against a custom live backend: post the offer, keep its answer,
 * and bind its call id to an opaque alias exactly as the relay path does.
 *
 * `sidebandBaseUrl` is the provider's own endpoint rather than the configured
 * realtime base, so the sideband join that follows lands on the same backend
 * that answered the offer. Without it the client would negotiate media with one
 * server and then open its control socket against another.
 */
async function createCustomLiveCall(args: {
  offerSdp: string;
  /** The client's session block, forwarded as it came: the backend reads the instructions and voice from it. */
  session?: Record<string, unknown>;
  frameless: boolean;
  selection: Extract<LiveVoiceTargetResolution, { kind: "custom" }>;
  owner: string;
  bindings: LiveCallBindings;
  signal: AbortSignal;
}): Promise<Response> {
  const base = args.selection.provider.liveUrl!.replace(/\/+$/, "");
  const headers = new Headers(resolveLiveVoiceHeaders(args.selection.provider.liveHeaders));
  // The offer travels in the shape the Codex client itself uses -- the multipart
  // with an `sdp` part and a `session` part -- when there is a session to carry,
  // so a backend that speaks the OpenAI call-create shape needs no second
  // parser. An SDP-only offer stays SDP-only.
  let body = args.offerSdp;
  headers.set("content-type", "application/sdp");
  if (args.session) {
    const boundary = "codex-realtime-call-boundary";
    body = [
      `--${boundary}`,
      `Content-Disposition: form-data; name="sdp"`,
      `Content-Type: application/sdp`,
      ``,
      args.offerSdp,
      `--${boundary}`,
      `Content-Disposition: form-data; name="session"`,
      `Content-Type: application/json`,
      ``,
      JSON.stringify(args.session),
      `--${boundary}--`,
      ``,
    ].join("\r\n");
    headers.set("content-type", `multipart/form-data; boundary=${boundary}`);
  }
  let upstream: Response;
  try {
    upstream = await fetch(`${base}/live`, {
      method: "POST", headers, body, signal: args.signal, redirect: "manual",
    });
  } catch {
    return formatErrorResponse(502, "upstream_error", `Live backend "${args.selection.providerName}" is unreachable`);
  }
  if (!upstream.ok) {
    return formatErrorResponse(upstream.status >= 400 ? upstream.status : 502, "upstream_error",
      `Live backend "${args.selection.providerName}" returned HTTP ${upstream.status}`);
  }
  const answer = await upstream.text();
  const callId = upstreamLiveCallId(upstream.headers.get("location"));
  if (!callId || !answer.trim()) {
    return formatErrorResponse(502, "upstream_error", `Live backend "${args.selection.providerName}" returned an invalid answer`);
  }
  const alias = args.bindings.create({
    owner: args.owner,
    upstreamCallId: callId,
    joinStyle: args.frameless ? "frameless-path" : "realtime-query",
    providerName: args.selection.providerName,
    callerOwned: false,
    sidebandBaseUrl: base,
  });
  if (!alias) return formatErrorResponse(503, "server_busy", "Live call could not be registered");
  return new Response(answer, { status: 200, headers: {
    "content-type": upstream.headers.get("content-type") ?? "application/sdp",
    location: `/v1/${args.frameless ? "live" : "realtime/calls"}/${alias}`,
  } });
}


/**
 * The desktop's live call arrives with a ChatGPT bearer, not a proxy key, and
 * on loopback it is admitted as a plain caller. `resolveAudioClient` rightly
 * refuses to call that an audio client -- the bearer is OpenAI's credential,
 * not ours -- so the call fell to the relay path, which never reads `liveVoice`.
 * A configured custom backend was unreachable from the one client it exists
 * for.
 *
 * This lends a plain admission the client shape the external path needs, ONLY
 * when `liveVoice` has named a custom backend (at call-create) or the join names
 * a proxy alias (whose binding then decides, by owner). The owner is derived
 * from the bearer the caller presented, so the join that follows a create is
 * recognised as the same caller, and a caller with a different bearer -- or
 * none -- does not inherit someone else's call.
 */
export function liveVoiceClientForPlainAdmission(
  req: Request, config: OcxConfig, admission: DataPlaneAdmission, target?: LiveSidebandTarget | null,
): AudioClient | null {
  const headers = new Headers(req.headers);
  const joiningAlias = !!target && "callId" in target && target.callId.startsWith(EXTERNAL_CALL_PREFIX);
  if (!joiningAlias) {
    const selection = selectLiveVoiceBackend(config, resolveLiveVoiceActiveModelId(headers));
    if (selection.kind !== "custom") return null;
  }
  const bearer = /^Bearer\s+([^\s,]+)$/i.exec(headers.get("authorization") ?? "")?.[1] ?? "";
  const owner = JSON.stringify(["plain", createHash("sha256").update(bearer).digest("hex")]);
  return { admission, headers, owner };
}

export async function handleExternalLive(
  req: Request, config: OcxConfig, log: RequestLogContext,
  options: { client: AudioClient; lease: AdmissionLease; bindings: LiveCallBindings },
): Promise<Response> {
  const controller = new AbortController();
  registerTurn(controller, options.lease);
  const deadline = clearableDeadline(120_000, AbortSignal.any([req.signal, controller.signal]));
  let relay: AudioUpstream | undefined;
  let outcome: number | "timeout" | "connect_error" | undefined;
  try {
    const offer = await parseExternalOffer(req, deadline.signal);
    if (offer instanceof Response) return offer;
    if (!options.bindings.hasCapacity()) return formatErrorResponse(503, "server_busy", "Live call capacity reached");
    const frameless = new URL(req.url).pathname === "/v1/live";
    const session = offer.session ? { ...offer.session } : frameless ? {
      model: LIVE_AUDIO_MODEL, instructions: "", audio: { output: { voice: "cove" } }, delegation: { type: "client" },
    } : undefined;
    if (session?.model === "gpt-live-1") session.model = LIVE_AUDIO_MODEL;
    const model = typeof session?.model === "string" ? session.model : LIVE_AUDIO_MODEL;

    // A CUSTOM LIVE BACKEND IS DECIDED HERE, BEFORE resolveAudioUpstream.
    //
    // `selectLiveVoiceBackend` used to be consulted only on the standalone
    // sideband socket below, which meant a client that CREATES a call -- which
    // is every Codex desktop call -- could never reach a custom backend: the
    // credential check ran first and answered "Audio requires a connected
    // ChatGPT account or OpenAI API provider" before the `liveVoice` block was
    // read at all. The config looked configurable and could not fire.
    //
    // The principle is already stated on the socket path: "a custom endpoint
    // relays frames verbatim, so it never resolves a relay account". That is
    // just as true at call-create, so the account is not resolved here either.
    const liveSelection = selectLiveVoiceBackend(config, resolveLiveVoiceActiveModelId(options.client.headers));
    if (liveSelection.kind === "invalid") {
      return formatErrorResponse(400, "invalid_request_error", liveSelection.error);
    }
    if (liveSelection.kind === "custom") {
      const endpointError = liveVoiceProviderEndpointError(liveSelection.providerName, liveSelection.provider);
      if (endpointError) return formatErrorResponse(400, "invalid_request_error", endpointError);
      return await createCustomLiveCall({
        offerSdp: offer.sdp, session, frameless, selection: liveSelection,
        owner: options.client.owner, bindings: options.bindings, signal: deadline.signal,
      });
    }

    const resolved = await resolveAudioUpstream(options.client.headers, config, log, {
      admission: options.client.admission, model, lease: options.lease, signal: deadline.signal,
    });
    if (!(resolved instanceof Response)) relay = resolved;
    deadline.signal.throwIfAborted();
    if (resolved instanceof Response) return resolved;
    relay = resolved;
    const headers = protocolHeaders(options.client, relay, frameless);
    let body: BodyInit;
    if (relay.keyed) {
      const form = new FormData();
      form.set("sdp", offer.sdp);
      if (session) form.set("session", JSON.stringify(session));
      headers.delete("content-type");
      body = form;
    } else {
      headers.set("content-type", "application/json");
      body = JSON.stringify({ sdp: offer.sdp, ...(session ? { session } : {}) });
    }
    const url = relay.keyed
      ? frameless ? forwardLiveUrl(relay.providerBaseUrl, false) : keyedLiveUrl(relay.providerBaseUrl)
      : forwardLiveUrl(relay.providerBaseUrl, true);
    const upstream = await fetch(url, { method: "POST", headers, body, signal: deadline.signal, redirect: "manual" });
    outcome = upstream.status;
    const detach = cancelBodyOnAbort(upstream.body, deadline.signal);
    let responseBody: ArrayBuffer | Response;
    try { responseBody = await readBodyCapped(upstream.body, LIVE_RESPONSE_MAX_BYTES, () => "Live answer too large", deadline.signal); }
    finally { detach(); }
    if (responseBody instanceof Response) return responseBody;
    if (!upstream.ok) return formatErrorResponse(upstream.status >= 400 ? upstream.status : 502, "upstream_error", `Live upstream returned HTTP ${upstream.status}`);
    const callId = upstreamLiveCallId(upstream.headers.get("location"));
    if (!callId || responseBody.byteLength === 0) return formatErrorResponse(502, "upstream_error", "Live upstream returned an invalid call answer");
    const context = relay.authContext;
    const callerOwned = context?.kind === "main" && captureExplicitOpenAiCallerAuth(options.client.headers, config) !== null;
    const alias = options.bindings.create({
      owner: options.client.owner, upstreamCallId: callId,
      joinStyle: frameless ? "frameless-path" : "realtime-query",
      providerName: relay.providerName,
      // A join carries no model of its own; this is the one the call settled on.
      model,
      accountId: context ? context.kind === "main" ? callerOwned ? undefined : MAIN_CODEX_ACCOUNT_ID : context.accountId : undefined,
      chatgptAccountId: new Headers(relay.headers).get("chatgpt-account-id") ?? undefined,
      keyedCredentialDigest: relay.keyed ? createHash("sha256").update(new Headers(relay.headers).get("authorization") ?? "").digest("hex") : undefined,
      callerOwned,
      sidebandBaseUrl: config.experimentalRealtimeWsBaseUrl,
    });
    if (!alias) return formatErrorResponse(503, "server_busy", "Live call could not be registered");
    return new Response(responseBody, { status: upstream.status, headers: {
      "content-type": upstream.headers.get("content-type") ?? "application/sdp",
      location: `/v1/${frameless ? "live" : "realtime/calls"}/${alias}`,
    } });
  } catch {
    if (req.signal.aborted || controller.signal.aborted) {
      outcome = undefined;
      return formatErrorResponse(req.signal.aborted ? 499 : 503, "client_closed_request", "Live call canceled");
    }
    outcome = deadline.didExpire() ? "timeout" : "connect_error";
    return formatErrorResponse(deadline.didExpire() ? 504 : 502, "upstream_error", deadline.didExpire() ? "Live call timed out" : "Live upstream connection failed");
  } finally {
    try { if (outcome !== undefined) relay?.recordOutcome?.(outcome); }
    finally { relay?.release(); deadline.clear(); unregisterTurn(controller); }
  }
}

export async function resolveExternalLiveSocket(
  client: AudioClient, config: OcxConfig, log: RequestLogContext, target: LiveSidebandTarget,
  options: { lease: AdmissionLease; bindings: LiveCallBindings; signal?: AbortSignal },
): Promise<AudioSocketTarget | Response> {
  const binding = "callId" in target ? options.bindings.get(target.callId, client.owner) : undefined;
  if ("callId" in target && !binding) return formatErrorResponse(404, "not_found", "Live call is unavailable for this key");
  if (!binding) {
    // Standalone session (no call-create against the built-in relay): the live voice block can
    // point the whole socket at a custom upstream. A session already bound to a relay call keeps
    // the relay — the call was created there, and repointing mid-call would strand the join.
    const selection = selectLiveVoiceBackend(config, resolveLiveVoiceActiveModelId(client.headers));
    if (selection.kind === "invalid") {
      return formatErrorResponse(400, "invalid_request_error", selection.error);
    }
    if (selection.kind === "custom") {
      // A custom endpoint relays frames verbatim, so it never resolves a relay account.
      const endpointError = liveVoiceProviderEndpointError(selection.providerName, selection.provider);
      if (endpointError) return formatErrorResponse(400, "invalid_request_error", endpointError);
      return {
        headers: resolveLiveVoiceHeaders(selection.provider.liveHeaders),
        upstreamWsUrl: selection.provider.liveUrl!,
        maxSessionMs: LIVE_CALL_TTL_MS,
        finish: () => {},
      };
    }
  }
  // A call created against a custom backend keeps THAT backend for its sideband,
  // for the same reason a relay-bound call keeps the relay: the call was created
  // there, and repointing mid-call strands the join. Without this the socket
  // would fall through to resolveAudioUpstream below and demand the relay
  // account the call-create path deliberately never asked for.
  if (binding) {
    const bound = config.providers?.[binding.providerName];
    if (bound?.liveUrl) {
      return {
        headers: resolveLiveVoiceHeaders(bound.liveHeaders),
        upstreamWsUrl: buildLiveSidebandUpstreamWsUrl(
          { style: binding.joinStyle, callId: binding.upstreamCallId } as LiveSidebandTarget,
          binding.sidebandBaseUrl ?? bound.liveUrl),
        maxSessionMs: LIVE_CALL_TTL_MS,
        finish: () => {},
      };
    }
  }
  if (binding?.callerOwned && !captureExplicitOpenAiCallerAuth(client.headers, config)) {
    return formatErrorResponse(401, "authentication_error", "Live call requires its original caller account");
  }
  let upstreamTarget = binding ? { style: binding.joinStyle, callId: binding.upstreamCallId } as LiveSidebandTarget : target;
  const frameless = upstreamTarget.style === "frameless-path" || upstreamTarget.style === "frameless-standalone";
  if (upstreamTarget.style === "frameless-standalone") {
    const query = new URLSearchParams(upstreamTarget.query);
    if (!query.has("model") || query.get("model") === "gpt-live-1") query.set("model", LIVE_AUDIO_MODEL);
    upstreamTarget = { ...upstreamTarget, query: query.toString() };
  }
  // What this socket will actually run: the model a bound call settled on when
  // this same key created it, or the one a standalone session forwards in its
  // own query. Reading only the Frameless query left a Realtime standalone
  // socket judged as the default while its query carried another model.
  const model = binding?.model ?? liveSidebandModel(upstreamTarget) ?? LIVE_AUDIO_MODEL;
  const relay = await resolveAudioUpstream(client.headers, config, log, {
    admission: client.admission, model, lease: options.lease,
    signal: options.signal,
    ...(binding ? { exactAccountId: binding.accountId, providerName: binding.providerName } : {}),
  });
  if (relay instanceof Response) return relay;
  try {
    if (binding && (relay.providerName !== binding.providerName
      || (new Headers(relay.headers).get("chatgpt-account-id") ?? undefined) !== binding.chatgptAccountId
      || (binding.keyedCredentialDigest !== undefined
        && createHash("sha256").update(new Headers(relay.headers).get("authorization") ?? "").digest("hex") !== binding.keyedCredentialDigest))) {
      relay.release();
      return formatErrorResponse(409, "authentication_error", "Live call account is no longer available");
    }
    return {
      headers: Object.fromEntries(protocolHeaders(client, relay, frameless)),
      upstreamWsUrl: buildLiveSidebandUpstreamWsUrl(upstreamTarget, binding ? binding.sidebandBaseUrl : config.experimentalRealtimeWsBaseUrl),
      maxSessionMs: LIVE_CALL_TTL_MS,
      finish: finishAudioUpstream(relay),
    };
  } catch {
    relay.release();
    return formatErrorResponse(400, "invalid_request_error", "Invalid live endpoint configuration");
  }
}
