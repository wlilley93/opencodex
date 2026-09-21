/**
 * liveVoice at CALL-CREATE, not only on the standalone socket.
 *
 * Every Codex desktop voice session begins with `POST /v1/realtime/calls`
 * carrying an SDP offer. Until this branch that route resolved an OpenAI
 * credential before it read the `liveVoice` block, so a custom backend could be
 * configured and never reached. These tests hold the door open: a call is
 * created against the configured backend with no OpenAI credential in the
 * config at all, the client's session block travels with the offer, and the
 * sideband join that follows lands on the same backend that answered.
 */
import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { saveConfig } from "../../src/config";
import { startServer } from "../../src/server";
import type { OcxConfig } from "../../src/types";
import { installIsolatedCodexHome, type IsolatedCodexHome } from "../helpers/isolated-codex-home";
import { removeTreeWithRetry } from "../helpers/remove-tree";
import { fakeChatGptJwt } from "../helpers/fake-chatgpt-jwt";

const KEY = "ocx_data_live_call_create_client";
const previousHome = process.env.OPENCODEX_HOME;
const previousToken = process.env.OPENCODEX_API_AUTH_TOKEN;
let home = "";
let codex: IsolatedCodexHome;
let server: ReturnType<typeof startServer> | undefined;
let backend: ReturnType<typeof Bun.serve> | undefined;

interface Captured {
  method: string;
  path: string;
  search: string;
  headers: Headers;
  body: string;
}

interface BackendOptions {
  status?: number;
  location?: string | null;
}

/** A stand-in for a realtime backend: answers the offer and echoes the sideband. */
function startBackend(captured: Captured[], options: BackendOptions = {}) {
  const status = options.status ?? 200;
  const location = options.location === undefined ? "/live/rtc_shim_1" : options.location;
  backend = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    async fetch(req, srv) {
      const url = new URL(req.url);
      captured.push({
        method: req.method,
        path: url.pathname,
        search: url.search,
        headers: new Headers(req.headers),
        body: req.headers.get("upgrade") ? "" : await req.text(),
      });
      if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
        if (srv.upgrade(req, { data: {} })) return undefined as unknown as Response;
        return new Response("upgrade failed", { status: 500 });
      }
      return new Response("v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=answer\r\n", {
        status,
        headers: { "content-type": "application/sdp", ...(location ? { location } : {}) },
      });
    },
    websocket: {
      message(ws, message) {
        ws.send(`backend:${String(message)}`);
      },
    },
  });
  return backend;
}

function config(liveUrl: string): OcxConfig {
  return {
    port: 0,
    hostname: "127.0.0.1",
    defaultProvider: "dummy",
    providers: {
      dummy: { adapter: "openai-chat", baseUrl: "https://example.test/v1" },
      shim: {
        adapter: "openai-chat",
        baseUrl: "https://example.test/v1",
        liveUrl,
        liveHeaders: { "x-shim-token": "shim-secret" },
      },
    },
    liveVoice: { provider: "shim" },
    apiKeys: [{ id: "live-client", name: "live", key: KEY, createdAt: "2026-09-21T00:00:00Z" }],
  } as OcxConfig;
}

/**
 * The desktop's shape: no proxy key anywhere. It is admitted on loopback as a
 * plain caller and carries its ChatGPT bearer, which is OpenAI's credential,
 * not ours. This is the client liveVoice exists for.
 */
function desktopConfig(liveUrl: string): OcxConfig {
  const cfg = config(liveUrl);
  delete (cfg as { apiKeys?: unknown }).apiKeys;
  return cfg;
}

const DESKTOP_BEARER = fakeChatGptJwt({ chatgpt_account_id: "acct-desktop" });

function multipartOffer(sdp: string, session: Record<string, unknown>) {
  const boundary = "codex-realtime-call-boundary";
  const body = [
    `--${boundary}`,
    `Content-Disposition: form-data; name="sdp"`,
    `Content-Type: application/sdp`,
    ``,
    sdp,
    `--${boundary}`,
    `Content-Disposition: form-data; name="session"`,
    `Content-Type: application/json`,
    ``,
    JSON.stringify(session),
    `--${boundary}--`,
    ``,
  ].join("\r\n");
  return { body, contentType: `multipart/form-data; boundary=${boundary}` };
}

async function createCall(body: BodyInit, contentType: string, path = "/v1/realtime/calls"): Promise<Response> {
  server ??= startServer(0);
  return fetch(new URL(path, server.url), {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": contentType },
    body,
  });
}

beforeEach(() => {
  home = mkdtempSync(join(tmpdir(), "ocx-live-call-"));
  process.env.OPENCODEX_HOME = home;
  delete process.env.OPENCODEX_API_AUTH_TOKEN;
  codex = installIsolatedCodexHome("ocx-live-call-codex-");
});

afterEach(async () => {
  await server?.stop(true);
  server = undefined;
  await backend?.stop(true);
  backend = undefined;
  codex.restore();
  if (previousHome === undefined) delete process.env.OPENCODEX_HOME;
  else process.env.OPENCODEX_HOME = previousHome;
  if (previousToken === undefined) delete process.env.OPENCODEX_API_AUTH_TOKEN;
  else process.env.OPENCODEX_API_AUTH_TOKEN = previousToken;
  removeTreeWithRetry(home);
});

test("a call is created against the custom live backend with no OpenAI credential", async () => {
  const captured: Captured[] = [];
  const shim = startBackend(captured);
  saveConfig(config(`http://127.0.0.1:${shim.port}`));

  const session = { instructions: "You are the voice of this workstation.", audio: { output: { voice: "cove" } } };
  const { body, contentType } = multipartOffer("v=0-offer-from-codex", session);
  const response = await createCall(body, contentType);

  expect(response.status).toBe(200);
  expect(response.headers.get("location")).toMatch(/^\/v1\/realtime\/calls\/rtc_ocx_[0-9a-f]{32}$/);
  expect(await response.text()).toContain("s=answer");

  expect(captured).toHaveLength(1);
  const offer = captured[0]!;
  expect(offer.method).toBe("POST");
  expect(offer.path).toBe("/live");
  expect(offer.headers.get("x-shim-token")).toBe("shim-secret");
  // The client's own shape, not a second one: the sdp part and the session part.
  expect(offer.headers.get("content-type")).toContain("multipart/form-data");
  expect(offer.body).toContain("v=0-offer-from-codex");
  expect(offer.body).toContain(JSON.stringify(session));
});

test("an SDP-only offer stays SDP-only on the way to the backend", async () => {
  const captured: Captured[] = [];
  const shim = startBackend(captured);
  saveConfig(config(`http://127.0.0.1:${shim.port}`));

  const response = await createCall("v=0-bare-offer", "application/sdp");
  expect(response.status).toBe(200);
  expect(captured).toHaveLength(1);
  expect(captured[0]!.headers.get("content-type")).toBe("application/sdp");
  expect(captured[0]!.body).toBe("v=0-bare-offer");
});

test("an unreachable backend is a 502 that names the provider, not a credential error", async () => {
  // A port nothing listens on: the refusal must be about the backend, because a
  // credential error here is exactly the wrong blame this branch removes.
  saveConfig(config("http://127.0.0.1:9"));
  const { body, contentType } = multipartOffer("v=0", { instructions: "" });
  const response = await createCall(body, contentType);
  expect(response.status).toBe(502);
  const payload = await response.json() as { error: { type: string; message: string } };
  // The claim is the blame: the backend, never the caller's credential.
  expect(payload.error.type).not.toBe("authentication_error");
  expect(payload.error.message).toContain('"shim"');
  expect(payload.error.message).toContain("unreachable");
});

test("a backend that answers without a call id is refused as an invalid answer", async () => {
  const captured: Captured[] = [];
  const shim = startBackend(captured, { location: null });
  saveConfig(config(`http://127.0.0.1:${shim.port}`));
  const { body, contentType } = multipartOffer("v=0", { instructions: "" });
  const response = await createCall(body, contentType);
  expect(response.status).toBe(502);
  const payload = await response.json() as { error: { message: string } };
  expect(payload.error.message).toContain("invalid answer");
});

test("a backend HTTP failure is reported with its own status", async () => {
  const captured: Captured[] = [];
  const shim = startBackend(captured, { status: 429 });
  saveConfig(config(`http://127.0.0.1:${shim.port}`));
  const { body, contentType } = multipartOffer("v=0", { instructions: "" });
  const response = await createCall(body, contentType);
  expect(response.status).toBe(429);
  const payload = await response.json() as { error: { message: string } };
  expect(payload.error.message).toContain("HTTP 429");
});

test("the sideband join of a custom-bound call reaches the backend that answered", async () => {
  const captured: Captured[] = [];
  const shim = startBackend(captured);
  saveConfig(config(`http://127.0.0.1:${shim.port}`));
  const { body, contentType } = multipartOffer("v=0", { instructions: "hello" });
  const created = await createCall(body, contentType);
  expect(created.status).toBe(200);
  const alias = created.headers.get("location")!.split("/").at(-1)!;

  const wsUrl = new URL(`/v1/realtime?intent=quicksilver&call_id=${alias}`, server!.url);
  wsUrl.protocol = "ws:";
  const echoed = await new Promise<string>((resolve, reject) => {
    const socket = new WebSocket(wsUrl, { headers: { authorization: `Bearer ${KEY}` } } as unknown as string[]);
    const timer = setTimeout(() => reject(new Error("sideband did not answer")), 8_000);
    socket.onopen = () => socket.send(JSON.stringify({ type: "session.update", session: { instructions: "hello" } }));
    socket.onmessage = event => { clearTimeout(timer); socket.close(); resolve(String(event.data)); };
    socket.onerror = () => { clearTimeout(timer); reject(new Error("sideband socket errored")); };
  });
  expect(echoed).toContain("backend:");
  expect(echoed).toContain("session.update");

  // The join went to the backend's own realtime path, with the backend's call id,
  // never the proxy alias and never api.openai.com.
  const join = captured.find(entry => entry.headers.get("upgrade")?.toLowerCase() === "websocket");
  expect(join).toBeDefined();
  expect(join!.path).toBe("/v1/realtime");
  expect(join!.search).toContain("call_id=rtc_shim_1");
  expect(join!.headers.get("x-shim-token")).toBe("shim-secret");
});

test("the desktop, admitted on loopback with its ChatGPT bearer, reaches the custom backend", async () => {
  const captured: Captured[] = [];
  const shim = startBackend(captured);
  saveConfig(desktopConfig(`http://127.0.0.1:${shim.port}`));
  server ??= startServer(0);
  const { body, contentType } = multipartOffer("v=0-desktop", { instructions: "desk" });
  const created = await fetch(new URL("/v1/realtime/calls", server.url), {
    method: "POST",
    headers: { authorization: `Bearer ${DESKTOP_BEARER}`, "chatgpt-account-id": "acct-desktop", "content-type": contentType },
    body,
  });
  expect(created.status).toBe(200);
  expect(created.headers.get("location")).toMatch(/^\/v1\/realtime\/calls\/rtc_ocx_/);
  expect(captured).toHaveLength(1);
  expect(captured[0]!.path).toBe("/live");
  expect(captured[0]!.body).toContain("v=0-desktop");
  // The ChatGPT bearer is the caller's credential to OpenAI; it does not travel to the backend.
  expect(captured[0]!.headers.get("authorization")).toBeNull();
  expect(captured[0]!.headers.get("chatgpt-account-id")).toBeNull();

  const alias = created.headers.get("location")!.split("/").at(-1)!;
  const wsUrl = new URL(`/v1/realtime?intent=quicksilver&call_id=${alias}`, server.url);
  wsUrl.protocol = "ws:";

  // Same bearer (and the account id codex-rs sends on every leg): the join is
  // recognised as the creator and lands on the backend.
  const echoed = await new Promise<string>((resolve, reject) => {
    const socket = new WebSocket(wsUrl, { headers: { authorization: `Bearer ${DESKTOP_BEARER}`, "chatgpt-account-id": "acct-desktop" } } as unknown as string[]);
    const timer = setTimeout(() => reject(new Error("sideband did not answer")), 8_000);
    socket.onopen = () => socket.send(JSON.stringify({ type: "session.update" }));
    socket.onmessage = event => { clearTimeout(timer); socket.close(); resolve(String(event.data)); };
    socket.onerror = () => { clearTimeout(timer); reject(new Error("sideband socket errored")); };
  });
  expect(echoed).toContain("backend:");
  const join = captured.find(entry => entry.headers.get("upgrade")?.toLowerCase() === "websocket");
  expect(join?.search).toContain("call_id=rtc_shim_1");

  // A different caller -- no bearer at all -- is not the creator, and is refused as before.
  const stranger = await fetch(wsUrl.toString().replace(/^ws:/, "http:"), {
    headers: { upgrade: "websocket", connection: "Upgrade", "sec-websocket-version": "13", "sec-websocket-key": "dGhlIHNhbXBsZSBub25jZQ==" },
  });
  expect(stranger.status).toBe(401);
  expect((await stranger.json() as { error: { message: string } }).error.message).toContain("creator");
});

test("a plain caller with no custom liveVoice is not turned into an external client", async () => {
  // Without liveVoice the relay path is untouched. The observable difference is
  // the Location: the relay returns the upstream's own call id verbatim, where
  // the external path would mint an rtc_ocx_ alias and bind it here.
  const captured: Captured[] = [];
  const shim = startBackend(captured);
  const upstreamCalls: string[] = [];
  const realFetch = globalThis.fetch;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url);
    if (url.hostname === "chatgpt.com") {
      upstreamCalls.push(url.pathname);
      return Promise.resolve(new Response("v=0\r\ns=upstream-answer\r\n", {
        status: 201, headers: { "content-type": "application/sdp", location: "/v1/realtime/calls/rtc_upstream_1" },
      }));
    }
    return realFetch(input, init);
  }) as typeof fetch;
  try {
    saveConfig({
      port: 0, hostname: "127.0.0.1", defaultProvider: "openai", openaiProviderTierVersion: 2,
      providers: {
        openai: { adapter: "openai-responses", baseUrl: "https://chatgpt.com/backend-api/codex", authMode: "forward", codexAccountMode: "direct" },
        shim: { adapter: "openai-chat", baseUrl: "https://example.test/v1", liveUrl: `http://127.0.0.1:${shim.port}` },
      },
    } as OcxConfig);
    server ??= startServer(0);
    const { body, contentType } = multipartOffer("v=0-plain", { instructions: "" });
    const response = await fetch(new URL("/v1/realtime/calls", server.url), {
      method: "POST",
      headers: { authorization: `Bearer ${DESKTOP_BEARER}`, "chatgpt-account-id": "acct-desktop", "content-type": contentType },
      body,
    });
    expect(response.status).toBe(201);
    expect(response.headers.get("location")).toBe("/v1/realtime/calls/rtc_upstream_1");
    expect(upstreamCalls).toHaveLength(1);
    expect(captured).toHaveLength(0);
  } finally {
    globalThis.fetch = realFetch;
  }
});
