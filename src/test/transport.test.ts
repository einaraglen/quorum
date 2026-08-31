import { test } from "node:test";
import assert from "node:assert";
import { EventEmitter } from "node:events";
import { App } from "@tinyhttp/app";
import { Transport, type TransportCallbacks } from "../core/transport.js";

let portCounter = 27000;
const nextPort = () => portCounter++;

const makeCallbacks = () => {
  const elections: string[] = [];
  const coordinators: string[] = [];
  const messages: { event: string; payload: unknown }[] = [];
  const cb: TransportCallbacks = {
    onElectionMessage: (id) => elections.push(id),
    onCoordinatorMessage: (id) => coordinators.push(id),
    onMessage: (event, payload) => messages.push({ event, payload }),
    channel: new EventEmitter(),
  };
  return { cb, elections, coordinators, messages };
};

test("postToPeer delivers the body and routes it to the matching callback", async (t) => {
  const port = nextPort();
  const host = "127.0.0.31";
  const transport = new Transport({ internalPort: port, internalHost: host });
  const { cb, elections, coordinators, messages } = makeCallbacks();
  transport.start(cb);
  t.after(() => transport.stop());

  assert.strictEqual(await transport.postToPeer(host, "/bully/election", { id: "pod-a" }), true);
  assert.strictEqual(await transport.postToPeer(host, "/bully/coordinator", { id: "pod-b" }), true);
  assert.strictEqual(
    await transport.postToPeer(host, "/bully/message", { event: "job-done", payload: { n: 1 } }),
    true,
  );

  assert.deepStrictEqual(elections, ["pod-a"]);
  assert.deepStrictEqual(coordinators, ["pod-b"]);
  assert.deepStrictEqual(messages, [{ event: "job-done", payload: { n: 1 } }]);
});

test("pingPeer succeeds against a running server and reports its health shape", async (t) => {
  const port = nextPort();
  const host = "127.0.0.32";
  const transport = new Transport({ internalPort: port, internalHost: host });
  const { cb } = makeCallbacks();
  transport.start(cb);
  t.after(() => transport.stop());

  assert.strictEqual(await transport.pingPeer(host), true);

  const res = await fetch(`http://${host}:${port}/health`);
  const body: any = await res.json();
  assert.strictEqual(res.status, 200);
  assert.strictEqual(body.status, "OK");
  assert.strictEqual(typeof body.timestamp, "string");
});

test("postToPeer and pingPeer return false, not throw, when the peer is unreachable", async (t) => {
  const port = nextPort();
  const transport = new Transport({ internalPort: port, requestTimeoutMs: 300 });
  t.after(() => {});

  assert.strictEqual(await transport.pingPeer("127.0.0.199"), false);
  assert.strictEqual(await transport.postToPeer("127.0.0.199", "/bully/election", { id: "x" }), false);
});

test("postToPeer times out and returns false rather than hanging indefinitely", async (t) => {
  const port = nextPort();
  const host = "127.0.0.33";

  const hangingApp = new App();
  hangingApp.post("/bully/election", () => {
    // never responds
  });
  const hangingServer = hangingApp.listen(port, undefined, host);
  t.after(() => hangingServer.close());

  const transport = new Transport({ internalPort: port, requestTimeoutMs: 150 });
  const start = Date.now();
  const ok = await transport.postToPeer(host, "/bully/election", { id: "x" });
  const elapsed = Date.now() - start;

  assert.strictEqual(ok, false);
  assert.ok(elapsed < 1000, `expected a fast timeout, took ${elapsed}ms`);
});

test("a malformed JSON body results in a 500 error response instead of crashing the server", async (t) => {
  const port = nextPort();
  const host = "127.0.0.34";
  const transport = new Transport({ internalPort: port, internalHost: host });
  const { cb, elections } = makeCallbacks();
  transport.start(cb);
  t.after(() => transport.stop());

  const res = await fetch(`http://${host}:${port}/bully/election`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: "{not valid json",
  });
  const body: any = await res.json();

  assert.strictEqual(res.status, 500);
  assert.strictEqual(body.status, "ERROR");
  assert.deepStrictEqual(elections, [], "the malformed request should never have reached the callback");

  // The server itself should still be alive and serving other requests afterwards.
  assert.strictEqual(await transport.pingPeer(host), true);
});

test("an unknown route returns 404", async (t) => {
  const port = nextPort();
  const host = "127.0.0.35";
  const transport = new Transport({ internalPort: port, internalHost: host });
  const { cb } = makeCallbacks();
  transport.start(cb);
  t.after(() => transport.stop());

  const res = await fetch(`http://${host}:${port}/does-not-exist`);
  assert.strictEqual(res.status, 404);
});

test("internalHost restricts which address the server actually binds to", async (t) => {
  const port = nextPort();
  const boundHost = "127.0.0.36";
  const otherHost = "127.0.0.37";
  const transport = new Transport({ internalPort: port, internalHost: boundHost, requestTimeoutMs: 300 });
  const { cb } = makeCallbacks();
  transport.start(cb);
  t.after(() => transport.stop());

  assert.strictEqual(await transport.pingPeer(boundHost), true);
  assert.strictEqual(await transport.pingPeer(otherHost), false);
});
