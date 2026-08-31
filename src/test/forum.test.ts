import { test } from "node:test";
import assert from "node:assert";
import { Transport } from "../core/transport.js";
import { Bully, type BullyPeer } from "../core/bully.js";
import { Forum } from "../core/forum.js";

const makeNode = (self: BullyPeer, peers: BullyPeer[], mockFetch: typeof fetch) => {
  const transport = new Transport({ fetchFn: mockFetch });
  const discovery = async () => ({ self, cluster: [self, ...peers] });
  const bully = new Bully({ discovery, transport });
  const forum = new Forum({ bully, transport, discovery });
  return { bully, forum };
};

const fakeFetch = (impl: (url: string) => Promise<{ ok: boolean }>): typeof fetch =>
  (async (url: any) => impl(String(url))) as unknown as typeof fetch;

const fakeFetchWithBody = (impl: (url: string, body: any) => Promise<{ ok: boolean }>): typeof fetch =>
  (async (url: any, init: any) => impl(String(url), JSON.parse(init?.body ?? "null"))) as unknown as typeof fetch;

/**
 * A tiny in-memory two-node "network": each node's fetchFn routes /bully/election,
 * /bully/coordinator and /bully/message posts straight into the matching peer's real Bully
 * instance, so request()/respond() can be exercised as an actual round trip across two distinct
 * Forum instances instead of one node talking to itself. Election is run to completion on every
 * node before returning, so `self` is populated and no node is left retrying in the background —
 * callers must still call `stopAll()` when done to clear the heartbeat interval the winner runs.
 */
const makeNetwork = async (names: string[]) => {
  const peers: BullyPeer[] = names.map((name, i) => ({ name, host: `10.0.0.${i + 1}` }));
  const bullies = new Map<string, Bully>();

  const fetchFn: typeof fetch = (async (url: any, init: any) => {
    const parsed = new URL(String(url));
    const target = peers.find((p) => p.host === parsed.hostname);
    const bully = target && bullies.get(target.name);
    if (!bully) return { ok: false } as Response;

    const body = init?.body ? JSON.parse(init.body) : {};
    if (parsed.pathname === "/bully/election") bully.onElectionMessage(body.id);
    else if (parsed.pathname === "/bully/coordinator") bully.onCoordinatorMessage(body.id);
    else if (parsed.pathname === "/bully/message") bully.onMessage(body.event, body.payload);
    return { ok: true } as Response;
  }) as unknown as typeof fetch;

  const forums = new Map<string, Forum>();
  for (const self of peers) {
    const discovery = async () => ({ self, cluster: peers });
    const transport = new Transport({ fetchFn });
    const bully = new Bully({ discovery, transport });
    bullies.set(self.name, bully);
    forums.set(self.name, new Forum({ bully, transport, discovery }));
  }

  await Promise.all([...bullies.values()].map((bully) => bully.startElection()));

  return {
    forum: (name: string) => forums.get(name)!,
    bully: (name: string) => bullies.get(name)!,
    stopAll: () => bullies.forEach((bully) => bully.stop()),
  };
};

test("broadcast fans an event out to every peer and fires it locally", async () => {
  const calls: { url: string; body: any }[] = [];

  const { bully, forum } = makeNode(
    { name: "b", host: "10.0.0.2" },
    [{ name: "a", host: "10.0.0.1" }],
    fakeFetchWithBody(async (url, body) => {
      calls.push({ url, body });
      return { ok: true };
    }),
  );

  await bully.startElection();
  calls.length = 0;

  let localPayload: unknown;
  bully.channel.on("cache-invalidated", (payload) => {
    localPayload = payload;
  });

  await forum.broadcast("cache-invalidated", { key: "pods" });

  assert.deepStrictEqual(localPayload, { key: "pods" });
  const messageCall = calls.find((c) => c.url.includes("/bully/message"));
  assert.ok(messageCall?.url.includes("10.0.0.1"));
  assert.deepStrictEqual(messageCall?.body, { event: "cache-invalidated", payload: { key: "pods" } });
});

test("broadcast is a no-op when this pod is not the leader", async () => {
  const calls: string[] = [];

  const { bully, forum } = makeNode(
    { name: "a", host: "10.0.0.1" },
    [{ name: "b", host: "10.0.0.2" }],
    fakeFetch(async (url) => {
      calls.push(url);
      return { ok: true };
    }),
  );

  let fired = false;
  bully.channel.on("cache-invalidated", () => {
    fired = true;
  });

  await forum.broadcast("cache-invalidated", { key: "pods" });

  assert.strictEqual(fired, false);
  assert.ok(!calls.some((url) => url.includes("/bully/message")));
});

test("send posts a custom event to whichever pod is currently leader", async () => {
  const calls: { url: string; body: any }[] = [];

  const { bully, forum } = makeNode(
    { name: "a", host: "10.0.0.1" },
    [{ name: "b", host: "10.0.0.2" }],
    fakeFetchWithBody(async (url, body) => {
      calls.push({ url, body });
      return { ok: true };
    }),
  );

  bully.onCoordinatorMessage("b");
  await forum.send("job-done", { id: 42 });

  assert.strictEqual(calls.length, 1);
  assert.ok(calls[0].url.includes("10.0.0.2") && calls[0].url.includes("/bully/message"));
  assert.deepStrictEqual(calls[0].body, { event: "job-done", payload: { id: 42 } });
});

test("send fires locally instead of posting when this pod is already the leader", async () => {
  const calls: string[] = [];

  const { bully, forum } = makeNode(
    { name: "b", host: "10.0.0.2" },
    [{ name: "a", host: "10.0.0.1" }],
    fakeFetch(async (url) => {
      calls.push(url);
      return { ok: true };
    }),
  );

  await bully.startElection();

  let received: unknown;
  bully.channel.on("job-done", (payload) => {
    received = payload;
  });

  await forum.send("job-done", { id: 42 });

  assert.deepStrictEqual(received, { id: 42 });
  assert.ok(!calls.some((url) => url.includes("/bully/message")));
});

test("tell posts a custom event to exactly one named peer", async () => {
  const calls: { url: string; body: any }[] = [];

  const { bully, forum } = makeNode(
    { name: "c", host: "10.0.0.3" },
    [
      { name: "a", host: "10.0.0.1" },
      { name: "b", host: "10.0.0.2" },
    ],
    fakeFetchWithBody(async (url, body) => {
      calls.push({ url, body });
      return { ok: true };
    }),
  );

  await bully.startElection();
  calls.length = 0;

  await forum.tell("b", "assign-connections", [1, 2, 3]);

  assert.strictEqual(calls.length, 1);
  assert.ok(calls[0].url.includes("10.0.0.2"));
  assert.deepStrictEqual(calls[0].body, { event: "assign-connections", payload: [1, 2, 3] });
});

test("tell fires locally instead of posting when the target is self", async () => {
  const calls: string[] = [];

  const { bully, forum } = makeNode(
    { name: "b", host: "10.0.0.2" },
    [{ name: "a", host: "10.0.0.1" }],
    fakeFetch(async (url) => {
      calls.push(url);
      return { ok: true };
    }),
  );

  await bully.startElection();
  calls.length = 0;

  let received: unknown;
  bully.channel.on("assign-connections", (payload) => {
    received = payload;
  });

  await forum.tell("b", "assign-connections", [1, 2, 3]);

  assert.deepStrictEqual(received, [1, 2, 3]);
  assert.ok(!calls.some((url) => url.includes("/bully/message")));
});

test("tell is a no-op when this pod is not the leader", async () => {
  const calls: string[] = [];

  const { bully: _, forum } = makeNode(
    { name: "a", host: "10.0.0.1" },
    [{ name: "b", host: "10.0.0.2" }],
    fakeFetch(async (url) => {
      calls.push(url);
      return { ok: true };
    }),
  );

  await forum.tell("b", "assign-connections", [1, 2, 3]);

  assert.ok(!calls.some((url) => url.includes("/bully/message")));
});

test("tell drops and warns when the target peer is unknown", async () => {
  const calls: string[] = [];

  const { bully, forum } = makeNode(
    { name: "b", host: "10.0.0.2" },
    [{ name: "a", host: "10.0.0.1" }],
    fakeFetch(async (url) => {
      calls.push(url);
      return { ok: true };
    }),
  );

  await bully.startElection();
  calls.length = 0;

  await forum.tell("does-not-exist", "assign-connections", [1, 2, 3]);

  assert.ok(!calls.some((url) => url.includes("/bully/message")));
});

test("messagePeer reaches a specific peer directly without requiring leadership", async () => {
  const calls: { url: string; body: any }[] = [];

  const { bully, forum } = makeNode(
    { name: "a", host: "10.0.0.1" },
    [
      { name: "b", host: "10.0.0.2" },
      { name: "c", host: "10.0.0.3" },
    ],
    fakeFetchWithBody(async (url, body) => {
      calls.push({ url, body });
      return { ok: true };
    }),
  );

  await bully.startElection();
  calls.length = 0;
  assert.strictEqual(bully.getStatus().isLeader, false);

  await forum.messagePeer("b", "subscribe-connection", { id: 7 });
  bully.stop();

  assert.strictEqual(calls.length, 1);
  assert.ok(calls[0].url.includes("10.0.0.2"));
  assert.deepStrictEqual(calls[0].body, { event: "subscribe-connection", payload: { id: 7 } });
  assert.ok(!calls.some((c) => c.url.includes("10.0.0.3")), "only the named peer should be contacted");
});

test("messagePeer fires locally when the target is self, without requiring leadership", async () => {
  const { bully, forum } = makeNode(
    { name: "a", host: "10.0.0.1" },
    [{ name: "b", host: "10.0.0.2" }],
    fakeFetch(async () => ({ ok: true })),
  );

  await bully.startElection();
  assert.strictEqual(bully.getStatus().isLeader, false);

  let received: unknown;
  bully.channel.on("connection-event", (payload) => {
    received = payload;
  });

  await forum.messagePeer("a", "connection-event", { id: 7, value: 42 });
  bully.stop();

  assert.deepStrictEqual(received, { id: 7, value: 42 });
});

test("request resolves with the value the target's respond() sends back, self included", async () => {
  const { bully, forum } = makeNode(
    { name: "a", host: "10.0.0.1" },
    [{ name: "b", host: "10.0.0.2" }],
    fakeFetch(async () => ({ ok: true })),
  );

  await bully.startElection();

  bully.channel.on("get-status", (envelope) => {
    forum.respond(envelope, { ok: true, status: "running" });
  });

  const result = await forum.request<null, { ok: boolean; status: string }>("a", "get-status", null);
  bully.stop();

  assert.deepStrictEqual(result, { ok: true, status: "running" });
});

test("request delivers to a specific peer over the network and resolves with its reply", async () => {
  const net = await makeNetwork(["a", "b"]);
  const forumA = net.forum("a");
  const forumB = net.forum("b");

  net.bully("b").channel.on("get-status", (envelope) => {
    forumB.respond(envelope, { status: "running-on-b" });
  });

  const result = await forumA.request<null, { status: string }>("b", "get-status", null);
  net.stopAll();

  assert.deepStrictEqual(result, { status: "running-on-b" });
});

test("request rejects when the target never responds within the timeout", async () => {
  const { bully, forum } = makeNode(
    { name: "a", host: "10.0.0.1" },
    [{ name: "b", host: "10.0.0.2" }],
    fakeFetch(async () => ({ ok: true })),
  );

  await bully.startElection();
  // No handler registered for "get-status" — nothing ever calls respond().

  await assert.rejects(() => forum.request("a", "get-status", null, 30), /timed out/);
  bully.stop();
});

const hasResponseListener = (bully: Bully) =>
  bully.channel.eventNames().some((n) => String(n).startsWith("__response:"));

test("request cleans up its response listener after timing out", async () => {
  const { bully, forum } = makeNode(
    { name: "a", host: "10.0.0.1" },
    [{ name: "b", host: "10.0.0.2" }],
    fakeFetch(async () => ({ ok: true })),
  );

  await bully.startElection();
  // No handler registered for "get-status" — nothing ever calls respond(), so this times out.
  await forum.request("b", "get-status", null, 20).catch(() => {});
  bully.stop();

  assert.strictEqual(hasResponseListener(bully), false);
});

test("request cleans up its response listener after resolving", async () => {
  const { bully, forum } = makeNode(
    { name: "a", host: "10.0.0.1" },
    [{ name: "b", host: "10.0.0.2" }],
    fakeFetch(async () => ({ ok: true })),
  );

  await bully.startElection();
  bully.channel.on("get-status", (envelope) => forum.respond(envelope, "ok"));

  await forum.request("a", "get-status", null);
  bully.stop();

  assert.strictEqual(hasResponseListener(bully), false);
});

test("concurrent requests to the same peer and event never cross-resolve", async () => {
  const { bully, forum } = makeNode(
    { name: "a", host: "10.0.0.1" },
    [{ name: "b", host: "10.0.0.2" }],
    fakeFetch(async () => ({ ok: true })),
  );

  await bully.startElection();

  bully.channel.on("echo", (envelope) => {
    // Reply out of order to prove the correlation id, not arrival order, decides resolution.
    setTimeout(() => forum.respond(envelope, envelope.payload), envelope.payload === "first" ? 20 : 5);
  });

  const [first, second] = await Promise.all([
    forum.request("a", "echo", "first"),
    forum.request("a", "echo", "second"),
  ]);
  bully.stop();

  assert.strictEqual(first, "first");
  assert.strictEqual(second, "second");
});

test("distribute splits work across the whole cluster in a stable order, self included", async () => {
  const calls: { url: string; body: any }[] = [];

  const { bully, forum } = makeNode(
    { name: "c", host: "10.0.0.3" },
    [
      { name: "a", host: "10.0.0.1" },
      { name: "b", host: "10.0.0.2" },
    ],
    fakeFetchWithBody(async (url, body) => {
      calls.push({ url, body });
      return { ok: true };
    }),
  );

  await bully.startElection();
  assert.strictEqual(bully.getStatus().isLeader, true);
  calls.length = 0;

  const connectionIds = Array.from({ length: 30 }, (_, i) => i);

  let ownShare: number[] | undefined;
  bully.channel.on("assign-connections", (share: number[]) => {
    ownShare = share;
  });

  await forum.distribute("assign-connections", (_peer, index, allPeers) => {
    const chunkSize = connectionIds.length / allPeers.length;
    return connectionIds.slice(index * chunkSize, (index + 1) * chunkSize);
  });

  assert.deepStrictEqual(ownShare, [20, 21, 22, 23, 24, 25, 26, 27, 28, 29]);
  const toA = calls.find((c) => c.url.includes("10.0.0.1"));
  const toB = calls.find((c) => c.url.includes("10.0.0.2"));
  assert.deepStrictEqual(toA?.body, { event: "assign-connections", payload: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] });
  assert.deepStrictEqual(toB?.body, { event: "assign-connections", payload: [10, 11, 12, 13, 14, 15, 16, 17, 18, 19] });
});

test("distribute is a no-op when this pod is not the leader", async () => {
  const calls: string[] = [];

  const { bully, forum } = makeNode(
    { name: "a", host: "10.0.0.1" },
    [{ name: "b", host: "10.0.0.2" }],
    fakeFetch(async (url) => {
      calls.push(url);
      return { ok: true };
    }),
  );

  let called = false;
  bully.channel.on("assign-connections", () => {
    called = true;
  });

  await forum.distribute("assign-connections", () => [1, 2, 3]);

  assert.strictEqual(called, false);
  assert.ok(!calls.some((url) => url.includes("/bully/message")));
});

test("distributeAndCollect gathers responses that arrive within the timeout window", async () => {
  const { bully, forum } = makeNode(
    { name: "c", host: "10.0.0.3" },
    [
      { name: "a", host: "10.0.0.1" },
      { name: "b", host: "10.0.0.2" },
    ],
    fakeFetch(async () => ({ ok: true })),
  );

  await bully.startElection();
  assert.strictEqual(bully.getStatus().isLeader, true);

  const collecting = forum.distributeAndCollect<null, string>("start-job", () => null, "job-result", 100);

  // Simulate peer "a" and self ("c") responding while the collection window is open — peer "b"
  // never responds, and should simply be absent from the result rather than causing a failure.
  bully.channel.emit("job-result", { peer: "a", response: "done-a" });
  bully.channel.emit("job-result", { peer: "c", response: "done-c" });

  const results = await collecting;

  assert.strictEqual(results.size, 2);
  assert.strictEqual(results.get("a"), "done-a");
  assert.strictEqual(results.get("c"), "done-c");
  assert.strictEqual(results.get("b"), undefined);
});

test("distributeAndCollect returns an empty map when this pod is not the leader", async () => {
  const { forum } = makeNode(
    { name: "a", host: "10.0.0.1" },
    [{ name: "b", host: "10.0.0.2" }],
    fakeFetch(async () => ({ ok: true })),
  );

  const results = await forum.distributeAndCollect("start-job", () => null, "job-result", 50);

  assert.strictEqual(results.size, 0);
});

test("distributeAndCollect stops listening once the timeout window closes", async () => {
  const { bully, forum } = makeNode(
    { name: "a", host: "10.0.0.1" },
    [],
    fakeFetch(async () => ({ ok: true })),
  );
  await bully.startElection();

  await forum.distributeAndCollect("start-job", () => null, "job-result", 30);

  assert.strictEqual(bully.channel.listenerCount("job-result"), 0);
});
