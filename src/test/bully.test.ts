import { test } from "node:test";
import assert from "node:assert";
import { Transport } from "../core/transport.js";
import { Bully, type BullyPeer } from "../core/bully.js";

const makeBully = (self: BullyPeer, peers: BullyPeer[], mockFetch: typeof fetch) => {
  const transport = new Transport({ fetchFn: mockFetch });
  return new Bully({
    discovery: async () => ({ self, cluster: [self, ...peers] }),
    transport,
  });
};

const fakeFetch = (impl: (url: string) => Promise<{ ok: boolean }>): typeof fetch =>
  (async (url: any) => impl(String(url))) as unknown as typeof fetch;

test("becomes leader when no peer has a higher id, and emits elected", async () => {
  const calls: string[] = [];
  let emittedMaster = false;

  const bully = makeBully(
    { name: "b", host: "10.0.0.2" },
    [{ name: "a", host: "10.0.0.1" }],
    fakeFetch(async (url) => {
      calls.push(url);
      return { ok: true };
    }),
  );
  bully.lifecycle.on("elected", () => {
    emittedMaster = true;
  });

  await bully.startElection();

  const status = bully.getStatus();
  assert.strictEqual(status.isLeader, true);
  assert.strictEqual(status.leader, "b");
  assert.strictEqual(emittedMaster, true);
  assert.ok(calls.some((url) => url.includes("10.0.0.1") && url.includes("/bully/coordinator")));
});

test("backs off and waits when a higher peer acknowledges the election", async () => {
  const calls: string[] = [];

  const bully = makeBully(
    { name: "a", host: "10.0.0.1" },
    [{ name: "b", host: "10.0.0.2" }],
    fakeFetch(async (url) => {
      calls.push(url);
      return { ok: true };
    }),
  );

  await bully.startElection();
  bully.stop();

  const status = bully.getStatus();
  assert.strictEqual(status.isLeader, false);
  assert.strictEqual(status.leader, undefined);
  assert.ok(calls.some((url) => url.includes("/bully/election")));
  assert.ok(!calls.some((url) => url.includes("/bully/coordinator")));
});

test("becomes leader when the only higher peer is unreachable", async () => {
  const bully = makeBully(
    { name: "a", host: "10.0.0.1" },
    [{ name: "b", host: "10.0.0.2" }],
    fakeFetch(async (url) => {
      if (url.includes("/bully/election")) throw new Error("ECONNREFUSED");
      return { ok: true };
    }),
  );

  await bully.startElection();

  assert.strictEqual(bully.getStatus().isLeader, true);
});

test("retries the election after the coordinator wait times out", async () => {
  let electionCalls = 0;

  const bully = new Bully({
    discovery: async () => ({
      self: { name: "a", host: "10.0.0.1" },
      cluster: [
        { name: "a", host: "10.0.0.1" },
        { name: "b", host: "10.0.0.2" },
      ],
    }),
    transport: new Transport({
      fetchFn: fakeFetch(async (url) => {
        if (url.includes("/bully/election")) electionCalls++;
        return { ok: true };
      }),
    }),
    coordinatorWaitMs: 20,
  });

  await bully.startElection();
  assert.strictEqual(electionCalls, 1);

  await new Promise((resolve) => setTimeout(resolve, 50));
  bully.stop();

  assert.ok(electionCalls >= 2, `expected at least one retry, got ${electionCalls} election call(s)`);
  assert.strictEqual(bully.getStatus().isLeader, false);
});

test("onElectionMessage triggers this pod's own election", async () => {
  const bully = makeBully(
    { name: "b", host: "10.0.0.2" },
    [{ name: "a", host: "10.0.0.1" }],
    fakeFetch(async () => ({ ok: true })),
  );

  bully.onElectionMessage("a");
  await new Promise((resolve) => setTimeout(resolve, 10));

  assert.strictEqual(bully.getStatus().isLeader, true);
});

test("onCoordinatorMessage records the announced leader", async () => {
  const bully = makeBully(
    { name: "a", host: "10.0.0.1" },
    [{ name: "b", host: "10.0.0.2" }],
    fakeFetch(async () => ({ ok: true })),
  );

  bully.onCoordinatorMessage("b");

  const status = bully.getStatus();
  assert.strictEqual(status.leader, "b");
  assert.strictEqual(status.isLeader, false);
});

test("demotes a leader and emits demoted when a new leader is announced", async () => {
  const bully = makeBully(
    { name: "b", host: "10.0.0.2" },
    [{ name: "a", host: "10.0.0.1" }],
    fakeFetch(async () => ({ ok: true })),
  );

  await bully.startElection();
  assert.strictEqual(bully.getStatus().isLeader, true);

  let demoted = false;
  bully.lifecycle.on("demoted", () => {
    demoted = true;
  });

  bully.onCoordinatorMessage("z");

  assert.strictEqual(demoted, true);
  assert.strictEqual(bully.getStatus().isLeader, false);
  assert.strictEqual(bully.getStatus().leader, "z");
});

test("onCoordinatorMessage ignores a lower-named announcement while self is leader (split-brain guard)", async () => {
  // Simulates the production scenario: two pods start near-simultaneously and each transiently
  // can't reach the other during its own election, so both self-elect. "z" is the legitimate
  // leader (highest name); "a" wrongly also believes it's leader and broadcasts a coordinator
  // message naming itself. "z" must not accept it — that lower announcement is stale/erroneous by
  // Bully's own invariant, and accepting it would let both pods settle into permanent mutual
  // disagreement about who's leader.
  const bully = makeBully(
    { name: "z", host: "10.0.0.2" },
    [{ name: "a", host: "10.0.0.1" }],
    fakeFetch(async () => ({ ok: true })),
  );

  await bully.startElection();
  assert.strictEqual(bully.getStatus().isLeader, true);

  let demoted = false;
  bully.lifecycle.on("demoted", () => {
    demoted = true;
  });

  bully.onCoordinatorMessage("a");

  assert.strictEqual(demoted, false);
  assert.strictEqual(bully.getStatus().isLeader, true);
  assert.strictEqual(bully.getStatus().leader, "z");
});

test("getPodRoles tags every pod as leader or follower", async () => {
  const bully = makeBully(
    { name: "b", host: "10.0.0.2" },
    [{ name: "a", host: "10.0.0.1" }],
    fakeFetch(async () => ({ ok: true })),
  );

  await bully.startElection();

  const roles = await bully.getPodRoles();
  assert.deepStrictEqual(roles.map((pod) => [pod.name, pod.role]).sort(), [
    ["a", "follower"],
    ["b", "leader"],
  ]);
});

test("onMessage emits the received event with its payload", () => {
  const bully = makeBully(
    { name: "a", host: "10.0.0.1" },
    [],
    fakeFetch(async () => ({ ok: true })),
  );

  let received: unknown;
  bully.channel.on("job-done", (payload) => {
    received = payload;
  });

  bully.onMessage("job-done", { id: 42 });

  assert.deepStrictEqual(received, { id: 42 });
});
