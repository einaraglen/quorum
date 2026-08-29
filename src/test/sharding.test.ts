import { test } from "node:test";
import assert from "node:assert";
import { Transport } from "../core/transport";
import { Bully, type BullyPeer } from "../core/bully";
import { Forum } from "../core/forum";
import { Sharding } from "../core/sharding";

type Node = { name: string; bully: Bully; shard: Sharding<number> };

const REPORT_TIMEOUT_MS = 30;
const REBALANCE_INTERVAL_MS = 60;

/**
 * An in-memory "network": every node's fetchFn routes directly to the matching peer's
 * Bully instance instead of hitting a real HTTP server. `cluster` is shared and mutable so a
 * test can simulate a pod being removed (e.g. a dead leader) by splicing it out, or a pod
 * rejoining after a restart.
 */
type ClusterOptions = {
  expectedClusterSize?: number;
  quorumFailureThreshold?: number;
  onSustainedQuorumLoss?: () => void;
  rebalanceIntervalMs?: number;
};

const makeCluster = (allNames: string[], ids: number[], opts: ClusterOptions = {}) => {
  const nodes = new Map<string, Node>();
  const hostFor = (name: string) => `10.0.0.${allNames.indexOf(name) + 1}`;
  let cluster: BullyPeer[] = [];

  const fetchFor = (): typeof fetch =>
    (async (url: any, init: any) => {
      const parsed = new URL(String(url));
      const targetName = allNames.find((name) => hostFor(name) === parsed.hostname);
      const target = targetName ? nodes.get(targetName) : undefined;
      if (!target) return { ok: false } as Response;

      if (parsed.pathname === "/health") return { ok: true } as Response;

      const body = init?.body ? JSON.parse(init.body) : {};
      if (parsed.pathname === "/bully/election") target.bully.onElectionMessage(body.id);
      else if (parsed.pathname === "/bully/coordinator") target.bully.onCoordinatorMessage(body.id);
      else if (parsed.pathname === "/bully/message") target.bully.onMessage(body.event, body.payload);

      return { ok: true } as Response;
    }) as unknown as typeof fetch;

  const addToCluster = (name: string) => {
    cluster = [...cluster, { name, host: hostFor(name) }];

    const discovery = async () => ({ self: { name, host: hostFor(name) }, cluster });
    const transport = new Transport({ fetchFn: fetchFor() });
    const bully = new Bully({ discovery, transport });
    const forum = new Forum({ bully, transport, discovery });
    const shard = new Sharding({
      bully,
      forum,
      ids,
      reportTimeoutMs: REPORT_TIMEOUT_MS,
      rebalanceIntervalMs: opts.rebalanceIntervalMs ?? REBALANCE_INTERVAL_MS,
      expectedClusterSize: opts.expectedClusterSize,
      quorumFailureThreshold: opts.quorumFailureThreshold,
      onSustainedQuorumLoss: opts.onSustainedQuorumLoss,
    });
    shard.start();
    nodes.set(name, { name, bully, shard });
    return nodes.get(name)!;
  };

  for (const name of allNames) addToCluster(name);

  return {
    nodes,
    startAll: () => Promise.all([...nodes.values()].map((node) => node.bully.startElection())),
    removeFromCluster: (name: string) => {
      cluster = cluster.filter((pod) => pod.name !== name);
    },
    rejoin: (name: string) => {
      nodes.get(name)?.shard.stop();
      nodes.get(name)?.bully.stop();
      return addToCluster(name);
    },
    stopAll: () => {
      for (const node of nodes.values()) {
        node.shard.stop();
        node.bully.stop();
      }
    },
  };
};

const flush = async (ms = REPORT_TIMEOUT_MS * 3) => new Promise((resolve) => setTimeout(resolve, ms));

const fairRange = (totalIds: number, peerCount: number) => {
  const fairShare = Math.floor(totalIds / peerCount);
  return { min: fairShare, max: totalIds % peerCount === 0 ? fairShare : fairShare + 1 };
};

test("initial election distributes every id across the cluster with no gaps or duplicates", async (t) => {
  const cluster = makeCluster(["a", "b", "c"], [0, 1, 2, 3, 4, 5, 6, 7]);
  t.after(() => cluster.stopAll());

  await cluster.startAll();
  await flush();

  const held = [...cluster.nodes.values()].flatMap((node) => node.shard.getHeldIds());
  assert.deepStrictEqual(
    [...held].sort((x, y) => x - y),
    [0, 1, 2, 3, 4, 5, 6, 7],
  );
});

test("lifecycle emits 'assigned' and 'released' as held ids change", async (t) => {
  const cluster = makeCluster(["a", "b"], [0, 1, 2, 3]);
  t.after(() => cluster.stopAll());

  const [a, b] = ["a", "b"].map((name) => cluster.nodes.get(name)!);

  const assignedOnA: number[][] = [];
  const assignedOnB: number[][] = [];
  a.shard.lifecycle.on("assigned", (ids: number[]) => assignedOnA.push(ids));
  b.shard.lifecycle.on("assigned", (ids: number[]) => assignedOnB.push(ids));

  await cluster.startAll();
  await flush();

  const allAssigned = [...assignedOnA.flat(), ...assignedOnB.flat()].sort((x, y) => x - y);
  assert.deepStrictEqual(allAssigned, [0, 1, 2, 3], "assigned events should account for every id handed out");

  const leader = a.bully.getStatus().isLeader ? a : b;
  const releasedOnA: number[][] = [];
  const releasedOnB: number[][] = [];
  a.shard.lifecycle.on("released", (ids: number[]) => releasedOnA.push(ids));
  b.shard.lifecycle.on("released", (ids: number[]) => releasedOnB.push(ids));

  await leader.shard.updateIds([]);
  await flush();

  assert.deepStrictEqual(a.shard.getHeldIds(), []);
  assert.deepStrictEqual(b.shard.getHeldIds(), []);

  const allReleased = [...releasedOnA.flat(), ...releasedOnB.flat()].sort((x, y) => x - y);
  assert.deepStrictEqual(allReleased, [0, 1, 2, 3], "released events should account for every id taken back");
});

test("reconciliation after a leader dies reassigns its share and rebalances the survivors", async (t) => {
  const cluster = makeCluster(["a", "b", "c"], [0, 1, 2, 3, 4, 5, 6, 7]);
  t.after(() => cluster.stopAll());

  const [a, b, c] = ["a", "b", "c"].map((name) => cluster.nodes.get(name)!);

  await cluster.startAll();
  await flush();

  assert.ok(c.shard.getHeldIds().length > 0, "sanity check: leader should hold a share too");

  cluster.removeFromCluster("c");
  c.bully.stop();

  await Promise.all([a.bully.startElection(), b.bully.startElection()]);
  await flush();

  const aIdsAfter = a.shard.getHeldIds();
  const bIdsAfter = b.shard.getHeldIds();

  assert.strictEqual(aIdsAfter.length, 4, "a should end up with a fair share");
  assert.strictEqual(bIdsAfter.length, 4, "b should end up with a fair share");

  const allHeld = [...aIdsAfter, ...bIdsAfter].sort((x, y) => x - y);
  assert.deepStrictEqual(allHeld, [0, 1, 2, 3, 4, 5, 6, 7]);
});

test("a returning pod gets rebalanced back into the rotation instead of sitting idle", async (t) => {
  const cluster = makeCluster(["a", "b", "c"], [0, 1, 2, 3, 4, 5, 6, 7]);
  t.after(() => cluster.stopAll());

  let [a, b, c] = ["a", "b", "c"].map((name) => cluster.nodes.get(name)!);

  await cluster.startAll();
  await flush();

  cluster.removeFromCluster("c");
  c.bully.stop();
  await Promise.all([a.bully.startElection(), b.bully.startElection()]);
  await flush();

  c = cluster.rejoin("c");
  assert.deepStrictEqual(c.shard.getHeldIds(), [], "sanity check: rejoined pod starts empty");

  await Promise.all([a.bully.startElection(), b.bully.startElection(), c.bully.startElection()]);
  await flush();

  const { min, max } = fairRange(8, 3);
  for (const [name, node] of [
    ["a", a],
    ["b", b],
    ["c", c],
  ] as const) {
    const count = node.shard.getHeldIds().length;
    assert.ok(count >= min && count <= max, `${name} should hold a fair share (${min}-${max}), got ${count}`);
  }

  const allHeld = [...a.shard.getHeldIds(), ...b.shard.getHeldIds(), ...c.shard.getHeldIds()].sort((x, y) => x - y);
  assert.deepStrictEqual(allHeld, [0, 1, 2, 3, 4, 5, 6, 7]);
});

test("a dead follower's share gets picked up by the leader's periodic reconcile, no election needed", async (t) => {
  const cluster = makeCluster(["a", "b", "c"], [0, 1, 2, 3, 4, 5, 6, 7]);
  t.after(() => cluster.stopAll());

  const [a, b, c] = ["a", "b", "c"].map((name) => cluster.nodes.get(name)!);

  await cluster.startAll();
  await flush();
  assert.strictEqual(c.bully.getStatus().isLeader, true, "sanity check: c should be leader");

  cluster.removeFromCluster("a");
  a.bully.stop();
  a.shard.stop();

  await new Promise((resolve) => setTimeout(resolve, REBALANCE_INTERVAL_MS + REPORT_TIMEOUT_MS * 3));

  assert.strictEqual(c.bully.getStatus().isLeader, true, "c should still be leader â€” no election happened");

  const bIdsAfter = b.shard.getHeldIds();
  const cIdsAfter = c.shard.getHeldIds();

  assert.strictEqual(bIdsAfter.length, 4, "b should end up with a's old share factored in");
  assert.strictEqual(cIdsAfter.length, 4, "c should end up with a's old share factored in");

  const allHeld = [...bIdsAfter, ...cIdsAfter].sort((x, y) => x - y);
  assert.deepStrictEqual(allHeld, [0, 1, 2, 3, 4, 5, 6, 7]);
});

test("growing the work bundle assigns the new ids without touching what's already fair", async (t) => {
  const cluster = makeCluster(["a", "b", "c"], [0, 1, 2, 3, 4, 5, 6, 7]);
  t.after(() => cluster.stopAll());

  const [a, b, c] = ["a", "b", "c"].map((name) => cluster.nodes.get(name)!);

  await cluster.startAll();
  await flush();

  await c.shard.updateIds([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
  await flush();

  const allHeld = [...a.shard.getHeldIds(), ...b.shard.getHeldIds(), ...c.shard.getHeldIds()].sort((x, y) => x - y);
  assert.deepStrictEqual(allHeld, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);

  const { min, max } = fairRange(12, 3);
  for (const [name, node] of [
    ["a", a],
    ["b", b],
    ["c", c],
  ] as const) {
    const count = node.shard.getHeldIds().length;
    assert.ok(count >= min && count <= max, `${name} should hold a fair share (${min}-${max}), got ${count}`);
  }
});

test("shrinking the work bundle releases the removed ids from whoever held them", async (t) => {
  const cluster = makeCluster(["a", "b", "c"], [0, 1, 2, 3, 4, 5, 6, 7]);
  t.after(() => cluster.stopAll());

  const [a, b, c] = ["a", "b", "c"].map((name) => cluster.nodes.get(name)!);

  await cluster.startAll();
  await flush();

  await c.shard.updateIds([0, 1, 2]);
  await flush();

  const allHeld = [...a.shard.getHeldIds(), ...b.shard.getHeldIds(), ...c.shard.getHeldIds()].sort((x, y) => x - y);
  assert.deepStrictEqual(allHeld, [0, 1, 2]);
});

test("updateIds is a no-op when called on a pod that isn't leader", async (t) => {
  const cluster = makeCluster(["a", "b", "c"], [0, 1, 2, 3, 4, 5, 6, 7]);
  t.after(() => cluster.stopAll());

  const [a, b, c] = ["a", "b", "c"].map((name) => cluster.nodes.get(name)!);

  await cluster.startAll();
  await flush();
  assert.strictEqual(a.bully.getStatus().isLeader, false, "sanity check: a is a follower");

  await a.shard.updateIds([0, 1]);
  await flush();

  const allHeld = [...a.shard.getHeldIds(), ...b.shard.getHeldIds(), ...c.shard.getHeldIds()].sort((x, y) => x - y);
  assert.deepStrictEqual(allHeld, [0, 1, 2, 3, 4, 5, 6, 7]);
});

test("every peer, not just the leader, can look up who owns any id", async (t) => {
  const cluster = makeCluster(["a", "b", "c"], [0, 1, 2, 3, 4, 5, 6, 7]);
  t.after(() => cluster.stopAll());

  const [a, b, c] = ["a", "b", "c"].map((name) => cluster.nodes.get(name)!);
  const allNodes = [a, b, c];

  await cluster.startAll();
  await flush();

  const actualOwner = new Map<number, string>();
  for (const node of allNodes) for (const id of node.shard.getHeldIds()) actualOwner.set(id, node.name);

  for (const id of [0, 1, 2, 3, 4, 5, 6, 7]) {
    for (const observer of allNodes) {
      assert.strictEqual(
        observer.shard.getOwner(id),
        actualOwner.get(id),
        `${observer.name}'s local view of who owns id ${id} should match reality`,
      );
    }
  }
});

test("ownership map updates after a leader dies and the survivors rebalance", async (t) => {
  const cluster = makeCluster(["a", "b", "c"], [0, 1, 2, 3, 4, 5, 6, 7]);
  t.after(() => cluster.stopAll());

  const [a, b, c] = ["a", "b", "c"].map((name) => cluster.nodes.get(name)!);

  await cluster.startAll();
  await flush();

  cluster.removeFromCluster("c");
  c.bully.stop();
  c.shard.stop();
  await Promise.all([a.bully.startElection(), b.bully.startElection()]);
  await flush();

  const actualOwner = new Map<number, string>();
  for (const node of [a, b]) for (const id of node.shard.getHeldIds()) actualOwner.set(id, node.name);

  for (const id of [0, 1, 2, 3, 4, 5, 6, 7]) {
    for (const observer of [a, b]) {
      assert.strictEqual(observer.shard.getOwner(id), actualOwner.get(id));
    }
    assert.notStrictEqual(a.shard.getOwner(id), "c");
    assert.notStrictEqual(b.shard.getOwner(id), "c");
  }
});

test("reconcile refuses to assign anything when reachable peers are below quorum", async (t) => {
  const cluster = makeCluster(["a", "b"], [0, 1, 2, 3, 4, 5, 6, 7], { expectedClusterSize: 4 });
  t.after(() => cluster.stopAll());

  const [a, b] = ["a", "b"].map((name) => cluster.nodes.get(name)!);

  await cluster.startAll();
  await flush();

  assert.deepStrictEqual(a.shard.getHeldIds(), []);
  assert.deepStrictEqual(b.shard.getHeldIds(), []);
  assert.strictEqual(a.shard.getOwner(0), undefined);
});

test("reconcile proceeds once reachable peers meet quorum", async (t) => {
  const cluster = makeCluster(["a", "b", "c"], [0, 1, 2, 3, 4, 5, 6, 7], { expectedClusterSize: 4 });
  t.after(() => cluster.stopAll());

  await cluster.startAll();
  await flush();

  const held = [...cluster.nodes.values()].flatMap((node) => node.shard.getHeldIds());
  assert.deepStrictEqual(
    [...held].sort((x, y) => x - y),
    [0, 1, 2, 3, 4, 5, 6, 7],
  );
});

test("onSustainedQuorumLoss fires once after quorumFailureThreshold consecutive failures", async (t) => {
  const discovery = async () => ({
    self: { name: "solo", host: "10.0.0.1" },
    cluster: [{ name: "solo", host: "10.0.0.1" }],
  });
  const transport = new Transport();
  const bully = new Bully({ discovery, transport });
  const forum = new Forum({ bully, transport, discovery });

  let triggerCount = 0;
  const shard = new Sharding<number>({
    bully,
    forum,
    ids: [0, 1, 2, 3],
    reportTimeoutMs: REPORT_TIMEOUT_MS,
    expectedClusterSize: 4,
    quorumFailureThreshold: 2,
    onSustainedQuorumLoss: () => triggerCount++,
  });
  t.after(() => {
    shard.stop();
    bully.stop();
  });

  await bully.startElection();

  await shard.reconcile();
  assert.strictEqual(triggerCount, 0, "should not fire before the threshold is reached");

  await shard.reconcile();
  assert.strictEqual(triggerCount, 1, "should fire exactly once once the threshold is reached");

  await shard.reconcile();
  assert.strictEqual(triggerCount, 1, "should not fire again for continued failures past the threshold");
});

test("consecutive quorum failure count resets once quorum is regained", async (t) => {
  let triggerCount = 0;
  const cluster = makeCluster(["a", "b"], [0, 1], {
    expectedClusterSize: 2,
    quorumFailureThreshold: 2,
    onSustainedQuorumLoss: () => triggerCount++,
    rebalanceIntervalMs: 600_000,
  });
  t.after(() => cluster.stopAll());

  const [a, b] = ["a", "b"].map((name) => cluster.nodes.get(name)!);
  await cluster.startAll();
  await flush();

  cluster.removeFromCluster("a");
  a.bully.stop();
  a.shard.stop();
  await b.shard.reconcile();
  assert.strictEqual(triggerCount, 0, "a single failure must not reach the threshold of 2");

  const rejoined = cluster.rejoin("a");
  await rejoined.bully.startElection();
  await b.shard.reconcile();

  cluster.removeFromCluster("a");
  rejoined.bully.stop();
  rejoined.shard.stop();
  await b.shard.reconcile();
  assert.strictEqual(triggerCount, 0, "the earlier failure must not have carried over past the reset");

  await b.shard.reconcile();
  assert.strictEqual(triggerCount, 1, "two fresh consecutive failures after the reset should trigger");
});
