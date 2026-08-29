import { test } from "node:test";
import assert from "node:assert";
import { Transport } from "../core/transport";
import { Bully, type BullyPeer } from "../core/bully";
import { Forum } from "../core/forum";
import { ShardManager } from "../core/sharding";

let portCounter = 26000;
const nextPort = () => portCounter++;

const flush = (ms = 50) => new Promise((resolve) => setTimeout(resolve, ms));

type TestNode = {
  bully: Bully;
  transport: Transport;
  forum: Forum;
  shard: ShardManager<number>;
  peer: BullyPeer;
  stop: () => void;
};

/**
 * A real Transport + Bully + Forum + ShardManager stack on its own loopback address.
 * `resolvableCluster` (defaults to just this node) is what this node's own election sees —
 * kept to itself so start() self-promotes instantly. Other nodes that need to resolve this
 * one by name get its entry through their own resolvableCluster.
 */
const makeNode = (name: string, host: string, port: number, resolvableCluster?: BullyPeer[]): TestNode => {
  const self: BullyPeer = { name, host };
  const cluster = resolvableCluster ?? [self];

  const getCluster = async () => ({ self, cluster });
  const transport = new Transport({ internalPort: port, internalHost: host });
  const bully = new Bully({ getCluster, transport });
  const forum = new Forum({ bully, transport, getCluster });
  const shard = new ShardManager<number>({ bully, forum, ids: [] });
  shard.start();

  return {
    bully,
    transport,
    forum,
    shard,
    peer: self,
    stop: () => { shard.stop(); bully.stop(); transport.stop(); },
  };
};

const startNode = (node: TestNode) => {
  node.transport.start({
    onElectionMessage: (id) => node.bully.onElectionMessage(id),
    onCoordinatorMessage: (id) => node.bully.onCoordinatorMessage(id),
    onMessage: (event, payload) => node.bully.onMessage(event, payload),
    channel: node.bully.channel,
  });
  node.bully.start();
};

test("subscribe receives events published by the actual remote owner", async (t) => {
  const port = nextPort();

  const owner = makeNode("owner", "127.0.0.21", port);
  const subscriber = makeNode("subscriber", "127.0.0.22", port, [owner.peer, { name: "subscriber", host: "127.0.0.22" }]);
  startNode(owner);
  t.after(() => { owner.stop(); subscriber.stop(); });
  await flush();

  owner.bully.channel.emit("assign-connections", [7]);
  subscriber.bully.channel.emit("ownership-map", [[7, "owner"]]);

  const received: unknown[] = [];
  const close = subscriber.shard.subscribe(7, (payload) => received.push(payload));
  await flush();

  owner.shard.publish(7, { reading: 42 });
  await flush();
  close();

  assert.deepStrictEqual(received, [{ reading: 42 }]);
});

test("subscribe reconnects to the new owner automatically when ownership changes", async (t) => {
  const port = nextPort();

  const owner1 = makeNode("owner1", "127.0.0.23", port);
  const owner2 = makeNode("owner2", "127.0.0.24", port);
  const subscriber = makeNode("subscriber", "127.0.0.25", port, [
    owner1.peer,
    owner2.peer,
    { name: "subscriber", host: "127.0.0.25" },
  ]);
  startNode(owner1);
  startNode(owner2);
  t.after(() => { owner1.stop(); owner2.stop(); subscriber.stop(); });
  await flush();

  owner1.bully.channel.emit("assign-connections", [7]);
  subscriber.bully.channel.emit("ownership-map", [[7, "owner1"]]);

  const received: unknown[] = [];
  const close = subscriber.shard.subscribe(7, (payload) => received.push(payload));
  await flush();

  owner1.shard.publish(7, { from: "owner1" });
  await flush();

  owner1.bully.channel.emit("release-connections", [7]);
  owner2.bully.channel.emit("assign-connections", [7]);
  subscriber.bully.channel.emit("ownership-map", [[7, "owner2"]]);
  await flush();

  owner1.shard.publish(7, { from: "owner1-again" }); // should be dropped: owner1 no longer holds it
  owner2.shard.publish(7, { from: "owner2" });
  await flush();
  close();

  assert.deepStrictEqual(received, [{ from: "owner1" }, { from: "owner2" }]);
});

test("unsubscribe stops delivery and drops the underlying connection", async (t) => {
  const port = nextPort();

  const owner = makeNode("owner", "127.0.0.26", port);
  const subscriber = makeNode("subscriber", "127.0.0.27", port, [owner.peer, { name: "subscriber", host: "127.0.0.27" }]);
  startNode(owner);
  t.after(() => { owner.stop(); subscriber.stop(); });
  await flush();

  owner.bully.channel.emit("assign-connections", [7]);
  subscriber.bully.channel.emit("ownership-map", [[7, "owner"]]);

  const received: unknown[] = [];
  const close = subscriber.shard.subscribe(7, (payload) => received.push(payload));
  await flush();

  owner.shard.publish(7, { first: true });
  await flush();
  close();
  await flush();

  owner.shard.publish(7, { second: true });
  await flush();

  assert.deepStrictEqual(received, [{ first: true }]);
});
