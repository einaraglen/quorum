import { test } from "node:test";
import assert from "node:assert";
import { Transport } from "../core/transport";
import { Bully, type BullyPeer } from "../core/bully";
import { Forum } from "../core/forum";

let portCounter = 25000;
const nextPort = () => portCounter++;

const flush = (ms = 50) => new Promise((resolve) => setTimeout(resolve, ms));

/** Polls until `check()` is true or `timeoutMs` elapses, instead of a fixed sleep. */
const waitFor = async (check: () => boolean, timeoutMs = 2000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!check() && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
};

type TestNode = { bully: Bully; transport: Transport; forum: Forum; start: () => void; stop: () => void };

/** Two real Transport + Bully + Forum instances, each on its own loopback address. */
const makePair = (): { owner: TestNode; subscriber: TestNode } => {
  const port = nextPort();
  const cluster: BullyPeer[] = [
    { name: "owner", host: "127.0.0.11" },
    { name: "subscriber", host: "127.0.0.12" },
  ];

  const makeNode = (self: BullyPeer): TestNode => {
    const getCluster = async () => ({ self, cluster });
    const transport = new Transport({ internalPort: port, internalHost: self.host });
    const bully = new Bully({ getCluster, transport });
    const forum = new Forum({ bully, transport, getCluster });
    const start = () => {
      transport.start({
        onElectionMessage: (id) => bully.onElectionMessage(id),
        onCoordinatorMessage: (id) => bully.onCoordinatorMessage(id),
        onMessage: (event, payload) => bully.onMessage(event, payload),
        channel: bully.channel,
      });
      bully.start();
    };
    const stop = () => { bully.stop(); transport.stop(); };
    return { bully, transport, forum, start, stop };
  };

  return {
    owner: makeNode(cluster[0]),
    subscriber: makeNode(cluster[1]),
  };
};

test("subscribeToPeer receives events a remote peer emits on its channel", async (t) => {
  const { owner, subscriber } = makePair();
  owner.start();
  t.after(() => { owner.stop(); subscriber.stop(); });
  await flush();

  const received: unknown[] = [];
  const close = subscriber.forum.subscribeToPeer("owner", "connection-event", (payload) => {
    received.push(payload);
  });
  // Wait for the SSE GET to actually land server-side, rather than assuming a fixed delay is
  // enough — otherwise events emitted before the connection is up are lost, not queued.
  await waitFor(() => owner.bully.channel.listenerCount("connection-event") > 0);

  owner.bully.channel.emit("connection-event", { id: 7, value: "first" });
  owner.bully.channel.emit("connection-event", { id: 7, value: "second" });
  await waitFor(() => received.length >= 2);

  close();

  assert.deepStrictEqual(received, [
    { id: 7, value: "first" },
    { id: 7, value: "second" },
  ]);
});

test("closing the subscription stops further delivery", async (t) => {
  const { owner, subscriber } = makePair();
  owner.start();
  t.after(() => { owner.stop(); subscriber.stop(); });
  await flush();

  const received: unknown[] = [];
  const close = subscriber.forum.subscribeToPeer("owner", "connection-event", (payload) => {
    received.push(payload);
  });
  await waitFor(() => owner.bully.channel.listenerCount("connection-event") > 0);

  owner.bully.channel.emit("connection-event", { id: 1 });
  await waitFor(() => received.length >= 1);
  close();
  await flush();

  owner.bully.channel.emit("connection-event", { id: 2 });
  await flush();

  assert.deepStrictEqual(received, [{ id: 1 }]);
});

test("subscribeToPeer targeting self listens locally with no network call", async (t) => {
  const { owner } = makePair();
  t.after(() => owner.stop());

  // selfId is only populated once getPeers() has run at least once (normally via start()).
  await owner.bully.startElection();

  const received: unknown[] = [];
  const close = owner.forum.subscribeToPeer("owner", "connection-event", (payload) => {
    received.push(payload);
  });

  owner.bully.channel.emit("connection-event", { id: 9 });
  close();

  assert.deepStrictEqual(received, [{ id: 9 }]);
});
