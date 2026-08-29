# quorum

A Node.js library for coordinating distributed services: leader election, inter-pod messaging, and automatic work sharding — with no external dependencies beyond an HTTP server.

Designed for deployments where pods discover each other through an existing mechanism (Kubernetes, DNS, a config file) and need to elect a leader, exchange messages, and divide work among themselves without introducing Zookeeper, etcd, or any external broker.

---

## Concepts

### Bully — leader election

Each pod has a name (typically its Kubernetes pod name). The pod with the lexicographically highest name that is reachable by its peers wins the election. This is a simplified version of the classic [Bully algorithm](https://en.wikipedia.org/wiki/Bully_algorithm).

When a pod starts up it runs an election: it contacts every peer with a higher name. If any of them acknowledges, it steps back and waits for a coordinator announcement. If none acknowledge (either because they are all unreachable, or because this pod already has the highest name), it declares itself leader and tells every peer. Each follower then runs a periodic heartbeat, and if the leader stops responding it triggers a new election automatically.

### Transport — the wire

All coordination travels over a single internal HTTP port (default `4001`). Each pod runs a small Express server on that port with five endpoints:

| Route | Purpose |
|---|---|
| `GET /health` | Heartbeat probe |
| `POST /bully/election` | Incoming election challenge from a lower peer |
| `POST /bully/coordinator` | Incoming leader announcement |
| `POST /bully/message` | Incoming application-level event |
| `GET /channel/stream/:event` | SSE stream for live subscriptions |

The Transport class owns both sides of this: the server that listens on those routes, and the HTTP client (`postToPeer`, `pingPeer`) that sends to them.

### Forum — inter-pod messaging

Four messaging patterns are available, each with a different routing model:

| Method | Who can call | Who receives |
|---|---|---|
| `broadcast(event, payload)` | Leader only | All followers + self |
| `send(event, payload)` | Any follower | Current leader |
| `tell(peer, event, payload)` | Leader only | One specific named peer |
| `messagePeer(peer, event, payload)` | Anyone | One specific named peer |

All four deliver to the pod's local `channel` EventEmitter when the target happens to be self, with no network round-trip.

`distribute(event, resolvePayload)` is a leader-only helper for partitioning work: it calls your callback once per peer (sorted by name for stability) and delivers the return value to each pod as a `channel` event. Useful for sending each pod its own slice of a large dataset without the leader needing to know each pod's address explicitly.

`subscribeToPeer(peer, event, onData)` opens a live SSE connection to a named peer and delivers every event it emits under that name. Returns an unsubscribe function. Targets itself locally — no loopback HTTP connection.

### ShardManager — work distribution

The leader periodically reconciles who owns what. Reconciliation works in four steps:

1. **Collect** — broadcast a request for holdings reports; wait for every peer to reply with what it currently holds.
2. **Compute** — divide the full set of IDs into a fair share per reporting peer (± 1 for remainders), sorted by name for a stable assignment order.
3. **Publish** — broadcast the full `id → peer` ownership map so every pod can answer "who owns X?" locally without a network call.
4. **Converge** — send each peer exactly the `assign` and `release` messages needed to reach the target, touching only what has to change.

Ownership changes drive subscriptions: `subscribe(id, onEvent)` listens to the current owner over SSE and reconnects automatically if the ownership map changes — no coordination with the old or new owner required.

**Quorum protection** (optional): set `expectedClusterSize` to guard against split-brain. Reconciliation refuses to act until more than half of the expected pods have reported in. After `quorumFailureThreshold` consecutive failures (default 3) the `onSustainedQuorumLoss` callback fires — defaulting to `process.exit(1)` so Kubernetes restarts the pod rather than letting it serve stale data indefinitely.

### Quorum — the wrapper

`Quorum` creates and wires all three layers (Transport, Bully, Forum, ShardManager) from a single options object. It is the primary entry point for production use. The individual classes are exported for testing and advanced composition.

---

## Quick start

```typescript
import { Quorum } from "quorum";
import { getCluster } from "./my-discovery.js"; // returns { self, cluster }

const q = new Quorum({
  getCluster,
  ids: ["uuid-1", "uuid-2", "uuid-3"], // the full set of work items to distribute
  expectedClusterSize: 4,              // enables quorum protection
});

q.start(); // starts the HTTP server, runs the first election, begins reconciling

// Subscribe to events for a specific id, from wherever it currently lives:
const unsubscribe = q.subscribe("uuid-1", (payload) => {
  console.log("received:", payload);
});

// Publish an event for an id this pod currently owns:
q.publish("uuid-1", { reading: 42 });

// Look up who owns an id without a network call:
console.log(q.getOwner("uuid-1")); // "pod-name-c"

// Clean shutdown (or use `using q = new Quorum(...)` for automatic teardown):
q.stop();
```

### With explicit resource management

```typescript
async function main() {
  using q = new Quorum({ getCluster, ids });
  q.start();

  await new Promise<void>((resolve) => {
    process.on("SIGTERM", resolve);
    process.on("SIGINT", resolve);
  });
  // q.stop() is called automatically here
}
```

---

## API

### `new Quorum(opts)`

| Option | Type | Default | Description |
|---|---|---|---|
| `getCluster` | `() => Promise<{ self, cluster }>` | required | Peer discovery. Return this pod as `self` and the full peer list (including self) as `cluster`. |
| `ids` | `TId[]` | required | The complete set of work IDs to distribute. |
| `fetchFn` | `typeof fetch` | `globalThis.fetch` | Swap in a custom fetch for testing. |
| `internalPort` | `number` | `4001` or `$INTERNAL_PORT` | Port for the internal coordination server. |
| `internalHost` | `string` | all interfaces | Bind address for the internal server. Useful in tests running multiple real instances on one machine. |
| `requestTimeoutMs` | `number` | `2000` | Timeout for outgoing HTTP requests to peers. |
| `coordinatorWaitMs` | `number` | `4000` | How long a pod waits for a coordinator announcement before restarting the election. |
| `heartbeatIntervalMs` | `number` | `5000` | How often followers ping the leader to check it is still alive. |
| `reportTimeoutMs` | `number` | `2000` | How long the leader waits for holdings reports before reconciling with whoever responded. |
| `rebalanceIntervalMs` | `number` | `10000` | How often the leader runs a full reconcile. |
| `expectedClusterSize` | `number` | none | Target pod count for quorum checks. |
| `quorumFailureThreshold` | `number` | `3` | Consecutive quorum failures before `onSustainedQuorumLoss` fires. |
| `onSustainedQuorumLoss` | `() => void` | `process.exit(1)` | Called once when the failure threshold is reached. |
| `logger` | `Logger` | `console` | Log sink, must implement `info`/`warn`/`error`/`debug`. `console` satisfies this as-is. |

**Methods:**

```typescript
q.start()                          // start server, election, heartbeat, reconcile loop
q.stop()                           // graceful shutdown
q.isLeader(): boolean
q.getStatus(): { self, leader, isLeader }
q.getPodRoles(): Promise<{ name, host, role }[]>

q.subscribe(id, onEvent): () => void   // live subscription to events for an id
q.publish(id, payload): void           // emit an event for an id this pod owns
q.getOwner(id): string | undefined     // local lookup — no network call
q.getOwnership(): Map<TId, string>     // full id→peer map as of last reconcile
q.getHeldIds(): TId[]                  // ids currently assigned to this pod
q.updateIds(ids): Promise<void>        // leader-only: replace the full id set and reconcile
```

---

## Advanced use

For testing or custom wiring you can create the layers individually:

```typescript
import { Transport, Bully, Forum, ShardManager } from "quorum";

const transport = new Transport({ fetchFn: mockFetch, internalPort: 9000 });
const bully = new Bully({ getCluster, transport });
const forum = new Forum({ bully, transport, getCluster });
const shard = new ShardManager({ bully, forum, ids: [] });

// Wire the transport callbacks manually:
transport.start({
  onElectionMessage: (id) => bully.onElectionMessage(id),
  onCoordinatorMessage: (id) => bully.onCoordinatorMessage(id),
  onMessage: (event, payload) => bully.onMessage(event, payload),
  channel: bully.channel,
});
shard.start();
bully.start();
await bully.startElection();
```

### Channel events

`bully.channel` is a plain Node.js `EventEmitter`. Every message received from a peer arrives here as a named event. You can listen directly for any custom event the leader broadcasts, or emit events directly in tests to simulate incoming messages without a real network:

```typescript
// Simulate the leader telling this pod to take ownership:
bully.channel.emit("assign-connections", [7, 12, 44]);

// Simulate the leader publishing an updated ownership map:
bully.channel.emit("ownership-map", [[7, "pod-c"], [12, "pod-a"]]);
```

### Lifecycle events

`bully.lifecycle` emits `"elected"` when this pod becomes leader (after peers have been notified), and `"demoted"` when it steps down. ShardManager uses these internally; you can also listen directly:

```typescript
bully.lifecycle.on("elected", () => {
  console.log("I am now the leader — starting reconcile");
});
bully.lifecycle.on("demoted", () => {
  console.log("Lost leadership");
});
```

---

## Peer discovery

`getCluster` is the only application-specific piece. It must return:

```typescript
{
  self: { name: string; host?: string },   // this pod
  cluster: { name: string; host?: string }[] // all pods, self included
}
```

`name` is used for election ordering — higher wins. `host` is the IP or hostname peers use to reach this pod over the internal port. A Kubernetes implementation typically reads from the pod list API:

```typescript
// In your application layer (not in quorum itself):
const getCluster = async () => {
  const pods = await k8s.listNamespacedPod({
    namespace: "default",
    labelSelector: "app=my-service",
    fieldSelector: "status.phase=Running",
  });
  const toPeer = (pod) => ({
    name: pod.metadata.name,
    host: pod.status.podIP,
  });
  const self = pods.find((p) => p.metadata.name === process.env.POD_NAME);
  return { self: toPeer(self), cluster: pods.map(toPeer) };
};
```

---

## File structure

```
src/
  index.ts             Public exports
  core/
    transport.ts       HTTP server (routes) + HTTP client (postToPeer, pingPeer)
    bully.ts           Election state machine — the Bully algorithm
    forum.ts           Inter-pod messaging — broadcast, send, tell, subscribe
    sharding.ts        Work distribution — reconcile, ownership map, quorum
    quorum.ts          Unified wrapper; the primary public entry point
    logger.ts          The `Logger` type every class accepts via `opts.logger`
  test/                Test suites, one file per core/ module
```

---

## Logging

Every class (and `Quorum` itself) accepts an optional `logger` implementing `info`/`warn`/`error`/`debug` — `console` satisfies this shape as-is and is the default. Pass your own (winston, pino, a wrapper around your APM, etc.) to route quorum's election/reconcile/messaging logs wherever the rest of your app's logs go:

```typescript
const q = new Quorum({ getCluster, ids, logger: myWinstonLogger });
```
