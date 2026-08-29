import { EventEmitter } from "events";
import type { Bully } from "./bully";
import type { Forum } from "./forum";
import type { Logger } from "./logger";

export type ShardingOptions<TId> = {
  bully: Bully;
  forum: Forum;
  ids: TId[];
  reportTimeoutMs?: number;
  rebalanceIntervalMs?: number;
  /**
   * The cluster's intended total pod count (e.g. the Deployment's `replicas`), used as a
   * quorum guard: reconcile() refuses to act unless more than half of this count actually
   * reported in. Left unset, quorum checking is disabled entirely — safe default for tests
   * and small ad-hoc clusters that don't care about split-brain protection.
   */
  expectedClusterSize?: number;
  /**
   * How many *consecutive* quorum failures to tolerate before treating this pod as sustainably
   * isolated (as opposed to one slow reconcile pass on an otherwise-healthy network) and
   * invoking onSustainedQuorumLoss. Only relevant when expectedClusterSize is set.
   */
  quorumFailureThreshold?: number;
  /**
   * Called once quorumFailureThreshold consecutive quorum failures are hit. Defaults to
   * `process.exit(1)` — fail loudly and let Kubernetes restart the pod, rather than keep
   * serving increasingly stale data while believing it's still leader. Injectable so tests
   * (and anyone who wants different behavior) don't have to actually kill the process.
   */
  onSustainedQuorumLoss?: () => void;
  logger?: Logger;
};

type HoldingsReport<TId> = { peer: string; ids: TId[] };
type ConnectionEvent<TId> = { id: TId; payload: unknown };

export class Sharding<TId = string> implements Disposable {
  /** Emits `"assigned"` / `"released"` (each with the delta of ids, not the full held set) whenever this pod's held ids change. */
  public readonly lifecycle = new EventEmitter();

  private bully: Bully;
  private forum: Forum;
  private allIds: TId[];
  private reportTimeoutMs: number;
  private rebalanceIntervalMs: number;
  private expectedClusterSize?: number;
  private quorumFailureThreshold: number;
  private sustainedQuorumLossCallback: () => void;
  private logger: Logger;
  private consecutiveQuorumFailures = 0;
  /** Ensures sustainedQuorumLossCallback fires once per episode, not on every failure past threshold. */
  private hasTriggeredSustainedLoss = false;
  /** Guards against a periodic rebalance tick overlapping an explicit reconcile() already in flight. */
  private reconcileInFlight = false;
  private heldIds = new Set<TId>();
  private ownership = new Map<TId, string>();
  private rebalanceInterval?: NodeJS.Timeout;
  /** Local-only: fires `<id>` whenever getOwner(id) changes, to drive subscribe() reconnects. */
  private ownershipEvents = new EventEmitter();
  /** Safety net: any subscribe() the caller never explicitly closed gets torn down in stop(). */
  private activeSubscriptions = new Set<() => void>();

  constructor(opts: ShardingOptions<TId>) {
    this.bully = opts.bully;
    this.forum = opts.forum;
    this.allIds = opts.ids;
    this.reportTimeoutMs = opts.reportTimeoutMs ?? 2000;
    this.rebalanceIntervalMs = opts.rebalanceIntervalMs ?? 10000;
    this.expectedClusterSize = opts.expectedClusterSize;
    this.quorumFailureThreshold = opts.quorumFailureThreshold ?? 3;
    this.sustainedQuorumLossCallback = opts.onSustainedQuorumLoss ?? (() => process.exit(1));
    this.logger = opts.logger ?? console;
  }

  public getHeldIds() {
    return [...this.heldIds];
  }

  /** Local, instant lookup — no network round-trip — of which peer currently owns an id. */
  public getOwner(id: TId) {
    return this.ownership.get(id);
  }

  /** Local, cluster-wide view of every id's current owner — every peer as of the last reconcile. */
  public getOwnership() {
    return new Map(this.ownership);
  }

  /**
   * Live-subscribe to events published for `id`, wherever it currently lives. Returns an
   * unsubscribe function. Reconnects automatically if ownership of this specific id changes
   * while the subscription is active, driven by the same ownership map reconcile() already
   * keeps fresh — no message from the old owner required, no subscriber bookkeeping on the
   * owner's side at all. Under the hood this is just an SSE connection (via
   * bully.subscribeToPeer); closing it is the entire unsubscribe.
   */
  public subscribe(id: TId, onEvent: (payload: unknown) => void): () => void {
    let close: (() => void) | undefined;

    const connect = () => {
      const owner = this.getOwner(id);
      if (!owner) {
        this.logger.warn(`subscribe(${id}) could not resolve an owner`);
        return;
      }
      close = this.forum.subscribeToPeer(owner, "connection-event", (event) => {
        const { id: eventId, payload } = event as ConnectionEvent<TId>;
        if (eventId === id) onEvent(payload);
      });
    };

    const onOwnerChanged = () => {
      close?.();
      connect();
    };

    connect();
    this.ownershipEvents.on(String(id), onOwnerChanged);

    const unsubscribe = () => {
      this.ownershipEvents.off(String(id), onOwnerChanged);
      close?.();
      this.activeSubscriptions.delete(unsubscribe);
    };
    this.activeSubscriptions.add(unsubscribe);

    return unsubscribe;
  }

  /** Called by the app whenever it has a new event for one of the ids this pod owns. */
  public publish(id: TId, payload: unknown) {
    if (!this.heldIds.has(id)) {
      this.logger.warn(`publish(${id}) called but this pod doesn't own that id, dropping`);
      return;
    }
    this.bully.channel.emit("connection-event", { id, payload } satisfies ConnectionEvent<TId>);
  }

  private onAssign = (ids: TId[]) => {
    for (const id of ids) this.heldIds.add(id);
    this.logger.debug(`Now holding ${this.heldIds.size} id(s)`);
    this.lifecycle.emit("assigned", ids);
  };

  private onRelease = (ids: TId[]) => {
    for (const id of ids) this.heldIds.delete(id);
    this.logger.debug(`Released ${ids.length} id(s), now holding ${this.heldIds.size}`);
    this.lifecycle.emit("released", ids);
  };

  private onReportRequest = () => {
    const { self } = this.bully.getStatus();
    if (!self) return;
    this.forum.send("holdings-report", { peer: self, ids: this.getHeldIds() } satisfies HoldingsReport<TId>);
  };

  private onUpdateIds = (ids: TId[]) => {
    this.allIds = ids;
  };

  private onOwnershipMap = (entries: [TId, string][]) => {
    const next = new Map(entries);
    const previous = this.ownership;
    // Swapped in before emitting: subscribe()'s reconnect handler calls getOwner() synchronously
    // in reaction to this event, so it must already see the new value, not the one it's replacing.
    this.ownership = next;

    for (const [id, peer] of next) {
      if (previous.get(id) !== peer) this.ownershipEvents.emit(String(id), peer);
    }
  };

  private onElected = () => {
    this.reconcile();
    this.rebalanceInterval = setInterval(() => this.reconcile(), this.rebalanceIntervalMs);
  };

  private onDemoted = () => {
    if (this.rebalanceInterval) clearInterval(this.rebalanceInterval);
    this.rebalanceInterval = undefined;
    this.consecutiveQuorumFailures = 0;
    this.hasTriggeredSustainedLoss = false;
  };

  public start() {
    this.bully.channel.on("assign-connections", this.onAssign);
    this.bully.channel.on("release-connections", this.onRelease);
    this.bully.channel.on("report-holdings-request", this.onReportRequest);
    this.bully.channel.on("update-ids", this.onUpdateIds);
    this.bully.channel.on("ownership-map", this.onOwnershipMap);
    this.bully.lifecycle.on("elected", this.onElected);
    this.bully.lifecycle.on("demoted", this.onDemoted);
  }

  public stop() {
    this.bully.channel.off("assign-connections", this.onAssign);
    this.bully.channel.off("release-connections", this.onRelease);
    this.bully.channel.off("report-holdings-request", this.onReportRequest);
    this.bully.channel.off("update-ids", this.onUpdateIds);
    this.bully.channel.off("ownership-map", this.onOwnershipMap);
    this.bully.lifecycle.off("elected", this.onElected);
    this.bully.lifecycle.off("demoted", this.onDemoted);
    this.onDemoted();

    for (const unsubscribe of this.activeSubscriptions) unsubscribe();
  }

  public [Symbol.dispose]() {
    this.stop();
  }

  /**
   * Leader-only: change the full set of ids that should be distributed across the cluster —
   * e.g. after re-querying the database this bundle came from and finding it grew or shrank.
   * Broadcasts the new set to every peer (so whichever pod is leader next already has it) and
   * immediately reconciles, rather than waiting for the next periodic tick.
   */
  public async updateIds(ids: TId[]) {
    if (!this.bully.isLeader()) {
      this.logger.warn("updateIds() called while not leader, ignoring");
      return;
    }

    await this.forum.broadcast("update-ids", ids);
    await this.reconcile();
  }

  /**
   * Gathers what every reachable peer currently holds, computes the fair target split of all
   * ids across those peers, broadcasts the full id→peer map so any pod can look up an owner
   * locally via getOwner() without asking around, and sends each peer exactly the
   * assign/release messages needed to converge on that target.
   */
  public async reconcile() {
    if (this.reconcileInFlight) return;
    this.reconcileInFlight = true;

    try {
      await this.doReconcile();
    } finally {
      this.reconcileInFlight = false;
    }
  }

  private async doReconcile() {
    const reports = new Map<string, TId[]>();
    const { self } = this.bully.getStatus();
    if (self) reports.set(self, this.getHeldIds());

    const collector = (payload: HoldingsReport<TId>) => {
      reports.set(payload.peer, payload.ids);
    };

    this.bully.channel.on("holdings-report", collector);
    await this.forum.broadcast("report-holdings-request");
    await new Promise((resolve) => setTimeout(resolve, this.reportTimeoutMs));
    this.bully.channel.off("holdings-report", collector);

    const reporters = [...reports.keys()].sort();
    if (reporters.length === 0) {
      this.logger.warn("Reconciled: no peer reported in, nothing we can do");
      return;
    }

    if (this.expectedClusterSize) {
      const majority = Math.floor(this.expectedClusterSize / 2) + 1;
      if (reporters.length < majority) {
        this.consecutiveQuorumFailures++;
        this.logger.warn(
          `Reconciled: only ${reporters.length}/${this.expectedClusterSize} peer(s) reachable ` +
            `(need ${majority} for quorum), refusing to reconcile ` +
            `[${this.consecutiveQuorumFailures}/${this.quorumFailureThreshold} consecutive failures]`,
        );

        if (this.consecutiveQuorumFailures >= this.quorumFailureThreshold && !this.hasTriggeredSustainedLoss) {
          this.hasTriggeredSustainedLoss = true;
          this.logger.error(
            `Sustained quorum loss after ${this.consecutiveQuorumFailures} consecutive attempts, ` +
              `invoking onSustainedQuorumLoss`,
          );
          this.sustainedQuorumLossCallback();
        }
        return;
      }
    }

    this.consecutiveQuorumFailures = 0;
    this.hasTriggeredSustainedLoss = false;

    const fairShare = Math.floor(this.allIds.length / reporters.length);
    const remainder = this.allIds.length % reporters.length;

    let cursor = 0;
    const targets = new Map<string, Set<TId>>();
    reporters.forEach((peer, index) => {
      const count = fairShare + (index < remainder ? 1 : 0);
      targets.set(peer, new Set(this.allIds.slice(cursor, cursor + count)));
      cursor += count;
    });

    // Broadcast unconditionally, even on a pass where nothing moves — a peer's cached map is
    // otherwise only as fresh as the last pass that happened to change something.
    const ownershipEntries: [TId, string][] = [];
    for (const [peer, ids] of targets) for (const id of ids) ownershipEntries.push([id, peer]);
    await this.forum.broadcast("ownership-map", ownershipEntries);

    const assignments = new Map<string, TId[]>();
    const releases = new Map<string, TId[]>();

    for (const peer of reporters) {
      const current = new Set(reports.get(peer));
      const target = targets.get(peer)!;

      const toAdd = [...target].filter((id) => !current.has(id));
      const toRemove = [...current].filter((id) => !target.has(id));

      if (toAdd.length > 0) assignments.set(peer, toAdd);
      if (toRemove.length > 0) releases.set(peer, toRemove);
    }

    if (assignments.size === 0 && releases.size === 0) {
      this.logger.debug("Reconciled: already balanced, nothing to do");
      return;
    }

    this.logger.info(
      `Reconciled: converging ${reporters.length} peer(s) on a fair split ` +
        `(${assignments.size} gaining ids, ${releases.size} releasing ids)`,
    );

    await Promise.all([
      ...[...assignments.entries()].map(([peer, ids]) => this.forum.tell(peer, "assign-connections", ids)),
      ...[...releases.entries()].map(([peer, ids]) => this.forum.tell(peer, "release-connections", ids)),
    ]);
  }
}
