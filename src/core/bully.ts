import { EventEmitter } from "events";
import type { Logger } from "./logger.js";
import type { Transport } from "./transport.js";

export type BullyPeer = { name?: string; host?: string };
export type BullyCluster = { self: BullyPeer; cluster: BullyPeer[] };
export type Discovery = () => Promise<BullyCluster>;

export type BullyOptions = {
  discovery: Discovery;
  transport: Transport;
  coordinatorWaitMs?: number;
  heartbeatIntervalMs?: number;
  logger?: Logger;
};

export class Bully implements Disposable {
  public readonly lifecycle = new EventEmitter();
  public readonly channel = new EventEmitter();

  private readonly discovery: Discovery;
  private readonly transport: Transport;
  private readonly coordinatorWaitMs: number;
  private readonly heartbeatIntervalMs: number;
  private readonly logger: Logger;

  private selfId?: string;
  private leaderId?: string;
  private electionInFlight = false;
  private coordinatorWaitTimeout?: NodeJS.Timeout;
  private heartbeatInterval?: NodeJS.Timeout;

  constructor(opts: BullyOptions) {
    this.discovery = opts.discovery;
    this.transport = opts.transport;
    this.coordinatorWaitMs = opts.coordinatorWaitMs ?? 4000;
    this.heartbeatIntervalMs = opts.heartbeatIntervalMs ?? 5000;
    this.logger = opts.logger ?? console;
  }

  public isLeader(): boolean {
    return !!this.selfId && this.selfId === this.leaderId;
  }

  public getStatus() {
    return { self: this.selfId, leader: this.leaderId, isLeader: this.isLeader() };
  }

  public async getPodRoles() {
    const { cluster } = await this.discovery();
    return cluster.map((pod) => ({
      name: pod.name,
      host: pod.host,
      role: pod.name === this.leaderId ? "leader" : "follower",
    }));
  }

  /** Returns all cluster peers except self. Side-effect: populates selfId. */
  public async getPeers(): Promise<BullyPeer[]> {
    const { self, cluster } = await this.discovery();
    this.selfId = self.name;
    return cluster.filter((pod) => pod.name !== self.name && pod.host);
  }

  private async becomeLeader(peers: BullyPeer[]): Promise<void> {
    const wasLeader = this.leaderId === this.selfId;
    this.leaderId = this.selfId;

    if (!wasLeader) this.logger.info(`Became leader (id=${this.selfId})`);

    await Promise.all(
      peers.map((peer) => this.transport.postToPeer(peer.host!, "/bully/coordinator", { id: this.selfId })),
    );

    // Emitted only after peers have been told, so an "elected" listener that immediately talks
    // to peers (e.g. Sharding.reconcile()) doesn't race ahead of them learning who's leader.
    if (!wasLeader) this.lifecycle.emit("elected");
  }

  public async startElection(): Promise<void> {
    if (this.electionInFlight) return;
    this.electionInFlight = true;
    if (this.coordinatorWaitTimeout) clearTimeout(this.coordinatorWaitTimeout);

    this.logger.info("Starting election");

    try {
      const peers = await this.getPeers();
      const higherPeers = peers.filter((peer) => peer.name! > this.selfId!);

      if (higherPeers.length === 0) {
        await this.becomeLeader(peers);
        return;
      }

      const acked = await Promise.all(
        higherPeers.map((peer) => this.transport.postToPeer(peer.host!, "/bully/election", { id: this.selfId })),
      );

      if (acked.some(Boolean)) {
        this.logger.debug("Higher peer(s) acknowledged, waiting for coordinator announcement");
        this.coordinatorWaitTimeout = setTimeout(() => {
          this.logger.debug("Timed out waiting for coordinator, restarting election");
          this.startElection();
        }, this.coordinatorWaitMs);
      } else {
        await this.becomeLeader(peers);
      }
    } catch (err: any) {
      this.logger.error(`Election error: ${err.message}`);
    } finally {
      this.electionInFlight = false;
    }
  }

  public onElectionMessage(fromId: string): void {
    this.logger.debug(`Received election message from ${fromId}, asserting alive and starting own election`);
    this.startElection();
  }

  public onCoordinatorMessage(id: string): void {
    // Bully's invariant: the highest-named reachable node is leader. If we currently, correctly
    // believe *we ourselves* are leader, an announcement naming a lower node must be stale or
    // erroneous — e.g. that node transiently failed to reach us during a concurrent election
    // (most likely when many nodes start at once) and wrongly self-elected. Accepting it
    // unconditionally, as before, let two nodes settle into a stable, mutual disagreement about
    // who's leader: each still considered its own (different) belief reachable, so neither's
    // heartbeat ever noticed anything was wrong, and nothing here ever re-reconciled them.
    //
    // This deliberately does NOT compare against a stale leaderId belonging to some other,
    // possibly-departed node (an earlier version of this check did, and broke the legitimate
    // "the old leader died, a lower-named survivor is taking over" case — that survivor's own
    // leaderId still pointed at the dead leader, so it wrongly rejected the takeover). Only our
    // own, current self-belief is trustworthy enough to reject an announcement against.
    if (this.isLeader() && id < this.selfId!) {
      this.logger.debug(`Ignoring coordinator announcement from lower-named "${id}" — I am leader`);
      return;
    }

    if (this.coordinatorWaitTimeout) clearTimeout(this.coordinatorWaitTimeout);

    const wasLeader = this.isLeader();
    const changed = this.leaderId !== id;
    this.leaderId = id;

    if (changed) this.logger.debug(`New leader announced: ${id}`);

    if (wasLeader && id !== this.selfId) {
      this.logger.info("Demoted to follower");
      this.lifecycle.emit("demoted");
    }
  }

  public onMessage(event: string, payload: unknown): void {
    this.channel.emit(event, payload);
  }

  public start(): void {
    this.heartbeatInterval = setInterval(async () => {
      try {
        if (this.electionInFlight) return;
        if (this.leaderId && this.leaderId === this.selfId) return;

        if (!this.leaderId) {
          this.startElection();
          return;
        }

        const peers = await this.getPeers();
        const leader = peers.find((peer) => peer.name === this.leaderId);

        if (!leader?.host || !(await this.transport.pingPeer(leader.host))) {
          this.logger.info(`Leader ${this.leaderId} unreachable, starting election`);
          this.leaderId = undefined;
          this.startElection();
        }
      } catch (err: any) {
        this.logger.error(`Heartbeat error: ${err.message}`);
      }
    }, this.heartbeatIntervalMs);
  }

  public stop(): void {
    this.logger.info("Stopping Bully Coordinator...");
    if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);
    if (this.coordinatorWaitTimeout) clearTimeout(this.coordinatorWaitTimeout);
  }

  public [Symbol.dispose](): void {
    this.stop();
  }
}
