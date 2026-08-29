import { Transport } from "./transport";
import { Bully, type GetCluster } from "./bully";
import { Forum } from "./forum";
import { ShardManager } from "./sharding";
import type { Logger } from "./logger";

export type QuorumOptions<TId = string> = {
  getCluster: GetCluster;
  ids: TId[];
  fetchFn?: typeof fetch;
  internalPort?: number;
  internalHost?: string;
  requestTimeoutMs?: number;
  coordinatorWaitMs?: number;
  heartbeatIntervalMs?: number;
  reportTimeoutMs?: number;
  rebalanceIntervalMs?: number;
  expectedClusterSize?: number;
  quorumFailureThreshold?: number;
  onSustainedQuorumLoss?: () => void;
  /** Injectable log sink; must implement `info`/`warn`/`error`/`debug`. Defaults to `console`. */
  logger?: Logger;
};

export class Quorum<TId = string> implements Disposable {
  private readonly transport: Transport;
  private readonly bully: Bully;
  private readonly forum: Forum;
  private readonly shard: ShardManager<TId>;

  constructor(opts: QuorumOptions<TId>) {
    this.transport = new Transport({
      fetchFn: opts.fetchFn,
      internalPort: opts.internalPort,
      internalHost: opts.internalHost,
      requestTimeoutMs: opts.requestTimeoutMs,
      logger: opts.logger,
    });
    this.bully = new Bully({
      getCluster: opts.getCluster,
      transport: this.transport,
      coordinatorWaitMs: opts.coordinatorWaitMs,
      heartbeatIntervalMs: opts.heartbeatIntervalMs,
      logger: opts.logger,
    });
    this.forum = new Forum({
      bully: this.bully,
      transport: this.transport,
      getCluster: opts.getCluster,
      fetchFn: opts.fetchFn,
      logger: opts.logger,
    });
    this.shard = new ShardManager<TId>({
      bully: this.bully,
      forum: this.forum,
      ids: opts.ids,
      reportTimeoutMs: opts.reportTimeoutMs,
      rebalanceIntervalMs: opts.rebalanceIntervalMs,
      expectedClusterSize: opts.expectedClusterSize,
      quorumFailureThreshold: opts.quorumFailureThreshold,
      onSustainedQuorumLoss: opts.onSustainedQuorumLoss,
      logger: opts.logger,
    });
  }

  public isLeader(): boolean { return this.bully.isLeader(); }
  public getStatus() { return this.bully.getStatus(); }
  public async getPodRoles() { return this.bully.getPodRoles(); }

  public subscribe(id: TId, onEvent: (payload: unknown) => void): () => void {
    return this.shard.subscribe(id, onEvent);
  }
  public publish(id: TId, payload: unknown): void { this.shard.publish(id, payload); }
  public getOwner(id: TId): string | undefined { return this.shard.getOwner(id); }
  public getOwnership(): Map<TId, string> { return this.shard.getOwnership(); }
  public getHeldIds(): TId[] { return this.shard.getHeldIds(); }
  public async updateIds(ids: TId[]): Promise<void> { return this.shard.updateIds(ids); }

  public start(): void {
    this.transport.start({
      onElectionMessage: (id) => this.bully.onElectionMessage(id),
      onCoordinatorMessage: (id) => this.bully.onCoordinatorMessage(id),
      onMessage: (event, payload) => this.bully.onMessage(event, payload),
      channel: this.bully.channel,
    });
    this.shard.start();
    this.bully.start();
    this.bully.startElection();
  }

  public stop(): void {
    this.shard.stop();
    this.bully.stop();
    this.transport.stop();
  }

  public [Symbol.dispose](): void {
    this.stop();
  }
}
