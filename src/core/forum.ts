import type { Logger } from "./logger.js";
import type { Bully, BullyPeer, Discovery } from "./bully.js";
import type { Transport } from "./transport.js";

export type ForumOptions = {
  bully: Bully;
  transport: Transport;
  discovery: Discovery;
  fetchFn?: typeof fetch;
  logger?: Logger;
};

/**
 * The shape a peer must respond with for distributeAndCollect() to attribute its answer —
 * responses aren't automatically tagged with who sent them, so the sender embeds its own name.
 */
export type DistributeResponse<R> = { peer: string; response: R };

/**
 * What a request() handler receives on the target side. Reply via `forum.respond(envelope, ...)`,
 * passing this same envelope back — it carries the correlation id and the requester's name.
 */
export type RequestEnvelope<T> = { requestId: string; from: string; payload: T };

export class Forum {
  private readonly bully: Bully;
  private readonly transport: Transport;
  private readonly discovery: Discovery;
  private readonly fetchFn: typeof fetch;
  private readonly logger: Logger;

  constructor(opts: ForumOptions) {
    this.bully = opts.bully;
    this.transport = opts.transport;
    this.discovery = opts.discovery;
    this.fetchFn = opts.fetchFn ?? this.transport.fetchFn;
    this.logger = opts.logger ?? console;
  }

  /** Leader-only: fan a custom event out to every follower, and fire it locally too. */
  public async broadcast(event: string, payload?: unknown): Promise<void> {
    if (!this.bully.isLeader()) {
      this.logger.warn(`broadcast('${event}') called while not leader, ignoring`);
      return;
    }
    this.bully.channel.emit(event, payload);
    const peers = await this.bully.getPeers();
    await Promise.all(peers.map((peer) => this.transport.postToPeer(peer.host!, "/bully/message", { event, payload })));
  }

  /** Follower-only: send a custom event to whichever pod is currently leader. */
  public async send(event: string, payload?: unknown): Promise<void> {
    if (this.bully.isLeader()) {
      this.bully.channel.emit(event, payload);
      return;
    }
    const { leader } = this.bully.getStatus();
    if (!leader) {
      this.logger.warn(`send('${event}') called with no known leader, dropping`);
      return;
    }
    const peers = await this.bully.getPeers();
    const leaderPeer = peers.find((peer) => peer.name === leader);
    if (!leaderPeer?.host) {
      this.logger.warn(`send('${event}') could not resolve leader host, dropping`);
      return;
    }
    await this.transport.postToPeer(leaderPeer.host, "/bully/message", { event, payload });
  }

  /** Leader-only: send a custom event to exactly one named peer (self included). */
  public async tell(peerName: string, event: string, payload?: unknown): Promise<void> {
    if (!this.bully.isLeader()) {
      this.logger.warn(`tell('${event}') called while not leader, ignoring`);
      return;
    }
    await this.messagePeer(peerName, event, payload);
  }

  /**
   * Peer-to-peer: message one specific named peer directly (self included), regardless of
   * leadership. Unlike broadcast/send/tell, this isn't leader-mediated.
   */
  public async messagePeer(peerName: string, event: string, payload?: unknown): Promise<void> {
    const { self } = this.bully.getStatus();
    if (peerName === self) {
      this.bully.channel.emit(event, payload);
      return;
    }
    const peers = await this.bully.getPeers();
    const target = peers.find((peer) => peer.name === peerName);
    if (!target?.host) {
      this.logger.warn(`messagePeer('${event}') could not resolve peer '${peerName}', dropping`);
      return;
    }
    await this.transport.postToPeer(target.host, "/bully/message", { event, payload });
  }

  /**
   * Peer-to-peer request/response: message one peer and wait for its reply, correlated by a
   * generated request id so concurrent requests to the same peer/event never cross-resolve.
   * Unlike distributeAndCollect this isn't leader-only and doesn't silently drop a missing
   * answer — it rejects if the target doesn't respond within `responseTimeoutMs`.
   *
   * The target's handler receives a `RequestEnvelope<T>` (via onMessage/channel) and must reply
   * with `forum.respond(envelope, response)`.
   */
  public async request<T, R>(peerName: string, event: string, payload: T, responseTimeoutMs = 2000): Promise<R> {
    const { self } = this.bully.getStatus();
    const requestId = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const responseEvent = `__response:${requestId}`;

    const response = new Promise<R>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.bully.channel.off(responseEvent, onResponse);
        reject(new Error(`request('${event}') to '${peerName}' timed out after ${responseTimeoutMs}ms`));
      }, responseTimeoutMs);

      const onResponse = (payload: R) => {
        clearTimeout(timer);
        resolve(payload);
      };

      this.bully.channel.once(responseEvent, onResponse);
    });

    const envelope: RequestEnvelope<T> = { requestId, from: self!, payload };
    await this.messagePeer(peerName, event, envelope);

    return response;
  }

  /** Reply to a request(), addressed back to the requester via the envelope it sent. */
  public async respond<R>(envelope: Pick<RequestEnvelope<unknown>, "requestId" | "from">, response: R): Promise<void> {
    await this.messagePeer(envelope.from, `__response:${envelope.requestId}`, response);
  }

  /**
   * Leader-only: partition work across the whole cluster (self included). `resolvePayload` is
   * called once per participant, in a stable order, and gets back that pod's own share to send.
   */
  public async distribute<T>(
    event: string,
    resolvePayload: (peer: BullyPeer, index: number, allPeers: BullyPeer[]) => T,
  ): Promise<void> {
    if (!this.bully.isLeader()) {
      this.logger.warn(`distribute('${event}') called while not leader, ignoring`);
      return;
    }
    const { self, cluster } = await this.discovery();
    const allPeers = [self, ...cluster.filter((pod) => pod.name !== self.name && pod.host)].sort((a, b) =>
      (a.name ?? "").localeCompare(b.name ?? ""),
    );

    await Promise.all(
      allPeers.map((peer, index) => {
        const payload = resolvePayload(peer, index, allPeers);
        if (peer.name === self.name) {
          this.bully.channel.emit(event, payload);
          return;
        }
        return this.transport.postToPeer(peer.host!, "/bully/message", { event, payload });
      }),
    );
  }

  /**
   * Leader-only: like distribute(), but waits up to `responseTimeoutMs` for each peer to answer
   * back on `responseEvent` and returns whatever came in. A peer that never responds is simply
   * absent from the map — this never rejects or throws on a missing/slow peer.
   *
   * Each peer must respond with:
   * `forum.send(responseEvent, { peer: self, response } satisfies DistributeResponse<R>)`
   * since responses aren't automatically tagged with who sent them (the same convention
   * ShardManager's own holdings-report collection uses internally).
   */
  public async distributeAndCollect<T, R>(
    event: string,
    resolvePayload: (peer: BullyPeer, index: number, allPeers: BullyPeer[]) => T,
    responseEvent: string,
    responseTimeoutMs = 2000,
  ): Promise<Map<string, R>> {
    if (!this.bully.isLeader()) {
      this.logger.warn(`distributeAndCollect('${event}') called while not leader, ignoring`);
      return new Map();
    }

    const responses = new Map<string, R>();
    const collector = (payload: DistributeResponse<R>) => {
      responses.set(payload.peer, payload.response);
    };

    this.bully.channel.on(responseEvent, collector);
    await this.distribute(event, resolvePayload);
    await new Promise((resolve) => setTimeout(resolve, responseTimeoutMs));
    this.bully.channel.off(responseEvent, collector);

    return responses;
  }

  /**
   * Peer-to-peer: open a live subscription to everything a specific peer emits under one
   * channel event name (self included, via a local listener — no loopback HTTP call needed).
   * Returns an unsubscribe function.
   */
  public subscribeToPeer(peerName: string, event: string, onData: (payload: unknown) => void): () => void {
    const { self } = this.bully.getStatus();
    if (peerName === self) {
      this.bully.channel.on(event, onData);
      return () => this.bully.channel.off(event, onData);
    }

    const controller = new AbortController();
    let stopped = false;

    (async () => {
      const peers = await this.bully.getPeers();
      const target = peers.find((peer) => peer.name === peerName);

      if (!target?.host) {
        this.logger.warn(`subscribeToPeer('${event}') could not resolve peer '${peerName}'`);
        return;
      }

      try {
        const res = await this.fetchFn(`http://${target.host}:${this.transport.port}/channel/stream/${event}`, {
          signal: controller.signal,
        });

        if (!res.ok || !res.body) {
          this.logger.warn(`subscribeToPeer('${event}') to '${peerName}' failed to connect`);
          return;
        }

        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (!stopped) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });

          let boundary;
          while ((boundary = buffer.indexOf("\n\n")) !== -1) {
            const frame = buffer.slice(0, boundary);
            buffer = buffer.slice(boundary + 2);
            const dataLine = frame.split("\n").find((line) => line.startsWith("data: "));
            if (!dataLine) continue;
            try {
              onData(JSON.parse(dataLine.slice(6)));
            } catch {
              this.logger.warn(`subscribeToPeer('${event}') received a malformed frame from '${peerName}'`);
            }
          }
        }
      } catch (err: any) {
        if (err.name !== "AbortError") {
          this.logger.error(`subscribeToPeer('${event}') stream to '${peerName}' failed: ${err.message}`);
        }
      }
    })();

    return () => {
      stopped = true;
      controller.abort();
    };
  }
}
