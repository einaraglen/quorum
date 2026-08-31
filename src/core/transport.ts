import { EventEmitter } from "events";
import { App } from "@tinyhttp/app";
import type { Logger } from "./logger.js";

export type TransportOptions = {
  fetchFn?: typeof fetch;
  internalPort?: number;
  internalHost?: string;
  requestTimeoutMs?: number;
  logger?: Logger;
};

export type TransportCallbacks = {
  onElectionMessage: (fromId: string) => void;
  onCoordinatorMessage: (id: string) => void;
  onMessage: (event: string, payload: unknown) => void;
  channel: EventEmitter;
};

const readJsonBody = async (req: AsyncIterable<Buffer>): Promise<any> => {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk);
  return chunks.length === 0 ? {} : JSON.parse(Buffer.concat(chunks).toString("utf8"));
};

export class Transport {
  public readonly fetchFn: typeof fetch;
  public readonly port: number;
  private readonly internalHost?: string;
  private readonly requestTimeoutMs: number;
  private readonly logger: Logger;
  private server?: ReturnType<App["listen"]>;

  constructor(opts: TransportOptions = {}) {
    this.fetchFn = opts.fetchFn ?? fetch;
    this.port = opts.internalPort ?? Number(process.env.INTERNAL_PORT || 4001);
    this.internalHost = opts.internalHost;
    this.requestTimeoutMs = opts.requestTimeoutMs ?? 2000;
    this.logger = opts.logger ?? console;
  }

  public async postToPeer(host: string, path: string, body: unknown): Promise<boolean> {
    try {
      const res = await this.fetchFn(`http://${host}:${this.port}${path}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(this.requestTimeoutMs),
      });
      return res.ok;
    } catch {
      return false;
    }
  }

  public async pingPeer(host: string): Promise<boolean> {
    try {
      const res = await this.fetchFn(`http://${host}:${this.port}/health`, {
        signal: AbortSignal.timeout(this.requestTimeoutMs),
      });
      return res.ok;
    } catch {
      return false;
    }
  }

  public start(cb: TransportCallbacks): void {
    const app = new App();

    const onJsonMessage = (handle: (body: any) => void) => async (req: any, res: any) => {
      try {
        handle(await readJsonBody(req));
        res.sendStatus(200);
      } catch (err: any) {
        this.logger.error(err);
        res.status(500).json({ status: "ERROR", error: err?.message ?? String(err) });
      }
    };

    app.get("/health", (_req, res) => {
      res.json({ status: "OK", timestamp: new Date().toISOString() });
    });

    app.post(
      "/bully/election",
      onJsonMessage((body) => cb.onElectionMessage(body?.id)),
    );
    app.post(
      "/bully/coordinator",
      onJsonMessage((body) => cb.onCoordinatorMessage(body?.id)),
    );
    app.post(
      "/bully/message",
      onJsonMessage((body) => cb.onMessage(body?.event, body?.payload)),
    );

    app.get("/channel/stream/:event", (req, res) => {
      const eventName = req.params.event;
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      });
      const forward = (payload: unknown) => res.write(`data: ${JSON.stringify(payload)}\n\n`);
      cb.channel.on(eventName, forward);
      req.on("close", () => cb.channel.off(eventName, forward));
    });

    const onListening = () => this.logger.info(`Internal coordination server listening on port ${this.port}`);
    this.server = this.internalHost
      ? app.listen(this.port, onListening, this.internalHost)
      : app.listen(this.port, onListening);
  }

  public stop(): void {
    if (this.server) this.server.close();
  }
}
