import { EventEmitter } from "events";
import type { Server } from "http";
import express, { NextFunction, Request, Response } from "express";
import type { Logger } from "./logger";

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

export class Transport {
  public readonly fetchFn: typeof fetch;
  public readonly port: number;
  private readonly internalHost?: string;
  private readonly requestTimeoutMs: number;
  private readonly logger: Logger;
  private server?: Server;

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
    const app = express();
    app.use(express.json());

    app.get("/health", (_, res) => {
      res.json({ status: "OK", timestamp: new Date().toISOString() });
    });

    app.post("/bully/election", (req, res) => {
      cb.onElectionMessage(req.body?.id);
      res.sendStatus(200);
    });

    app.post("/bully/coordinator", (req, res) => {
      cb.onCoordinatorMessage(req.body?.id);
      res.sendStatus(200);
    });

    app.post("/bully/message", (req, res) => {
      cb.onMessage(req.body?.event, req.body?.payload);
      res.sendStatus(200);
    });

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

    app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
      this.logger.error(err);
      res.status(500).json({ status: "ERROR", error: err?.message ?? String(err) });
    });

    const onListening = () => this.logger.info(`Internal coordination server listening on port ${this.port}`);
    this.server = this.internalHost
      ? app.listen(this.port, this.internalHost, onListening)
      : app.listen(this.port, onListening);
  }

  public stop(): void {
    if (this.server) this.server.close();
  }
}
