/** Minimal logging shape every class accepts as `opts.logger`. `console` satisfies this as-is. */
export type Logger = Pick<Console, "info" | "warn" | "error" | "debug">;
