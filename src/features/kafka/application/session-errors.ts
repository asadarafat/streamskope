export class ConnectionAttemptSupersededError extends Error {
  readonly cleanupFailure: unknown;

  constructor(cleanupFailure?: unknown) {
    super("The connection attempt was superseded by a newer lifecycle operation.", {
      cause: cleanupFailure,
    });
    this.name = "ConnectionAttemptSupersededError";
    this.cleanupFailure = cleanupFailure;
  }
}

export class NoActiveKafkaConnectionError extends Error {
  constructor(operation = "listing topics") {
    super(`Connect to a Kafka cluster before ${operation}.`);
    this.name = "NoActiveKafkaConnectionError";
  }
}

export class KafkaCleanupTimeoutError extends Error {
  readonly code = "TIMEOUT" as const;
  readonly stage = "kafka" as const;
  readonly retryable = true;
  readonly recovery: string;
  readonly target: string;

  constructor(kind: "consumption" | "connection" | "shutdown") {
    const resource = kind === "consumption" ? "message stream" : kind;
    super(`Kafka ${resource} cleanup did not finish within five seconds.`);
    this.name = "KafkaCleanupTimeoutError";
    this.target = `kafka-${kind}-cleanup`;
    this.recovery =
      kind === "consumption"
        ? "Cleanup continues. Wait for it to finish, then retry Stop."
        : "Cleanup continues. Wait for it to finish before retrying the connection operation.";
  }
}
