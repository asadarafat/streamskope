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
