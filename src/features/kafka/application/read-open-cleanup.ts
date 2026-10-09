/** A failed open still owns its resource; consumers receive only a retryable close capability. */
export interface KafkaReadOpenCleanup {
  close(): Promise<void>;
}

export class KafkaReadOpenCleanupError extends Error {
  constructor(
    override readonly cause: unknown,
    readonly cleanupCause: unknown,
    readonly cleanup: KafkaReadOpenCleanup,
  ) {
    super("The Kafka reader failed to open and its cleanup is not confirmed.", { cause });
    this.name = "KafkaReadOpenCleanupError";
  }
}
