export const KAFKA_RULE_SAMPLE_FAILURE_REASONS = ["limit-exceeded", "malformed"] as const;

export type KafkaRuleSampleFailureReason = (typeof KAFKA_RULE_SAMPLE_FAILURE_REASONS)[number];

export interface KafkaRuleSampleLimits {
  readonly bytes: number;
  readonly depth: number;
  readonly nodes: number;
}

export class KafkaRuleSampleParseError extends Error {
  constructor(
    message: string,
    readonly reason: KafkaRuleSampleFailureReason,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "KafkaRuleSampleParseError";
  }
}
