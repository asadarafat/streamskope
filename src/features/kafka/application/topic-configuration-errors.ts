import type { HostErrorCode, HostErrorStage } from "../contracts";

export class KafkaTopicConfigurationValidationError extends Error {
  readonly code: HostErrorCode;
  readonly recovery: string;
  readonly retryable = false;
  readonly stage: HostErrorStage = "validation";
  readonly target: string | undefined;

  constructor(
    message: string,
    target: string | undefined,
    code: Extract<HostErrorCode, "INVALID_TOPIC_CONFIG" | "VALIDATION"> = "INVALID_TOPIC_CONFIG",
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "KafkaTopicConfigurationValidationError";
    this.code = code;
    this.recovery = "Correct the pending topic configuration and dry-run it again.";
    this.target = target;
  }
}
