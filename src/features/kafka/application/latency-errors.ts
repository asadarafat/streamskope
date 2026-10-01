import type { HostErrorCode, HostErrorStage } from "../contracts";

export class KafkaLatencyProbeValidationError extends Error {
  readonly code: HostErrorCode = "VALIDATION";
  readonly recovery: string;
  readonly retryable = false;
  readonly stage: HostErrorStage = "validation";
  readonly target: string | undefined;

  constructor(message: string, target?: string, recovery?: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "KafkaLatencyProbeValidationError";
    this.target = target;
    this.recovery =
      recovery ??
      "Connect to the intended cluster, select a writable topic, and retry one bounded probe.";
  }
}
