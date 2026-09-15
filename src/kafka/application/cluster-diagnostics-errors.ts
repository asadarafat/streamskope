import type { HostErrorCode, HostErrorStage } from "../contracts";

export class KafkaClusterDiagnosticsValidationError extends Error {
  readonly code: HostErrorCode = "VALIDATION";
  readonly recovery =
    "Connect to the intended Kafka profile, refresh cluster details, and retry the operation.";
  readonly retryable = false;
  readonly stage: HostErrorStage = "validation";
  readonly target: string | undefined;

  constructor(message: string, target?: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "KafkaClusterDiagnosticsValidationError";
    this.target = target;
  }
}
