import type { HostErrorCode, HostErrorStage } from "../contracts";

import type { KafkaProfileStructuredError } from "./profile-types";

abstract class KafkaTrustAcquisitionError extends Error implements KafkaProfileStructuredError {
  abstract readonly code: HostErrorCode;
  abstract readonly recovery: string;
  abstract readonly retryable: boolean;
  abstract readonly stage: HostErrorStage;
  readonly target: string | undefined;

  protected constructor(message: string, target?: string) {
    super(message);
    this.name = new.target.name;
    this.target = target;
  }
}

export class KafkaTrustAcquisitionCapacityError extends KafkaTrustAcquisitionError {
  readonly code = "ACQUISITION_CAPACITY" as const;
  readonly recovery = "Discard an unused remote trust acquisition, then try again.";
  readonly retryable = false;
  readonly stage = "acquisition" as const;

  constructor(limit: number) {
    super(`Remote trust acquisition capacity of ${String(limit)} has been reached.`);
  }
}

export class KafkaTrustAcquisitionUnavailableError extends KafkaTrustAcquisitionError {
  readonly code = "BACKEND_UNAVAILABLE" as const;
  readonly recovery = "Restart StreamSkope to restore remote trust acquisition.";
  readonly retryable = true;
  readonly stage = "backend" as const;

  constructor() {
    super("The remote trust acquisition owner is unavailable.");
  }
}

export class KafkaTrustAcquisitionNotFoundError extends KafkaTrustAcquisitionError {
  readonly code = "ACQUISITION_NOT_FOUND" as const;
  readonly recovery = "Acquire the remote trust values again, then retry the operation.";
  readonly retryable = false;
  readonly stage = "acquisition" as const;

  constructor(acquisitionId: string) {
    super("The remote trust acquisition is no longer available.", acquisitionId);
  }
}

export class KafkaTrustAcquisitionExpiredError extends KafkaTrustAcquisitionError {
  readonly code = "ACQUISITION_EXPIRED" as const;
  readonly recovery = "Acquire the remote trust values again, then retry within ten minutes.";
  readonly retryable = false;
  readonly stage = "acquisition" as const;

  constructor(acquisitionId: string) {
    super("The remote trust acquisition expired.", acquisitionId);
  }
}

export class KafkaTrustAcquisitionIncompleteError extends KafkaTrustAcquisitionError {
  readonly code = "ACQUISITION_INCOMPLETE" as const;
  readonly recovery = "Acquire the required remote password and trust material before continuing.";
  readonly retryable = false;
  readonly stage = "acquisition" as const;

  constructor(acquisitionId?: string) {
    super(
      "The remote trust acquisition does not contain complete matching trust values.",
      acquisitionId,
    );
  }
}

export class KafkaTrustAcquisitionValidationError extends KafkaTrustAcquisitionError {
  readonly code = "VALIDATION" as const;
  readonly recovery = "Correct the remote trust acquisition fields and try again.";
  readonly retryable = false;
  readonly stage = "validation" as const;

  constructor(message: string, target?: string) {
    super(message, target);
  }
}

export class KafkaTrustAcquisitionPasswordError extends KafkaTrustAcquisitionError {
  readonly code = "TRUSTSTORE_PASSWORD" as const;
  readonly recovery =
    "Supply the matching truststore password or verify that the selected password command returns one bounded non-empty value.";
  readonly retryable = false;
  readonly stage = "trust" as const;

  constructor() {
    super("The trust acquisition did not receive a valid truststore password.");
  }
}

export class KafkaTrustAcquisitionTimeoutError extends KafkaTrustAcquisitionError {
  readonly code = "TIMEOUT" as const;
  readonly recovery =
    "Check the remote target and recipe execution budget, then acquire again. Existing trust is unchanged.";
  readonly retryable = false;
  readonly stage = "acquisition" as const;

  constructor() {
    super("The complete trust acquisition exceeded its execution budget.");
  }
}

export class KafkaTrustAcquisitionCancelledError extends KafkaTrustAcquisitionError {
  readonly code = "CANCELLED" as const;
  readonly recovery =
    "Acquire again when ready. Existing trust and the active connection are unchanged.";
  readonly retryable = true;
  readonly stage = "acquisition" as const;

  constructor() {
    super("Remote trust acquisition was cancelled.");
  }
}

export class KafkaTrustAcquisitionIdentityError extends KafkaTrustAcquisitionError {
  readonly code = "SSH_IDENTITY" as const;
  readonly stage = "ssh" as const;
  readonly retryable = false;
  readonly recovery =
    "Verify the replacement identity independently. Reset the saved identity explicitly before accepting a new server key.";
  constructor(target: string) {
    super(
      "The SSH identity differs from the identity saved for this endpoint. No credentials were sent.",
      target,
    );
  }
}

export class KafkaTrustAcquisitionMaterialError extends KafkaTrustAcquisitionError {
  readonly code = "TRUST_MATERIAL" as const;
  readonly recovery: string;
  readonly retryable = false;
  readonly stage = "trust" as const;

  constructor(reason: "invalid" | "empty-command-output" = "invalid", target?: string) {
    super(
      reason === "empty-command-output"
        ? "The material command completed but returned no certificate or truststore bytes on stdout."
        : "The selected remote template did not produce valid trust material.",
      target,
    );
    this.recovery =
      reason === "empty-command-output"
        ? "Return raw certificate or truststore bytes on stdout. Do not redirect output to a file. For an existing remote file, select Remote file."
        : "Verify the selected template produces bounded trust material of the declared kind.";
  }
}
