import type { HostErrorCode, HostErrorStage } from "../contracts";

import type { KafkaOperationalPreferenceStructuredError } from "./operational-preference-types";

abstract class KafkaOperationalPreferenceError
  extends Error
  implements KafkaOperationalPreferenceStructuredError
{
  abstract readonly code: HostErrorCode;
  abstract readonly recovery: string;
  readonly retryable = false;
  abstract readonly stage: HostErrorStage;
  readonly target = undefined;

  protected constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class KafkaOperationalPreferenceValidationError extends KafkaOperationalPreferenceError {
  readonly code = "VALIDATION" as const;
  readonly recovery = "Correct the identified operational preference and try again.";
  readonly stage = "validation" as const;

  constructor(message: string) {
    super(message);
  }
}

export class KafkaOperationalPreferenceStoreUnavailableError extends KafkaOperationalPreferenceError {
  readonly code = "PREFERENCE_STORE_UNAVAILABLE" as const;
  readonly recovery =
    "Verify operational preference storage is writable, then retry or reset the preferences.";
  readonly stage = "preference" as const;

  constructor(message = "Operational preference storage could not complete the request.") {
    super(message);
  }
}

export class KafkaOperationalPreferenceCorruptError extends KafkaOperationalPreferenceError {
  readonly code = "PREFERENCE_CORRUPT" as const;
  readonly recovery = "Reset operational preferences to replace the unreadable file.";
  readonly stage = "preference" as const;

  constructor(message = "Stored operational preferences are unreadable or unsupported.") {
    super(message);
  }
}
