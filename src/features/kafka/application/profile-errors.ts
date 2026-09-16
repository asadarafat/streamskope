import type { HostErrorCode, HostErrorStage } from "../contracts";

import type { KafkaProfileIssue, KafkaProfileStructuredError } from "./profile-types";

abstract class KafkaProfileError extends Error implements KafkaProfileStructuredError {
  abstract readonly code: HostErrorCode;
  abstract readonly recovery: string;
  readonly retryable = false;
  abstract readonly stage: HostErrorStage;
  readonly target: string | undefined;

  protected constructor(message: string, target?: string) {
    super(message);
    this.name = new.target.name;
    this.target = target;
  }
}

export class KafkaProfileValidationError extends KafkaProfileError {
  readonly code = "VALIDATION" as const;
  readonly recovery = "Correct the identified profile fields and try again.";
  readonly stage = "validation" as const;

  constructor(readonly issues: readonly KafkaProfileIssue[]) {
    super(issues.map((issue) => `${issue.field}: ${issue.message}`).join("\n"));
  }
}

export class DuplicateKafkaProfileError extends KafkaProfileError {
  readonly code = "PROFILE_DUPLICATE" as const;
  readonly recovery = "Choose a unique profile name or edit the existing profile.";
  readonly stage = "profile" as const;

  constructor(name: string) {
    super(`A Kafka profile named "${name}" already exists.`, name);
  }
}

export class KafkaProfileCapacityError extends KafkaProfileError {
  readonly code = "VALIDATION" as const;
  readonly recovery = "Delete an unused profile before creating another.";
  readonly stage = "profile" as const;

  constructor(limit: number) {
    super(`Kafka profile capacity of ${limit} has been reached.`);
  }
}

export class KafkaProfileNotFoundError extends KafkaProfileError {
  readonly code = "PROFILE_NOT_FOUND" as const;
  readonly recovery = "Refresh the profile list and choose an existing profile.";
  readonly stage = "profile" as const;

  constructor(profileId: string) {
    super("The selected Kafka profile no longer exists.", profileId);
  }
}

export class ActiveKafkaProfileMutationError extends KafkaProfileError {
  readonly code = "PROFILE_ACTIVE" as const;
  readonly recovery = "Disconnect this profile before editing or deleting it.";
  readonly stage = "profile" as const;

  constructor(name: string) {
    super(`Kafka profile "${name}" is active and cannot be changed.`, name);
  }
}

export class KafkaProfileStoreUnavailableError extends KafkaProfileError {
  readonly code = "PROFILE_STORE_UNAVAILABLE" as const;
  readonly recovery = "Unlock or configure protected profile storage, then restart StreamSkope.";
  readonly stage = "storage" as const;

  constructor() {
    super("Protected Kafka profile storage is unavailable.");
  }
}

export class KafkaProfileRevisionError extends KafkaProfileError {
  readonly code = "VALIDATION" as const;
  readonly recovery = "Close this editor, refresh profiles and reopen the profile before retrying.";
  readonly stage = "profile" as const;

  constructor() {
    super("This profile changed after the editor was opened. No changes were saved.");
  }
}
