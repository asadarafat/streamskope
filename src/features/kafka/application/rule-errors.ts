import type { HostErrorCode, HostErrorStage, KafkaRuleIssue } from "../contracts";

import type { KafkaRuleStructuredError } from "./rule-types";

abstract class KafkaRuleError extends Error implements KafkaRuleStructuredError {
  abstract readonly code: HostErrorCode;
  abstract readonly recovery: string;
  readonly retryable = false;
  abstract readonly stage: HostErrorStage;
  readonly target: string | undefined;

  protected constructor(message: string, errorTarget?: string, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
    this.target = errorTarget;
  }
}

export class KafkaRuleValidationError extends KafkaRuleError {
  readonly code: HostErrorCode = "RULE_VALIDATION";
  readonly recovery: string = "Correct the identified rule fields and try again.";
  readonly stage = "rule" as const;

  constructor(
    readonly issues: readonly KafkaRuleIssue[],
    code: "RULE_SAMPLE" | "RULE_VALIDATION" = "RULE_VALIDATION",
    options?: ErrorOptions,
    message?: string,
  ) {
    super(
      message ?? issues.map((issue) => `${issue.field}: ${issue.message}`).join("\n"),
      undefined,
      options,
    );
    this.code = code;
  }
}

export class KafkaRuleSampleValidationError extends KafkaRuleValidationError {
  override readonly recovery: string = "Provide a bounded valid JSON sample and try again.";

  constructor(message: string, options?: ErrorOptions) {
    super([], "RULE_SAMPLE", options, message);
  }
}

export class DuplicateKafkaRuleError extends KafkaRuleError {
  readonly code = "RULE_DUPLICATE" as const;
  readonly recovery = "Choose a unique rule name or edit the existing rule.";
  readonly stage = "rule" as const;

  constructor(name: string) {
    super(`A Kafka rule named "${name}" already exists.`, name);
  }
}

export class KafkaRuleCapacityError extends KafkaRuleError {
  readonly code = "RULE_CAPACITY" as const;
  readonly recovery = "Delete an unused rule before creating another.";
  readonly stage = "rule" as const;

  constructor(limit: number) {
    super(`Kafka rule capacity of ${String(limit)} has been reached.`);
  }
}

export class KafkaRuleNotFoundError extends KafkaRuleError {
  readonly code = "RULE_NOT_FOUND" as const;
  readonly recovery = "Refresh rules and choose an existing entry.";
  readonly stage = "rule" as const;

  constructor(name: string) {
    super(`The Kafka rule "${name}" no longer exists.`, name);
  }
}

export class KafkaRuleCatalogNotLoadedError extends KafkaRuleError {
  readonly code = "RULE_STORE_UNAVAILABLE" as const;
  readonly recovery = "Load the rule catalog before evaluating all rules.";
  readonly stage = "rule" as const;

  constructor() {
    super("The Kafka rule catalog has not been loaded.");
  }
}

export class KafkaRuleStoreUnavailableError extends KafkaRuleError {
  readonly code: "RULE_CORRUPT" | "RULE_STORE_UNAVAILABLE";
  readonly recovery: string =
    "Verify rule storage is readable and writable, then retry or restart StreamSkope.";
  readonly stage = "storage" as const;

  constructor(
    errorTarget?: string,
    code: "RULE_CORRUPT" | "RULE_STORE_UNAVAILABLE" = "RULE_STORE_UNAVAILABLE",
    options?: ErrorOptions,
  ) {
    super("Kafka rule storage could not complete the requested operation.", errorTarget, options);
    this.code = code;
  }
}

export class KafkaRuleCorruptError extends KafkaRuleStoreUnavailableError {
  override readonly recovery: string =
    "Preserve the rule data, correct it outside the running application, then restart StreamSkope.";

  constructor(options?: ErrorOptions) {
    super(undefined, "RULE_CORRUPT", options);
  }
}
