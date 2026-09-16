import type {
  ConnectionTemplateCatalog,
  ConnectionTemplateIssue,
  HostErrorCode,
  HostErrorStage,
} from "../contracts";

import type { KafkaConnectionTemplateStructuredError } from "./connection-template-types";

function target(catalog: ConnectionTemplateCatalog, name?: string): string {
  return name === undefined ? catalog : `${catalog} · ${name}`;
}

abstract class KafkaConnectionTemplateError
  extends Error
  implements KafkaConnectionTemplateStructuredError
{
  abstract readonly code: HostErrorCode;
  abstract readonly recovery: string;
  readonly retryable = false;
  abstract readonly stage: HostErrorStage;
  readonly target: string | undefined;

  protected constructor(message: string, errorTarget?: string) {
    super(message);
    this.name = new.target.name;
    this.target = errorTarget;
  }
}

export class KafkaConnectionTemplateValidationError extends KafkaConnectionTemplateError {
  readonly code = "VALIDATION" as const;
  readonly recovery = "Correct the identified template fields and try again.";
  readonly stage = "validation" as const;

  constructor(readonly issues: readonly ConnectionTemplateIssue[]) {
    super(issues.map((issue) => `${issue.field}: ${issue.message}`).join("\n"));
  }
}

export class DuplicateKafkaConnectionTemplateError extends KafkaConnectionTemplateError {
  readonly code = "TEMPLATE_DUPLICATE" as const;
  readonly recovery = "Choose a unique template name or edit the existing template.";
  readonly stage = "template" as const;

  constructor(catalog: ConnectionTemplateCatalog, name: string) {
    super(
      `A connection template named "${name}" already exists in ${catalog}.`,
      target(catalog, name),
    );
  }
}

export class KafkaConnectionTemplateCapacityError extends KafkaConnectionTemplateError {
  readonly code = "VALIDATION" as const;
  readonly recovery = "Delete an unused template from this catalog before creating another.";
  readonly stage = "template" as const;

  constructor(catalog: ConnectionTemplateCatalog, limit: number) {
    super(
      `Connection template capacity of ${limit} has been reached for ${catalog}.`,
      target(catalog),
    );
  }
}

export class KafkaConnectionTemplateNotFoundError extends KafkaConnectionTemplateError {
  readonly code = "TEMPLATE_NOT_FOUND" as const;
  readonly recovery = "Refresh templates and choose an existing entry.";
  readonly stage = "template" as const;

  constructor(catalog: ConnectionTemplateCatalog, name: string) {
    super(
      `The selected connection template no longer exists in ${catalog}.`,
      target(catalog, name),
    );
  }
}

export class KafkaConnectionTemplateStoreUnavailableError extends KafkaConnectionTemplateError {
  readonly code = "TEMPLATE_STORE_UNAVAILABLE" as const;
  readonly recovery = "Verify template storage is writable, then retry or restart StreamSkope.";
  readonly stage = "template" as const;

  constructor(errorTarget?: string) {
    super("Connection template storage could not commit the requested change.", errorTarget);
  }
}
