import { HOST_PROTOCOL_VERSION, type HostCommandResponse } from "../contracts";
import type { SchemaPolicyCommand } from "../contracts/schema-policy-protocol";
import {
  parseSchemaPolicyReview,
  parseSchemaPolicyOutcome,
  parseSchemaPolicyBaseline,
} from "../contracts/schema-policy";
import type {
  SchemaRegistryPort,
  SchemaRegistryReviewPort,
  SchemaRegistryPolicyPort,
  ReviewedSchemaPolicyPort,
} from "../application/schema-registry-types";
import { SchemaPolicyService } from "../application/schema-policy-service";
import type { SchemaReviewOperations } from "../application/schema-review-operations";
import { SchemaChangeReviewError } from "../application/schema-change-errors";
import { serviceConnectionDiagnostic } from "../application/connection-diagnostics";

import { failureResponse, type ActivityInput } from "./facade-support";

export class SchemaPolicyFacade {
  private readonly service?: SchemaPolicyService;
  constructor(
    port:
      | (SchemaRegistryPort & Partial<SchemaRegistryReviewPort & SchemaRegistryPolicyPort>)
      | undefined,
    operations: SchemaReviewOperations,
    private readonly activity: (input: ActivityInput) => void,
  ) {
    if (port?.loadReviewSchema && port.loadCompatibilityPolicy && port.changeSubjectCompatibility)
      this.service = new SchemaPolicyService(port as ReviewedSchemaPolicyPort, operations);
  }
  async execute(command: SchemaPolicyCommand, correlationId: string): Promise<HostCommandResponse> {
    try {
      if (!this.service) throw new Error("Reviewed Registry policy changes are unavailable.");
      const common = { id: command.id, version: HOST_PROTOCOL_VERSION, ok: true as const };
      if (command.command === "schemas.policy.load")
        return {
          ...common,
          command: command.command,
          result: {
            correlationId,
            baseline: parseSchemaPolicyBaseline(await this.service.load(command.payload.subject)),
          },
        };
      if (command.command === "schemas.policy.review")
        return {
          ...common,
          command: command.command,
          result: {
            correlationId,
            review: parseSchemaPolicyReview(await this.service.review(command.payload)),
          },
        };
      const outcome = parseSchemaPolicyOutcome(
        await this.service.apply(command.payload.planId, command.payload.confirmation),
      );
      this.activity({
        correlationId,
        operation: "Apply reviewed subject compatibility policy",
        object: command.payload.confirmation,
        detail: outcome.detail,
        outcome:
          outcome.state === "acknowledged" || outcome.state === "unchanged"
            ? "succeeded"
            : "failed",
        severity: outcome.verification === "verified" ? "info" : "warning",
      });
      return { ...common, command: command.command, result: { correlationId, outcome } };
    } catch (error) {
      return failureResponse(command, {
        activeStateChanged: false,
        code: "VALIDATION",
        stage: "validation",
        correlationId,
        retryable: false,
        summary:
          error instanceof SchemaChangeReviewError
            ? error.message
            : "The Registry policy request could not be accepted.",
        recovery:
          "Refresh and select the latest writer. Check Registry read and subject configuration permissions and supported basic configuration. Confirm the exact subject; changed or expired reviews require a fresh review.",
        ...(serviceConnectionDiagnostic(error, "Schema Registry") ?? {}),
      });
    }
  }
}
