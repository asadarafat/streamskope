import { HOST_PROTOCOL_VERSION, type HostCommandResponse } from "../contracts";
import type { SchemaChangeCommand } from "../contracts/schema-change-protocol";
import type { KafkaApplicationSession, SchemaRegistryPort } from "../application";
import type { SchemaRegistryReviewPort } from "../application/schema-registry-types";
import type { SchemaReviewOperations } from "../application/schema-review-operations";
import { SchemaChangeService } from "../application/schema-change-service";
import { SchemaChangeReviewError } from "../application/schema-change-errors";
import { serviceConnectionDiagnostic } from "../application/connection-diagnostics";
import { parseSchemaChangeReview, parseSchemaChangeOutcome } from "../contracts/schema-changes";

import { failureResponse, type ActivityInput } from "./facade-support";

export class SchemaChangeFacade {
  private readonly service?: SchemaChangeService;
  constructor(
    session: KafkaApplicationSession,
    port: (SchemaRegistryPort & Partial<SchemaRegistryReviewPort>) | undefined,
    private readonly activity: (input: ActivityInput) => void,
    operations: SchemaReviewOperations,
  ) {
    if (port?.loadReviewSchema && port.loadCompatibilityPolicy && port.checkProposedCompatibility)
      this.service = new SchemaChangeService(
        () => session.schemaRegistryReviewScope(),
        port as SchemaRegistryPort & SchemaRegistryReviewPort,
        Date.now,
        operations,
      );
  }
  async execute(command: SchemaChangeCommand, correlationId: string): Promise<HostCommandResponse> {
    try {
      if (!this.service) throw new Error("Reviewed Registry changes are unavailable.");
      if (command.command === "schemas.change.review") {
        const review = parseSchemaChangeReview(await this.service.review(command.payload));
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: { correlationId, review },
        };
      }
      const outcome = parseSchemaChangeOutcome(
        await this.service.apply(command.payload.planId, command.payload.confirmation),
      );
      this.activity({
        correlationId,
        operation: "Apply reviewed schema registration",
        object: command.payload.confirmation,
        detail: outcome.detail,
        outcome: outcome.state === "acknowledged" ? "succeeded" : "failed",
        severity: outcome.verification === "verified" ? "info" : "warning",
      });
      return {
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId, outcome },
      };
    } catch (error) {
      const diagnostic = serviceConnectionDiagnostic(error, "Schema Registry");
      return failureResponse(command, {
        activeStateChanged: false,
        code: "VALIDATION",
        stage: "validation",
        correlationId,
        retryable: false,
        summary:
          error instanceof SchemaChangeReviewError
            ? error.message
            : "The schema change review could not be accepted.",
        recovery:
          "Select the latest writer or an absent new subject. Check Registry read/compatibility permissions, references, supported configuration and review bounds. Confirm the exact subject; expired or changed connections require a new review.",
        ...(diagnostic ?? {}),
      });
    }
  }
}
