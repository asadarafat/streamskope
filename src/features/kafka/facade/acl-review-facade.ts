import { HOST_PROTOCOL_VERSION, type HostCommand, type HostCommandResponse } from "../contracts";
import { AclReviewService } from "../application/acl-review-service";
import type { KafkaApplicationSession } from "../application";

import { failureResponse, type ActivityInput } from "./facade-support";

export class AclReviewFacade {
  private readonly service: AclReviewService;
  constructor(
    session: KafkaApplicationSession,
    private readonly activity: (input: ActivityInput) => void,
  ) {
    this.service = new AclReviewService(() => session.aclReviewScope());
  }
  async execute(
    command: Extract<
      HostCommand,
      { command: "acls.access.explain" | "acls.change.review" | "acls.change.apply" }
    >,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    try {
      if (command.command === "acls.access.explain")
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: { correlationId, explanation: await this.service.explain(command.payload) },
        };
      if (command.command === "acls.change.review")
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: { correlationId, review: await this.service.review(command.payload) },
        };
      const outcome = await this.service.apply(
        command.payload.planId,
        command.payload.confirmation,
      );
      this.activity({
        correlationId,
        operation: "Apply reviewed ACL change",
        object: command.payload.confirmation,
        detail: outcome.detail,
        outcome: outcome.state === "acknowledged" ? "succeeded" : "failed",
        severity:
          outcome.state === "acknowledged" && outcome.verification === "verified"
            ? "info"
            : "warning",
      });
      return {
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId, outcome },
      };
    } catch {
      return failureResponse(command, {
        code: "VALIDATION",
        stage: "authorization",
        correlationId,
        retryable: false,
        activeStateChanged: false,
        summary: "The ACL review could not be accepted.",
        recovery:
          "Check ACL describe permission, the concrete topic/principal/client IP and exact confirmation, then review again. Expired plans or a changed connection require a new review. Broker policy that cannot be read remains unknown.",
      });
    }
  }
}
