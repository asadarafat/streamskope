import { HOST_PROTOCOL_VERSION, type HostCommandResponse } from "../contracts";
import {
  parseAclReviewResult,
  type AclReviewCommand,
  type AclReviewCommandName,
  type AclReviewPayloads,
  type AclReviewResults,
  type AclReviewSuccess,
} from "../contracts/acl-review-commands";
import { AclReviewService } from "../application/acl-review-service";
import type { KafkaApplicationSession } from "../application";

import { failureResponse, type ActivityInput } from "./facade-support";

export type AclReviewHandlers = {
  readonly [Name in AclReviewCommandName]: (
    payload: AclReviewPayloads[Name],
    correlationId: string,
  ) => Promise<Omit<AclReviewResults[Name], "correlationId">>;
};

/** Keep command/payload/result correlation and validate the host-owned response envelope. */
export async function dispatchAclReviewCommand<Name extends AclReviewCommandName>(
  handlers: AclReviewHandlers,
  command: AclReviewCommand<Name>,
  correlationId: string,
): Promise<AclReviewSuccess<Name>> {
  const result = parseAclReviewResult(command.command, {
    ...(await handlers[command.command](command.payload, correlationId)),
    correlationId,
  });
  return {
    command: command.command,
    id: command.id,
    version: HOST_PROTOCOL_VERSION,
    ok: true,
    result,
  };
}

export class AclReviewFacade {
  private readonly handlers: AclReviewHandlers;

  constructor(session: KafkaApplicationSession, activity: (input: ActivityInput) => void) {
    const service = new AclReviewService(() => session.aclReviewScope());
    this.handlers = {
      "acls.access.explain": async (
        payload,
      ): ReturnType<AclReviewHandlers["acls.access.explain"]> => ({
        explanation: await service.explain(payload),
      }),
      "acls.change.review": async (
        payload,
      ): ReturnType<AclReviewHandlers["acls.change.review"]> => ({
        review: await service.review(payload),
      }),
      "acls.change.apply": async (
        payload,
        correlationId,
      ): ReturnType<AclReviewHandlers["acls.change.apply"]> => {
        const outcome = await service.apply(payload.planId, payload.confirmation);
        activity({
          correlationId,
          operation: "Apply reviewed ACL change",
          object: payload.confirmation,
          detail: outcome.detail,
          outcome: outcome.state === "acknowledged" ? "succeeded" : "failed",
          severity:
            outcome.state === "acknowledged" && outcome.verification === "verified"
              ? "info"
              : "warning",
        });
        return { outcome };
      },
    };
  }

  async execute(command: AclReviewCommand, correlationId: string): Promise<HostCommandResponse> {
    try {
      return await dispatchAclReviewCommand(this.handlers, command, correlationId);
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
