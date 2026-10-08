import {
  ACL_REVIEW_DESCRIPTORS,
  type AclReviewCommandDescriptors,
  type AclReviewPayloads,
} from "../../src/features/kafka/contracts/acl-review-commands";
import type { AclReviewHandlers } from "../../src/features/kafka/facade/acl-review-facade";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommandResponse,
  type StreamSkopeBackend,
} from "../../src/features/kafka/contracts";

// Compiled by the normal test TypeScript project; never executed.
declare const handlers: AclReviewHandlers;
const explain = ACL_REVIEW_DESCRIPTORS["acls.access.explain"];

// @ts-expect-error Every descriptor needs an explicit access decision.
export const missingAccess: AclReviewCommandDescriptors["acls.access.explain"] = {
  parsePayload: explain.parsePayload,
  parseResult: explain.parseResult,
};

// @ts-expect-error Every descriptor needs a result parser.
export const missingResultParser: AclReviewCommandDescriptors["acls.access.explain"] = {
  access: "remote-read",
  parsePayload: explain.parsePayload,
};

// @ts-expect-error Registering only the two read handlers cannot leave apply unbound.
export const missingApplyHandler: AclReviewHandlers = {
  "acls.access.explain": handlers["acls.access.explain"],
  "acls.change.review": handlers["acls.change.review"],
};

export const wrongHandlerPayload: AclReviewHandlers = {
  ...handlers,
  // @ts-expect-error Explanation receives topic/principal/host, never an apply plan.
  "acls.access.explain": (payload: AclReviewPayloads["acls.change.apply"], correlationId) =>
    handlers["acls.access.explain"](
      { topic: payload.planId, principal: "User:reader", host: "127.0.0.1" },
      correlationId,
    ),
};

export const wrongHandlerResult: AclReviewHandlers = {
  ...handlers,
  // @ts-expect-error A review handler must return review data, not an acknowledgement.
  "acls.change.review": () => Promise.resolve({ correlationId: "not-a-review" }),
};

export const missingApplyOutcome: HostCommandResponse<"acls.change.apply"> = {
  command: "acls.change.apply",
  id: "apply",
  version: HOST_PROTOCOL_VERSION,
  ok: true,
  // @ts-expect-error Applying an ACL requires its structured outcome.
  result: { correlationId: "host-correlation" },
};

export async function assertAclResponseInference(backend: StreamSkopeBackend): Promise<void> {
  const response = await backend.execute({
    command: "acls.access.explain",
    id: "explain",
    version: HOST_PROTOCOL_VERSION,
    payload: { topic: "orders", principal: "User:reader", host: "127.0.0.1" },
  });
  if (response.ok) {
    const decision: "allowed" | "denied" | "unknown" = response.result.explanation.effective;
    void decision;
    // @ts-expect-error Command inference does not expose another command's result.
    void response.result.outcome;
  }
}

export function assertAclDiscriminatedResponse(response: HostCommandResponse): void {
  if (response.ok && response.command === "acls.change.review") {
    const planId: string = response.result.review.planId;
    void planId;
    // @ts-expect-error A narrowed review response has no explanation result member.
    void response.result.explanation;
  }
}
