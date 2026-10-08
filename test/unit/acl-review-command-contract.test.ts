import { expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  isHostAcknowledgementCommand,
  parseCorrelatedHostResponse,
  parseHostCommand,
  parseHostCommandResponse,
} from "../../src/features/kafka/contracts";

// Independent wire examples: these must not be generated from the registration map.
const access = { topic: "orders.events", principal: "User:reader", host: "127.0.0.1" };
const input = {
  action: "create",
  acl: {
    resourceType: "TOPIC",
    resourceName: "orders.events",
    patternType: "LITERAL",
    principal: "User:reader",
    host: "*",
    operation: "READ",
    permission: "ALLOW",
  },
  access,
};
const explanation = {
  input: access,
  effective: "denied",
  aclDecision: "broker-default",
  resourceBindings: 0,
  matching: [],
  omittedBindings: 0,
  policy: { brokers: 1, standardAuthorizer: true, allowIfNoAcl: false, superuser: "no" },
  reasons: ["The observed broker policy denies access without a matching binding."],
};
const examples = [
  {
    command: "acls.access.explain",
    payload: access,
    result: { explanation },
    wrongResult: {
      outcome: {
        state: "acknowledged",
        verification: "verified",
        detail: "Applied.",
        receipt: null,
      },
    },
  },
  {
    command: "acls.change.review",
    payload: input,
    result: {
      review: {
        planId: "reviewed-plan",
        connectionName: "Fixture",
        expiresAt: "2026-10-08T12:02:00.000Z",
        input,
        beforePresent: false,
        afterPresent: true,
        beforeAccess: explanation,
        afterAccess: { ...explanation, effective: "allowed", aclDecision: "allowed" },
      },
    },
    wrongResult: { explanation },
  },
  {
    command: "acls.change.apply",
    payload: { planId: "reviewed-plan", confirmation: "create the reviewed binding" },
    result: {
      outcome: {
        state: "acknowledged",
        verification: "verified",
        detail: "Applied.",
        receipt: null,
      },
    },
    wrongResult: { explanation },
  },
] as const;

it.each(examples)(
  "preserves exact $command wire payload/results and rejects acknowledgement or other command data",
  ({ command, payload, result, wrongResult }) => {
    const request = { command, id: "renderer-request", version: HOST_PROTOCOL_VERSION, payload };
    const parsed = parseHostCommand(request);
    expect(parsed).toEqual(request);
    expect(isHostAcknowledgementCommand(command)).toBe(false);
    const response = {
      command,
      id: request.id,
      version: HOST_PROTOCOL_VERSION,
      ok: true,
      result: { correlationId: "h".repeat(128), ...result },
    };
    expect(parseCorrelatedHostResponse(response, parsed)).toEqual(response);
    for (const invalidPayload of [{ ...payload, unexpected: true }, {}, null]) {
      expect(() => parseHostCommand({ ...request, payload: invalidPayload })).toThrow(
        HostContractValidationError,
      );
    }
    for (const invalidResult of [
      { correlationId: "host-correlation" },
      { correlationId: "host-correlation", ...wrongResult },
      { ...response.result, unexpected: true },
      { ...response.result, correlationId: "h".repeat(129) },
    ]) {
      expect(() => parseHostCommandResponse({ ...response, result: invalidResult })).toThrow(
        HostContractValidationError,
      );
    }
    expect(() =>
      parseCorrelatedHostResponse({ ...response, id: "different-request" }, parsed),
    ).toThrow(/identifier and name/u);
  },
);

it("retains apply payload limits at their exact wire boundary", () => {
  const request = {
    command: "acls.change.apply",
    id: "bounded-apply",
    version: HOST_PROTOCOL_VERSION,
    payload: { planId: "p".repeat(128), confirmation: "c".repeat(8192) },
  };
  expect(parseHostCommand(request)).toEqual(request);
  for (const payload of [
    { ...request.payload, planId: "p".repeat(129) },
    { ...request.payload, confirmation: "c".repeat(8193) },
    { ...request.payload, planId: "" },
    { ...request.payload, confirmation: "" },
  ])
    expect(() => parseHostCommand({ ...request, payload })).toThrow(HostContractValidationError);
});

it("rejects a structurally valid response from another ACL command sharing the request ID", () => {
  const request = parseHostCommand({
    command: "acls.change.apply",
    id: "shared-request",
    version: HOST_PROTOCOL_VERSION,
    payload: { planId: "p", confirmation: "c" },
  });
  const wrongResponse = {
    command: "acls.access.explain",
    id: request.id,
    version: HOST_PROTOCOL_VERSION,
    ok: true,
    result: { correlationId: "host-correlation", explanation },
  };
  expect(() => parseHostCommandResponse(wrongResponse)).not.toThrow();
  expect(() => parseCorrelatedHostResponse(wrongResponse, request)).toThrow(/identifier and name/u);
});
