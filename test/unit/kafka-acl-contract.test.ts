import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  kafkaAclIdentity,
  parseHostCommand,
  parseHostEvent,
} from "../../src/kafka/contracts";

const acl = {
  host: "*",
  operation: "READ",
  patternType: "LITERAL",
  permission: "ALLOW",
  principal: "User:orders-api",
  resourceName: "orders.events",
  resourceType: "TOPIC",
} as const;

describe("Kafka ACL host contract", () => {
  it("parses bounded list, create, and exact-delete commands", () => {
    expect(
      parseHostCommand({
        command: "acls.list",
        id: "acl-list",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({ command: "acls.list" });
    expect(
      parseHostCommand({
        command: "acls.create",
        id: "acl-create",
        payload: acl,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({ command: "acls.create", payload: acl });
    expect(
      parseHostCommand({
        command: "acls.delete",
        id: "acl-delete",
        payload: { acl, confirmation: kafkaAclIdentity(acl) },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({ command: "acls.delete", payload: { acl } });
  });

  it("rejects broad and mismatched ACL deletion", () => {
    expect(() =>
      parseHostCommand({
        command: "acls.delete",
        id: "acl-delete",
        payload: { acl, confirmation: "*" },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("parses a bounded ACL inventory event", () => {
    expect(
      parseHostEvent({
        event: "acls.changed",
        payload: {
          acls: [acl],
          connectionName: "Local Kafka",
          omittedAcls: 0,
          refreshedAt: "2026-08-12T12:00:00.000Z",
          state: "ready",
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({ event: "acls.changed", payload: { acls: [acl] } });
  });
});
