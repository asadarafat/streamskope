import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  parseHostCommand,
  parseHostEvent,
} from "../../src/features/kafka/contracts";

const schema = {
  references: [
    {
      name: "com.example.Customer",
      subject: "customer-value",
      version: 2,
    },
  ],
  schema: '{"type":"record","name":"Order","fields":[]}',
  schemaType: "AVRO",
} as const;

describe("Schema Registry host contract", () => {
  it("parses bounded inventory, detail, compatibility, registration, and exact deletion commands", () => {
    const commands = [
      { command: "schemas.list", payload: {} },
      { command: "schemas.load", payload: { subject: "orders-value", version: 3 } },
      {
        command: "schemas.compatibility.check",
        payload: { ...schema, subject: "orders-value", version: 2 },
      },
      {
        command: "schemas.register",
        payload: { ...schema, normalize: true, subject: "orders-value", version: 2 },
      },
      {
        command: "schemas.delete",
        payload: {
          confirmation: "orders-value@3",
          mode: "soft",
          target: { kind: "version", subject: "orders-value", version: 3 },
        },
      },
      {
        command: "schemas.delete",
        payload: {
          confirmation: "orders-value",
          mode: "permanent",
          target: { kind: "subject", subject: "orders-value" },
        },
      },
    ] as const;

    for (const [index, command] of commands.entries()) {
      expect(
        parseHostCommand({
          ...command,
          id: `schema-command-${String(index)}`,
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toMatchObject(command);
    }
  });

  it.each([
    {
      label: "unknown schema type",
      payload: { ...schema, schemaType: "YAML", subject: "orders-value", version: 2 },
    },
    {
      label: "mismatched deletion confirmation",
      payload: {
        confirmation: "orders-value",
        mode: "soft",
        target: { kind: "version", subject: "orders-value", version: 3 },
      },
    },
    {
      label: "broad deletion target",
      payload: {
        confirmation: "*",
        mode: "permanent",
        target: { kind: "all" },
      },
    },
  ])("rejects $label", ({ payload }) => {
    expect(() =>
      parseHostCommand({
        command: "schemaType" in payload ? "schemas.compatibility.check" : "schemas.delete",
        id: "invalid-schema-command",
        payload,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("parses validated snapshots without accepting unowned response fields", () => {
    expect(
      parseHostEvent({
        event: "schemas.changed",
        payload: {
          connectionName: "Production",
          endpoint: "https://schema.example.test:8081",
          omittedSubjects: 0,
          refreshedAt: "2026-08-12T12:00:00.000Z",
          state: "ready",
          subjects: ["orders-key", "orders-value"],
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      event: "schemas.changed",
      payload: { state: "ready", subjects: ["orders-key", "orders-value"] },
    });

    expect(
      parseHostEvent({
        event: "schema.changed",
        payload: {
          compatibilityLevel: "BACKWARD",
          connectionName: "Production",
          endpoint: "https://schema.example.test:8081",
          refreshedAt: "2026-08-12T12:00:01.000Z",
          schema: {
            id: 42,
            references: schema.references,
            schema: schema.schema,
            schemaType: schema.schemaType,
            subject: "orders-value",
            version: 3,
          },
          state: "ready",
          subject: "orders-value",
          versions: [1, 2, 3],
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({ event: "schema.changed", payload: { state: "ready", versions: [1, 2, 3] } });
  });
});
