import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  parseHostCommand,
  parseHostEvent,
  summarizeRedpandaTransformStatuses,
} from "../../src/features/kafka/contracts";

describe("Redpanda transform host contract", () => {
  it("parses bounded inventory, detail, logs, and exact-delete commands", () => {
    for (const [index, command] of [
      { command: "transforms.list", payload: {} },
      { command: "transforms.load", payload: { name: "mask-orders" } },
      { command: "transforms.logs.load", payload: { name: "mask-orders" } },
      {
        command: "transforms.delete",
        payload: { confirmation: "mask-orders", name: "mask-orders" },
      },
    ].entries()) {
      expect(
        parseHostCommand({
          ...command,
          id: `transform-${String(index)}`,
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toMatchObject(command);
    }
  });

  it("rejects a mismatched transform deletion confirmation", () => {
    expect(() =>
      parseHostCommand({
        command: "transforms.delete",
        id: "transform-delete",
        payload: { confirmation: "*", name: "mask-orders" },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("parses transform metadata while exposing environment names but never values", () => {
    const transform = {
      aggregateStatus: "running",
      compression: "none",
      environment: [
        { name: "API_ENDPOINT", valuePresent: true },
        { name: "TOKEN", valuePresent: true },
      ],
      inputTopic: "orders.raw",
      name: "mask-orders",
      maximumLag: 0,
      offset: null,
      outputTopics: ["orders.masked"],
      statuses: [{ lag: 0, nodeId: 1, partition: 0, status: "running" }],
    };
    expect(
      parseHostEvent({
        event: "transforms.changed",
        payload: {
          connectionName: "Local Redpanda",
          endpoint: "http://clab.orb.local:19644",
          omittedTransforms: 0,
          refreshedAt: "2026-08-12T12:00:00.000Z",
          state: "ready",
          transforms: [transform],
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({ event: "transforms.changed", payload: { transforms: [transform] } });
  });

  it("derives aggregate status without hiding degraded or empty processor evidence", () => {
    expect(
      summarizeRedpandaTransformStatuses([
        { lag: 1, nodeId: 1, partition: 0, status: "running" },
        { lag: 9, nodeId: 2, partition: 1, status: "errored" },
      ]),
    ).toEqual({ aggregateStatus: "errored", maximumLag: 9 });
    expect(summarizeRedpandaTransformStatuses([])).toEqual({
      aggregateStatus: "unknown",
      maximumLag: 0,
    });
  });
});
