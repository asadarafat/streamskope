import { describe, expect, it } from "vitest";

import {
  HOST_COMMANDS,
  HOST_EVENTS,
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  parseHostCommand,
  parseHostEvent,
} from "../../src/kafka/contracts";

const inventory = {
  connectionName: "Local Kafka",
  groups: [
    {
      groupType: "classic",
      id: "orders-worker",
      protocolType: "consumer",
      state: "stable",
    },
  ],
  omittedGroups: 0,
  refreshedAt: "2026-08-12T08:00:00.000Z",
  state: "ready",
} as const;

const detail = {
  connectionName: "Local Kafka",
  group: {
    id: "orders-worker",
    members: [
      {
        assignments: [{ partitions: [0, 1], topic: "orders.events" }],
        clientHost: "/127.0.0.1",
        clientId: "orders-client",
        groupInstanceId: null,
        id: "member-1",
      },
    ],
    offsets: [
      {
        committedOffset: "9007199254740993",
        endOffset: "9007199254741003",
        lag: "10",
        partition: 0,
        topic: "orders.events",
      },
    ],
    omittedAssignments: 0,
    omittedMembers: 0,
    omittedOffsets: 0,
    protocol: "range",
    protocolType: "consumer",
    state: "stable",
  },
  groupId: "orders-worker",
  refreshedAt: "2026-08-12T08:00:01.000Z",
  state: "ready",
} as const;

describe("Kafka consumer-group host contract", () => {
  it("declares versioned inventory and detail vocabulary", () => {
    expect(HOST_PROTOCOL_VERSION).toBe(17);
    expect(HOST_COMMANDS).toContain("consumerGroups.list");
    expect(HOST_COMMANDS).toContain("consumerGroups.load");
    expect(HOST_EVENTS).toContain("consumerGroups.changed");
    expect(HOST_EVENTS).toContain("consumerGroup.changed");
  });

  it("parses bounded inventory and selected-group commands", () => {
    expect(
      parseHostCommand({
        command: "consumerGroups.list",
        id: "groups-1",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toEqual({
      command: "consumerGroups.list",
      id: "groups-1",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });
    expect(
      parseHostCommand({
        command: "consumerGroups.load",
        id: "group-1",
        payload: { groupId: "orders-worker" },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toEqual({
      command: "consumerGroups.load",
      id: "group-1",
      payload: { groupId: "orders-worker" },
      version: HOST_PROTOCOL_VERSION,
    });
  });

  it("parses complete inventory and detail snapshots without losing offset precision", () => {
    expect(
      parseHostEvent({
        event: "consumerGroups.changed",
        payload: inventory,
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toEqual({
      event: "consumerGroups.changed",
      payload: inventory,
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });
    expect(
      parseHostEvent({
        event: "consumerGroup.changed",
        payload: detail,
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toEqual({
      event: "consumerGroup.changed",
      payload: detail,
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    });
  });

  it.each(["01", "-1", "1.5", "9e3", "9007199254740993 "])(
    "rejects non-canonical Kafka offset text %s",
    (committedOffset) => {
      expect(() =>
        parseHostEvent({
          event: "consumerGroup.changed",
          payload: {
            ...detail,
            group: {
              ...detail.group,
              offsets: [{ ...detail.group.offsets[0], committedOffset }],
            },
          },
          sequence: 2,
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toThrow(HostContractValidationError);
    },
  );

  it("rejects internally inconsistent ready and empty inventory states", () => {
    expect(() =>
      parseHostEvent({
        event: "consumerGroups.changed",
        payload: { ...inventory, groups: [], state: "ready" },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseHostEvent({
        event: "consumerGroups.changed",
        payload: { ...inventory, state: "empty" },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("rejects fabricated lag and undeclared detail fields", () => {
    expect(() =>
      parseHostEvent({
        event: "consumerGroup.changed",
        payload: {
          ...detail,
          group: {
            ...detail.group,
            offsets: [{ ...detail.group.offsets[0], lag: "11" }],
          },
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseHostEvent({
        event: "consumerGroup.changed",
        payload: { ...detail, secret: "must-not-cross-host" },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("accepts unavailable lag only when one authoritative offset is unavailable", () => {
    const unavailable = {
      ...detail,
      group: {
        ...detail.group,
        offsets: [
          {
            ...detail.group.offsets[0],
            committedOffset: null,
            lag: null,
          },
        ],
      },
    };
    expect(
      parseHostEvent({
        event: "consumerGroup.changed",
        payload: unavailable,
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toEqual({
      event: "consumerGroup.changed",
      payload: unavailable,
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    });
  });
});
