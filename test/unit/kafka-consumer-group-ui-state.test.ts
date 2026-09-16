import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostError,
  type HostEvent,
  type KafkaConsumerGroupDetailSnapshot,
  type KafkaConsumerGroupInventorySnapshot,
} from "../../src/features/kafka/contracts";
import { initialKafkaUiState, reduceKafkaHostEvent } from "../../src/features/kafka/ui/state";

const timeout: HostError = {
  activeStateChanged: false,
  code: "TIMEOUT",
  correlationId: "consumer-group-timeout",
  recovery: "Retry the consumer-group request.",
  retryable: true,
  stage: "broker",
  summary: "Consumer-group metadata timed out.",
};

const inventory: KafkaConsumerGroupInventorySnapshot = {
  connectionName: "local-aio",
  groups: [
    {
      groupType: "consumer",
      id: "orders-workers",
      protocolType: "consumer",
      state: "stable",
    },
  ],
  omittedGroups: 0,
  refreshedAt: "2026-08-12T09:00:00.000Z",
  state: "ready",
};

const detail: KafkaConsumerGroupDetailSnapshot = {
  connectionName: "local-aio",
  group: {
    id: "orders-workers",
    members: [
      {
        assignments: [{ partitions: [0], topic: "orders.events" }],
        clientHost: "/127.0.0.1",
        clientId: "orders-worker-1",
        groupInstanceId: null,
        id: "member-1",
      },
    ],
    offsets: [
      {
        committedOffset: "9007199254740993",
        endOffset: "9007199254741000",
        lag: "7",
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
  groupId: "orders-workers",
  refreshedAt: "2026-08-12T09:00:01.000Z",
  state: "ready",
};

function inventoryEvent(
  payload: KafkaConsumerGroupInventorySnapshot,
  sequence: number,
): Extract<HostEvent, { readonly event: "consumerGroups.changed" }> {
  return {
    event: "consumerGroups.changed",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

function detailEvent(
  payload: KafkaConsumerGroupDetailSnapshot,
  sequence: number,
): Extract<HostEvent, { readonly event: "consumerGroup.changed" }> {
  return {
    event: "consumerGroup.changed",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

describe("Kafka consumer-group UI state", () => {
  it("starts unavailable and replaces inventory and detail independently", () => {
    expect(initialKafkaUiState.consumerGroupInventory).toEqual({
      connectionName: null,
      groups: [],
      omittedGroups: 0,
      refreshedAt: null,
      state: "unavailable",
    });
    expect(initialKafkaUiState.consumerGroupDetail).toEqual({
      connectionName: null,
      group: null,
      groupId: null,
      refreshedAt: null,
      state: "unavailable",
    });

    const listed = reduceKafkaHostEvent(initialKafkaUiState, inventoryEvent(inventory, 1));
    const selected = reduceKafkaHostEvent(listed, detailEvent(detail, 2));

    expect(selected.consumerGroupInventory).toEqual(inventory);
    expect(selected.consumerGroupDetail).toEqual(detail);
    expect(selected.consumerGroupDetail.group?.offsets[0]?.committedOffset).toBe(
      "9007199254740993",
    );
  });

  it("retains explicit stale evidence and rejects an obsolete terminal event", () => {
    const ready = reduceKafkaHostEvent(
      reduceKafkaHostEvent(initialKafkaUiState, inventoryEvent(inventory, 3)),
      detailEvent(detail, 4),
    );
    const stale = reduceKafkaHostEvent(
      ready,
      detailEvent({ ...detail, error: timeout, state: "stale" }, 5),
    );
    const obsolete = reduceKafkaHostEvent(stale, detailEvent({ ...detail, state: "ready" }, 4));

    expect(stale.consumerGroupInventory).toEqual(inventory);
    expect(stale.consumerGroupDetail).toMatchObject({
      error: { code: "TIMEOUT" },
      groupId: "orders-workers",
      state: "stale",
    });
    expect(obsolete).toBe(stale);
  });

  it("clears both snapshots when the owning connection changes", () => {
    const ready = reduceKafkaHostEvent(
      reduceKafkaHostEvent(initialKafkaUiState, inventoryEvent(inventory, 1)),
      detailEvent(detail, 2),
    );
    const disconnected = reduceKafkaHostEvent(ready, {
      event: "connection.state",
      payload: { connectionName: null, state: "disconnected" },
      sequence: 3,
      version: HOST_PROTOCOL_VERSION,
    });

    expect(disconnected.consumerGroupInventory).toEqual(initialKafkaUiState.consumerGroupInventory);
    expect(disconnected.consumerGroupDetail).toEqual(initialKafkaUiState.consumerGroupDetail);
  });
});
