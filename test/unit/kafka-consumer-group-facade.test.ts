import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEvent,
  type KafkaConsumerGroupDetails,
} from "../../src/kafka/contracts";
import type { KafkaConsumerGroupInventory } from "../../src/kafka/application";
import { KafkaEngineFailure } from "../../src/kafka/engine";
import {
  RecordingActiveConnection,
  RecordingConnectionPort,
  command,
  createFacade,
} from "../support/kafka-backend-facade-fixture";

const inventory: KafkaConsumerGroupInventory = {
  groups: [
    {
      groupType: "classic",
      id: "orders-worker",
      protocolType: "consumer",
      state: "stable",
    },
  ],
  omittedGroups: 0,
};

const details: KafkaConsumerGroupDetails = {
  id: "orders-worker",
  members: [],
  offsets: [
    {
      committedOffset: "8",
      endOffset: "10",
      lag: "2",
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
};

function groupCommand(
  name: "consumerGroups.list" | "consumerGroups.load",
  id: string,
): HostCommand {
  return name === "consumerGroups.list"
    ? { command: name, id, payload: {}, version: HOST_PROTOCOL_VERSION }
    : {
        command: name,
        id,
        payload: { groupId: "orders-worker" },
        version: HOST_PROTOCOL_VERSION,
      };
}

function inventoryEvents(
  events: readonly HostEvent[],
): Array<Extract<HostEvent, { readonly event: "consumerGroups.changed" }>> {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "consumerGroups.changed" }> =>
      event.event === "consumerGroups.changed",
  );
}

function detailEvents(
  events: readonly HostEvent[],
): Array<Extract<HostEvent, { readonly event: "consumerGroup.changed" }>> {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "consumerGroup.changed" }> =>
      event.event === "consumerGroup.changed",
  );
}

function activityEvents(
  events: readonly HostEvent[],
): Array<Extract<HostEvent, { readonly event: "activity.recorded" }>> {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "activity.recorded" }> =>
      event.event === "activity.recorded",
  );
}

async function setup(): Promise<{
  readonly active: RecordingActiveConnection;
  readonly events: HostEvent[];
  readonly facade: ReturnType<typeof createFacade>;
}> {
  const active = new RecordingActiveConnection();
  const port = new RecordingConnectionPort();
  port.openOperations.push(() => Promise.resolve(active));
  const facade = createFacade(port);
  const events: HostEvent[] = [];
  facade.subscribe((event) => {
    events.push(event);
  });
  await facade.execute(command("connection.connect", "connect-1"));
  return { active, events, facade };
}

describe("Kafka consumer-group facade", () => {
  it("publishes loading and ready inventory with bounded Activity evidence", async () => {
    const value = await setup();
    value.active.consumerGroupListOperations.push(() => Promise.resolve(inventory));

    await expect(
      value.facade.execute(groupCommand("consumerGroups.list", "groups-1")),
    ).resolves.toMatchObject({ ok: true, result: { correlationId: "correlation-2" } });

    expect(
      inventoryEvents(value.events)
        .slice(-2)
        .map((event) => event.payload),
    ).toEqual([
      {
        connectionName: "Local aio",
        groups: [],
        omittedGroups: 0,
        refreshedAt: null,
        state: "loading",
      },
      {
        connectionName: "Local aio",
        groups: inventory.groups,
        omittedGroups: 0,
        refreshedAt: "2026-07-25T13:00:00.000Z",
        state: "ready",
      },
    ]);
    expect(activityEvents(value.events).at(-1)?.payload).toMatchObject({
      object: "Local aio",
      operation: "Refresh consumer groups",
      outcome: "succeeded",
      severity: "info",
    });
  });

  it("distinguishes an empty inventory from failure", async () => {
    const value = await setup();
    value.active.consumerGroupListOperations.push(() =>
      Promise.resolve({ groups: [], omittedGroups: 0 }),
    );

    await value.facade.execute(groupCommand("consumerGroups.list", "groups-empty"));

    expect(inventoryEvents(value.events).at(-1)?.payload).toMatchObject({
      groups: [],
      state: "empty",
    });
  });

  it("publishes authorization-denied inventory without representing it as empty", async () => {
    const value = await setup();
    value.active.consumerGroupListOperations.push(() =>
      Promise.reject(
        new KafkaEngineFailure({
          code: "AUTHORIZATION_DENIED",
          recovery: "Request DESCRIBE permission.",
          retryable: false,
          stage: "authorization",
          summary: "Kafka denied consumer-group access.",
          target: "Local aio / consumer groups",
        }),
      ),
    );

    await expect(
      value.facade.execute(groupCommand("consumerGroups.list", "groups-denied")),
    ).resolves.toMatchObject({ error: { code: "AUTHORIZATION_DENIED" }, ok: false });

    expect(inventoryEvents(value.events).at(-1)?.payload).toMatchObject({
      groups: [],
      state: "denied",
    });
    expect(activityEvents(value.events).at(-1)?.payload).toMatchObject({
      operation: "Refresh consumer groups",
      outcome: "failed",
      severity: "error",
    });
  });

  it("publishes selected-group loading and ready detail", async () => {
    const value = await setup();
    value.active.consumerGroupDetailOperations.push(() => Promise.resolve(details));

    await expect(
      value.facade.execute(groupCommand("consumerGroups.load", "group-1")),
    ).resolves.toMatchObject({ ok: true, result: { correlationId: "correlation-2" } });

    expect(
      detailEvents(value.events)
        .slice(-2)
        .map((event) => event.payload),
    ).toEqual([
      {
        connectionName: "Local aio",
        group: null,
        groupId: "orders-worker",
        refreshedAt: null,
        state: "loading",
      },
      {
        connectionName: "Local aio",
        group: details,
        groupId: "orders-worker",
        refreshedAt: "2026-07-25T13:00:00.000Z",
        state: "ready",
      },
    ]);
  });

  it("publishes precise not-found detail and recovery", async () => {
    const value = await setup();
    value.active.consumerGroupDetailOperations.push(() =>
      Promise.reject(
        new KafkaEngineFailure({
          code: "CONSUMER_GROUP_NOT_FOUND",
          recovery: "Refresh consumer groups.",
          retryable: false,
          stage: "kafka",
          summary: "The selected Kafka consumer group was not found.",
          target: "orders-worker",
        }),
      ),
    );

    await expect(
      value.facade.execute(groupCommand("consumerGroups.load", "group-missing")),
    ).resolves.toMatchObject({ error: { code: "CONSUMER_GROUP_NOT_FOUND" }, ok: false });

    expect(detailEvents(value.events).at(-1)?.payload).toMatchObject({
      group: null,
      groupId: "orders-worker",
      state: "not-found",
    });
  });

  it("invalidates inventory and detail when disconnect begins", async () => {
    const value = await setup();
    value.active.consumerGroupListOperations.push(() => Promise.resolve(inventory));
    value.active.consumerGroupDetailOperations.push(() => Promise.resolve(details));
    await value.facade.execute(groupCommand("consumerGroups.list", "groups-1"));
    await value.facade.execute(groupCommand("consumerGroups.load", "group-1"));

    await value.facade.execute(command("connection.disconnect", "disconnect-1"));

    expect(inventoryEvents(value.events).at(-1)?.payload).toEqual({
      connectionName: null,
      groups: [],
      omittedGroups: 0,
      refreshedAt: null,
      state: "unavailable",
    });
    expect(detailEvents(value.events).at(-1)?.payload).toEqual({
      connectionName: null,
      group: null,
      groupId: null,
      refreshedAt: null,
      state: "unavailable",
    });
  });
});
