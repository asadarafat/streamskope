import { describe, expect, it } from "vitest";

import {
  HOST_ACTIVITY_HISTORY_LIMIT,
  HOST_PROTOCOL_VERSION,
  KAFKA_MESSAGE_LIMITS,
  kafkaMessageRetainedBytes,
  type KafkaExploredMessage,
  type HostEvent,
  type KafkaFetchRequest,
  type KafkaLiveRuleEvaluation,
} from "../../src/features/kafka/contracts";
import { initialKafkaUiState, reduceKafkaHostEvent } from "../../src/features/kafka/ui";

const evaluated: KafkaLiveRuleEvaluation = {
  activeMatchCount: 0,
  activeMatches: [],
  durationMicros: 0,
  errorCount: 0,
  errors: [],
  evaluatedRules: 0,
  omittedEvidence: 0,
  omittedRules: 0,
  state: "evaluated",
  suppressedMatchCount: 0,
  suppressedMatches: [],
};

const readyCapability = {
  applicableRules: 0,
  omittedRules: 0,
  state: "ready",
} as const;

it("retains additive recipe state without changing the legacy profile or template workflow", () => {
  const payload = { recipes: [], store: { durability: "session", state: "ready" } } as const;
  const next = reduceKafkaHostEvent(initialKafkaUiState, {
    event: "recipes.changed",
    payload,
    sequence: 1,
    version: HOST_PROTOCOL_VERSION,
  });
  expect(next.recipeSnapshot).toEqual(payload);
  expect(next.templateSnapshot).toBe(initialKafkaUiState.templateSnapshot);
  expect(next.connectionState).toBe(initialKafkaUiState.connectionState);
  expect(next.ruleState.snapshot).toBe(initialKafkaUiState.ruleState.snapshot);
});

function message(
  id: string,
  payload = "value",
  topic = "test",
  overrides: Partial<KafkaExploredMessage> = {},
): KafkaExploredMessage {
  return {
    headers: {},
    id,
    key: "key",
    offset: id,
    originalByteSize: Buffer.byteLength(payload) + 3,
    partition: 0,
    payload,
    preview: payload.slice(0, KAFKA_MESSAGE_LIMITS.previewBytes),
    ruleEvaluation: evaluated,
    timestamp: "2026-07-25T15:00:00.000Z",
    topic,
    truncated: false,
    ...overrides,
  };
}

function tailRequest(topic = "test", maxMessages = 1_000): KafkaFetchRequest {
  return {
    maxMessages,
    mode: "tail",
    topic,
  };
}

describe("Kafka renderer state", () => {
  it("retains only the canonical newest activity entries", () => {
    let state = initialKafkaUiState;
    for (let index = 0; index <= HOST_ACTIVITY_HISTORY_LIMIT; index += 1) {
      state = reduceKafkaHostEvent(state, {
        event: "activity.recorded",
        payload: {
          correlationId: `correlation-${index}`,
          detail: `Detail ${index}`,
          id: `activity-${index}`,
          object: "Local aio",
          operation: "Connection test",
          outcome: "succeeded",
          severity: "info",
          timestamp: "2026-07-25T12:00:00.000Z",
        },
        sequence: index,
        version: HOST_PROTOCOL_VERSION,
      });
    }

    expect(state.activities).toHaveLength(HOST_ACTIVITY_HISTORY_LIMIT);
    expect(state.activities[0]?.id).toBe("activity-1");
    expect(state.activities.at(-1)?.id).toBe(`activity-${HOST_ACTIVITY_HISTORY_LIMIT}`);
  });

  it("retains topic failure context and clears stale topic data when the connection changes", () => {
    const denied = {
      activeStateChanged: false,
      code: "AUTHORIZATION_DENIED" as const,
      correlationId: "correlation-topics",
      recovery: "Request topic metadata permission.",
      retryable: false,
      stage: "authorization" as const,
      summary: "Kafka denied topic metadata access.",
    };
    let state = reduceKafkaHostEvent(initialKafkaUiState, {
      event: "topics.changed",
      payload: {
        error: denied,
        refreshedAt: null,
        state: "denied",
        topics: [],
      },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });

    expect(state.topicError).toEqual(denied);

    state = reduceKafkaHostEvent(state, {
      event: "connection.state",
      payload: {
        connectionName: "Replacement cluster",
        state: "connecting",
      },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    });
    expect(state).toMatchObject({
      consumptionRequest: null,
      messages: [],
      refreshedAt: null,
      topicError: null,
      topicListState: "unavailable",
      topics: [],
    });
  });

  it("retains only the newest canonical message count and reports host plus renderer drops", () => {
    let state = reduceKafkaHostEvent(initialKafkaUiState, {
      event: "consumption.state",
      payload: {
        droppedMessages: 2,
        receivedMessages: 0,
        request: tailRequest(),
        ruleEvaluation: readyCapability,
        state: "streaming",
      },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });
    let nextId = 0;
    for (let batch = 0; batch < 6; batch += 1) {
      const batchSize = batch === 5 ? 1 : KAFKA_MESSAGE_LIMITS.batchMessages;
      const messages = Array.from({ length: batchSize }, () => message(String(nextId++)));
      state = reduceKafkaHostEvent(state, {
        event: "messages.batch",
        payload: {
          droppedMessages: 2,
          messages,
          topic: "test",
        },
        sequence: batch + 2,
        version: HOST_PROTOCOL_VERSION,
      });
    }

    expect(state.messages).toHaveLength(KAFKA_MESSAGE_LIMITS.retainedMessages);
    expect(state.messages[0]?.id).toBe("1000");
    expect(state.messages.at(-1)?.id).toBe("1");
    expect(state.droppedMessages).toBe(2);
    expect(state.rendererDroppedMessages).toBe(0);
    expect(state.rendererWindowEvictions).toBe(1);
  });

  it("merges interleaved batches with exact large offsets without mutating old snapshots", () => {
    const initial = { ...initialKafkaUiState, consumptionRequest: tailRequest() };
    const batch = (sequence: number, ids: readonly string[]): HostEvent => ({
      event: "messages.batch" as const,
      payload: {
        topic: "test",
        droppedMessages: 0,
        messages: ids.map((id) => message(id, "é漢😀")),
      },
      sequence,
      version: HOST_PROTOCOL_VERSION,
    });
    const first = reduceKafkaHostEvent(initial, batch(1, ["9007199254740994", "9007199254740992"]));
    const second = reduceKafkaHostEvent(first, batch(2, ["9007199254740993", "9007199254740995"]));
    expect(second.messages.map(({ offset }) => offset)).toEqual([
      "9007199254740995",
      "9007199254740994",
      "9007199254740993",
      "9007199254740992",
    ]);
    expect(first.messages.map(({ offset }) => offset)).toEqual([
      "9007199254740994",
      "9007199254740992",
    ]);
    expect(second.retainedMessageBytes).toBe(4 * kafkaMessageRetainedBytes(message("1", "é漢😀")));
    expect(initial.messages).toEqual([]);
  });

  it("evicts oldest rows before retained key and payload bytes exceed the canonical total", () => {
    const payload = "x".repeat(KAFKA_MESSAGE_LIMITS.messageBytes - 3);
    let state = initialKafkaUiState;
    for (let index = 0; index < 65; index += 1) {
      state = reduceKafkaHostEvent(state, {
        event: "messages.batch",
        payload: {
          droppedMessages: 0,
          messages: [message(String(index), payload)],
          topic: "test",
        },
        sequence: index + 1,
        version: HOST_PROTOCOL_VERSION,
      });
    }

    const retainedCount = Math.floor(
      KAFKA_MESSAGE_LIMITS.retainedBytes / kafkaMessageRetainedBytes(message("sample", payload)),
    );
    expect(state.messages).toHaveLength(retainedCount);
    expect(state.messages[0]?.id).toBe(String(65 - retainedCount));
    expect(state.retainedMessageBytes).toBe(
      retainedCount * kafkaMessageRetainedBytes(message("sample", payload)),
    );
    expect(state.rendererDroppedMessages).toBe(65 - retainedCount);
    expect(state.droppedMessages).toBe(65 - retainedCount);
  });

  it("clears the prior result when consumption starts for a replacement topic", () => {
    let state = reduceKafkaHostEvent(initialKafkaUiState, {
      event: "messages.batch",
      payload: {
        droppedMessages: 3,
        messages: [message("1")],
        topic: "test",
      },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });

    state = reduceKafkaHostEvent(state, {
      event: "consumption.state",
      payload: {
        droppedMessages: 0,
        receivedMessages: 0,
        request: tailRequest("audit.events"),
        ruleEvaluation: readyCapability,
        state: "loading",
      },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    });

    expect(state).toMatchObject({
      consumptionRequest: tailRequest("audit.events"),
      droppedMessages: 0,
      messages: [],
      rendererDroppedMessages: 0,
      retainedMessageBytes: 0,
    });
  });

  it("keeps a Tail request as a newest-first rolling selection without claiming loss", () => {
    let state = reduceKafkaHostEvent(initialKafkaUiState, {
      event: "consumption.state",
      payload: {
        droppedMessages: 0,
        receivedMessages: 0,
        request: tailRequest("test", 2),
        ruleEvaluation: readyCapability,
        state: "streaming",
      },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });

    state = reduceKafkaHostEvent(state, {
      event: "messages.batch",
      payload: {
        droppedMessages: 0,
        messages: [
          message("1", "one", "test", { timestamp: "2026-07-25T15:00:00.000Z" }),
          message("3", "three", "test", { timestamp: "2026-07-25T15:00:02.000Z" }),
          message("2", "two", "test", { timestamp: "2026-07-25T15:00:01.000Z" }),
        ],
        topic: "test",
      },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    });

    expect(state.messages.map(({ id }) => id)).toEqual(["3", "2"]);
    expect(state.rendererDroppedMessages).toBe(0);
    expect(state.droppedMessages).toBe(0);
  });

  it("orders finite snapshots deterministically for recent and chronological modes", () => {
    const unordered = [
      message("2", "two", "test", {
        partition: 1,
        timestamp: "2026-07-25T15:00:01.000Z",
      }),
      message("1", "one", "test", {
        partition: 0,
        timestamp: "2026-07-25T15:00:00.000Z",
      }),
      message("3", "three", "test", {
        partition: 0,
        timestamp: "2026-07-25T15:00:02.000Z",
      }),
    ];
    const newestRequest: KafkaFetchRequest = {
      maxMessages: 3,
      mode: "newest",
      topic: "test",
    };
    let newest = reduceKafkaHostEvent(initialKafkaUiState, {
      event: "consumption.state",
      payload: {
        droppedMessages: 0,
        receivedMessages: 3,
        request: newestRequest,
        ruleEvaluation: readyCapability,
        state: "complete",
      },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });
    newest = reduceKafkaHostEvent(newest, {
      event: "messages.batch",
      payload: { droppedMessages: 0, messages: unordered, topic: "test" },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    });
    expect(newest.messages.map(({ id }) => id)).toEqual(["3", "2", "1"]);
    expect(newest.receivedMessages).toBe(3);

    const earliestRequest: KafkaFetchRequest = {
      maxMessages: 3,
      mode: "earliest",
      topic: "test",
    };
    let earliest = reduceKafkaHostEvent(initialKafkaUiState, {
      event: "consumption.state",
      payload: {
        droppedMessages: 0,
        receivedMessages: 3,
        request: earliestRequest,
        ruleEvaluation: readyCapability,
        state: "complete",
      },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });
    earliest = reduceKafkaHostEvent(earliest, {
      event: "messages.batch",
      payload: { droppedMessages: 0, messages: unordered, topic: "test" },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    });
    expect(earliest.messages.map(({ id }) => id)).toEqual(["1", "2", "3"]);
  });

  it("preserves consumed rows as explicitly stale when the active connection ends", () => {
    let state = reduceKafkaHostEvent(initialKafkaUiState, {
      event: "messages.batch",
      payload: {
        droppedMessages: 0,
        messages: [message("1")],
        topic: "test",
      },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });

    state = reduceKafkaHostEvent(state, {
      event: "connection.state",
      payload: {
        connectionName: "Local aio",
        state: "disconnecting",
      },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    });

    expect(state.messages).toEqual([message("1")]);
    expect(state.messagesStale).toBe(true);
    expect(state.consumptionState).toBe("unavailable");
  });

  it("stores only safe profile summaries and explicit profile-store capability", () => {
    const state = reduceKafkaHostEvent(initialKafkaUiState, {
      event: "profiles.changed",
      payload: {
        profiles: [
          {
            active: true,
            brokers: ["127.0.0.1:19093"],
            createdAt: "2026-07-25T18:00:00.000Z",
            id: "profile-1",
            name: "Local validation",
            oauth: {
              clientId: "admin",
              clientSecretPresent: true,
              scope: "kafka",
              tokenEndpoint: "http://127.0.0.1:15000/token",
            },
            trust: {
              kind: "pem",
              label: "ca.pem",
              materialPresent: true,
              passwordPresent: false,
            },
            updatedAt: "2026-07-25T18:00:00.000Z",
          },
        ],
        store: {
          durability: "session",
          protection: "memory",
          state: "ready",
        },
      },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });

    expect(state).toMatchObject({
      profileStore: {
        durability: "session",
        protection: "memory",
        state: "ready",
      },
      profiles: [{ active: true, id: "profile-1", name: "Local validation" }],
    });
    expect(JSON.stringify(state.profiles)).not.toMatch(
      /clientSecret"|material"|password"|ciphertext|storagePath/,
    );
  });

  it("stores only the host-confirmed connection-template snapshot", () => {
    const payload = {
      catalogs: [
        {
          catalog: "truststore-fetch" as const,
          entries: [
            {
              name: "nsp-25-4",
              template: "copy source {truststorePath}",
            },
          ],
          selectedName: "nsp-25-4",
        },
        {
          catalog: "truststore-password" as const,
          entries: [{ name: "nsp-25-11", template: "read password" }],
          selectedName: "nsp-25-11",
        },
        {
          catalog: "oauth-endpoint" as const,
          entries: [{ name: "nsp-25-4", template: "https://{host}/token" }],
          selectedName: "nsp-25-4",
        },
      ],
      store: {
        durability: "session" as const,
        state: "ready" as const,
      },
    };

    const state = reduceKafkaHostEvent(initialKafkaUiState, {
      event: "templates.changed",
      payload,
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });

    expect(state.templateSnapshot).toEqual(payload);
    expect(initialKafkaUiState.templateSnapshot).toBeNull();
  });

  it("stores host-confirmed topic configuration and clears it on connection change", () => {
    let state = reduceKafkaHostEvent(initialKafkaUiState, {
      event: "topicConfiguration.changed",
      payload: {
        connectionName: "Local aio",
        entries: [
          {
            documentation: null,
            isDefault: false,
            isSensitive: false,
            name: "retention.ms",
            readOnly: false,
            source: "topic",
            synonyms: [],
            type: "long",
            value: "86400000",
          },
        ],
        refreshedAt: "2026-07-25T12:00:00.000Z",
        state: "ready",
        topic: "orders.events",
      },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });
    state = reduceKafkaHostEvent(state, {
      event: "topicConfiguration.history",
      payload: {
        connectionName: "Local aio",
        entries: [],
        store: { durability: "session", state: "ready" },
        topic: "orders.events",
      },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    });

    expect(state.topicConfiguration).toMatchObject({
      entries: [{ name: "retention.ms", value: "86400000" }],
      state: "ready",
      topic: "orders.events",
    });
    expect(state.topicConfigurationHistory).toMatchObject({
      connectionName: "Local aio",
      store: { durability: "session", state: "ready" },
      topic: "orders.events",
    });

    state = reduceKafkaHostEvent(state, {
      event: "connection.state",
      payload: {
        connectionName: "Replacement",
        state: "connecting",
      },
      sequence: 3,
      version: HOST_PROTOCOL_VERSION,
    });
    expect(state.topicConfiguration).toEqual({
      connectionName: null,
      entries: [],
      refreshedAt: null,
      state: "unavailable",
      topic: null,
    });
    expect(state.topicConfigurationHistory).toBeNull();
  });
});
