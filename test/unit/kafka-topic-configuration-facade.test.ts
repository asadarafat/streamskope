import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEvent,
  type KafkaFetchRequest,
  type KafkaTopicConfigurationEntry,
  type KafkaTopicConfigurationHistorySnapshot,
  type KafkaTopicConfigurationOperationInput,
  type SecureConnectionInput,
} from "../../src/features/kafka/contracts";
import {
  InMemoryKafkaConnectionTemplateStore,
  InMemoryKafkaProfileStore,
  InMemoryKafkaRuleStore,
  KafkaApplicationSession,
  KafkaConnectionTemplateService,
  KafkaLiveRuleRuntime,
  KafkaProfileService,
  KafkaRuleService,
  type KafkaActiveConnection,
  type KafkaConnectionPort,
  type KafkaConnectionTestResult,
  type KafkaMessageStream,
  type KafkaTopicConfigurationHistoryRead,
  type KafkaTopicConfigurationOperationResult,
  type KafkaTopicConfigurationServicePort,
  type KafkaTopicConfigurationView,
} from "../../src/features/kafka/application";
import { KafkaBackendFacade } from "../../src/features/kafka/facade";
import { KafkaEngineFailure, StreamSkopeKafkaRuleEvaluator } from "../../src/features/kafka/engine";

const connection: SecureConnectionInput = {
  brokers: ["localhost:19093"],
  name: "Local aio",
  tls: {
    caPem: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
    enabled: true,
  },
};

const entries: readonly KafkaTopicConfigurationEntry[] = [
  {
    documentation: "Retention in milliseconds.",
    isDefault: false,
    isSensitive: false,
    name: "retention.ms",
    readOnly: false,
    source: "topic",
    synonyms: [],
    type: "long",
    value: "86400000",
  },
];

const view: KafkaTopicConfigurationView = {
  connectionName: "Local aio",
  connectionTarget: "localhost:19093",
  entries,
  refreshedAt: "2026-07-25T12:00:00.000Z",
  topic: "orders.events",
};

const history: KafkaTopicConfigurationHistorySnapshot = {
  connectionName: "Local aio",
  entries: [
    {
      action: "apply",
      at: "2026-07-25T12:00:00.000Z",
      changes: [
        {
          from: "86400000",
          isSensitive: false,
          name: "retention.ms",
          to: "604800000",
          wasDefault: false,
        },
      ],
      connectionName: "Local aio",
      connectionTarget: "localhost:19093",
      id: "history-1",
      success: true,
      topic: "orders.events",
    },
  ],
  store: {
    durability: "session",
    state: "ready",
  },
  topic: "orders.events",
};

const operationInput: KafkaTopicConfigurationOperationInput = {
  changes: [
    {
      isSensitive: false,
      name: "retention.ms",
      value: "604800000",
    },
  ],
  topic: "orders.events",
};

class ActiveConnection implements KafkaActiveConnection {
  alterTopicConfiguration(): Promise<void> {
    return Promise.resolve();
  }

  close(): Promise<void> {
    return Promise.resolve();
  }

  describeBrokerConfiguration(): never {
    throw new Error("No broker-configuration operation was requested.");
  }

  describeClusterMetadata(): never {
    throw new Error("No cluster-metadata operation was requested.");
  }

  describeTopicConfiguration(): Promise<readonly KafkaTopicConfigurationEntry[]> {
    return Promise.resolve(entries);
  }

  listTopics(): Promise<readonly string[]> {
    return Promise.resolve(["orders.events"]);
  }

  openMessageStream(
    _request: KafkaFetchRequest,
    _signal: AbortSignal,
  ): Promise<KafkaMessageStream> {
    return Promise.reject(new Error("No message operation was requested."));
  }
}

class ConnectionPort implements KafkaConnectionPort {
  openConnection(): Promise<KafkaActiveConnection> {
    return Promise.resolve(new ActiveConnection());
  }

  testConnection(): Promise<KafkaConnectionTestResult> {
    return Promise.resolve({ checks: ["metadata"], topicCount: 1 });
  }
}

class RecordingTopicConfigurationService implements KafkaTopicConfigurationServicePort {
  applyResult: KafkaTopicConfigurationOperationResult | Error = {
    configuration: view,
    history,
  };
  historyResult: KafkaTopicConfigurationHistoryRead | Error = { snapshot: history };
  loadResult: KafkaTopicConfigurationView | Error = view;
  validateResult: KafkaTopicConfigurationOperationResult | Error = {
    configuration: view,
    history: {
      ...history,
      entries: [{ ...history.entries[0]!, action: "validate" }],
    },
  };

  apply(): Promise<KafkaTopicConfigurationOperationResult> {
    return result(this.applyResult);
  }

  history(): Promise<KafkaTopicConfigurationHistoryRead> {
    return result(this.historyResult);
  }

  load(): Promise<KafkaTopicConfigurationView> {
    return result(this.loadResult);
  }

  validate(): Promise<KafkaTopicConfigurationOperationResult> {
    return result(this.validateResult);
  }
}

function result<T>(value: T | Error): Promise<T> {
  return value instanceof Error ? Promise.reject(value) : Promise.resolve(value);
}

function topicCommand(
  name:
    | "topicConfiguration.apply"
    | "topicConfiguration.history"
    | "topicConfiguration.load"
    | "topicConfiguration.validate",
  id: string,
): HostCommand {
  return name === "topicConfiguration.load" || name === "topicConfiguration.history"
    ? {
        command: name,
        id,
        payload: { topic: "orders.events" },
        version: HOST_PROTOCOL_VERSION,
      }
    : {
        command: name,
        id,
        payload: operationInput,
        version: HOST_PROTOCOL_VERSION,
      };
}

async function fixture(): Promise<{
  readonly events: HostEvent[];
  readonly facade: KafkaBackendFacade;
  readonly service: RecordingTopicConfigurationService;
}> {
  const session = new KafkaApplicationSession(new ConnectionPort());
  await session.connect(connection);
  const evaluator = new StreamSkopeKafkaRuleEvaluator();
  const rules = new KafkaRuleService(
    new InMemoryKafkaRuleStore({ durability: "session", state: "ready" }),
    evaluator,
  );
  const service = new RecordingTopicConfigurationService();
  let correlation = 0;
  const facade = new KafkaBackendFacade(
    session,
    new KafkaProfileService(
      new InMemoryKafkaProfileStore({
        durability: "session",
        protection: "memory",
        state: "ready",
      }),
      {
        decode: (): Promise<never> => Promise.reject(new Error("No profile decode was requested.")),
      },
    ),
    new KafkaConnectionTemplateService(
      new InMemoryKafkaConnectionTemplateStore({
        durability: "session",
        state: "ready",
      }),
    ),
    rules,
    new KafkaLiveRuleRuntime(rules, evaluator),
    service,
    {
      createCorrelationId: (): string => `correlation-${String(++correlation)}`,
      now: (): Date => new Date("2026-07-25T13:00:00.000Z"),
    },
  );
  const events: HostEvent[] = [];
  facade.subscribe((event) => {
    events.push(event);
  });
  return { events, facade, service };
}

describe("Kafka topic-configuration facade", () => {
  it("publishes honest loading and ready state for selected-topic discovery", async () => {
    const value = await fixture();

    await expect(
      value.facade.execute(topicCommand("topicConfiguration.load", "load-1")),
    ).resolves.toMatchObject({
      command: "topicConfiguration.load",
      id: "load-1",
      ok: true,
      result: { correlationId: "correlation-1" },
    });

    expect(value.events.slice(1).map((event) => event.event)).toEqual([
      "topicConfiguration.changed",
      "topicConfiguration.changed",
      "activity.recorded",
    ]);
    expect(value.events[1]).toMatchObject({
      event: "topicConfiguration.changed",
      payload: {
        connectionName: "Local aio",
        entries: [],
        refreshedAt: null,
        state: "loading",
        topic: "orders.events",
      },
    });
    expect(value.events[2]).toMatchObject({
      event: "topicConfiguration.changed",
      payload: {
        entries: [{ name: "retention.ms", value: "86400000" }],
        refreshedAt: "2026-07-25T12:00:00.000Z",
        state: "ready",
      },
    });
    expect(value.events[3]).toMatchObject({
      event: "activity.recorded",
      payload: {
        correlationId: "correlation-1",
        object: "Local aio · orders.events",
        operation: "Load topic configuration",
        outcome: "succeeded",
      },
    });
  });

  it("publishes validate history and keeps the broker view ready", async () => {
    const value = await fixture();

    await expect(
      value.facade.execute(topicCommand("topicConfiguration.validate", "validate-1")),
    ).resolves.toMatchObject({ ok: true });

    expect(value.events.slice(1).map((event) => event.event)).toEqual([
      "topicConfiguration.changed",
      "topicConfiguration.history",
      "activity.recorded",
    ]);
    expect(value.events[1]).toMatchObject({
      event: "topicConfiguration.changed",
      payload: { state: "ready" },
    });
    expect(value.events[2]).toMatchObject({
      event: "topicConfiguration.history",
      payload: { entries: [{ action: "validate", success: true }] },
    });
    expect(value.events[3]).toMatchObject({
      event: "activity.recorded",
      payload: {
        operation: "Dry-run topic configuration",
        outcome: "succeeded",
      },
    });
  });

  it("reports confirmed apply as successful but stale when its refresh fails", async () => {
    const value = await fixture();
    value.service.applyResult = {
      configuration: view,
      history: {
        ...history,
        entries: [
          {
            ...history.entries[0]!,
            warning: "Kafka applied the named changes, but refreshed configuration is unavailable.",
          },
        ],
      },
      refreshFailure: new KafkaEngineFailure({
        code: "BROKER_UNREACHABLE",
        recovery: "Reconnect and refresh the selected topic.",
        retryable: true,
        stage: "kafka",
        summary: "Kafka applied the change, but refresh failed.",
        target: "localhost:19093 / orders.events",
      }),
    };

    await expect(
      value.facade.execute(topicCommand("topicConfiguration.apply", "apply-1")),
    ).resolves.toMatchObject({ ok: true });

    expect(value.events[1]).toMatchObject({
      event: "topicConfiguration.changed",
      payload: {
        entries: [{ name: "retention.ms", value: "86400000" }],
        error: {
          code: "BROKER_UNREACHABLE",
          correlationId: "correlation-1",
        },
        refreshedAt: "2026-07-25T12:00:00.000Z",
        state: "stale",
      },
    });
    const historyEvent = value.events[2];
    expect(historyEvent?.event).toBe("topicConfiguration.history");
    if (historyEvent?.event !== "topicConfiguration.history") {
      throw new Error("Expected topic-configuration history evidence.");
    }
    expect(historyEvent.payload.entries[0]).toMatchObject({ success: true });
    expect(historyEvent.payload.entries[0]?.warning).toContain("refreshed");
    expect(value.events[3]).toMatchObject({
      event: "activity.recorded",
      payload: {
        operation: "Apply topic configuration",
        outcome: "succeeded",
        severity: "warning",
      },
    });
  });

  it.each([
    ["AUTHORIZATION_DENIED", "denied"],
    ["TOPIC_NOT_FOUND", "not-found"],
    ["INVALID_TOPIC_CONFIG", "failed"],
  ] as const)("maps %s failure to explicit %s state", async (code, state) => {
    const value = await fixture();
    value.service.loadResult = new KafkaEngineFailure({
      code,
      recovery: "Correct access or topic configuration and retry.",
      retryable: false,
      stage: code === "AUTHORIZATION_DENIED" ? "authorization" : "kafka",
      summary: `Safe ${code} failure.`,
      target: "orders.events",
    });

    await expect(
      value.facade.execute(topicCommand("topicConfiguration.load", "load-failure")),
    ).resolves.toMatchObject({
      error: {
        code,
        correlationId: "correlation-1",
      },
      ok: false,
    });

    expect(value.events.at(-2)).toMatchObject({
      event: "topicConfiguration.changed",
      payload: {
        entries: [],
        error: { code },
        refreshedAt: null,
        state,
      },
    });
    expect(value.events.at(-1)).toMatchObject({
      event: "activity.recorded",
      payload: {
        operation: "Load topic configuration",
        outcome: "failed",
      },
    });
  });

  it("publishes unavailable history and a warning without failing Kafka administration", async () => {
    const value = await fixture();
    value.service.historyResult = {
      failure: new Error("private-storage-path must not cross"),
      snapshot: {
        ...history,
        entries: [],
        store: {
          durability: "durable",
          recovery: "Preserve the history file and restore a known-good copy.",
          state: "unavailable",
        },
      },
    };

    const response = await value.facade.execute(
      topicCommand("topicConfiguration.history", "history-1"),
    );

    expect(response).toMatchObject({ ok: true });
    expect(value.events[1]).toMatchObject({
      event: "topicConfiguration.history",
      payload: {
        entries: [],
        store: { durability: "durable", state: "unavailable" },
      },
    });
    const activity = value.events[2];
    expect(activity?.event).toBe("activity.recorded");
    if (activity?.event !== "activity.recorded") {
      throw new Error("Expected history degradation Activity.");
    }
    expect(activity.payload).toMatchObject({
      operation: "Load topic configuration history",
      outcome: "succeeded",
      severity: "warning",
    });
    expect(activity.payload.detail).toContain("could not be recorded");
    expect(JSON.stringify({ response, events: value.events })).not.toContain(
      "private-storage-path",
    );
  });
});
