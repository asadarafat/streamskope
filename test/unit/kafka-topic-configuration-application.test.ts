import { describe, expect, it } from "vitest";

import {
  KAFKA_TOPIC_CONFIGURATION_LIMITS,
  KAFKA_TOPIC_CONFIGURATION_REDACTION,
  type KafkaTopicConfigurationChange,
  type KafkaTopicConfigurationEntry,
  type KafkaTopicConfigurationHistoryStoreCapability,
  type KafkaTopicConfigurationOperationInput,
} from "../../src/kafka/contracts";
import {
  InMemoryKafkaTopicConfigurationHistoryStore,
  KafkaTopicConfigurationService,
  type KafkaTopicConfigurationHistoryDocument,
  type KafkaTopicConfigurationHistoryStore,
  type KafkaTopicConfigurationSessionPort,
} from "../../src/kafka/application";

const writableRetention: KafkaTopicConfigurationEntry = {
  documentation: null,
  isDefault: false,
  isSensitive: false,
  name: "retention.ms",
  readOnly: false,
  source: "topic",
  synonyms: [],
  type: "long",
  value: "86400000",
};

const unrelatedSegment: KafkaTopicConfigurationEntry = {
  ...writableRetention,
  name: "segment.ms",
  value: "1800000",
};

const readOnlyEntry: KafkaTopicConfigurationEntry = {
  ...writableRetention,
  name: "message.format.version",
  readOnly: true,
  value: "3.8-IV0",
};

const sensitiveEntry: KafkaTopicConfigurationEntry = {
  ...writableRetention,
  isSensitive: true,
  name: "ssl.keystore.password",
  type: "password",
  value: null,
};

class RecordingConfigurationSession implements KafkaTopicConfigurationSessionPort {
  readonly alterCalls: Array<{
    readonly changes: readonly KafkaTopicConfigurationChange[];
    readonly topic: string;
    readonly validateOnly: boolean;
  }> = [];
  context: {
    readonly connectionName: string;
    readonly connectionTarget: string;
  } | null = {
    connectionName: "Local aio",
    connectionTarget: "localhost:19093",
  };
  entries: KafkaTopicConfigurationEntry[] = [
    writableRetention,
    unrelatedSegment,
    readOnlyEntry,
    sensitiveEntry,
  ];
  refreshFailure: Error | undefined;

  activeConnectionContext(): {
    readonly connectionName: string;
    readonly connectionTarget: string;
  } | null {
    return this.context;
  }

  alterTopicConfiguration(
    topic: string,
    changes: readonly KafkaTopicConfigurationChange[],
    validateOnly: boolean,
  ): Promise<void> {
    this.alterCalls.push({ changes, topic, validateOnly });
    if (!validateOnly) {
      const values = new Map(changes.map((change) => [change.name, change.value]));
      this.entries = this.entries.map((entry) =>
        values.has(entry.name) ? { ...entry, value: values.get(entry.name) ?? null } : entry,
      );
    }
    return Promise.resolve();
  }

  describeTopicConfiguration(): Promise<readonly KafkaTopicConfigurationEntry[]> {
    if (this.refreshFailure !== undefined) {
      const failure = this.refreshFailure;
      this.refreshFailure = undefined;
      return Promise.reject(failure);
    }
    return Promise.resolve(this.entries.map((entry) => ({ ...entry })));
  }
}

class UnavailableHistoryStore implements KafkaTopicConfigurationHistoryStore {
  private readonly unavailable: KafkaTopicConfigurationHistoryStoreCapability = {
    durability: "durable",
    recovery: "Preserve the history file and restore a known-good copy.",
    state: "unavailable",
  };

  capability(): KafkaTopicConfigurationHistoryStoreCapability {
    return this.unavailable;
  }

  commit(_document: KafkaTopicConfigurationHistoryDocument): Promise<void> {
    return Promise.reject(new Error("history-secret must not escape"));
  }

  load(): Promise<KafkaTopicConfigurationHistoryDocument | undefined> {
    return Promise.reject(new Error("history-secret must not escape"));
  }
}

class AbortOnceHistoryStore implements KafkaTopicConfigurationHistoryStore {
  loadCalls = 0;

  capability(): KafkaTopicConfigurationHistoryStoreCapability {
    return { durability: "session", state: "ready" };
  }

  commit(): Promise<void> {
    return Promise.resolve();
  }

  load(): Promise<KafkaTopicConfigurationHistoryDocument | undefined> {
    this.loadCalls += 1;
    return this.loadCalls === 1
      ? Promise.reject(new DOMException("Aborted", "AbortError"))
      : Promise.resolve({ entries: [] });
  }
}

function input(name = "retention.ms", value = "604800000"): KafkaTopicConfigurationOperationInput {
  return {
    changes: [{ isSensitive: false, name, value }],
    topic: "orders.events",
  };
}

function service(
  session = new RecordingConfigurationSession(),
  store: KafkaTopicConfigurationHistoryStore = new InMemoryKafkaTopicConfigurationHistoryStore({
    durability: "session",
    state: "ready",
  }),
): {
  readonly session: RecordingConfigurationSession;
  readonly service: KafkaTopicConfigurationService;
} {
  let id = 0;
  return {
    service: new KafkaTopicConfigurationService(session, store, {
      createHistoryId: () => `history-${String(++id)}`,
      now: () => new Date("2026-07-25T12:00:00.000Z"),
    }),
    session,
  };
}

describe("Kafka topic-configuration application service", () => {
  it("loads a sorted safe view for the active connection", async () => {
    const value = service();

    await expect(value.service.load("orders.events")).resolves.toMatchObject({
      connectionName: "Local aio",
      connectionTarget: "localhost:19093",
      entries: [
        { name: "message.format.version" },
        { name: "retention.ms" },
        { name: "segment.ms" },
        { isSensitive: true, name: "ssl.keystore.password", value: null },
      ],
      refreshedAt: "2026-07-25T12:00:00.000Z",
      topic: "orders.events",
    });
  });

  it("rejects broker metadata beyond the synonym bound instead of truncating it", async () => {
    const session = new RecordingConfigurationSession();
    session.entries = [
      {
        ...writableRetention,
        synonyms: Array.from(
          { length: KAFKA_TOPIC_CONFIGURATION_LIMITS.synonymsPerEntry + 1 },
          (_, index) => ({
            name: `retention.ms.${String(index)}`,
            source: "default" as const,
            value: String(index),
          }),
        ),
      },
    ];
    const value = service(session);

    await expect(value.service.load("orders.events")).rejects.toMatchObject({
      code: "VALIDATION",
      message: "Kafka returned too many synonyms for retention.ms.",
    });
  });

  it("dry-runs against a fresh baseline without mutation and records history", async () => {
    const value = service();

    const result = await value.service.validate({
      ...input(),
      presetId: "retention-7d",
    });

    expect(value.session.alterCalls).toEqual([
      {
        changes: [{ isSensitive: false, name: "retention.ms", value: "604800000" }],
        topic: "orders.events",
        validateOnly: true,
      },
    ]);
    expect(value.session.entries.find((entry) => entry.name === "retention.ms")?.value).toBe(
      "86400000",
    );
    expect(result.configuration.entries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "retention.ms", value: "86400000" }),
      ]),
    );
    expect(result.history.entries).toMatchObject([
      {
        action: "validate",
        changes: [{ from: "86400000", name: "retention.ms", to: "604800000" }],
        presetId: "retention-7d",
        success: true,
      },
    ]);
  });

  it("rejects misleading preset attribution before reading or altering Kafka", async () => {
    const value = service();

    await expect(
      value.service.validate({
        ...input("retention.ms", "123"),
        presetId: "retention-7d",
      }),
    ).rejects.toMatchObject({
      code: "VALIDATION",
      message: "Preset retention-7d does not define retention.ms with the submitted value.",
    });
    expect(value.session.alterCalls).toEqual([]);
  });

  it("rejects fresh read-only metadata before alter and records safe failure", async () => {
    const value = service();

    await expect(
      value.service.validate(input("message.format.version", "3.9-IV0")),
    ).rejects.toMatchObject({
      code: "INVALID_TOPIC_CONFIG",
      message: "Read-only topic configurations cannot be changed: message.format.version.",
    });
    expect(value.session.alterCalls).toEqual([]);
    await expect(value.service.history("orders.events")).resolves.toMatchObject({
      snapshot: {
        entries: [
          {
            action: "validate",
            error: "Read-only topic configurations cannot be changed: message.format.version.",
            success: false,
          },
        ],
      },
    });
  });

  it("records a failed apply when fresh Kafka discovery rejects before mutation", async () => {
    const session = new RecordingConfigurationSession();
    session.refreshFailure = Object.assign(new Error("The topic no longer exists."), {
      code: "TOPIC_NOT_FOUND",
      recovery: "Refresh topics and select an existing topic.",
    });
    const value = service(session);

    await expect(value.service.apply(input())).rejects.toMatchObject({
      code: "TOPIC_NOT_FOUND",
    });
    expect(value.session.alterCalls).toEqual([]);
    await expect(value.service.history("orders.events")).resolves.toMatchObject({
      snapshot: {
        entries: [
          {
            action: "apply",
            error: "The topic no longer exists.",
            success: false,
          },
        ],
      },
    });
  });

  it("applies only named values, preserves unrelated values, refreshes and records success", async () => {
    const value = service();

    const result = await value.service.apply(input());

    expect(value.session.alterCalls).toHaveLength(1);
    expect(value.session.alterCalls[0]).toMatchObject({ validateOnly: false });
    expect(result.configuration.entries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "retention.ms", value: "604800000" }),
        expect.objectContaining({ name: "segment.ms", value: "1800000" }),
      ]),
    );
    expect(result.refreshFailure).toBeUndefined();
    expect(result.history.entries[0]).toMatchObject({
      action: "apply",
      success: true,
    });
  });

  it("returns applied-but-stale evidence when refresh fails after mutation", async () => {
    const session = new RecordingConfigurationSession();
    let describeCount = 0;
    const originalDescribe = session.describeTopicConfiguration.bind(session);
    session.describeTopicConfiguration = async (): Promise<
      readonly KafkaTopicConfigurationEntry[]
    > => {
      describeCount += 1;
      if (describeCount === 2) {
        throw new Error("refresh unavailable");
      }
      return originalDescribe();
    };
    const value = service(session);

    const result = await value.service.apply(input());

    expect(result.refreshFailure).toBeInstanceOf(Error);
    expect(result.configuration.entries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "retention.ms", value: "86400000" }),
      ]),
    );
    expect(result.history.entries[0]).toMatchObject({
      action: "apply",
      success: true,
      warning: "Kafka applied the named changes, but refreshed configuration is unavailable.",
    });
  });

  it("redacts sensitive before and after values before the store boundary", async () => {
    const value = service();

    const result = await value.service.validate({
      changes: [
        {
          isSensitive: true,
          name: "ssl.keystore.password",
          value: "new-broker-secret",
        },
      ],
      topic: "orders.events",
    });

    expect(result.history.entries[0]?.changes[0]).toEqual({
      from: KAFKA_TOPIC_CONFIGURATION_REDACTION,
      isSensitive: true,
      name: "ssl.keystore.password",
      to: KAFKA_TOPIC_CONFIGURATION_REDACTION,
      wasDefault: false,
    });
    expect(JSON.stringify(result)).not.toContain("new-broker-secret");
  });

  it("keeps only the newest bounded history and filters exact topic and connection", async () => {
    const value = service();

    for (let index = 0; index < KAFKA_TOPIC_CONFIGURATION_LIMITS.historyEntries + 5; index += 1) {
      await value.service.validate(input("retention.ms", String(index)));
    }

    const history = await value.service.history("orders.events");
    expect(history.snapshot.entries).toHaveLength(
      KAFKA_TOPIC_CONFIGURATION_LIMITS.historyVisibleEntries,
    );
    expect(history.snapshot.entries[0]?.id).toBe("history-55");
    expect(history.snapshot.entries.at(-1)?.id).toBe("history-16");
  });

  it("keeps Kafka administration usable when history is unavailable", async () => {
    const value = service(new RecordingConfigurationSession(), new UnavailableHistoryStore());

    const result = await value.service.apply(input());

    expect(result.configuration.entries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "retention.ms", value: "604800000" }),
      ]),
    );
    expect(result.history.entries).toEqual([]);
    expect(result.history.store).toMatchObject({
      durability: "durable",
      state: "unavailable",
    });
    expect(result.historyFailure).toBeInstanceOf(Error);
    expect(JSON.stringify(result)).not.toContain("history-secret");
  });

  it("retries history loading after cancellation instead of treating it as loaded", async () => {
    const store = new AbortOnceHistoryStore();
    const value = service(new RecordingConfigurationSession(), store);

    await expect(value.service.history("orders.events")).rejects.toMatchObject({
      name: "AbortError",
    });
    await expect(value.service.history("orders.events")).resolves.toMatchObject({
      snapshot: {
        entries: [],
        store: { durability: "session", state: "ready" },
      },
    });
    expect(store.loadCalls).toBe(2);
  });
});
