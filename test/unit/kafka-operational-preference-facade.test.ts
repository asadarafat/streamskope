import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  type HostCommand,
  type HostEvent,
  type KafkaOperationalPreferenceStoreCapability,
  type KafkaOperationalPreferences,
} from "../../src/features/kafka/contracts";
import {
  InMemoryKafkaConnectionTemplateStore,
  InMemoryKafkaOperationalPreferenceStore,
  InMemoryKafkaProfileStore,
  InMemoryKafkaRuleStore,
  InMemoryKafkaTopicConfigurationHistoryStore,
  KafkaApplicationSession,
  KafkaConnectionTemplateService,
  KafkaLiveRuleRuntime,
  KafkaOperationalPreferenceService,
  KafkaProfileService,
  KafkaRuleService,
  KafkaTopicConfigurationService,
  type KafkaActiveConnection,
  type KafkaConnectionPort,
  type KafkaConnectionTestResult,
  type KafkaOperationalPreferenceStore,
} from "../../src/features/kafka/application";
import { KafkaBackendFacade } from "../../src/features/kafka/facade";
import { StreamSkopeKafkaRuleEvaluator } from "../../src/features/kafka/engine";

class UnusedConnectionPort implements KafkaConnectionPort {
  openConnection(): Promise<KafkaActiveConnection> {
    return Promise.reject(new Error("Kafka connection was not expected."));
  }

  testConnection(): Promise<KafkaConnectionTestResult> {
    return Promise.reject(new Error("Kafka connection test was not expected."));
  }
}

class UnavailablePreferenceStore implements KafkaOperationalPreferenceStore {
  capability(): KafkaOperationalPreferenceStoreCapability {
    return {
      durability: "durable",
      recovery: "Reset operational preferences to replace the unreadable file.",
      state: "unavailable",
    };
  }

  commit(): Promise<void> {
    return Promise.reject(new Error("storage remains unavailable"));
  }

  load(): Promise<KafkaOperationalPreferences> {
    return Promise.reject(new Error("private-path and runbook must remain redacted"));
  }
}

function preferenceCommand(
  command: "preferences.get" | "preferences.reset" | "preferences.update",
  payload: HostCommand["payload"],
  id: string = command,
): HostCommand {
  return {
    command,
    id,
    payload,
    version: HOST_PROTOCOL_VERSION,
  } as HostCommand;
}

function setup(store: KafkaOperationalPreferenceStore): {
  readonly events: HostEvent[];
  readonly facade: KafkaBackendFacade;
} {
  const evaluator = new StreamSkopeKafkaRuleEvaluator();
  const rules = new KafkaRuleService(
    new InMemoryKafkaRuleStore({ durability: "session", state: "ready" }),
    evaluator,
  );
  const session = new KafkaApplicationSession(new UnusedConnectionPort());
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
        decode: (): Promise<never> =>
          Promise.reject(new Error("Profile decoding was not expected.")),
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
    new KafkaTopicConfigurationService(
      session,
      new InMemoryKafkaTopicConfigurationHistoryStore({
        durability: "session",
        state: "ready",
      }),
    ),
    {
      createCorrelationId: (): string => `preference-correlation-${++correlation}`,
      now: (): Date => new Date("2026-07-26T12:00:00.000Z"),
      preferences: new KafkaOperationalPreferenceService(store),
    },
  );
  const events: HostEvent[] = [];
  facade.subscribe((event) => {
    events.push(event);
  });
  return { events, facade };
}

function preferenceEvents(
  events: readonly HostEvent[],
): readonly Extract<HostEvent, { readonly event: "preferences.changed" }>[] {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "preferences.changed" }> =>
      event.event === "preferences.changed",
  );
}

describe("Kafka operational-preference facade", () => {
  it("routes get, atomic update and reset with complete correlated snapshots", async () => {
    const store = new InMemoryKafkaOperationalPreferenceStore({
      durability: "session",
      state: "ready",
    });
    const { events, facade } = setup(store);

    const loaded = await facade.execute(preferenceCommand("preferences.get", {}));
    const changed = await facade.execute(
      preferenceCommand(
        "preferences.update",
        {
          patch: {
            fetch: { maxMessages: 100, mode: "newest" },
            latency: {
              runbookUrl: "https://private.example.test/runbooks/latency",
            },
          },
        },
        "save-preferences",
      ),
    );
    const reset = await facade.execute(preferenceCommand("preferences.reset", {}));

    expect(loaded).toMatchObject({
      command: "preferences.get",
      ok: true,
      result: {
        correlationId: "preference-correlation-1",
        snapshot: {
          preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
          store: { durability: "session", state: "ready" },
        },
      },
    });
    expect(changed).toMatchObject({
      command: "preferences.update",
      id: "save-preferences",
      ok: true,
      result: {
        correlationId: "preference-correlation-2",
        snapshot: {
          preferences: {
            fetch: { maxMessages: 100, mode: "newest" },
            latency: {
              runbookUrl: "https://private.example.test/runbooks/latency",
            },
          },
        },
      },
    });
    expect(reset).toMatchObject({
      command: "preferences.reset",
      ok: true,
      result: {
        correlationId: "preference-correlation-3",
        snapshot: { preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS },
      },
    });
    const emittedSnapshots = preferenceEvents(events);
    expect(emittedSnapshots).toHaveLength(3);
    expect(emittedSnapshots[0]?.payload.preferences).toEqual(KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS);
    expect(emittedSnapshots[1]?.payload).toMatchObject({
      preferences: {
        fetch: { maxMessages: 100, mode: "newest" },
        latency: {
          runbookUrl: "https://private.example.test/runbooks/latency",
        },
      },
      store: { durability: "session", state: "ready" },
    });
    expect(emittedSnapshots[2]?.payload.preferences).toEqual(KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS);
    expect(store.commitCount).toBe(2);

    const activity = events.filter((event) => event.event === "activity.recorded");
    expect(
      activity.map((event) => [
        event.payload.operation,
        event.payload.object,
        event.payload.outcome,
      ]),
    ).toEqual([
      ["Load preferences", "Operational preferences", "succeeded"],
      ["Save preferences", "fetch, latency", "succeeded"],
      ["Reset preferences", "Operational preferences", "succeeded"],
    ]);
    expect(JSON.stringify(activity)).not.toMatch(/private\.example|runbooks|newest|queueDepth/);
  });

  it("publishes honest fallback and keeps unrelated commands available after storage failure", async () => {
    const { events, facade } = setup(new UnavailablePreferenceStore());

    const loaded = await facade.execute(preferenceCommand("preferences.get", {}));
    const failed = await facade.execute(
      preferenceCommand("preferences.update", {
        patch: { rules: { loggingEnabled: false } },
      }),
    );
    const topics = await facade.execute({
      command: "topics.list",
      id: "topics-after-preference-failure",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });

    expect(loaded).toMatchObject({
      ok: true,
      result: {
        snapshot: {
          preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
          store: {
            durability: "durable",
            state: "unavailable",
          },
        },
      },
    });
    expect(failed).toMatchObject({
      error: {
        activeStateChanged: false,
        code: "PREFERENCE_STORE_UNAVAILABLE",
        stage: "preference",
      },
      ok: false,
    });
    expect(topics).toMatchObject({ ok: false });
    expect(preferenceEvents(events)).toHaveLength(2);
    expect(preferenceEvents(events).at(-1)?.payload).toEqual(preferenceEvents(events)[0]?.payload);
    const serialized = JSON.stringify(events);
    expect(serialized).not.toMatch(/private-path|runbook must remain redacted/);
    expect(
      events
        .filter((event) => event.event === "activity.recorded")
        .map((event) => [event.payload.operation, event.payload.outcome, event.payload.severity]),
    ).toEqual([
      ["Load preferences", "succeeded", "warning"],
      ["Save preferences", "failed", "error"],
      ["Refresh topics", "failed", "error"],
    ]);
  });
});
