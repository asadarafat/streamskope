import { describe, expect, it } from "vitest";

import { createRecipeLibrary, LegacyTemplateFixture } from "../support/recipe-library";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEvent,
  type ProfileCreateInput,
  type ProfileStoreCapability,
  type SecureConnectionInput,
} from "../../src/features/kafka/contracts";
import { InMemoryKafkaProfileStore } from "../../src/features/kafka/application/in-memory-profile-store";
import { InMemoryKafkaRuleStore } from "../../src/features/kafka/application/in-memory-rule-store";
import { InMemoryKafkaTopicConfigurationHistoryStore } from "../../src/features/kafka/application/in-memory-topic-configuration-history-store";
import { KafkaApplicationSession } from "../../src/features/kafka/application/session";
import { KafkaLiveRuleRuntime } from "../../src/features/kafka/application/live-rule-runtime";
import { KafkaProfileService } from "../../src/features/kafka/application/profile-service";
import { KafkaRuleService } from "../../src/features/kafka/application/rule-service";
import { KafkaTopicConfigurationService } from "../../src/features/kafka/application/topic-configuration-service";
import type {
  KafkaActiveConnection,
  KafkaConnectionPort,
  KafkaConnectionTestResult,
} from "../../src/features/kafka/application/types";
import type {
  KafkaProfileRecord,
  KafkaProfileTrustDecoder,
} from "../../src/features/kafka/application/profile-types";
import { KafkaBackendFacade } from "../../src/features/kafka/facade/facade";
import { StreamSkopeKafkaRuleEvaluator } from "../../src/features/kafka/engine/rule-evaluator";

const capability: ProfileStoreCapability = {
  durability: "session",
  protection: "memory",
  state: "ready",
};

const storedProfile: KafkaProfileRecord = {
  brokers: ["127.0.0.1:19093"],
  createdAt: "2026-07-25T18:00:00.000Z",
  id: "profile-1",
  name: "Local validation",
  oauth: {
    clientId: "admin",
    clientSecret: "fixture-secret",
    scope: "kafka",
    tokenEndpoint: "http://127.0.0.1:15000/token",
  },
  trust: {
    kind: "pem",
    label: "ca.pem",
    material: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
  },
  updatedAt: "2026-07-25T18:00:00.000Z",
};

const createInput: ProfileCreateInput = {
  brokers: ["second.example.test:9093"],
  name: "Second",
  trust: {
    kind: "pem",
    label: "second.pem",
    material: {
      mode: "replace",
      value: "-----BEGIN CERTIFICATE-----\nsecond\n-----END CERTIFICATE-----",
    },
    password: { mode: "clear" },
  },
};

class AcceptingTrustDecoder implements KafkaProfileTrustDecoder {
  decode(input: { readonly kind: "jks" | "pem" | "pkcs12" }): Promise<{
    readonly caPem: string;
    readonly kind: "jks" | "pem" | "pkcs12";
  }> {
    return Promise.resolve({
      caPem: "-----BEGIN CERTIFICATE-----\nvalidated\n-----END CERTIFICATE-----",
      kind: input.kind,
    });
  }
}

class ActiveConnection implements KafkaActiveConnection {
  closeCalls = 0;

  alterTopicConfiguration(): Promise<void> {
    return Promise.reject(new Error("Topic configuration is not used in profile facade tests."));
  }

  close(): Promise<void> {
    this.closeCalls += 1;
    return Promise.resolve();
  }

  describeBrokerConfiguration(): never {
    throw new Error("Broker configuration is not used in profile facade tests.");
  }

  describeClusterMetadata(): never {
    throw new Error("Cluster metadata is not used in profile facade tests.");
  }

  listTopics(): Promise<readonly string[]> {
    return Promise.resolve([]);
  }

  describeTopicConfiguration(): Promise<readonly never[]> {
    return Promise.reject(new Error("Topic configuration is not used in profile facade tests."));
  }

  openMessageStream(): Promise<never> {
    return Promise.reject(new Error("Message consumption is not used in profile facade tests."));
  }
}

class RecordingConnectionPort implements KafkaConnectionPort {
  readonly active = new ActiveConnection();
  readonly openInputs: SecureConnectionInput[] = [];
  readonly testInputs: SecureConnectionInput[] = [];
  openFailure: Error | undefined;
  testFailure: Error | undefined;

  openConnection(
    input: SecureConnectionInput,
    _signal: AbortSignal,
  ): Promise<KafkaActiveConnection> {
    this.openInputs.push(input);
    return this.openFailure === undefined
      ? Promise.resolve(this.active)
      : Promise.reject(this.openFailure);
  }

  testConnection(
    input: SecureConnectionInput,
    _signal: AbortSignal,
  ): Promise<KafkaConnectionTestResult> {
    this.testInputs.push(input);
    return this.testFailure === undefined
      ? Promise.resolve({
          checks: [
            ...(input.oauth === undefined ? [] : (["oauth"] as const)),
            ...(input.tls.enabled ? (["tls"] as const) : []),
            "kafka-authentication",
            "metadata",
          ],
          topicCount: 2,
        })
      : Promise.reject(this.testFailure);
  }
}

function profileCommand(
  command:
    | "profiles.connect"
    | "profiles.create"
    | "profiles.delete"
    | "profiles.list"
    | "profiles.test"
    | "profiles.update",
  payload: HostCommand["payload"],
): HostCommand {
  return {
    command,
    id: command,
    payload,
    version: HOST_PROTOCOL_VERSION,
  } as HostCommand;
}

function setup(records: readonly KafkaProfileRecord[] = [storedProfile]): {
  readonly events: HostEvent[];
  readonly facade: KafkaBackendFacade;
  readonly port: RecordingConnectionPort;
  readonly store: InMemoryKafkaProfileStore;
} {
  const port = new RecordingConnectionPort();
  const store = new InMemoryKafkaProfileStore(capability, records);
  const profiles = new KafkaProfileService(store, new AcceptingTrustDecoder(), {
    createId: (): string => "profile-2",
    now: (): Date => new Date("2026-07-25T18:05:00.000Z"),
  });
  const evaluator = new StreamSkopeKafkaRuleEvaluator();
  const rules = new KafkaRuleService(
    new InMemoryKafkaRuleStore({ durability: "session", state: "ready" }),
    evaluator,
  );
  let correlation = 0;
  const session = new KafkaApplicationSession(port);
  const facade = new KafkaBackendFacade(
    session,
    profiles,
    createRecipeLibrary(
      new LegacyTemplateFixture({
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
      createCorrelationId: (): string => `profile-correlation-${++correlation}`,
      now: (): Date => new Date("2026-07-25T18:05:00.000Z"),
    },
  );
  const events: HostEvent[] = [];
  facade.subscribe((event) => {
    events.push(event);
  });
  return { events, facade, port, store };
}

describe("Kafka profile facade", () => {
  it("preserves unavailable plugin profiles and rejects connection and testing before contacting Kafka", async () => {
    const source = {
      kind: "plugin" as const,
      pluginId: "example.capture",
      version: 1 as const,
      data: { sessionId: "recoverable-session", broker: "127.0.0.1:19092" },
    };
    const record: KafkaProfileRecord = {
      id: "plugin-profile",
      name: "Saved capture",
      transport: "plaintext",
      brokers: ["127.0.0.1:19092"],
      source,
      createdAt: storedProfile.createdAt,
      updatedAt: storedProfile.updatedAt,
    };
    const { facade, port, store, events } = setup([record]);
    await expect(facade.execute(profileCommand("profiles.list", {}))).resolves.toMatchObject({
      ok: true,
    });
    expect(events.find((event) => event.event === "profiles.changed")).toMatchObject({
      payload: { profiles: [{ source }] },
    });
    const expectedFailure = {
      ok: false,
      error: {
        code: "BACKEND_UNAVAILABLE",
        recovery: expect.stringContaining("Preferences → Plugins") as unknown,
      },
    };
    await expect(
      facade.execute(profileCommand("profiles.connect", { profileId: record.id })),
    ).resolves.toMatchObject(expectedFailure);
    await expect(
      facade.execute(
        profileCommand("profiles.test", {
          mode: "create",
          profile: { brokers: record.brokers, name: record.name, transport: "plaintext", source },
        }),
      ),
    ).resolves.toMatchObject(expectedFailure);
    await expect(
      facade.execute(
        profileCommand("profiles.test", {
          mode: "update",
          profileId: record.id,
          profile: { brokers: record.brokers, name: "Renamed capture", transport: "plaintext" },
        }),
      ),
    ).resolves.toMatchObject(expectedFailure);
    expect(port.openInputs).toEqual([]);
    expect(port.testInputs).toEqual([]);
    expect(store.records()).toEqual([record]);
    expect(store.commitCount).toBe(0);
    await facade.shutdown();
  });

  it("tests a plaintext profile draft without trust or TLS evidence", async () => {
    const { events, facade, port, store } = setup([]);

    await expect(
      facade.execute(
        profileCommand("profiles.test", {
          mode: "create",
          profile: {
            brokers: ["127.0.0.1:19092"],
            name: "Plaintext draft",
            transport: "plaintext",
          },
        }),
      ),
    ).resolves.toMatchObject({ ok: true });

    expect(port.testInputs).toEqual([
      {
        brokers: ["127.0.0.1:19092"],
        name: "Plaintext draft",
        tls: { enabled: false },
      },
    ]);
    expect(store.commitCount).toBe(0);
    expect(events.filter((event) => event.event === "activity.recorded").at(-1)).toMatchObject({
      payload: {
        detail:
          "Confirmed checks: kafka-authentication, metadata. Visible topics: 2. No profile or active connection changed.",
        outcome: "succeeded",
      },
    });
    expect(JSON.stringify(events)).not.toMatch(/BEGIN CERTIFICATE|caPem/);
  });

  it("tests a direct plaintext host connection without trust acquisition", async () => {
    const { facade, port } = setup([]);

    await expect(
      facade.execute({
        command: "connection.test",
        id: "connection-test-plaintext",
        payload: {
          brokers: ["127.0.0.1:19092"],
          name: "Direct plaintext",
          tls: { enabled: false },
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ ok: true });
    expect(port.testInputs).toEqual([
      {
        brokers: ["127.0.0.1:19092"],
        name: "Direct plaintext",
        tls: { enabled: false },
      },
    ]);
  });

  it("publishes safe inventory for list, create, update and delete commands", async () => {
    const { events, facade, store } = setup();

    await expect(facade.execute(profileCommand("profiles.list", {}))).resolves.toMatchObject({
      ok: true,
    });
    await expect(
      facade.execute(
        profileCommand("profiles.create", {
          profile: createInput,
        }),
      ),
    ).resolves.toMatchObject({ ok: true });
    await expect(
      facade.execute(
        profileCommand("profiles.update", {
          profile: {
            ...createInput,
            name: "Second renamed",
            expectedRevision: 1,
            trust: {
              ...createInput.trust,
              material: { mode: "retain" },
              password: { mode: "retain" },
            },
          },
          profileId: "profile-2",
        }),
      ),
    ).resolves.toMatchObject({ ok: true });
    await expect(
      facade.execute(
        profileCommand("profiles.delete", {
          profileId: "profile-2",
        }),
      ),
    ).resolves.toMatchObject({ ok: true });

    const profileEvents = events.filter(
      (event): event is Extract<HostEvent, { readonly event: "profiles.changed" }> =>
        event.event === "profiles.changed",
    );
    expect(
      profileEvents.map((event) => event.payload.profiles.map((profile) => profile.name)),
    ).toEqual([
      ["Local validation"],
      ["Local validation", "Second"],
      ["Local validation", "Second renamed"],
      ["Local validation"],
    ]);
    expect(JSON.stringify(profileEvents)).not.toMatch(/fixture-secret|BEGIN CERTIFICATE|validated/);
    expect(store.records()).toEqual([{ ...storedProfile, transport: "tls" }]);
    expect(
      events
        .filter((event) => event.event === "activity.recorded")
        .map((event) => [event.payload.operation, event.payload.object]),
    ).toEqual([
      ["Load profiles", "Kafka profiles"],
      ["Create profile", "Second · second.example.test:9093"],
      ["Update profile", "Second renamed · second.example.test:9093"],
      ["Delete profile", "Second renamed · second.example.test:9093"],
    ]);
  });

  it("tests a retained profile draft without changing profiles or active connection state", async () => {
    const { events, facade, port, store } = setup();

    await expect(
      facade.execute(
        profileCommand("profiles.test", {
          mode: "update",
          profile: {
            brokers: ["changed.example.test:9093"],
            name: "Changed draft",
            oauth: {
              clientId: "changed-client",
              clientSecret: { mode: "retain" },
              scope: "changed-scope",
              tokenEndpoint: "http://127.0.0.1:15000/changed-token",
            },
            trust: {
              kind: "pem",
              label: storedProfile.trust.label,
              material: { mode: "retain" },
              password: { mode: "clear" },
            },
          },
          profileId: storedProfile.id,
        }),
      ),
    ).resolves.toMatchObject({ ok: true });

    expect(port.testInputs).toEqual([
      {
        brokers: ["changed.example.test:9093"],
        name: "Changed draft",
        oauth: {
          clientId: "changed-client",
          clientSecret: "fixture-secret",
          scope: "changed-scope",
          tokenEndpoint: "http://127.0.0.1:15000/changed-token",
        },
        tls: {
          caPem: "-----BEGIN CERTIFICATE-----\nvalidated\n-----END CERTIFICATE-----",
          enabled: true,
        },
      },
    ]);
    expect(store.commitCount).toBe(0);
    expect(store.records()).toEqual([storedProfile]);
    expect(facade.connectionSnapshot()).toEqual({
      connectionName: null,
      state: "disconnected",
    });
    expect(events.filter((event) => event.event === "profiles.changed")).toEqual([]);
    expect(events.filter((event) => event.event === "activity.recorded").at(-1)).toMatchObject({
      payload: {
        object: "Changed draft · changed.example.test:9093",
        operation: "Test profile connection",
        outcome: "succeeded",
      },
    });
    expect(JSON.stringify(events)).not.toMatch(/fixture-secret|BEGIN CERTIFICATE/);
  });

  it("redacts draft protected values when a profile connection test fails", async () => {
    const { events, facade, port } = setup([]);
    port.testFailure = new Error(
      "rejected draft-secret and draft-password and -----BEGIN CERTIFICATE-----",
    );

    const response = await facade.execute(
      profileCommand("profiles.test", {
        mode: "create",
        profile: {
          brokers: ["draft.example.test:9093"],
          name: "Draft profile",
          oauth: {
            clientId: "draft-client",
            clientSecret: { mode: "replace", value: "draft-secret" },
            scope: "kafka",
            tokenEndpoint: "http://127.0.0.1:15000/token",
          },
          trust: {
            kind: "jks",
            label: "draft.jks",
            material: {
              mode: "replace",
              value: "-----BEGIN CERTIFICATE----- draft-material",
            },
            password: { mode: "replace", value: "draft-password" },
          },
        },
      }),
    );

    expect(response).toMatchObject({
      error: { activeStateChanged: false },
      ok: false,
    });
    expect(JSON.stringify({ events, response })).not.toMatch(
      /draft-secret|draft-password|draft-material|BEGIN CERTIFICATE/,
    );
  });

  it("resolves a stored profile, delegates connection, and marks it active only after confirmation", async () => {
    const { events, facade, port } = setup();

    await expect(
      facade.execute(
        profileCommand("profiles.connect", {
          profileId: storedProfile.id,
        }),
      ),
    ).resolves.toMatchObject({ ok: true });

    expect(port.openInputs).toEqual([
      {
        brokers: storedProfile.brokers,
        name: storedProfile.name,
        oauth: storedProfile.oauth,
        tls: {
          caPem: "-----BEGIN CERTIFICATE-----\nvalidated\n-----END CERTIFICATE-----",
          enabled: true,
        },
      },
    ]);
    expect(events.filter((event) => event.event === "profiles.changed").at(-1)).toMatchObject({
      payload: {
        profiles: [{ active: true, id: storedProfile.id }],
      },
    });
    expect(
      JSON.stringify(events.filter((event) => event.event === "activity.recorded")),
    ).not.toContain("fixture-secret");
    expect(events.filter((event) => event.event === "activity.recorded").at(-1)).toMatchObject({
      payload: {
        object: "Local validation · 127.0.0.1:19093",
        operation: "Connect profile",
      },
    });
  });

  it("clears active profile identity after confirmed disconnect", async () => {
    const { events, facade } = setup();
    await facade.execute(
      profileCommand("profiles.connect", {
        profileId: storedProfile.id,
      }),
    );

    await facade.execute({
      command: "connection.disconnect",
      id: "disconnect",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });

    expect(events.filter((event) => event.event === "profiles.changed").at(-1)).toMatchObject({
      payload: {
        profiles: [{ active: false, id: storedProfile.id }],
      },
    });
  });

  it("keeps the profile inactive and redacts host-only secrets when connection fails", async () => {
    const { events, facade, port } = setup();
    port.openFailure = new Error("broker rejected fixture-secret and -----BEGIN CERTIFICATE-----");

    await expect(
      facade.execute(
        profileCommand("profiles.connect", {
          profileId: storedProfile.id,
        }),
      ),
    ).resolves.toMatchObject({
      error: {
        activeStateChanged: true,
      },
      ok: false,
    });

    expect(events.filter((event) => event.event === "profiles.changed").at(-1)).toMatchObject({
      payload: {
        profiles: [{ active: false, id: storedProfile.id }],
      },
    });
    expect(JSON.stringify(events)).not.toMatch(/fixture-secret|BEGIN CERTIFICATE/);
  });
});
