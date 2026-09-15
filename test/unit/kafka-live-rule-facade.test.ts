import { describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_LIVE_RULE_LIMITS,
  type HostCommand,
  type HostEvent,
  type KafkaFetchRequest,
  type KafkaMessage,
  type KafkaOperationalPreferencePatch,
  type KafkaRuleDefinition,
  type KafkaRuleStoreCapability,
  type SecureConnectionInput,
} from "../../src/kafka/contracts";
import {
  InMemoryKafkaConnectionTemplateStore,
  InMemoryKafkaProfileStore,
  InMemoryKafkaTopicConfigurationHistoryStore,
  InMemoryKafkaRuleStore,
  KafkaApplicationSession,
  KafkaConnectionTemplateService,
  KafkaLiveRuleRuntime,
  KafkaProfileService,
  KafkaRuleService,
  KafkaTopicConfigurationService,
  type KafkaActiveConnection,
  type KafkaConnectionPort,
  type KafkaConnectionTestResult,
  type KafkaMessageStream,
  type KafkaRuleDocument,
  type KafkaRuleEvaluator,
  type KafkaRulePredicate,
  type KafkaRuleStore,
} from "../../src/kafka/application";
import { KafkaBackendFacade } from "../../src/kafka/facade";
import { StreamSkopeKafkaRuleEvaluator } from "../../src/kafka/engine";

const connection: SecureConnectionInput = {
  brokers: ["localhost:19093"],
  name: "Local aio",
  tls: {
    caPem: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
    enabled: true,
  },
};

const sessionRuleCapability: KafkaRuleStoreCapability = {
  durability: "session",
  state: "ready",
};

function rule(
  name: string,
  expression: string,
  overrides: Partial<KafkaRuleDefinition> = {},
): KafkaRuleDefinition {
  return {
    cooldownMs: 0,
    enabled: true,
    expression,
    level: "warn",
    name,
    topic: "orders",
    ...overrides,
  };
}

function message(id: string, payload: string): KafkaMessage {
  return {
    headers: {},
    id,
    key: null,
    offset: id,
    originalByteSize: payload.length,
    partition: 0,
    payload,
    preview: payload,
    timestamp: "2026-07-25T22:00:00.000Z",
    topic: "orders",
    truncated: false,
  };
}

type StreamResult =
  { readonly kind: "end" } | { readonly kind: "message"; readonly message: KafkaMessage };

class ControlledStream implements KafkaMessageStream {
  closeCalls = 0;
  deliveredMessages = 0;
  private readonly queued: StreamResult[] = [];
  private readonly waiting: Array<(result: StreamResult) => void> = [];

  constructor(private readonly endOnClose = true) {}

  close(): Promise<void> {
    this.closeCalls += 1;
    if (this.endOnClose) {
      this.deliver({ kind: "end" });
    }
    return Promise.resolve();
  }

  push(value: KafkaMessage): void {
    this.deliver({ kind: "message", message: value });
  }

  async *[Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
    while (true) {
      const result = await this.next();
      if (result.kind === "end") {
        return;
      }
      this.deliveredMessages += 1;
      yield result.message;
    }
  }

  private deliver(result: StreamResult): void {
    const waiter = this.waiting.shift();
    if (waiter === undefined) {
      this.queued.push(result);
    } else {
      waiter(result);
    }
  }

  private next(): Promise<StreamResult> {
    const result = this.queued.shift();
    return result === undefined
      ? new Promise((resolve) => {
          this.waiting.push(resolve);
        })
      : Promise.resolve(result);
  }
}

class ActiveConnection implements KafkaActiveConnection {
  closeCalls = 0;
  readonly openedTopics: string[] = [];

  constructor(
    private readonly streams: ControlledStream[],
    private readonly order: string[],
  ) {}

  alterTopicConfiguration(): Promise<void> {
    return Promise.reject(new Error("No topic-configuration operation was configured."));
  }

  close(): Promise<void> {
    this.closeCalls += 1;
    return Promise.resolve();
  }

  describeBrokerConfiguration(): never {
    throw new Error("No broker-configuration operation was configured.");
  }

  describeClusterMetadata(): never {
    throw new Error("No cluster-metadata operation was configured.");
  }

  listTopics(): Promise<readonly string[]> {
    return Promise.resolve(["orders"]);
  }

  describeTopicConfiguration(): Promise<readonly never[]> {
    return Promise.reject(new Error("No topic-configuration operation was configured."));
  }

  openMessageStream(request: KafkaFetchRequest): Promise<KafkaMessageStream> {
    this.order.push(`stream-open:${request.topic}`);
    this.openedTopics.push(request.topic);
    const stream = this.streams.shift();
    return stream === undefined
      ? Promise.reject(new Error("No controlled stream remains."))
      : Promise.resolve(stream);
  }
}

class ConnectionPort implements KafkaConnectionPort {
  constructor(private readonly active: ActiveConnection) {}

  openConnection(): Promise<KafkaActiveConnection> {
    return Promise.resolve(this.active);
  }

  testConnection(): Promise<KafkaConnectionTestResult> {
    return Promise.resolve({ checks: ["metadata"], topicCount: 1 });
  }
}

class OrderedRuleStore implements KafkaRuleStore {
  private readonly inner: InMemoryKafkaRuleStore;

  constructor(
    private readonly order: string[],
    definitions: readonly KafkaRuleDefinition[],
  ) {
    this.inner = new InMemoryKafkaRuleStore(sessionRuleCapability, {
      rules: definitions,
    });
  }

  capability(): KafkaRuleStoreCapability {
    return this.inner.capability();
  }

  commit(document: KafkaRuleDocument, signal?: AbortSignal): Promise<void> {
    this.order.push("rule-commit");
    return this.inner.commit(document, signal);
  }

  load(signal?: AbortSignal): Promise<KafkaRuleDocument | undefined> {
    this.order.push("rule-load");
    return this.inner.load(signal);
  }
}

class UnavailableRuleStore implements KafkaRuleStore {
  capability(): KafkaRuleStoreCapability {
    return {
      durability: "session",
      recovery: "Restore the local rule catalog and retry consumption.",
      state: "unavailable",
    };
  }

  commit(): Promise<void> {
    return Promise.reject(new Error("rule store unavailable"));
  }

  load(): Promise<KafkaRuleDocument | undefined> {
    return Promise.reject(new Error("rule store unavailable"));
  }
}

class EvidencePressureEvaluator implements KafkaRuleEvaluator {
  compile(expression: string): KafkaRulePredicate {
    if (expression === "compile-failure") {
      throw new Error("bounded compile failure");
    }
    return {
      evaluate(): boolean {
        return expression === "active";
      },
    };
  }

  evaluate(expression: string): boolean {
    return this.compile(expression).evaluate(undefined);
  }

  parseSample(sample: string): unknown {
    return JSON.parse(sample) as unknown;
  }

  validate(): { readonly valid: boolean } {
    return { valid: true };
  }
}

interface Fixture {
  readonly active: ActiveConnection;
  readonly events: HostEvent[];
  readonly facade: KafkaBackendFacade;
  readonly liveRules: KafkaLiveRuleRuntime;
  readonly order: string[];
  readonly rules: KafkaRuleService;
}

function fixture(
  options: {
    readonly definitions?: readonly KafkaRuleDefinition[];
    readonly durationNow?: () => number;
    readonly evaluator?: KafkaRuleEvaluator;
    readonly monotonicNow?: () => number;
    readonly scheduleMessageFlush?: (flush: () => void) => void;
    readonly store?: KafkaRuleStore;
    readonly streams?: ControlledStream[];
  } = {},
): Fixture {
  const order: string[] = [];
  const streams = options.streams ?? [new ControlledStream()];
  const active = new ActiveConnection(streams, order);
  const evaluator = options.evaluator ?? new StreamSkopeKafkaRuleEvaluator();
  const store =
    options.store ??
    new OrderedRuleStore(order, options.definitions ?? [rule("High", '$.p == "h"')]);
  const rules = new KafkaRuleService(store, evaluator);
  const liveRules = new KafkaLiveRuleRuntime(rules, evaluator, {
    durationNow: options.durationNow ?? ((): number => 0),
    monotonicNow: options.monotonicNow ?? ((): number => 0),
  });
  let correlation = 0;
  const session = new KafkaApplicationSession(new ConnectionPort(active));
  const facade = new KafkaBackendFacade(
    session,
    new KafkaProfileService(
      new InMemoryKafkaProfileStore({
        durability: "session",
        protection: "memory",
        state: "ready",
      }),
      {
        decode: (): Promise<never> => Promise.reject(new Error("Profile decode was not expected.")),
      },
    ),
    new KafkaConnectionTemplateService(
      new InMemoryKafkaConnectionTemplateStore({
        durability: "session",
        state: "ready",
      }),
    ),
    rules,
    liveRules,
    new KafkaTopicConfigurationService(
      session,
      new InMemoryKafkaTopicConfigurationHistoryStore({
        durability: "session",
        state: "ready",
      }),
    ),
    {
      createCorrelationId: (): string => `live-correlation-${String(++correlation)}`,
      now: (): Date => new Date("2026-07-25T22:00:00.000Z"),
      scheduleMessageFlush:
        options.scheduleMessageFlush ??
        ((flush): void => {
          flush();
        }),
    },
  );
  const events: HostEvent[] = [];
  facade.subscribe((event) => {
    events.push(event);
  });
  return { active, events, facade, liveRules, order, rules };
}

function command(
  name: "connection.connect" | "messages.start" | "messages.stop",
  id: string,
): HostCommand {
  if (name === "connection.connect") {
    return {
      command: name,
      id,
      payload: connection,
      version: HOST_PROTOCOL_VERSION,
    };
  }
  return {
    command: name,
    id,
    payload:
      name === "messages.start"
        ? {
            maxMessages: 1_000,
            mode: "tail",
            topic: "orders",
          }
        : {},
    version: HOST_PROTOCOL_VERSION,
  } as HostCommand;
}

function updateRule(definition: KafkaRuleDefinition, id: string): HostCommand {
  return {
    command: "rules.update",
    id,
    payload: {
      originalName: "High",
      rule: definition,
    },
    version: HOST_PROTOCOL_VERSION,
  };
}

function updatePreferences(
  patch: KafkaOperationalPreferencePatch,
  id: string,
): Extract<HostCommand, { readonly command: "preferences.update" }> {
  return {
    command: "preferences.update",
    id,
    payload: { patch },
    version: HOST_PROTOCOL_VERSION,
  };
}

function batches(
  events: readonly HostEvent[],
): readonly Extract<HostEvent, { readonly event: "messages.batch" }>[] {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "messages.batch" }> =>
      event.event === "messages.batch",
  );
}

function notifications(
  events: readonly HostEvent[],
): readonly Extract<HostEvent, { readonly event: "rules.notification" }>[] {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "rules.notification" }> =>
      event.event === "rules.notification",
  );
}

function matchActivities(
  events: readonly HostEvent[],
): readonly Extract<HostEvent, { readonly event: "activity.recorded" }>[] {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "activity.recorded" }> =>
      event.event === "activity.recorded" && event.payload.operation === "Match live rules",
  );
}

function consumptionStates(
  events: readonly HostEvent[],
): readonly Extract<HostEvent, { readonly event: "consumption.state" }>[] {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "consumption.state" }> =>
      event.event === "consumption.state",
  );
}

async function connectAndStart(value: Fixture, id = "start"): Promise<void> {
  await value.facade.execute(command("connection.connect", `connect-${id}`));
  await value.facade.execute(command("messages.start", id));
}

describe("Kafka live rule facade", () => {
  it("prepares the catalog before opening Kafka and enriches accepted records", async () => {
    const stream = new ControlledStream();
    const value = fixture({ streams: [stream] });

    await connectAndStart(value);

    expect(value.order).toEqual(["rule-load", "stream-open:orders"]);
    expect(consumptionStates(value.events).map((event) => event.payload)).toMatchObject([
      {
        ruleEvaluation: {
          applicableRules: 1,
          omittedRules: 0,
          state: "ready",
        },
        state: "loading",
      },
      {
        ruleEvaluation: {
          applicableRules: 1,
          omittedRules: 0,
          state: "ready",
        },
        state: "streaming",
      },
    ]);

    stream.push(message("1", '{"p":"h","private":"payload-must-not-enter-activity"}'));
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(1);
    });

    expect(batches(value.events).at(-1)?.payload.messages[0]).toMatchObject({
      id: "1",
      ruleEvaluation: {
        activeMatchCount: 1,
        activeMatches: [{ level: "warn", name: "High" }],
        state: "evaluated",
      },
    });
    expect(notifications(value.events).at(-1)?.payload).toEqual({
      activeMatchCount: 1,
      highestSeverity: "warn",
      matches: [{ count: 1, level: "warn", name: "High" }],
      omittedMatches: 0,
      topic: "orders",
    });
    expect(matchActivities(value.events).at(-1)?.payload).toMatchObject({
      object: "orders",
      outcome: "succeeded",
      severity: "warning",
    });
    expect(matchActivities(value.events).at(-1)?.payload.detail).toMatch(/High.*1/u);
    expect(
      JSON.stringify(value.events.filter((event) => event.event === "activity.recorded")),
    ).not.toMatch(/payload-must-not-enter-activity|\$\.p/);
  });

  it("captures optional-output preferences per accepted record and preserves message evidence", async () => {
    const stream = new ControlledStream();
    const value = fixture({
      scheduleMessageFlush: (): void => undefined,
      streams: [stream],
    });
    await connectAndStart(value);

    stream.push(message("enabled", '{"p":"h"}'));
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(1);
    });
    await expect(
      value.facade.execute(
        updatePreferences(
          {
            rules: {
              loggingEnabled: false,
              notificationsEnabled: false,
            },
          },
          "disable-outputs",
        ),
      ),
    ).resolves.toMatchObject({ ok: true });
    stream.push(message("disabled", '{"p":"h"}'));
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(2);
    });
    await value.facade.execute(command("messages.stop", "flush-captured-outputs"));

    expect(batches(value.events).flatMap((event) => event.payload.messages)).toHaveLength(2);
    expect(
      batches(value.events)
        .flatMap((event) => event.payload.messages)
        .every((record) => record.ruleEvaluation.activeMatchCount === 1),
    ).toBe(true);
    expect(notifications(value.events)).toHaveLength(1);
    expect(notifications(value.events)[0]?.payload.activeMatchCount).toBe(1);
    expect(matchActivities(value.events)).toHaveLength(1);
  });

  it("does not produce optional output for a cooldown-suppressed match", async () => {
    let monotonic = 1_000;
    const stream = new ControlledStream();
    const value = fixture({
      definitions: [rule("High", '$.p == "h"', { cooldownMs: 10_000 })],
      monotonicNow: (): number => monotonic,
      streams: [stream],
    });
    await connectAndStart(value);

    stream.push(message("active", '{"p":"h"}'));
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(1);
    });
    monotonic = 1_001;
    stream.push(message("suppressed", '{"p":"h"}'));
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(2);
    });

    expect(batches(value.events).at(-1)?.payload.messages[0]?.ruleEvaluation).toMatchObject({
      activeMatchCount: 0,
      suppressedMatchCount: 1,
    });
    expect(notifications(value.events)).toHaveLength(1);
    expect(matchActivities(value.events)).toHaveLength(1);
  });

  it.each([
    { expected: ["Informational", "Warning", "Critical"], threshold: "debug" },
    { expected: ["Informational", "Warning", "Critical"], threshold: "info" },
    { expected: ["Warning", "Critical"], threshold: "warn" },
    { expected: ["Critical"], threshold: "error" },
  ] as const)(
    "applies the $threshold logging threshold without suppressing notices or evidence",
    async ({ expected, threshold }) => {
      const stream = new ControlledStream();
      const value = fixture({
        definitions: [
          rule("Informational", "$.match == true", { level: "info" }),
          rule("Warning", "$.match == true", { level: "warn" }),
          rule("Critical", "$.match == true", { level: "error" }),
        ],
        streams: [stream],
      });
      await connectAndStart(value);
      await value.facade.execute(
        updatePreferences({ rules: { logLevel: threshold } }, `${threshold}-threshold`),
      );

      stream.push(message("threshold", '{"match":true}'));
      await vi.waitFor(() => {
        expect(stream.deliveredMessages).toBe(1);
      });

      expect(
        notifications(value.events)
          .at(-1)
          ?.payload.matches.map((match) => match.name),
      ).toEqual(["Informational", "Warning", "Critical"]);
      const detail = matchActivities(value.events).at(-1)?.payload.detail ?? "";
      for (const name of ["Informational", "Warning", "Critical"]) {
        expect(detail.includes(name)).toBe(expected.some((expectedName) => expectedName === name));
      }
      expect(matchActivities(value.events).at(-1)?.payload.severity).toBe("error");
    },
  );

  it("coalesces one delivery burst, truncates names deterministically and reports omissions", async () => {
    const stream = new ControlledStream();
    const value = fixture({
      definitions: Array.from({ length: 12 }, (_value, index) =>
        rule(`Rule ${String(index).padStart(2, "0")}`, "$.match == true", {
          level: index === 11 ? "error" : "warn",
        }),
      ),
      scheduleMessageFlush: (): void => undefined,
      streams: [stream],
    });
    await connectAndStart(value);

    stream.push(message("burst", '{"match":true,"private":"never-log-this"}'));
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(1);
    });
    await value.facade.execute(command("messages.stop", "flush-burst"));

    const notification = notifications(value.events).at(-1)?.payload;
    expect(notification).toMatchObject({
      activeMatchCount: 12,
      highestSeverity: "error",
      omittedMatches: 2,
      topic: "orders",
    });
    expect(notification?.matches).toHaveLength(10);
    expect(notification?.matches.map((match) => match.name)).toEqual(
      Array.from({ length: 10 }, (_value, index) => `Rule ${String(index).padStart(2, "0")}`),
    );
    const activity = matchActivities(value.events).at(-1)?.payload;
    expect(activity?.detail).toContain("2 additional matches omitted");
    expect(JSON.stringify([notification, activity])).not.toMatch(
      /never-log-this|\$\.match|clientSecret/u,
    );
  });

  it("notifies without inventing a name when all active-name evidence was already omitted", async () => {
    const stream = new ControlledStream();
    const value = fixture({
      definitions: [
        ...Array.from({ length: 49 }, (_value, index) =>
          rule(`${"😀".repeat(60)} ${String(index)}`, "compile-failure"),
        ),
        rule(`${"😀".repeat(60)} active`, "active", { level: "error" }),
      ],
      evaluator: new EvidencePressureEvaluator(),
      streams: [stream],
    });
    await connectAndStart(value);

    stream.push(message("omitted-active-name", '{"match":true}'));
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(1);
    });

    expect(batches(value.events).at(-1)?.payload.messages[0]?.ruleEvaluation).toMatchObject({
      activeMatchCount: 1,
      activeMatches: [],
      highestActiveSeverity: "error",
      state: "partial",
    });
    expect(notifications(value.events).at(-1)?.payload).toEqual({
      activeMatchCount: 1,
      highestSeverity: "error",
      matches: [],
      omittedMatches: 1,
      topic: "orders",
    });
    expect(matchActivities(value.events)).toHaveLength(0);
  });

  it("drops optional output with the same bounded queue record", async () => {
    const stream = new ControlledStream();
    const value = fixture({
      scheduleMessageFlush: (): void => undefined,
      streams: [stream],
    });
    await value.facade.execute(command("connection.connect", "connect-drop-alignment"));
    await value.facade.execute(updatePreferences({ stream: { queueDepth: 100 } }, "queue-depth"));
    await value.facade.execute(command("messages.start", "start-drop-alignment"));

    for (let index = 0; index < 101; index += 1) {
      stream.push(message(String(index), '{"p":"h"}'));
    }
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(101);
    });
    await value.facade.execute(command("messages.stop", "flush-drop-alignment"));

    expect(batches(value.events).flatMap((event) => event.payload.messages)).toHaveLength(100);
    expect(notifications(value.events)).toHaveLength(1);
    expect(notifications(value.events).at(-1)?.payload).toMatchObject({
      activeMatchCount: 100,
      matches: [{ count: 100, level: "warn", name: "High" }],
      omittedMatches: 0,
    });
    expect(matchActivities(value.events)).toHaveLength(1);
    expect(matchActivities(value.events).at(-1)?.payload.detail).toContain("100");
  });

  it("publishes partial capacity and fail-open unavailable catalog state before records", async () => {
    const capacityStream = new ControlledStream();
    const capacity = fixture({
      definitions: Array.from(
        { length: KAFKA_LIVE_RULE_LIMITS.applicableRules + 2 },
        (_value, index) => rule(`Rule ${String(index)}`, "$.match == true"),
      ),
      streams: [capacityStream],
    });
    await connectAndStart(capacity, "capacity");

    expect(consumptionStates(capacity.events)[0]?.payload.ruleEvaluation).toEqual({
      applicableRules: KAFKA_LIVE_RULE_LIMITS.applicableRules + 2,
      omittedRules: 2,
      state: "partial",
    });
    capacityStream.push(message("capacity", '{"match":true}'));
    await vi.waitFor(() => {
      expect(capacityStream.deliveredMessages).toBe(1);
    });
    expect(batches(capacity.events).at(-1)?.payload.messages[0]?.ruleEvaluation).toMatchObject({
      evaluatedRules: KAFKA_LIVE_RULE_LIMITS.applicableRules,
      omittedRules: 2,
      state: "partial",
    });

    const unavailableStream = new ControlledStream();
    const unavailable = fixture({
      store: new UnavailableRuleStore(),
      streams: [unavailableStream],
    });
    await connectAndStart(unavailable, "unavailable");
    expect(consumptionStates(unavailable.events)[0]?.payload.ruleEvaluation).toEqual({
      applicableRules: 0,
      omittedRules: 0,
      recovery: "Restore the local rule catalog and retry consumption.",
      state: "unavailable",
    });
    unavailableStream.push(message("unavailable", '{"match":true}'));
    await vi.waitFor(() => {
      expect(unavailableStream.deliveredMessages).toBe(1);
    });
    expect(batches(unavailable.events).at(-1)?.payload.messages[0]?.ruleEvaluation).toMatchObject({
      reason: "catalog-unavailable",
      state: "unavailable",
    });
  });

  it("applies only confirmed catalog mutations to subsequent records", async () => {
    const stream = new ControlledStream();
    const value = fixture({ streams: [stream] });
    await connectAndStart(value);

    stream.push(message("before", '{"p":"low"}'));
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(1);
    });
    expect(batches(value.events).at(-1)?.payload.messages[0]?.ruleEvaluation.activeMatchCount).toBe(
      0,
    );

    await expect(
      value.facade.execute(updateRule(rule("High", '$.p == "low"'), "committed")),
    ).resolves.toMatchObject({ ok: true });
    stream.push(message("after", '{"p":"low"}'));
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(2);
    });
    expect(batches(value.events).at(-1)?.payload.messages[0]?.ruleEvaluation.activeMatches).toEqual(
      [{ level: "warn", name: "High" }],
    );

    await expect(
      value.facade.execute(updateRule(rule("High", "$.p =="), "rejected")),
    ).resolves.toMatchObject({ ok: false });
    stream.push(message("after-rejection", '{"p":"h"}'));
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(3);
    });
    expect(batches(value.events).at(-1)?.payload.messages[0]?.ruleEvaluation.activeMatchCount).toBe(
      0,
    );
  });

  it("redacts and coalesces systemic rule failures while Kafka consumption continues", async () => {
    const stream = new ControlledStream();
    const value = fixture({
      durationNow: (): never => {
        throw new Error("payload-secret and $.private must never enter Activity");
      },
      streams: [stream],
    });
    await connectAndStart(value);
    await value.facade.execute(
      updatePreferences(
        { rules: { loggingEnabled: false, notificationsEnabled: false } },
        "disable-optional-failure-output",
      ),
    );

    stream.push(message("1", '{"private":"payload-secret"}'));
    stream.push(message("2", '{"private":"payload-secret"}'));
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(2);
    });

    expect(
      batches(value.events).map((event) => event.payload.messages[0]?.ruleEvaluation),
    ).toMatchObject([
      { reason: "internal", state: "unavailable" },
      { reason: "internal", state: "unavailable" },
    ]);
    const liveFailures = value.events.filter(
      (event) =>
        event.event === "activity.recorded" && event.payload.operation === "Evaluate live rules",
    );
    expect(liveFailures).toHaveLength(1);
    expect(matchActivities(value.events)).toHaveLength(0);
    expect(notifications(value.events)).toHaveLength(0);
    expect(JSON.stringify(liveFailures)).not.toMatch(/payload-secret|\$\.private/);
  });

  it("rejects an obsolete stream before it can advance live cooldown", async () => {
    const obsolete = new ControlledStream(false);
    const current = new ControlledStream();
    let monotonic = 1_000;
    const value = fixture({
      definitions: [rule("High", '$.p == "h"', { cooldownMs: 10_000 })],
      monotonicNow: (): number => monotonic,
      streams: [obsolete, current],
    });
    await connectAndStart(value, "first");

    const secondStart = value.facade.execute(command("messages.start", "second"));
    await vi.waitFor(() => {
      expect(obsolete.closeCalls).toBeGreaterThan(0);
    });
    obsolete.push(message("obsolete", '{"p":"h"}'));
    await expect(secondStart).resolves.toMatchObject({ ok: true });

    monotonic = 1_001;
    current.push(message("current", '{"p":"h"}'));
    await vi.waitFor(() => {
      expect(current.deliveredMessages).toBe(1);
    });

    expect(batches(value.events).map((event) => event.payload.messages[0]?.id)).toEqual([
      "current",
    ]);
    expect(batches(value.events)[0]?.payload.messages[0]?.ruleEvaluation).toMatchObject({
      activeMatchCount: 1,
      suppressedMatchCount: 0,
    });
  });

  it("splits facade batches when bounded rule evidence crosses the canonical byte limit", async () => {
    const stream = new ControlledStream();
    const value = fixture({
      definitions: Array.from({ length: 50 }, (_value, index) =>
        rule(`${"😀".repeat(60)} ${String(index)}`, "$.match == true"),
      ),
      scheduleMessageFlush: (): void => undefined,
      streams: [stream],
    });
    await connectAndStart(value);
    const payload = JSON.stringify({
      match: true,
      padding: "x".repeat(250 * 1_024),
    });

    for (let index = 0; index < 4; index += 1) {
      stream.push(message(String(index), payload));
    }
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(4);
    });
    await value.facade.execute(command("messages.stop", "stop-bounded-batch"));

    expect(batches(value.events).map((event) => event.payload.messages.length)).toEqual([3, 1]);
    expect(
      batches(value.events).every((event) =>
        event.payload.messages.every(
          (record) =>
            record.ruleEvaluation.state === "partial" && record.ruleEvaluation.omittedEvidence > 0,
        ),
      ),
    ).toBe(true);
  });
});
