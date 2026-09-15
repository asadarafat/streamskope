import { describe, expect, it } from "vitest";

import {
  HOST_COMMANDS,
  HOST_ERROR_CODES,
  HOST_ERROR_STAGES,
  HOST_EVENTS,
  HOST_PROTOCOL_VERSION,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  KAFKA_OPERATIONAL_PREFERENCE_LIMITS,
  KAFKA_OPERATIONAL_PREFERENCE_LOG_LEVELS,
  HostContractValidationError,
  parseHostCommand,
  parseHostCommandResponse,
  parseHostEvent,
  parseKafkaOperationalPreferenceSnapshot,
} from "../../src/kafka/contracts";

const sessionStore = {
  durability: "session",
  state: "ready",
} as const;

const durableStore = {
  durability: "durable",
  state: "ready",
} as const;

const changedPreferences = {
  fetch: {
    maxMessages: 500,
    mode: "newest",
  },
  latency: {
    acknowledgements: -1,
    messageCount: 50,
    runbookUrl: "https://runbooks.example.test/kafka/latency",
    timeoutMs: 30_000,
  },
  rules: {
    logLevel: "warn",
    loggingEnabled: false,
    notificationsEnabled: true,
  },
  stream: {
    batchSize: 50,
    historySamples: 25,
    intervalMs: 100,
    queueDepth: 500,
  },
} as const;

describe("Kafka operational-preference contract", () => {
  it("declares the complete bounded preference vocabulary on protocol version 15", () => {
    expect(HOST_PROTOCOL_VERSION).toBe(17);
    expect(HOST_COMMANDS).toEqual(
      expect.arrayContaining(["preferences.get", "preferences.update", "preferences.reset"]),
    );
    expect(HOST_EVENTS).toContain("preferences.changed");
    expect(HOST_ERROR_CODES).toEqual(
      expect.arrayContaining(["PREFERENCE_STORE_UNAVAILABLE", "PREFERENCE_CORRUPT"]),
    );
    expect(HOST_ERROR_STAGES).toContain("preference");
    expect(KAFKA_OPERATIONAL_PREFERENCE_LOG_LEVELS).toEqual(["error", "warn", "info", "debug"]);
    expect(KAFKA_OPERATIONAL_PREFERENCE_LIMITS).toEqual({
      batchSize: { maximum: 200, minimum: 10 },
      fetchMessages: { maximum: 1_000, minimum: 1 },
      historySamples: { maximum: 400, minimum: 10 },
      intervalMs: { maximum: 500, minimum: 5 },
      latencyMessages: { maximum: 200, minimum: 1 },
      latencyTimeoutMs: { maximum: 60_000, minimum: 1_000 },
      queueDepth: { maximum: 1_000, minimum: 100 },
      runbookCharacters: 2_048,
    });
    expect(KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS).toEqual({
      fetch: { maxMessages: 1_000, mode: "tail" },
      latency: {
        acknowledgements: 1,
        messageCount: 20,
        runbookUrl: null,
        timeoutMs: 10_000,
      },
      rules: {
        logLevel: "info",
        loggingEnabled: true,
        notificationsEnabled: true,
      },
      stream: {
        batchSize: 200,
        historySamples: 50,
        intervalMs: 20,
        queueDepth: 1_000,
      },
    });
  });

  it("parses exact get, reset and grouped update commands", () => {
    for (const command of ["preferences.get", "preferences.reset"] as const) {
      expect(
        parseHostCommand({
          command,
          id: command,
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toEqual({
        command,
        id: command,
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
    }

    expect(
      parseHostCommand({
        command: "preferences.update",
        id: "preferences-update",
        payload: {
          patch: {
            fetch: changedPreferences.fetch,
            latency: changedPreferences.latency,
            rules: changedPreferences.rules,
            stream: changedPreferences.stream,
          },
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toEqual({
      command: "preferences.update",
      id: "preferences-update",
      payload: {
        patch: changedPreferences,
      },
      version: HOST_PROTOCOL_VERSION,
    });
  });

  it("parses ready snapshots and correlated preference results without losing complete values", () => {
    const snapshot = {
      preferences: changedPreferences,
      store: durableStore,
    };
    expect(parseKafkaOperationalPreferenceSnapshot(snapshot)).toEqual(snapshot);
    expect(
      parseHostEvent({
        event: "preferences.changed",
        payload: snapshot,
        sequence: 10,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toEqual({
      event: "preferences.changed",
      payload: snapshot,
      sequence: 10,
      version: HOST_PROTOCOL_VERSION,
    });
    for (const command of ["preferences.get", "preferences.update", "preferences.reset"] as const) {
      expect(
        parseHostCommandResponse({
          command,
          id: command,
          ok: true,
          result: {
            correlationId: `${command}-correlation`,
            snapshot,
          },
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toEqual({
        command,
        id: command,
        ok: true,
        result: {
          correlationId: `${command}-correlation`,
          snapshot,
        },
        version: HOST_PROTOCOL_VERSION,
      });
    }
  });

  it("accepts a labelled factory fallback only with unavailable recovery", () => {
    const unavailable = {
      preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
      store: {
        durability: "durable",
        recovery:
          "Reset Kafka operational preferences or repair application-data permissions, then retry.",
        state: "unavailable",
      },
    } as const;
    expect(parseKafkaOperationalPreferenceSnapshot(unavailable)).toEqual(unavailable);

    for (const invalidStore of [
      { durability: "durable", state: "unavailable" },
      { durability: "session", recovery: "Not needed.", state: "ready" },
      { durability: "volatile", state: "ready" },
    ]) {
      expect(() =>
        parseKafkaOperationalPreferenceSnapshot({
          preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
          store: invalidStore,
        }),
      ).toThrow(HostContractValidationError);
    }
  });

  it.each([
    {
      label: "unknown group",
      patch: { unknown: true },
    },
    {
      label: "unknown fetch field",
      patch: { fetch: { maxMessages: 10, mode: "tail", perPartition: true } },
    },
    {
      label: "zero fetch maximum",
      patch: { fetch: { maxMessages: 0 } },
    },
    {
      label: "fractional queue depth",
      patch: { stream: { queueDepth: 100.5 } },
    },
    {
      label: "batch above canonical maximum",
      patch: { stream: { batchSize: 201 } },
    },
    {
      label: "unsupported acknowledgement",
      patch: { latency: { acknowledgements: 2 } },
    },
    {
      label: "unsafe runbook scheme",
      patch: { latency: { runbookUrl: "http://runbooks.example.test/private" } },
    },
    {
      label: "runbook credentials",
      patch: { latency: { runbookUrl: "https://operator:secret@example.test/runbook" } },
    },
    {
      label: "unknown rule level",
      patch: { rules: { logLevel: "trace" } },
    },
    {
      label: "empty patch",
      patch: {},
    },
  ])("rejects $label before application work", ({ patch }) => {
    expect(() =>
      parseHostCommand({
        command: "preferences.update",
        id: "invalid-preference",
        payload: { patch },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("rejects oversized private runbook data and stale protocol input", () => {
    expect(() =>
      parseHostCommand({
        command: "preferences.update",
        id: "oversized-runbook",
        payload: {
          patch: {
            latency: {
              runbookUrl: `https://example.test/${"x".repeat(
                KAFKA_OPERATIONAL_PREFERENCE_LIMITS.runbookCharacters,
              )}`,
            },
          },
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);

    expect(() =>
      parseHostEvent({
        event: "preferences.changed",
        payload: {
          preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
          store: sessionStore,
        },
        sequence: 1,
        version: 11,
      }),
    ).toThrow(HostContractValidationError);
  });
});
