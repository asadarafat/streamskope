import { describe, expect, it } from "vitest";

import {
  HOST_COMMANDS,
  HOST_ERROR_CODES,
  HOST_EVENTS,
  HOST_PROTOCOL_VERSION,
  KAFKA_TOPIC_CONFIGURATION_LIMITS,
  KAFKA_TOPIC_CONFIGURATION_PRESETS,
  KAFKA_TOPIC_CONFIGURATION_REDACTION,
  HostContractValidationError,
  parseHostCommand,
  parseHostEvent,
} from "../../src/features/kafka/contracts";

const writableEntry = {
  documentation: "The retention time in milliseconds.",
  isDefault: false,
  isSensitive: false,
  name: "retention.ms",
  readOnly: false,
  source: "topic",
  synonyms: [
    {
      name: "retention.ms",
      source: "default",
      value: "604800000",
    },
  ],
  type: "long",
  value: "86400000",
} as const;

const configurationSnapshot = {
  connectionName: "Local validation",
  entries: [writableEntry],
  refreshedAt: "2026-07-25T12:00:00.000Z",
  state: "ready",
  topic: "orders.events",
} as const;

const historyEntry = {
  action: "validate",
  at: "2026-07-25T12:01:00.000Z",
  changes: [
    {
      from: "86400000",
      isSensitive: false,
      name: "retention.ms",
      to: "604800000",
      wasDefault: false,
    },
  ],
  connectionName: "Local validation",
  connectionTarget: "127.0.0.1:19093",
  id: "topic-config-history-1",
  presetId: "retention-7d",
  success: true,
  topic: "orders.events",
} as const;

describe("Kafka topic-configuration contract", () => {
  it("retains the bounded topic-configuration vocabulary on protocol v14", () => {
    expect(HOST_PROTOCOL_VERSION).toBe(17);
    expect(HOST_COMMANDS).toEqual(
      expect.arrayContaining([
        "topicConfiguration.load",
        "topicConfiguration.validate",
        "topicConfiguration.apply",
        "topicConfiguration.history",
      ]),
    );
    expect(HOST_EVENTS).toEqual(
      expect.arrayContaining(["topicConfiguration.changed", "topicConfiguration.history"]),
    );
    expect(HOST_ERROR_CODES).toEqual(
      expect.arrayContaining([
        "TOPIC_NOT_FOUND",
        "INVALID_TOPIC_CONFIG",
        "TOPIC_CONFIG_HISTORY_UNAVAILABLE",
        "TOPIC_CONFIG_HISTORY_CORRUPT",
      ]),
    );
    expect(KAFKA_TOPIC_CONFIGURATION_LIMITS).toEqual({
      changes: 50,
      configurationEntries: 4_096,
      configurationNameCharacters: 512,
      configurationValueCharacters: 65_536,
      documentationCharacters: 16_384,
      historyEntries: 50,
      historyErrorCharacters: 2_048,
      historyVisibleEntries: 40,
      synonymsPerEntry: 128,
      topicCharacters: 512,
    });
    expect(KAFKA_TOPIC_CONFIGURATION_REDACTION).toBe("<redacted>");
    expect(KAFKA_TOPIC_CONFIGURATION_PRESETS).toEqual([
      {
        changes: [
          { name: "cleanup.policy", value: "delete" },
          { name: "retention.ms", value: "604800000" },
          { name: "segment.ms", value: "3600000" },
        ],
        description: "cleanup.policy=delete, retention.ms=7d, segment.ms=1h",
        id: "retention-7d",
        label: "Delete after 7 days",
      },
      {
        changes: [
          { name: "cleanup.policy", value: "delete" },
          { name: "retention.ms", value: "86400000" },
          { name: "segment.ms", value: "1800000" },
        ],
        description: "cleanup.policy=delete, retention.ms=24h, segment.ms=30m",
        id: "retention-24h",
        label: "Short TTL (24h)",
      },
      {
        changes: [
          { name: "cleanup.policy", value: "compact,delete" },
          { name: "delete.retention.ms", value: "86400000" },
          { name: "min.cleanable.dirty.ratio", value: "0.5" },
        ],
        description: "cleanup.policy=compact,delete with 24h delete retention",
        id: "compact-preferred",
        label: "Compact + 24h delete",
      },
      {
        changes: [
          { name: "cleanup.policy", value: "compact" },
          { name: "min.cleanable.dirty.ratio", value: "0.5" },
        ],
        description: "cleanup.policy=compact with conservative dirty ratio",
        id: "compact-only",
        label: "Compaction only",
      },
    ]);
  });

  it("parses exact load, history, validate and apply commands", () => {
    for (const command of ["topicConfiguration.load", "topicConfiguration.history"] as const) {
      expect(
        parseHostCommand({
          command,
          id: command,
          payload: { topic: "orders.events" },
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toEqual({
        command,
        id: command,
        payload: { topic: "orders.events" },
        version: HOST_PROTOCOL_VERSION,
      });
    }

    for (const command of ["topicConfiguration.validate", "topicConfiguration.apply"] as const) {
      expect(
        parseHostCommand({
          command,
          id: command,
          payload: {
            changes: [
              {
                isSensitive: false,
                name: "retention.ms",
                value: "604800000",
              },
            ],
            presetId: "retention-7d",
            topic: "orders.events",
          },
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toEqual({
        command,
        id: command,
        payload: {
          changes: [
            {
              isSensitive: false,
              name: "retention.ms",
              value: "604800000",
            },
          ],
          presetId: "retention-7d",
          topic: "orders.events",
        },
        version: HOST_PROTOCOL_VERSION,
      });
    }
  });

  it("rejects empty, duplicate, oversized, over-capacity and unknown-preset changes", () => {
    const validChange = {
      isSensitive: false,
      name: "retention.ms",
      value: "604800000",
    };
    const invalidPayloads = [
      { changes: [], topic: "orders.events" },
      { changes: [validChange, validChange], topic: "orders.events" },
      {
        changes: [
          {
            ...validChange,
            value: "x".repeat(KAFKA_TOPIC_CONFIGURATION_LIMITS.configurationValueCharacters + 1),
          },
        ],
        topic: "orders.events",
      },
      {
        changes: Array.from(
          { length: KAFKA_TOPIC_CONFIGURATION_LIMITS.changes + 1 },
          (_value, index) => ({
            ...validChange,
            name: `config.${String(index)}`,
          }),
        ),
        topic: "orders.events",
      },
      {
        changes: [validChange],
        presetId: "unknown",
        topic: "orders.events",
      },
    ];

    for (const payload of invalidPayloads) {
      expect(() =>
        parseHostCommand({
          command: "topicConfiguration.validate",
          id: "invalid-topic-config",
          payload,
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toThrow(HostContractValidationError);
    }
  });

  it("parses a safe configuration snapshot and rejects leaked sensitive values", () => {
    expect(
      parseHostEvent({
        event: "topicConfiguration.changed",
        payload: configurationSnapshot,
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toEqual({
      event: "topicConfiguration.changed",
      payload: configurationSnapshot,
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });

    const sensitiveEntry = {
      ...writableEntry,
      isSensitive: true,
      name: "ssl.keystore.password",
      synonyms: [{ name: "ssl.keystore.password", source: "topic", value: null }],
      type: "password",
      value: null,
    } as const;
    expect(
      parseHostEvent({
        event: "topicConfiguration.changed",
        payload: { ...configurationSnapshot, entries: [sensitiveEntry] },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      payload: {
        entries: [{ isSensitive: true, value: null }],
      },
    });

    for (const leakedEntry of [
      { ...sensitiveEntry, value: "broker-secret" },
      {
        ...sensitiveEntry,
        synonyms: [
          {
            name: "ssl.keystore.password",
            source: "topic",
            value: "broker-secret",
          },
        ],
      },
    ]) {
      expect(() =>
        parseHostEvent({
          event: "topicConfiguration.changed",
          payload: { ...configurationSnapshot, entries: [leakedEntry] },
          sequence: 3,
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toThrow(HostContractValidationError);
    }
  });

  it("parses bounded history capability and rejects unredacted sensitive history", () => {
    expect(
      parseHostEvent({
        event: "topicConfiguration.history",
        payload: {
          connectionName: "Local validation",
          entries: [historyEntry],
          store: { durability: "session", state: "ready" },
          topic: "orders.events",
        },
        sequence: 4,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      event: "topicConfiguration.history",
      payload: {
        entries: [{ action: "validate", success: true }],
        store: { durability: "session", state: "ready" },
      },
    });

    expect(() =>
      parseHostEvent({
        event: "topicConfiguration.history",
        payload: {
          connectionName: "Local validation",
          entries: [
            {
              ...historyEntry,
              changes: [
                {
                  from: "old-secret",
                  isSensitive: true,
                  name: "ssl.keystore.password",
                  to: "new-secret",
                  wasDefault: false,
                },
              ],
            },
          ],
          store: { durability: "session", state: "ready" },
          topic: "orders.events",
        },
        sequence: 5,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);

    expect(
      parseHostEvent({
        event: "topicConfiguration.history",
        payload: {
          connectionName: "Local validation",
          entries: [
            {
              ...historyEntry,
              changes: [
                {
                  from: KAFKA_TOPIC_CONFIGURATION_REDACTION,
                  isSensitive: true,
                  name: "ssl.keystore.password",
                  to: KAFKA_TOPIC_CONFIGURATION_REDACTION,
                  wasDefault: false,
                },
              ],
            },
          ],
          store: { durability: "session", state: "ready" },
          topic: "orders.events",
        },
        sequence: 6,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      payload: {
        entries: [{ changes: [{ isSensitive: true }] }],
      },
    });
  });
});
