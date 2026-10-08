import { describe, expect, it } from "vitest";

import {
  HOST_COMMANDS,
  HOST_EVENTS,
  HOST_PROTOCOL_VERSION,
  KAFKA_MESSAGE_LIMITS,
  HostContractValidationError,
  parseCorrelatedHostResponse,
  parseHostCommand,
  parseHostCommandResponse,
  parseHostEvent,
} from "../../src/features/kafka/contracts";

const localConnection = {
  brokers: ["127.0.0.1:19093"],
  name: "Local validation",
  oauth: {
    clientId: "admin",
    clientSecret: "fixture-secret",
    scope: "kafka",
    tokenEndpoint: "http://127.0.0.1:15000/rest-gateway/rest/api/v1/auth/token",
  },
  tls: {
    caPem: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
    enabled: true,
  },
} as const;

const zeroRuleEvaluation = {
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
} as const;

describe("Kafka host contract", () => {
  it("declares the complete current command and event vocabulary", () => {
    expect(HOST_PROTOCOL_VERSION).toBe(53);
    expect(HOST_COMMANDS).toEqual([
      "relationships.capture",
      "relationships.cancel",
      "observations.capture",
      "observations.history",
      "observations.cancel",
      "observations.clear",
      "connect.list",
      "connect.load",
      "connect.validate",
      "connect.review",
      "connect.apply",
      "environments.capture",
      "environments.review",
      "environments.apply",
      "schemas.client",
      "records.replay.review",
      "records.replay.apply",
      "records.replay.cancel",
      "records.trace",
      "records.trace.cancel",
      "records.decode",
      "schemas.inspect",
      "schemas.samples",
      "records.batch.review",
      "records.batch.apply",
      "records.batch.cancel",
      "connection.test",
      "connection.connect",
      "connection.disconnect",
      "profiles.list",
      "profiles.binding.get",
      "profiles.create",
      "profiles.update",
      "profiles.test",
      "profiles.delete",
      "profiles.connect",
      "recipes.list",
      "recipes.create",
      "recipes.update",
      "recipes.delete",
      "recipes.usage",
      "recipes.duplicate",
      "recipes.import.preview",
      "recipes.export",
      "recipes.legacy.preview",
      "recipes.legacy.convert",
      "preferences.get",
      "preferences.update",
      "preferences.reset",
      "rules.list",
      "rules.create",
      "rules.update",
      "rules.delete",
      "rules.validate",
      "rules.evaluate",
      "topics.list",
      "writes.review",
      "writes.apply",
      "consumerGroups.reset.review",
      "consumerGroups.reset.apply",
      "consumerGroups.list",
      "consumerGroups.load",
      "schemas.list",
      "schemas.load",
      "schemas.compatibility.check",
      "schemas.register",
      "schemas.delete",
      "acls.access.explain",
      "acls.change.review",
      "acls.change.apply",
      "acls.list",
      "acls.create",
      "acls.delete",
      "transforms.list",
      "transforms.load",
      "transforms.logs.load",
      "transforms.delete",
      "topicConfiguration.load",
      "topicConfiguration.validate",
      "topicConfiguration.apply",
      "topicConfiguration.history",
      "clusterDetails.load",
      "clusterDetails.export",
      "latency.start",
      "latency.stop",
      "latency.export",
      "queries.list",
      "queries.put",
      "queries.delete",
      "messages.start",
      "messages.stop",
      "trustAcquisition.hostKey.discover",
      "trustAcquisition.capabilities",
      "trustAcquisition.editor.open",
      "trustAcquisition.editor.advance",
      "trustAcquisition.editor.close",
      "trustAcquisition.apply",
      "trustAcquisition.material.fetch",
      "trustAcquisition.https.fetch",
      "trustAcquisition.discard",
      "trustAcquisition.cancel",
      "plugins.list",
      "plugins.catalog",
      "plugins.delivery",
      "plugins.network.get",
      "plugins.network.update",
      "plugins.network.test",
      "plugins.network.cancel",
      "plugins.package.inspect",
      "plugins.package.change.prepare",
      "plugins.package.install",
      "plugins.package.discard",
      "plugins.change.prepare",
      "plugins.renderer.failed",
      "plugins.install",
      "plugins.retry",
      "plugins.remove",
      "plugins.restart",
      "plugins.exit.prepare",
      "plugins.exit.resolve",
      "plugin.execute",
    ]);
    expect(HOST_EVENTS).toEqual([
      "backend.availability",
      "plugin.event",
      "plugins.changed",
      "plugins.network.progress",
      "connection.state",
      "topics.changed",
      "consumerGroups.changed",
      "consumerGroup.changed",
      "schemas.changed",
      "schema.changed",
      "schemaCompatibility.changed",
      "acls.changed",
      "transforms.changed",
      "transform.changed",
      "transformLogs.changed",
      "consumption.state",
      "messages.batch",
      "activity.recorded",
      "profiles.changed",
      "recipes.changed",
      "preferences.changed",
      "rules.changed",
      "rules.evaluation",
      "rules.notification",
      "topicConfiguration.changed",
      "topicConfiguration.history",
      "clusterDetails.changed",
      "latency.changed",
      "latency.history.changed",
      "streamMetrics.changed",
    ]);
  });

  it.each([51, 52])(
    "rejects protocol-%s renderer traffic at every current host boundary",
    (version) => {
      const command = {
        command: "observations.cancel",
        id: "previous-renderer",
        payload: {},
        version,
      };
      expect(() => parseHostCommand(command)).toThrow(
        `command.version: must equal ${HOST_PROTOCOL_VERSION}`,
      );
      expect(() =>
        parseHostCommandResponse({
          command: command.command,
          id: command.id,
          ok: true,
          result: { correlationId: "previous-renderer" },
          version,
        }),
      ).toThrow(`response.version: must equal ${HOST_PROTOCOL_VERSION}`);
      expect(() =>
        parseHostEvent({
          event: "connection.state",
          payload: { connectionName: "Local validation", state: "connected" },
          sequence: 1,
          version,
        }),
      ).toThrow(`event.version: must equal ${HOST_PROTOCOL_VERSION}`);
    },
  );

  it("keeps unimplemented integration names outside the public host vocabulary", () => {
    expect(HOST_COMMANDS.filter((name) => name.startsWith("integrations."))).toEqual([]);
    expect(HOST_EVENTS.filter((name) => name.startsWith("integrations."))).toEqual([]);
    expect(() =>
      parseHostCommand({
        command: "integrations.list",
        id: "declined-integration-command",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseHostEvent({
        event: "integrations.changed",
        payload: {},
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("parses a versioned secure connection command without dropping its secret", () => {
    expect(
      parseHostCommand({
        command: "connection.test",
        id: "request-1",
        payload: localConnection,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toEqual({
      command: "connection.test",
      id: "request-1",
      payload: localConnection,
      version: HOST_PROTOCOL_VERSION,
    });
  });

  it.each([
    {
      expected: {
        maxMessages: 1_000,
        mode: "tail",
        topic: "orders.events",
      },
      label: "Tail",
    },
    {
      expected: {
        maxMessages: 100,
        mode: "newest",
        topic: "orders.events",
      },
      label: "Newest N",
    },
    {
      expected: {
        maxMessages: 10,
        mode: "earliest",
        topic: "orders.events",
      },
      label: "First N",
    },
    {
      expected: {
        endTimeMs: 1_722_000_120_000,
        maxMessages: 500,
        mode: "time-window",
        startTimeMs: 1_722_000_000_000,
        topic: "orders.events",
      },
      label: "time window",
    },
  ])("parses one explicit bounded $label message request", ({ expected }) => {
    expect(
      parseHostCommand({
        command: "messages.start",
        id: `request-${expected.mode}`,
        payload: expected,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toEqual({
      command: "messages.start",
      id: `request-${expected.mode}`,
      payload: expected,
      version: HOST_PROTOCOL_VERSION,
    });
  });

  it.each([
    {
      label: "zero result limit",
      payload: { maxMessages: 0, mode: "tail", topic: "orders.events" },
    },
    {
      label: "fractional result limit",
      payload: { maxMessages: 1.5, mode: "tail", topic: "orders.events" },
    },
    {
      label: "result limit above the canonical retained bound",
      payload: {
        maxMessages: KAFKA_MESSAGE_LIMITS.retainedMessages + 1,
        mode: "newest",
        topic: "orders.events",
      },
    },
    {
      label: "equal time boundaries",
      payload: {
        endTimeMs: 100,
        maxMessages: 10,
        mode: "time-window",
        startTimeMs: 100,
        topic: "orders.events",
      },
    },
    {
      label: "reversed time boundaries",
      payload: {
        endTimeMs: 100,
        maxMessages: 10,
        mode: "time-window",
        startTimeMs: 200,
        topic: "orders.events",
      },
    },
    {
      label: "missing time boundary",
      payload: {
        endTimeMs: 200,
        maxMessages: 10,
        mode: "time-window",
        topic: "orders.events",
      },
    },
    {
      label: "time fields on Tail",
      payload: {
        endTimeMs: 200,
        maxMessages: 10,
        mode: "tail",
        startTimeMs: 100,
        topic: "orders.events",
      },
    },
    {
      label: "unknown fetch mode",
      payload: { maxMessages: 10, mode: "committed", topic: "orders.events" },
    },
  ])("rejects an explicit message request with $label", ({ payload }) => {
    expect(() =>
      parseHostCommand({
        command: "messages.start",
        id: "request-invalid-fetch",
        payload,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it.each([
    {
      label: "unsupported version",
      value: {
        command: "connection.disconnect",
        id: "request-2",
        payload: {},
        version: 4,
      },
    },
    {
      label: "unknown command",
      value: {
        command: "cluster.delete",
        id: "request-3",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      },
    },
    {
      label: "empty broker list",
      value: {
        command: "connection.connect",
        id: "request-4",
        payload: { ...localConnection, brokers: [] },
        version: HOST_PROTOCOL_VERSION,
      },
    },
    {
      label: "malformed OAuth secret",
      value: {
        command: "connection.test",
        id: "request-5",
        payload: {
          ...localConnection,
          oauth: { ...localConnection.oauth, clientSecret: 42 },
        },
        version: HOST_PROTOCOL_VERSION,
      },
    },
    {
      label: "unexpected payload field",
      value: {
        command: "connection.disconnect",
        id: "request-6",
        payload: { force: true },
        version: HOST_PROTOCOL_VERSION,
      },
    },
  ])("rejects $label", ({ value }) => {
    expect(() => parseHostCommand(value)).toThrow(HostContractValidationError);
  });

  it("parses only the owned renderer-visible error shape", () => {
    expect(
      parseHostCommandResponse({
        command: "connection.test",
        error: {
          activeStateChanged: false,
          code: "OAUTH_REJECTED",
          correlationId: "correlation-1",
          recovery: "Check the token endpoint and client credentials.",
          retryable: false,
          stage: "oauth",
          summary: "OAuth credentials were rejected.",
          target: "http://127.0.0.1:15000",
        },
        id: "request-7",
        ok: false,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      error: {
        code: "OAUTH_REJECTED",
        stage: "oauth",
      },
      ok: false,
    });

    expect(() =>
      parseHostCommandResponse({
        command: "connection.test",
        error: {
          activeStateChanged: false,
          code: "OAUTH_REJECTED",
          correlationId: "correlation-1",
          recovery: "Retry.",
          retryable: false,
          stack: "vendor stack",
          stage: "oauth",
          summary: "Rejected.",
        },
        id: "request-8",
        ok: false,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it.each(["trustAcquisition.editor.open", "recipes.import.preview", "plugin.execute"])(
    "rejects a plain acknowledgement for %s",
    (command) => {
      expect(() =>
        parseHostCommandResponse({
          command,
          id: "structured-result",
          ok: true,
          result: { correlationId: "structured-result" },
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toThrow(HostContractValidationError);
    },
  );

  it("rejects a response that does not correlate to its command", () => {
    const command = parseHostCommand({
      command: "connection.disconnect",
      id: "request-9",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });

    expect(() =>
      parseCorrelatedHostResponse(
        {
          command: "topics.list",
          id: "request-10",
          ok: true,
          result: { correlationId: "correlation-2" },
          version: HOST_PROTOCOL_VERSION,
        },
        command,
      ),
    ).toThrow(HostContractValidationError);
  });

  it("parses sequenced events and rejects undeclared event names", () => {
    expect(
      parseHostEvent({
        event: "connection.state",
        payload: {
          connectionName: "Local validation",
          state: "connected",
        },
        sequence: 4,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      event: "connection.state",
      sequence: 4,
    });

    expect(() =>
      parseHostEvent({
        event: "cluster.deleted",
        payload: {},
        sequence: 5,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("parses the authoritative fetch request and received count in lifecycle state", () => {
    expect(
      parseHostEvent({
        event: "consumption.state",
        payload: {
          droppedMessages: 0,
          receivedMessages: 42,
          request: {
            maxMessages: 100,
            mode: "newest",
            topic: "orders.events",
          },
          ruleEvaluation: {
            applicableRules: 0,
            omittedRules: 0,
            state: "ready",
          },
          state: "complete",
        },
        sequence: 5,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      event: "consumption.state",
      payload: {
        receivedMessages: 42,
        request: {
          maxMessages: 100,
          mode: "newest",
          topic: "orders.events",
        },
        state: "complete",
      },
    });
  });

  it("preserves valid zero-length Kafka data in a message event", () => {
    expect(
      parseHostEvent({
        event: "messages.batch",
        payload: {
          droppedMessages: 0,
          messages: [
            {
              headers: { empty: "" },
              id: "test:0:1",
              key: "",
              offset: "1",
              originalByteSize: 0,
              partition: 0,
              payload: "",
              preview: "",
              ruleEvaluation: zeroRuleEvaluation,
              timestamp: "2026-07-25T11:00:00.000Z",
              topic: "test",
              truncated: false,
            },
          ],
          topic: "test",
        },
        sequence: 6,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      payload: {
        messages: [{ headers: { empty: "" }, key: "", payload: "", preview: "" }],
      },
    });
  });

  it("rejects a message batch whose retained UTF-8 data exceeds the byte contract", () => {
    const payload = "x".repeat(600 * 1_024);
    expect(() =>
      parseHostEvent({
        event: "messages.batch",
        payload: {
          droppedMessages: 0,
          messages: [
            {
              headers: {},
              id: "test:0:1",
              key: null,
              offset: "1",
              originalByteSize: payload.length,
              partition: 0,
              payload,
              preview: payload.slice(0, KAFKA_MESSAGE_LIMITS.previewBytes),
              ruleEvaluation: zeroRuleEvaluation,
              timestamp: "2026-07-25T11:00:00.000Z",
              topic: "test",
              truncated: false,
            },
            {
              headers: {},
              id: "test:0:2",
              key: null,
              offset: "2",
              originalByteSize: payload.length,
              partition: 0,
              payload,
              preview: payload.slice(0, KAFKA_MESSAGE_LIMITS.previewBytes),
              ruleEvaluation: zeroRuleEvaluation,
              timestamp: "2026-07-25T11:00:00.000Z",
              topic: "test",
              truncated: false,
            },
          ],
          topic: "test",
        },
        sequence: 7,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("enforces message limits as UTF-8 bytes rather than JavaScript characters", () => {
    const payload = "😀".repeat(KAFKA_MESSAGE_LIMITS.messageBytes / 2);
    expect(payload).toHaveLength(KAFKA_MESSAGE_LIMITS.messageBytes);

    expect(() =>
      parseHostEvent({
        event: "messages.batch",
        payload: {
          droppedMessages: 0,
          messages: [
            {
              headers: {},
              id: "test:0:3",
              key: null,
              offset: "3",
              originalByteSize: KAFKA_MESSAGE_LIMITS.messageBytes * 2,
              partition: 0,
              payload,
              preview: "",
              ruleEvaluation: zeroRuleEvaluation,
              timestamp: "2026-07-25T11:00:00.000Z",
              topic: "test",
              truncated: false,
            },
          ],
          topic: "test",
        },
        sequence: 8,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });
});
