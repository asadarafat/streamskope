import { describe, expect, it } from "vitest";

import {
  HOST_COMMANDS,
  HOST_EVENTS,
  HOST_PROTOCOL_VERSION,
  KAFKA_LATENCY_ACKNOWLEDGEMENTS,
  KAFKA_LATENCY_ISSUE_STAGES,
  KAFKA_LATENCY_LIMITS,
  KAFKA_LATENCY_STATES,
  HostContractValidationError,
  parseHostCommand,
  parseHostCommandResponse,
  parseHostEvent,
  parseKafkaLatencyEvidence,
} from "../../src/features/kafka/contracts";

const request = {
  acknowledgements: -1,
  messageCount: 20,
  timeoutMs: 10_000,
  topic: "orders.events",
} as const;

const metric = {
  averageMs: 4.25,
  p95Ms: 7.5,
  samples: 20,
} as const;

const evidence = {
  acknowledgements: -1,
  completedAt: "2026-07-25T16:00:01.000Z",
  connection: {
    endpoint: "127.0.0.1:19093",
    name: "local-aio",
  },
  endToEnd: metric,
  fetch: {
    perBroker: [
      {
        broker: "127.0.0.1:19093",
        nodeId: 1,
        summary: { ...metric, samples: 2 },
      },
    ],
    summary: { ...metric, samples: 2 },
  },
  issues: [],
  network: {
    endpoint: "127.0.0.1:19093",
    tcpConnectMs: 1.25,
    tlsHandshakeMs: 2.5,
  },
  observedMessages: 20,
  producer: {
    semantics: "acknowledged",
    summary: metric,
  },
  requestedMessages: 20,
  runId: "run-123",
  sampleIds: ["sample-1", "sample-2"],
  schema: "streamskope.kafka-latency.v1",
  startedAt: "2026-07-25T16:00:00.000Z",
  topic: "orders.events",
} as const;

describe("Kafka latency contract", () => {
  it("declares the bounded protocol vocabulary", () => {
    expect(HOST_PROTOCOL_VERSION).toBe(17);
    expect(HOST_COMMANDS).toEqual(
      expect.arrayContaining(["latency.start", "latency.stop", "latency.export"]),
    );
    expect(HOST_EVENTS).toEqual(expect.arrayContaining(["latency.changed"]));
    expect(HOST_EVENTS).toEqual(
      expect.arrayContaining(["latency.changed", "latency.history.changed"]),
    );
    expect(KAFKA_LATENCY_ACKNOWLEDGEMENTS).toEqual([-1, 0, 1]);
    expect(KAFKA_LATENCY_STATES).toEqual([
      "unavailable",
      "idle",
      "running",
      "ready",
      "partial",
      "cancelled",
      "failed",
      "stale",
    ]);
    expect(KAFKA_LATENCY_ISSUE_STAGES).toEqual([
      "tcp",
      "tls",
      "produce",
      "fetch",
      "end-to-end",
      "cleanup",
    ]);
    expect(KAFKA_LATENCY_LIMITS).toMatchObject({
      defaultMessageCount: 20,
      defaultTimeoutMs: 10_000,
      exportBytes: 1_048_576,
      maxMessageCount: 200,
      maxSampleIds: 10,
      maxTimeoutMs: 60_000,
      minMessageCount: 1,
      minTimeoutMs: 1_000,
    });
  });

  it("parses only bounded connection-owned latency summaries", () => {
    const entry = {
      acknowledgements: -1,
      completedAt: evidence.completedAt,
      endToEnd: { averageMs: 4.25, p95Ms: 7.5 },
      fetch: { averageMs: 4.25, p95Ms: 7.5 },
      issueCount: 0,
      observedMessages: 20,
      producer: { averageMs: 4.25, p95Ms: 7.5 },
      requestedMessages: 20,
      runId: "run-123",
      state: "ready",
      topic: "orders.events",
    } as const;
    const event = {
      event: "latency.history.changed",
      payload: {
        connectionName: "local-aio",
        entries: [entry],
      },
      sequence: 5,
      version: HOST_PROTOCOL_VERSION,
    } as const;

    expect(parseHostEvent(event)).toEqual(event);
    expect(
      parseHostEvent({
        ...event,
        payload: { connectionName: null, entries: [] },
      }),
    ).toEqual({
      ...event,
      payload: { connectionName: null, entries: [] },
    });
    expect(() =>
      parseHostEvent({
        ...event,
        payload: {
          connectionName: "local-aio",
          entries: Array.from({ length: 21 }, (_unused, index) => ({
            ...entry,
            runId: `run-${String(index)}`,
          })),
        },
      }),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseHostEvent({
        ...event,
        payload: {
          connectionName: "local-aio",
          entries: [{ ...entry, payload: "must-not-cross" }],
        },
      }),
    ).toThrow(HostContractValidationError);
  });

  it("parses exact start, stop and export commands", () => {
    expect(
      parseHostCommand({
        command: "latency.start",
        id: "latency-start",
        payload: request,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toEqual({
      command: "latency.start",
      id: "latency-start",
      payload: request,
      version: HOST_PROTOCOL_VERSION,
    });

    for (const command of ["latency.stop", "latency.export"] as const) {
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
  });

  it.each([
    ["zero messages", { ...request, messageCount: 0 }],
    ["too many messages", { ...request, messageCount: 201 }],
    ["fractional messages", { ...request, messageCount: 1.5 }],
    ["short timeout", { ...request, timeoutMs: 999 }],
    ["long timeout", { ...request, timeoutMs: 60_001 }],
    ["unknown acknowledgement", { ...request, acknowledgements: 2 }],
    ["unknown field", { ...request, repeatEveryMs: 5_000 }],
  ])("rejects a request with %s", (_label, payload) => {
    expect(() =>
      parseHostCommand({
        command: "latency.start",
        id: "latency-invalid",
        payload,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("parses ready and partial evidence without manufacturing missing values", () => {
    expect(
      parseHostEvent({
        event: "latency.changed",
        payload: {
          evidence,
          request: null,
          state: "ready",
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      payload: {
        evidence: {
          network: { tcpConnectMs: 1.25, tlsHandshakeMs: 2.5 },
          producer: { semantics: "acknowledged", summary: { samples: 20 } },
        },
        state: "ready",
      },
    });

    const partialEvidence = {
      ...evidence,
      issues: [
        {
          recovery: "Verify broker TLS access.",
          stage: "tls",
          summary: "The TLS handshake was unavailable.",
        },
      ],
      network: {
        ...evidence.network,
        tlsHandshakeMs: null,
      },
    } as const;
    expect(
      parseHostEvent({
        event: "latency.changed",
        payload: {
          evidence: partialEvidence,
          request: null,
          state: "partial",
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      payload: {
        evidence: {
          issues: [{ stage: "tls" }],
          network: { tlsHandshakeMs: null },
        },
        state: "partial",
      },
    });

    for (const payload of [
      { evidence: partialEvidence, request: null, state: "ready" },
      { evidence, request: null, state: "partial" },
      {
        evidence: {
          ...evidence,
          endToEnd: { averageMs: 0, p95Ms: 0, samples: 0 },
          observedMessages: 0,
        },
        request: null,
        state: "ready",
      },
    ]) {
      expect(() =>
        parseHostEvent({
          event: "latency.changed",
          payload,
          sequence: 3,
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toThrow(HostContractValidationError);
    }
  });

  it("rejects impossible one-sample statistics", () => {
    expect(() =>
      parseKafkaLatencyEvidence(
        {
          ...evidence,
          endToEnd: { averageMs: 4, p95Ms: 5, samples: 1 },
          fetch: {
            perBroker: [
              {
                broker: "127.0.0.1:19093",
                nodeId: 1,
                summary: { averageMs: 2, p95Ms: 2, samples: 1 },
              },
            ],
            summary: { averageMs: 2, p95Ms: 2, samples: 1 },
          },
          observedMessages: 1,
          producer: {
            semantics: "acknowledged",
            summary: { averageMs: 3, p95Ms: 3, samples: 1 },
          },
          requestedMessages: 1,
          sampleIds: ["sample-1"],
        },
        "latency.evidence",
      ),
    ).toThrow(HostContractValidationError);
  });

  it.each([
    [
      "missing TCP measurement without a TCP issue",
      {
        ...evidence,
        network: { ...evidence.network, tcpConnectMs: null },
      },
    ],
    [
      "available TLS measurement with a TLS issue",
      {
        ...evidence,
        issues: [
          {
            recovery: "Verify TLS.",
            stage: "tls",
            summary: "TLS is unavailable.",
          },
        ],
      },
    ],
    [
      "missing fetch evidence without a fetch issue",
      {
        ...evidence,
        fetch: { perBroker: [], summary: null },
      },
    ],
    [
      "available fetch evidence with a fetch issue",
      {
        ...evidence,
        issues: [
          {
            recovery: "Verify READ permission.",
            stage: "fetch",
            summary: "Fetch evidence is unavailable.",
          },
        ],
      },
    ],
    [
      "partial producer evidence without a produce issue",
      {
        ...evidence,
        producer: {
          ...evidence.producer,
          summary: { ...metric, samples: 19 },
        },
      },
    ],
    [
      "complete producer evidence with a produce issue",
      {
        ...evidence,
        issues: [
          {
            recovery: "Verify WRITE permission.",
            stage: "produce",
            summary: "Produce evidence is incomplete.",
          },
        ],
      },
    ],
    [
      "partial observation evidence without an end-to-end issue",
      {
        ...evidence,
        endToEnd: { ...metric, samples: 19 },
        observedMessages: 19,
      },
    ],
    [
      "complete observation evidence with an end-to-end issue",
      {
        ...evidence,
        issues: [
          {
            recovery: "Inspect the consumer path.",
            stage: "end-to-end",
            summary: "Observation evidence is incomplete.",
          },
        ],
      },
    ],
  ])("rejects %s", (_label, inconsistentEvidence) => {
    expect(() => parseKafkaLatencyEvidence(inconsistentEvidence, "latency.evidence")).toThrow(
      HostContractValidationError,
    );
  });

  it("parses exact unavailable, idle, running, cancelled, failed and stale states", () => {
    const error = {
      activeStateChanged: false,
      code: "CANCELLED",
      correlationId: "latency-correlation",
      recovery: "Run another probe when ready.",
      retryable: true,
      stage: "kafka",
      summary: "The latency probe was cancelled.",
      target: "orders.events",
    } as const;
    for (const payload of [
      { evidence: null, request: null, state: "unavailable" },
      { evidence: null, request: null, state: "idle" },
      { evidence: null, request, state: "running" },
      { error, evidence: null, request, state: "cancelled" },
      {
        error: { ...error, code: "TIMEOUT", summary: "The latency probe failed." },
        evidence: null,
        request,
        state: "failed",
      },
      { evidence, request: null, state: "stale" },
    ]) {
      expect(
        parseHostEvent({
          event: "latency.changed",
          payload,
          sequence: 4,
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toMatchObject({ event: "latency.changed", payload });
    }
  });

  it("accepts only canonical, secret-free current latency export", () => {
    const content = `${JSON.stringify(evidence, null, 2)}\n`;
    const response = {
      command: "latency.export",
      id: "latency-export",
      ok: true,
      result: {
        correlationId: "latency-correlation",
        document: {
          byteSize: new TextEncoder().encode(content).byteLength,
          content,
          fileName: "streamskope-latency-orders.events-run-123.json",
          mediaType: "application/json",
        },
      },
      version: HOST_PROTOCOL_VERSION,
    } as const;
    expect(parseHostCommandResponse(response)).toEqual(response);

    expect(() =>
      parseHostCommandResponse({
        ...response,
        result: {
          ...response.result,
          document: {
            ...response.result.document,
            content: content.replace(
              '"issues": []',
              '"issues": [],\\n  "clientSecret": "must-not-cross"',
            ),
          },
        },
      }),
    ).toThrow(HostContractValidationError);
  });
});
