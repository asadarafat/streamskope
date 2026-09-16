import { describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEvent,
  type KafkaLatencyProbeEvidence,
} from "../../src/features/kafka/contracts";
import type { KafkaLatencyProbeServicePort } from "../../src/features/kafka/application";
import { executeLatencyCommand } from "../../src/features/kafka/facade";

const request = {
  acknowledgements: -1,
  messageCount: 2,
  timeoutMs: 10_000,
  topic: "orders.events",
} as const;

const evidence: KafkaLatencyProbeEvidence = {
  acknowledgements: -1,
  completedAt: "2026-07-25T16:00:01.000Z",
  connection: {
    endpoint: "127.0.0.1:19093",
    name: "local-aio",
  },
  endToEnd: { averageMs: 5, p95Ms: 6, samples: 2 },
  fetch: {
    perBroker: [
      {
        broker: "kafka-1:9093",
        nodeId: 1,
        summary: { averageMs: 2, p95Ms: 3, samples: 2 },
      },
    ],
    summary: { averageMs: 2, p95Ms: 3, samples: 2 },
  },
  issues: [],
  network: {
    endpoint: "127.0.0.1:19093",
    tcpConnectMs: 1,
    tlsHandshakeMs: 2,
  },
  observedMessages: 2,
  producer: {
    semantics: "acknowledged",
    summary: { averageMs: 3, p95Ms: 4, samples: 2 },
  },
  requestedMessages: 2,
  runId: "run-123",
  sampleIds: ["sample-1", "sample-2"],
  schema: "streamskope.kafka-latency.v1",
  startedAt: "2026-07-25T16:00:00.000Z",
  topic: "orders.events",
};

function command(
  name: "latency.export" | "latency.start" | "latency.stop",
): Extract<HostCommand, { readonly command: typeof name }> {
  return {
    command: name,
    id: name,
    payload: name === "latency.start" ? request : {},
    version: HOST_PROTOCOL_VERSION,
  } as Extract<HostCommand, { readonly command: typeof name }>;
}

class FakeLatencyService implements KafkaLatencyProbeServicePort {
  readonly activeRequest = vi.fn<KafkaLatencyProbeServicePort["activeRequest"]>(() => null);
  readonly currentEvidence = vi.fn<KafkaLatencyProbeServicePort["currentEvidence"]>(() => evidence);
  readonly exportDocument = vi.fn<KafkaLatencyProbeServicePort["exportDocument"]>(() => {
    const content = `${JSON.stringify(evidence, null, 2)}\n`;
    return {
      byteSize: new TextEncoder().encode(content).byteLength,
      content,
      fileName: "streamskope-latency-orders.events-run-123.json",
      mediaType: "application/json" as const,
    };
  });
  readonly invalidate = vi.fn<KafkaLatencyProbeServicePort["invalidate"]>();
  readonly historySnapshot = vi.fn<KafkaLatencyProbeServicePort["historySnapshot"]>(() => ({
    connectionName: "local-aio",
    entries: [
      {
        acknowledgements: -1,
        completedAt: evidence.completedAt,
        endToEnd: { averageMs: 5, p95Ms: 6 },
        fetch: { averageMs: 2, p95Ms: 3 },
        issueCount: 0,
        observedMessages: 2,
        producer: { averageMs: 3, p95Ms: 4 },
        requestedMessages: 2,
        runId: "run-123",
        state: "ready",
        topic: "orders.events",
      },
    ],
  }));
  readonly staleEvidence = vi.fn<KafkaLatencyProbeServicePort["staleEvidence"]>(() => null);
  readonly start = vi.fn<KafkaLatencyProbeServicePort["start"]>(() =>
    Promise.resolve({ evidence, state: "ready" as const }),
  );
  readonly stop = vi.fn<KafkaLatencyProbeServicePort["stop"]>(() => Promise.resolve(request));
}

function service(): FakeLatencyService {
  return new FakeLatencyService();
}

function bindings(latency: KafkaLatencyProbeServicePort): {
  readonly activities: Parameters<
    NonNullable<Parameters<typeof executeLatencyCommand>[2]["recordActivity"]>
  >[0][];
  readonly binding: Parameters<typeof executeLatencyCommand>[2];
  readonly events: HostEvent[];
} {
  const events: HostEvent[] = [];
  const activities: Parameters<
    NonNullable<Parameters<typeof executeLatencyCommand>[2]["recordActivity"]>
  >[0][] = [];
  let sequence = 0;
  return {
    activities,
    binding: {
      nextSequence: (): number => ++sequence,
      publish: (event): void => {
        events.push(event);
      },
      recordActivity: (activity): void => {
        activities.push(activity);
      },
      service: latency,
    },
    events,
  };
}

describe("Kafka latency facade", () => {
  it("publishes running then ready and records exact non-secret activity", async () => {
    const latency = service();
    const { activities, binding, events } = bindings(latency);

    const response = await executeLatencyCommand(
      command("latency.start"),
      "correlation-1",
      binding,
    );

    expect(response).toMatchObject({ ok: true, result: { correlationId: "correlation-1" } });
    expect(events).toEqual([
      expect.objectContaining({
        event: "latency.changed",
        payload: { evidence: null, request, state: "running" },
      }),
      expect.objectContaining({
        event: "latency.changed",
        payload: { evidence, request: null, state: "ready" },
      }),
      expect.objectContaining({
        event: "latency.history.changed",
        payload: {
          connectionName: "local-aio",
          entries: [expect.objectContaining({ runId: "run-123", state: "ready" })],
        },
      }),
    ]);
    expect(activities).toHaveLength(1);
    expect(activities[0]).toMatchObject({
      object: "local-aio · orders.events",
      operation: "Run latency probe",
      outcome: "succeeded",
    });
    expect(activities[0]?.detail).toContain("2 synthetic");
    expect(JSON.stringify(activities)).not.toContain("CERTIFICATE");
  });

  it("publishes partial evidence as a warning and exports the exact host document", async () => {
    const partial = {
      ...evidence,
      issues: [
        {
          recovery: "Verify TLS trust.",
          stage: "tls" as const,
          summary: "TLS evidence is unavailable.",
        },
      ],
      network: { ...evidence.network, tlsHandshakeMs: null },
    };
    const latency = service();
    latency.start.mockResolvedValue({ evidence: partial, state: "partial" });
    latency.historySnapshot.mockReturnValue({
      connectionName: "local-aio",
      entries: [
        {
          acknowledgements: -1,
          completedAt: partial.completedAt,
          endToEnd: { averageMs: 5, p95Ms: 6 },
          fetch: { averageMs: 2, p95Ms: 3 },
          issueCount: 1,
          observedMessages: 2,
          producer: { averageMs: 3, p95Ms: 4 },
          requestedMessages: 2,
          runId: "run-123",
          state: "partial",
          topic: "orders.events",
        },
      ],
    });
    const { activities, binding, events } = bindings(latency);
    await executeLatencyCommand(command("latency.start"), "correlation-2", binding);

    expect(events.at(-2)).toMatchObject({
      payload: { evidence: partial, state: "partial" },
    });
    expect(events.at(-1)).toMatchObject({
      event: "latency.history.changed",
      payload: {
        entries: [{ issueCount: 1, state: "partial" }],
      },
    });
    expect(activities.at(-1)).toMatchObject({ severity: "warning" });

    const response = await executeLatencyCommand(
      command("latency.export"),
      "correlation-3",
      binding,
    );
    expect(response).toMatchObject({
      ok: true,
      result: {
        correlationId: "correlation-3",
        document: { fileName: "streamskope-latency-orders.events-run-123.json" },
      },
    });
  });

  it("reports stop idempotently while the start path owns cancelled publication", async () => {
    const latency = service();
    const { binding, events } = bindings(latency);
    await expect(
      executeLatencyCommand(command("latency.stop"), "correlation-stop", binding),
    ).resolves.toMatchObject({ ok: true });
    expect(events).toEqual([]);

    latency.stop.mockResolvedValue(null);
    await expect(
      executeLatencyCommand(command("latency.stop"), "correlation-stop-2", binding),
    ).resolves.toMatchObject({ ok: true });
  });

  it("rejects an overlapping start without replacing the active running state", async () => {
    const latency = service();
    latency.activeRequest.mockReturnValue(request);
    latency.start.mockRejectedValue(
      Object.assign(new Error("A Kafka latency probe is already running."), {
        code: "VALIDATION",
        recovery: "Stop the active probe or wait for it to complete.",
        retryable: false,
        stage: "validation",
        target: "orders.events",
      }),
    );
    const { activities, binding, events } = bindings(latency);

    const response = await executeLatencyCommand(
      command("latency.start"),
      "correlation-overlap",
      binding,
    );

    expect(response).toMatchObject({ error: { code: "VALIDATION" }, ok: false });
    expect(events).toEqual([]);
    expect(activities.at(-1)).toMatchObject({
      outcome: "failed",
      severity: "warning",
    });
  });

  it("publishes a structured engine cancellation as cancelled rather than failed", async () => {
    const latency = service();
    latency.start.mockRejectedValue(
      Object.assign(new Error("The Kafka latency probe was cancelled."), {
        code: "CANCELLED",
        recovery: "Run another bounded probe when ready.",
        retryable: true,
        stage: "kafka",
        target: "orders.events",
      }),
    );
    const { activities, binding, events } = bindings(latency);

    const response = await executeLatencyCommand(
      command("latency.start"),
      "correlation-cancelled",
      binding,
    );

    expect(response).toMatchObject({ error: { code: "CANCELLED" }, ok: false });
    expect(events.at(-1)).toMatchObject({
      payload: {
        error: { code: "CANCELLED" },
        evidence: null,
        request,
        state: "cancelled",
      },
    });
    expect(activities.at(-1)).toMatchObject({
      outcome: "cancelled",
      severity: "warning",
    });
  });

  it("publishes structured failed evidence and recovery without a false ready state", async () => {
    const latency = service();
    latency.start.mockRejectedValue(
      Object.assign(new Error("Kafka denied topic write access."), {
        code: "AUTHORIZATION_DENIED",
        recovery: "Grant WRITE permission and retry.",
        retryable: false,
        stage: "authorization",
        target: "orders.events",
      }),
    );
    const { activities, binding, events } = bindings(latency);
    const response = await executeLatencyCommand(
      command("latency.start"),
      "correlation-failed",
      binding,
    );

    expect(response).toMatchObject({
      error: {
        code: "AUTHORIZATION_DENIED",
        recovery: "Grant WRITE permission and retry.",
      },
      ok: false,
    });
    expect(events.at(-1)).toMatchObject({
      payload: {
        error: { code: "AUTHORIZATION_DENIED" },
        evidence: null,
        request,
        state: "failed",
      },
    });
    expect(
      events.some((event) => event.event === "latency.changed" && event.payload.state === "ready"),
    ).toBe(false);
    expect(activities.at(-1)).toMatchObject({ outcome: "failed", severity: "error" });
  });
});
