import { randomUUID } from "node:crypto";

import {
  KAFKA_LATENCY_LIMITS,
  KAFKA_LATENCY_HISTORY_LIMIT,
  KAFKA_LATENCY_SCHEMA,
  parseKafkaLatencyEvidence,
  parseKafkaLatencyHistorySnapshot,
  parseKafkaLatencyProbeRequest,
  parseKafkaLatencyTextDocument,
  utf8ByteLength,
  type HostTextDocument,
  type KafkaLatencyBrokerMetric,
  type KafkaLatencyHistoryEntry,
  type KafkaLatencyHistoryMetric,
  type KafkaLatencyHistorySnapshot,
  type KafkaLatencyMetricSummary,
  type KafkaLatencyProbeEvidence,
  type KafkaLatencyProbeIssue,
  type KafkaLatencyProbeRequest,
} from "../contracts";

import { ConnectionAttemptSupersededError, NoActiveKafkaConnectionError } from "./session";
import { KafkaLatencyProbeValidationError } from "./latency-errors";
import type {
  KafkaLatencyFetchSample,
  KafkaLatencyProbeMeasurement,
  KafkaLatencyProbeResult,
  KafkaLatencyProbeServiceOptions,
  KafkaLatencyProbeServicePort,
  KafkaLatencyProbeSessionPort,
} from "./latency-types";

interface ConnectionContext {
  readonly connectionBrokers: readonly string[];
  readonly connectionName: string;
  readonly connectionTarget: string;
}

interface ActiveProbe {
  readonly context: ConnectionContext;
  readonly controller: AbortController;
  readonly generation: number;
  readonly operation: Promise<KafkaLatencyProbeResult>;
  readonly request: KafkaLatencyProbeRequest;
}

interface CachedEvidence {
  readonly context: ConnectionContext;
  readonly evidence: KafkaLatencyProbeEvidence;
  fresh: boolean;
}

function rounded(value: number): number {
  return Math.round(value * 1_000) / 1_000;
}

function summarize(values: readonly number[]): KafkaLatencyMetricSummary | null {
  if (values.length === 0) {
    return null;
  }
  const sorted = [...values].sort((left, right) => left - right);
  const average = values.reduce((total, value) => total + value, 0) / values.length;
  return {
    averageMs: rounded(average),
    p95Ms: rounded(sorted[Math.ceil(sorted.length * 0.95) - 1]!),
    samples: values.length,
  };
}

function summarizeBrokers(
  samples: readonly KafkaLatencyFetchSample[],
): readonly KafkaLatencyBrokerMetric[] {
  const grouped = new Map<number, { broker: string; values: number[] }>();
  for (const sample of samples) {
    const existing = grouped.get(sample.nodeId);
    if (existing === undefined) {
      grouped.set(sample.nodeId, { broker: sample.broker, values: [sample.durationMs] });
    } else {
      if (existing.broker !== sample.broker) {
        throw new KafkaLatencyProbeValidationError(
          "Kafka returned conflicting endpoint identities for one broker.",
          `broker-${String(sample.nodeId)}`,
        );
      }
      existing.values.push(sample.durationMs);
    }
  }
  return [...grouped]
    .sort(([left], [right]) => left - right)
    .map(([nodeId, value]) => ({
      broker: value.broker,
      nodeId,
      summary: summarize(value.values)!,
    }));
}

function issue(issues: KafkaLatencyProbeIssue[], candidate: KafkaLatencyProbeIssue): void {
  if (!issues.some((existing) => existing.stage === candidate.stage)) {
    issues.push(candidate);
  }
}

function isCancellation(error: unknown): boolean {
  return (
    error instanceof ConnectionAttemptSupersededError ||
    (error instanceof Error &&
      (error.name === "AbortError" || ("code" in error && error.code === "CANCELLED")))
  );
}

function hasCleanupFailure(error: unknown): boolean {
  return (
    error !== null &&
    typeof error === "object" &&
    "cleanupCause" in error &&
    error.cleanupCause !== undefined
  );
}

function fileSegment(value: string): string {
  const normalized = value
    .normalize("NFKD")
    .replaceAll(/[^A-Za-z0-9._-]+/gu, "-")
    .replaceAll(/^-+|-+$/gu, "")
    .slice(0, 96);
  return normalized.length === 0 ? "unknown" : normalized;
}

export class KafkaLatencyProbeService implements KafkaLatencyProbeServicePort {
  private active: ActiveProbe | undefined;
  private cached: CachedEvidence | undefined;
  private historyContext: ConnectionContext | undefined;
  private historyEntries: readonly KafkaLatencyHistoryEntry[] = [];
  private readonly createRunId;
  private generation = 0;
  private readonly now;

  constructor(
    private readonly session: KafkaLatencyProbeSessionPort,
    options: KafkaLatencyProbeServiceOptions = {},
  ) {
    this.createRunId = options.createRunId ?? randomUUID;
    this.now = options.now ?? ((): Date => new Date());
  }

  activeRequest(): KafkaLatencyProbeRequest | null {
    return this.active?.request ?? null;
  }

  currentEvidence(): KafkaLatencyProbeEvidence | null {
    return this.cached !== undefined && this.cached.fresh && this.cacheMatchesCurrent()
      ? this.cached.evidence
      : null;
  }

  exportDocument(): HostTextDocument {
    const evidence = this.currentEvidence();
    if (evidence === null) {
      throw new KafkaLatencyProbeValidationError(
        "A current ready or partial latency probe must complete before export.",
        "latency-export",
        "Run one latency probe for the active connection and selected topic, then retry export.",
      );
    }
    const content = `${JSON.stringify(evidence, null, 2)}\n`;
    const byteSize = utf8ByteLength(content);
    if (byteSize > KAFKA_LATENCY_LIMITS.exportBytes) {
      throw new KafkaLatencyProbeValidationError(
        `Latency JSON exceeds the ${String(KAFKA_LATENCY_LIMITS.exportBytes)} byte export limit.`,
        "latency-export",
      );
    }
    return parseKafkaLatencyTextDocument(
      {
        byteSize,
        content,
        fileName: `streamskope-latency-${fileSegment(evidence.topic)}-${fileSegment(
          evidence.runId,
        )}.json`,
        mediaType: "application/json",
      },
      "latency.export",
    );
  }

  historySnapshot(): KafkaLatencyHistorySnapshot {
    return parseKafkaLatencyHistorySnapshot({
      connectionName:
        this.historyEntries.length === 0 ? null : (this.historyContext?.connectionName ?? null),
      entries: this.historyEntries,
    });
  }

  invalidate(): void {
    this.generation += 1;
    this.active?.controller.abort();
    if (this.cached !== undefined) {
      this.cached.fresh = false;
    }
    this.historyContext = undefined;
    this.historyEntries = [];
  }

  staleEvidence(): KafkaLatencyProbeEvidence | null {
    return this.cached !== undefined && (!this.cached.fresh || !this.cacheMatchesCurrent())
      ? this.cached.evidence
      : null;
  }

  start(input: KafkaLatencyProbeRequest): Promise<KafkaLatencyProbeResult> {
    if (this.active !== undefined) {
      return Promise.reject(
        new KafkaLatencyProbeValidationError(
          "A Kafka latency probe is already running.",
          input.topic,
          "Stop the active probe or wait for it to complete before starting another.",
        ),
      );
    }
    let request: KafkaLatencyProbeRequest;
    try {
      request = parseKafkaLatencyProbeRequest(input, "latency");
    } catch (error) {
      return Promise.reject(
        new KafkaLatencyProbeValidationError(
          error instanceof Error ? error.message : "The latency request is invalid.",
          "latency-request",
          "Choose a valid topic, message count, timeout, and acknowledgement mode.",
          { cause: error },
        ),
      );
    }
    const context = this.session.activeConnectionContext();
    if (context === null) {
      return Promise.reject(new NoActiveKafkaConnectionError("running a latency probe"));
    }
    this.cached = undefined;
    const generation = ++this.generation;
    const controller = new AbortController();
    const runId = this.createRunId();
    const startedAt = this.now().toISOString();
    const operation = this.completeStart(
      context,
      request,
      runId,
      startedAt,
      generation,
      controller,
    );
    const active: ActiveProbe = {
      context,
      controller,
      generation,
      operation,
      request,
    };
    this.active = active;
    operation.then(
      () => {
        this.clearActive(active);
      },
      () => {
        this.clearActive(active);
      },
    );
    return operation;
  }

  async stop(): Promise<KafkaLatencyProbeRequest | null> {
    const active = this.active;
    if (active === undefined) {
      return null;
    }
    active.controller.abort();
    try {
      await active.operation;
    } catch (error) {
      if (!isCancellation(error) || hasCleanupFailure(error)) {
        throw error;
      }
    }
    return active.request;
  }

  private assertCurrent(activeGeneration: number, context: ConnectionContext): void {
    const current = this.session.activeConnectionContext();
    if (
      activeGeneration !== this.generation ||
      current === null ||
      current.connectionName !== context.connectionName ||
      current.connectionTarget !== context.connectionTarget
    ) {
      throw new ConnectionAttemptSupersededError();
    }
  }

  private assembleEvidence(
    context: ConnectionContext,
    request: KafkaLatencyProbeRequest,
    measurement: KafkaLatencyProbeMeasurement,
    runId: string,
    startedAt: string,
  ): KafkaLatencyProbeEvidence {
    if (measurement.producerDurationsMs.length === 0) {
      throw new KafkaLatencyProbeValidationError(
        "The latency probe produced no valid Kafka write evidence.",
        request.topic,
        "Verify WRITE permission and broker availability, then retry a bounded probe.",
      );
    }
    if (
      measurement.producerDurationsMs.length > request.messageCount ||
      measurement.endToEndDurationsMs.length > request.messageCount ||
      measurement.observedSampleIds.length !== measurement.endToEndDurationsMs.length
    ) {
      throw new KafkaLatencyProbeValidationError(
        "The Kafka adapter returned inconsistent latency sample counts.",
        request.topic,
      );
    }
    const issues = [...measurement.issues];
    if (measurement.producerDurationsMs.length < request.messageCount) {
      issue(issues, {
        recovery: "Verify WRITE permission and retry with a smaller bounded sample count.",
        stage: "produce",
        summary: `${String(
          request.messageCount - measurement.producerDurationsMs.length,
        )} probe produce call(s) did not complete; published records may remain in the topic.`,
      });
    }
    if (measurement.fetchSamples.length === 0) {
      issue(issues, {
        recovery: "Verify READ permission and the broker fetch path.",
        stage: "fetch",
        summary: "No successful Kafka fetch request was observed for the probe consumer.",
      });
    }
    if (measurement.endToEndDurationsMs.length < request.messageCount) {
      issue(issues, {
        recovery: "Increase the bounded timeout or inspect the topic consumer path.",
        stage: "end-to-end",
        summary: `${String(
          request.messageCount - measurement.endToEndDurationsMs.length,
        )} probe record(s) were not observed before completion.`,
      });
    }
    if (measurement.network.tcpConnectMs === null) {
      issue(issues, {
        recovery: "Verify the primary broker endpoint and TCP network path.",
        stage: "tcp",
        summary: "TCP connection latency is unavailable.",
      });
    }
    if (measurement.network.tlsHandshakeMs === null) {
      issue(issues, {
        recovery: "Verify the broker certificate, hostname, CA trust, and TLS network path.",
        stage: "tls",
        summary: "TLS handshake latency is unavailable.",
      });
    }
    const perBroker = summarizeBrokers(measurement.fetchSamples);
    const evidence = parseKafkaLatencyEvidence(
      {
        acknowledgements: request.acknowledgements,
        completedAt: this.now().toISOString(),
        connection: {
          endpoint: context.connectionTarget,
          name: context.connectionName,
        },
        endToEnd: summarize(measurement.endToEndDurationsMs),
        fetch: {
          perBroker,
          summary: summarize(measurement.fetchSamples.map((sample) => sample.durationMs)),
        },
        issues,
        network: measurement.network,
        observedMessages: measurement.endToEndDurationsMs.length,
        producer: {
          semantics: request.acknowledgements === 0 ? "send-completion" : "acknowledged",
          summary: summarize(measurement.producerDurationsMs),
        },
        requestedMessages: request.messageCount,
        runId,
        sampleIds: measurement.observedSampleIds.slice(0, KAFKA_LATENCY_LIMITS.maxSampleIds),
        schema: KAFKA_LATENCY_SCHEMA,
        startedAt,
        topic: request.topic,
      },
      "latency.evidence",
    );
    return evidence;
  }

  private cacheMatchesCurrent(): boolean {
    const current = this.session.activeConnectionContext();
    return (
      current !== null &&
      this.cached !== undefined &&
      current.connectionName === this.cached.context.connectionName &&
      current.connectionTarget === this.cached.context.connectionTarget
    );
  }

  private clearActive(active: ActiveProbe): void {
    if (this.active === active) {
      this.active = undefined;
    }
  }

  private historyMetric(
    metric: KafkaLatencyMetricSummary | null,
  ): KafkaLatencyHistoryMetric | null {
    return metric === null
      ? null
      : {
          averageMs: metric.averageMs,
          p95Ms: metric.p95Ms,
        };
  }

  private recordHistory(
    context: ConnectionContext,
    evidence: KafkaLatencyProbeEvidence,
    state: "partial" | "ready",
  ): void {
    const sameOwner =
      this.historyContext !== undefined &&
      this.historyContext.connectionName === context.connectionName &&
      this.historyContext.connectionTarget === context.connectionTarget;
    const existing = sameOwner ? this.historyEntries : [];
    const entry: KafkaLatencyHistoryEntry = {
      acknowledgements: evidence.acknowledgements,
      completedAt: evidence.completedAt,
      endToEnd: this.historyMetric(evidence.endToEnd),
      fetch: this.historyMetric(evidence.fetch.summary),
      issueCount: evidence.issues.length,
      observedMessages: evidence.observedMessages,
      producer: this.historyMetric(evidence.producer.summary),
      requestedMessages: evidence.requestedMessages,
      runId: evidence.runId,
      state,
      topic: evidence.topic,
    };
    const snapshot = parseKafkaLatencyHistorySnapshot({
      connectionName: context.connectionName,
      entries: [...existing, entry].slice(-KAFKA_LATENCY_HISTORY_LIMIT),
    });
    this.historyContext = context;
    this.historyEntries = snapshot.entries;
  }

  private async completeStart(
    context: ConnectionContext,
    request: KafkaLatencyProbeRequest,
    runId: string,
    startedAt: string,
    generation: number,
    controller: AbortController,
  ): Promise<KafkaLatencyProbeResult> {
    try {
      const measurement = await this.session.runLatencyProbe(request, runId, controller.signal);
      this.assertCurrent(generation, context);
      if (controller.signal.aborted) {
        throw new DOMException("The latency probe was cancelled.", "AbortError");
      }
      const evidence = this.assembleEvidence(context, request, measurement, runId, startedAt);
      this.assertCurrent(generation, context);
      const state = evidence.issues.length === 0 ? "ready" : "partial";
      this.recordHistory(context, evidence, state);
      this.cached = { context, evidence, fresh: true };
      return {
        evidence,
        state,
      };
    } catch (error) {
      if (controller.signal.aborted && !isCancellation(error)) {
        throw new ConnectionAttemptSupersededError();
      }
      throw error;
    }
  }
}
