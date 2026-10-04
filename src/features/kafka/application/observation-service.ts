import {
  OBSERVATION_LIMITS as limits,
  observationIdentity,
  observationLag,
  type KafkaObservation,
  type ObservationCapture,
  type ObservationInput,
  type ObservationSnapshot,
  type ObservationHistory,
  type TopicHealth,
  type ObservationGroupHealth,
  type ObservationIssue,
} from "../contracts/observations";
import { parseObservation, parseObservationInput } from "../contracts/observation-validation";
import type { KafkaConsumerGroupDetails } from "../contracts";

import { observationRecordWindow, sampleObservationRecords } from "./observation-record-sample";
import {
  ObservationOperationError,
  observationAborted,
  observationIssue,
} from "./observation-errors";
import type { KafkaApplicationSession } from "./session";
import {
  MemoryObservationStore,
  retainObservations,
  type ObservationStore,
} from "./observation-store";

type Context = NonNullable<ReturnType<KafkaApplicationSession["writeContext"]>>;
export class ObservationService {
  private segmentId = crypto.randomUUID();
  private controller: AbortController | undefined;
  private operation: Promise<ObservationCapture> | undefined;
  private queue: Promise<unknown> = Promise.resolve();
  private lastAttempt: { context: Context; at: number } | undefined;
  constructor(
    private readonly context: () => Context | null,
    private readonly store: ObservationStore = new MemoryObservationStore(),
    private readonly now = Date.now,
  ) {}
  cancel(): void {
    this.controller?.abort();
    this.segmentId = crypto.randomUUID();
  }
  idle(): Promise<void> {
    return Promise.allSettled([this.queue, this.operation]).then(() => undefined);
  }
  private serial<T>(fn: () => Promise<T>): Promise<T> {
    const operation = this.queue.then(fn, fn);
    this.queue = operation.catch(() => undefined);
    return operation;
  }
  history(): Promise<ObservationSnapshot> {
    return this.serial(async () => {
      return { ...(await this.loadHistory()), durability: this.store.durability };
    });
  }
  private async loadHistory(): Promise<ObservationHistory> {
    try {
      return retainObservations(await this.store.load(), this.now());
    } catch (cause) {
      throw new ObservationOperationError(
        "OBSERVATION_HISTORY_UNAVAILABLE",
        "Observation history could not be loaded.",
        "Check the history-file permissions or restore a supported backup. The unreadable file has been preserved; explicitly clear history only if you intend to discard it.",
        false,
        { cause, stage: "storage" },
      );
    }
  }
  async clear(): Promise<ObservationSnapshot> {
    this.cancel();
    await this.operation?.catch(() => undefined);
    return this.serial(async () => {
      await this.commit({ schemaVersion: 1, series: [] });
      return { schemaVersion: 1, series: [], durability: this.store.durability };
    });
  }
  capture(request: ObservationInput): Promise<ObservationCapture> {
    if (this.operation)
      return Promise.reject(
        new ObservationOperationError(
          "OBSERVATION_BUSY",
          "An observation is already running.",
          "Wait for it to finish or stop collection before capturing again.",
          true,
        ),
      );
    let input: ObservationInput;
    try {
      input = parseObservationInput(request);
    } catch (cause) {
      return Promise.reject(
        new ObservationOperationError(
          "VALIDATION",
          "The observation selection or threshold is invalid.",
          "Select a valid topic and optional group. Thresholds must be finite, non-negative values.",
          false,
          { cause, stage: "validation" },
        ),
      );
    }
    const context = this.context(),
      startedAt = this.now();
    if (!context)
      return Promise.reject(
        new ObservationOperationError(
          "OBSERVATION_DISCONNECTED",
          "No Kafka connection is active.",
          "Connect a profile, then capture an observation.",
          true,
        ),
      );
    if (!context.connection.observeTopicHealth)
      return Promise.reject(
        new ObservationOperationError(
          "UNSUPPORTED_OPERATION",
          "This Kafka adapter does not support observations.",
          "Connect with a supported Kafka adapter.",
          false,
        ),
      );
    if (
      this.lastAttempt?.context.connection === context.connection &&
      startedAt - this.lastAttempt.at < limits.intervalMs
    )
      return Promise.reject(
        new ObservationOperationError(
          "OBSERVATION_COOLDOWN",
          "Wait ten seconds between observations.",
          "Capture again after the cooldown ends.",
          true,
          { retryAfterMs: Math.ceil(limits.intervalMs - (startedAt - this.lastAttempt.at)) },
        ),
      );
    this.lastAttempt = { context, at: startedAt };
    this.controller = new AbortController();
    const signal = AbortSignal.any([
      this.controller.signal,
      AbortSignal.timeout(limits.deadlineMs),
    ]);
    this.operation = this.collect(context, input, startedAt, signal)
      .catch((error: unknown) => {
        this.segmentId = crypto.randomUUID();
        if (signal.aborted) throw observationAborted(signal, error);
        throw error;
      })
      .finally(() => {
        this.operation = undefined;
        this.controller = undefined;
      });
    return this.operation;
  }
  private current(context: Context, signal: AbortSignal): void {
    if (signal.aborted) throw observationAborted(signal);
    const current = this.context();
    if (
      !current ||
      current.connection !== context.connection ||
      current.generation !== context.generation
    )
      throw new ObservationOperationError(
        "OBSERVATION_DISCONNECTED",
        "The connection changed during observation.",
        "Capture again using the current connection.",
        true,
      );
  }
  private async collect(
    context: Context,
    input: ObservationInput,
    startedAt: number,
    signal: AbortSignal,
  ): Promise<ObservationCapture> {
    // Load first: an unreadable durable store must never be silently replaced.
    const prior = await this.history();
    this.current(context, signal);
    const health: TopicHealth = await context.connection.observeTopicHealth!(input.topic, signal);
    this.current(context, signal);
    if (health.topic !== input.topic || !health.clusterId || !health.topicId)
      throw new Error("The observation identity is unavailable.");
    const issues: ObservationIssue[] = [...(health.issues ?? [])];
    let group: KafkaConsumerGroupDetails | undefined;
    let selectedGroup: ObservationGroupHealth | undefined;
    if (input.groupId !== null) {
      try {
        if (context.connection.observeConsumerGroup) {
          selectedGroup = await context.connection.observeConsumerGroup(
            input.groupId,
            input.topic,
            health.partitions.map((p) => p.partition),
            signal,
          );
          issues.push(...(selectedGroup.issues ?? []));
        } else {
          group = await context.connection.describeConsumerGroup?.(input.groupId, signal);
        }
      } catch (error) {
        this.current(context, signal);
        issues.push(observationIssue(error, "group-offsets"));
      }
    }
    this.current(context, signal);
    const groupCoverage =
      input.groupId === null
        ? "not-selected"
        : selectedGroup?.id === input.groupId
          ? "complete"
          : !group || group.id !== input.groupId
            ? "unavailable"
            : group.omittedOffsets || group.omittedAssignments || group.omittedMembers
              ? "partial"
              : "complete";
    const partitions = health.partitions.map((p) => {
      const offset =
        group?.id === input.groupId
          ? group.offsets.find((o) => o.topic === input.topic && o.partition === p.partition)
          : undefined;
      const committedOffset =
        selectedGroup?.id === input.groupId
          ? (selectedGroup.offsets.find((o) => o.partition === p.partition)?.committedOffset ??
            null)
          : (offset?.committedOffset ?? null);
      const lag =
        committedOffset !== null &&
        p.endOffset !== null &&
        BigInt(p.endOffset) >= BigInt(committedOffset)
          ? (BigInt(p.endOffset) - BigInt(committedOffset)).toString()
          : null;
      return { ...p, committedOffset, lag };
    });
    const coverage =
      selectedGroup &&
      issues.some((i) => i.measurement === "group-offsets") &&
      partitions.every((p) => p.lag === null)
        ? "unavailable"
        : groupCoverage === "complete" && partitions.some((p) => p.lag === null)
          ? "partial"
          : groupCoverage;
    if (coverage === "partial" && !issues.some((i) => i.measurement === "group-offsets")) {
      const ahead = partitions.some(
        (p) =>
          p.committedOffset !== null &&
          p.endOffset !== null &&
          BigInt(p.committedOffset) > BigInt(p.endOffset),
      );
      issues.push({
        measurement: "group-offsets",
        code: "OBSERVATION_INCOMPLETE",
        summary: ahead
          ? "A committed position is ahead of the observed end position; lag is unknown."
          : "One or more selected-topic committed/end positions are missing or omitted; lag is unknown.",
        recovery:
          "Capture again and inspect partition coverage, group permissions and any recent offset resets.",
        retryable: true,
      });
    }
    if (coverage === "unavailable" && !issues.some((i) => i.measurement === "group-offsets"))
      issues.push(observationIssue(undefined, "group-offsets"));
    const candidate = prior.series
      .find(
        (s) =>
          s.clusterId === health.clusterId &&
          s.topicId === health.topicId &&
          s.groupId === input.groupId,
      )
      ?.samples.at(-1);
    const previous =
      candidate &&
      candidate.segmentId === this.segmentId &&
      this.now() >= candidate.observedAt &&
      this.now() - candidate.observedAt <= limits.staleMs
        ? candidate
        : undefined;
    const elapsed = previous ? (this.now() - previous.observedAt) / 1000 : 0;
    const changes =
      previous?.partitions.length === health.partitions.length
        ? health.partitions.map((p) => {
            const before = previous.partitions.find((v) => v.partition === p.partition)?.endOffset;
            return before !== null &&
              before !== undefined &&
              p.endOffset !== null &&
              BigInt(p.endOffset) >= BigInt(before)
              ? BigInt(p.endOffset) - BigInt(before)
              : null;
          })
        : [];
    const growth =
      changes.length && changes.every((n) => n !== null)
        ? changes.reduce<bigint>((n, p) => n + p, 0n)
        : null;
    const windowMs = observationRecordWindow(
      growth !== null && growth <= BigInt(Number.MAX_SAFE_INTEGER) && elapsed > 0
        ? Number(growth) / elapsed
        : null,
    );
    const records = input.sampleRecords
      ? await sampleObservationRecords(
          context.connection,
          input.topic,
          this.now(),
          signal,
          undefined,
          {
            windowMs,
            expectedPartitions: health.partitions.map((p) => p.partition),
            onIssue: (issue): void => {
              issues.push(issue);
            },
          },
        )
      : null;
    if (records && !records.analysisEligible && !issues.some((i) => i.measurement === "records"))
      issues.push({
        measurement: "records",
        code: "OBSERVATION_INCOMPLETE",
        summary: `Record sampling ${records.reason}; key and size diagnoses are unavailable without a complete protected window.`,
        recovery:
          "Capture again. The next window adapts to observed offset growth; capped or incomplete samples remain descriptive only.",
        retryable: true,
      });
    if (records?.analysisEligible && records.unavailableKeys > 0)
      issues.push({
        measurement: "records",
        code: "OBSERVATION_INCOMPLETE",
        summary:
          "Some sampled key identities are protected or unavailable; size measurements remain available, but hot-key analysis is unknown.",
        recovery:
          "Use permitted key access or inspect the available aggregate sizes; do not infer key concentration from masked records.",
        retryable: false,
      });
    this.current(context, signal);
    const requestMs = this.now() - startedAt;
    let sample: KafkaObservation = parseObservation({
      id: crypto.randomUUID(),
      segmentId: this.segmentId,
      startedAt,
      observedAt: this.now(),
      source: "kafka-api",
      requestMs,
      providerCalls: (input.groupId === null ? 1 : 2) + (input.sampleRecords ? 1 : 0),
      state:
        issues.length > 0 ||
        coverage === "partial" ||
        coverage === "unavailable" ||
        (records !== null && records.state !== "complete") ||
        partitions.some((p) => p.endOffset === null)
          ? "partial"
          : "ready",
      groupState: selectedGroup?.state ?? group?.state ?? null,
      members: selectedGroup?.members ?? group?.members.length ?? null,
      issues,
      brokerCount: health.brokerCount,
      controllerKnown: health.controllerKnown,
      groupCoverage: coverage,
      partitions,
      alerts: [],
      records,
    });
    const lag = observationLag(sample);
    sample = {
      ...sample,
      alerts: [
        { metric: "lag" as const, observed: lag, threshold: input.thresholds.lag },
        {
          metric: "requestMs" as const,
          observed: requestMs,
          threshold: input.thresholds.requestMs,
        },
      ].flatMap((v) =>
        v.observed !== null && v.threshold !== null && v.observed > v.threshold
          ? [{ metric: v.metric, observed: v.observed, threshold: v.threshold }]
          : [],
      ),
    };
    return this.serial(async () => {
      this.current(context, signal);
      const history = await this.loadHistory();
      const identity = {
        clusterId: health.clusterId,
        topicId: health.topicId,
        topic: health.topic,
        groupId: input.groupId,
      };
      const previous = history.series.find(
        (s) => observationIdentity(s) === observationIdentity(identity),
      );
      const next = { ...identity, samples: [...(previous?.samples ?? []), sample] };
      const retained = retainObservations(history, this.now(), next);
      this.current(context, signal);
      await this.commit(retained);
      return {
        series: retained.series.find(
          (s) => observationIdentity(s) === observationIdentity(identity),
        )!,
        durability: this.store.durability,
      };
    });
  }
  private async commit(
    history: import("../contracts/observations").ObservationHistory,
  ): Promise<void> {
    try {
      await this.store.commit(history);
    } catch (cause) {
      throw new ObservationOperationError(
        "OBSERVATION_HISTORY_UNAVAILABLE",
        "Observation history could not be saved.",
        "Check the history-file permissions and available disk space, then capture again.",
        true,
        { cause, stage: "storage" },
      );
    }
  }
}
