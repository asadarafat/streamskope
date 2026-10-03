import {
  OBSERVATION_LIMITS as limits,
  observationIdentity,
  observationLag,
  type KafkaObservation,
  type ObservationCapture,
  type ObservationInput,
  type ObservationSnapshot,
  type TopicHealth,
} from "../contracts/observations";
import { parseObservation, parseObservationInput } from "../contracts/observation-validation";
import type { KafkaConsumerGroupDetails } from "../contracts";

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
    return this.serial(async () => ({
      ...retainObservations(await this.store.load(), this.now()),
      durability: this.store.durability,
    }));
  }
  async clear(): Promise<ObservationSnapshot> {
    this.cancel();
    await this.operation?.catch(() => undefined);
    return this.serial(async () => {
      await this.store.commit({ schemaVersion: 1, series: [] });
      return { schemaVersion: 1, series: [], durability: this.store.durability };
    });
  }
  capture(request: ObservationInput): Promise<ObservationCapture> {
    if (this.operation) return Promise.reject(new Error("An observation is already running."));
    const input = parseObservationInput(request),
      context = this.context(),
      startedAt = this.now();
    if (!context?.connection.observeTopicHealth)
      return Promise.reject(new Error("Connect a supported Kafka adapter."));
    if (
      this.lastAttempt?.context.connection === context.connection &&
      startedAt - this.lastAttempt.at < limits.intervalMs
    )
      return Promise.reject(new Error("Wait ten seconds between observations."));
    this.lastAttempt = { context, at: startedAt };
    this.controller = new AbortController();
    const signal = AbortSignal.any([
      this.controller.signal,
      AbortSignal.timeout(limits.deadlineMs),
    ]);
    this.operation = this.collect(context, input, startedAt, signal)
      .catch((error: unknown) => {
        this.segmentId = crypto.randomUUID();
        throw error;
      })
      .finally(() => {
        this.operation = undefined;
        this.controller = undefined;
      });
    return this.operation;
  }
  private current(context: Context, signal: AbortSignal): void {
    signal.throwIfAborted();
    const current = this.context();
    if (
      !current ||
      current.connection !== context.connection ||
      current.generation !== context.generation
    )
      throw new Error("The connection changed.");
  }
  private async collect(
    context: Context,
    input: ObservationInput,
    startedAt: number,
    signal: AbortSignal,
  ): Promise<ObservationCapture> {
    // Load first: an unreadable durable store must never be silently replaced.
    await this.history();
    this.current(context, signal);
    const health: TopicHealth = await context.connection.observeTopicHealth!(input.topic, signal);
    this.current(context, signal);
    if (health.topic !== input.topic || !health.clusterId || !health.topicId)
      throw new Error("The observation identity is unavailable.");
    let group: KafkaConsumerGroupDetails | undefined;
    if (input.groupId !== null) {
      try {
        group = await context.connection.describeConsumerGroup?.(input.groupId, signal);
      } catch {
        this.current(context, signal);
      }
    }
    this.current(context, signal);
    const groupCoverage =
      input.groupId === null
        ? "not-selected"
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
      const committedOffset = offset?.committedOffset ?? null;
      const lag =
        committedOffset !== null &&
        p.endOffset !== null &&
        BigInt(p.endOffset) >= BigInt(committedOffset)
          ? (BigInt(p.endOffset) - BigInt(committedOffset)).toString()
          : null;
      return { ...p, committedOffset, lag };
    });
    const coverage =
      groupCoverage === "complete" && partitions.some((p) => p.lag === null)
        ? "partial"
        : groupCoverage;
    const requestMs = this.now() - startedAt;
    let sample: KafkaObservation = parseObservation({
      id: crypto.randomUUID(),
      segmentId: this.segmentId,
      startedAt,
      observedAt: this.now(),
      source: "kafka-api",
      requestMs,
      providerCalls: input.groupId === null ? 1 : 2,
      state:
        coverage === "partial" ||
        coverage === "unavailable" ||
        partitions.some((p) => p.endOffset === null)
          ? "partial"
          : "ready",
      groupState: group?.state ?? null,
      members: group?.members.length ?? null,
      brokerCount: health.brokerCount,
      controllerKnown: health.controllerKnown,
      groupCoverage: coverage,
      partitions,
      alerts: [],
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
      const history = retainObservations(await this.store.load(), this.now());
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
      await this.store.commit(retained);
      return {
        series: retained.series.find(
          (s) => observationIdentity(s) === observationIdentity(identity),
        )!,
        durability: this.store.durability,
      };
    });
  }
}
