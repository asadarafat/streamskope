import {
  HOST_PROTOCOL_VERSION,
  KAFKA_MESSAGE_LIMITS,
  KAFKA_STREAM_QUEUE_BYTE_PRESSURE_THRESHOLD,
  type ConsumptionState,
  type HostError,
  type HostEvent,
  type KafkaLiveRuleCapability,
  type KafkaStreamMonitorStatus,
  type KafkaStreamPressureReason,
} from "../contracts";

import type { ActiveFacadeConsumption } from "./facade-support";

type StreamMonitoring = ActiveFacadeConsumption["streamMonitoring"];
type StreamMetricsChangedEvent = Extract<HostEvent, { readonly event: "streamMetrics.changed" }>;

interface StreamFlushMeasurement {
  readonly batchCount: number;
  readonly publishedMessages: number;
  readonly lastBatchMessages: number;
  readonly publicationDurationMs: number;
  readonly queueWaitMs: number | null;
  readonly sampledAt: string;
}

interface StreamPublicationContext {
  readonly connectionName: string | null;
  readonly nextSequence: () => number;
  readonly publish: (event: HostEvent) => void;
  readonly sampledAt: string;
  readonly monotonicNowMs: number;
}

export function createStreamMonitoring(startedAtMs: number): StreamMonitoring {
  return {
    batchCount: 0,
    publishedMessages: 0,
    dropReasons: { countCapacity: 0, byteCapacity: 0, oversized: 0, terminalDiscarded: 0 },
    droppedPerSecond: null,
    droppedSincePrevious: 0,
    lastBatchMessages: 0,
    lastMeasuredAtMs: startedAtMs,
    lastPublicationDurationMs: null,
    lastQueueWaitMs: null,
    lastReportedPublishedMessages: 0,
    lastReportedDroppedMessages: 0,
    messagesPerSecond: null,
    peakQueuedBytes: 0,
    peakQueuedMessages: 0,
    rateSampledAt: null,
    rateWindowMs: null,
    publicationSampledAt: null,
    queueWaitSampledAt: null,
  };
}

export function recordStreamQueueBounds(consumption: ActiveFacadeConsumption): void {
  const monitoring = consumption.streamMonitoring;
  monitoring.peakQueuedMessages = Math.max(
    monitoring.peakQueuedMessages,
    consumption.messages.length,
  );
  monitoring.peakQueuedBytes = Math.max(monitoring.peakQueuedBytes, consumption.queuedBytes);
}

export function streamQueueWaitMs(
  consumption: ActiveFacadeConsumption,
  nowMs: number,
): number | null {
  const oldest = consumption.messages[0];
  return oldest === undefined ? null : Math.max(0, nowMs - oldest.enqueuedAtMs);
}

/** Last actual nonempty publication, not the timestamp of a later heartbeat. */
export function recordStreamFlush(
  consumption: ActiveFacadeConsumption,
  measurement: StreamFlushMeasurement,
): void {
  if (measurement.publishedMessages === 0) return;
  const monitoring = consumption.streamMonitoring;
  monitoring.publishedMessages += measurement.publishedMessages;
  monitoring.batchCount += measurement.batchCount;
  monitoring.lastBatchMessages = measurement.lastBatchMessages;
  monitoring.lastPublicationDurationMs = measurement.publicationDurationMs;
  monitoring.lastQueueWaitMs = measurement.queueWaitMs;
  monitoring.publicationSampledAt = measurement.sampledAt;
  monitoring.queueWaitSampledAt = measurement.sampledAt;
}

/** Rates share a monotonic observation window, including windows with no messages. */
export function recordStreamSample(
  consumption: ActiveFacadeConsumption,
  nowMs: number,
  sampledAt: string,
): void {
  const monitoring = consumption.streamMonitoring;
  const elapsedMs = nowMs - monitoring.lastMeasuredAtMs;
  if (elapsedMs <= 0) return;
  monitoring.droppedSincePrevious =
    consumption.droppedMessages - monitoring.lastReportedDroppedMessages;
  monitoring.messagesPerSecond =
    ((monitoring.publishedMessages - monitoring.lastReportedPublishedMessages) * 1_000) / elapsedMs;
  monitoring.droppedPerSecond = (monitoring.droppedSincePrevious * 1_000) / elapsedMs;
  monitoring.rateWindowMs = elapsedMs;
  monitoring.rateSampledAt = sampledAt;
  monitoring.lastMeasuredAtMs = nowMs;
  monitoring.lastReportedPublishedMessages = monitoring.publishedMessages;
  monitoring.lastReportedDroppedMessages = consumption.droppedMessages;
}

function currentPressure(
  consumption: ActiveFacadeConsumption,
  state: ConsumptionState,
): KafkaStreamPressureReason[] {
  if (state === "stopped" || state === "complete" || state === "failed") return [];
  const reasons: KafkaStreamPressureReason[] = [];
  if (consumption.presentationPaused) reasons.push("transport");
  if (consumption.messages.length >= consumption.streamTuning.queueDepth)
    reasons.push("count-capacity");
  if (consumption.queuedBytes >= KAFKA_STREAM_QUEUE_BYTE_PRESSURE_THRESHOLD)
    reasons.push("byte-capacity");
  return reasons;
}

function streamMonitorStatus(
  consumption: ActiveFacadeConsumption,
  state: ConsumptionState,
  pressure: readonly KafkaStreamPressureReason[],
): KafkaStreamMonitorStatus {
  if (state === "failed") return "degraded";
  if (pressure.length > 0) return "backpressure";
  return consumption.streamMonitoring.publishedMessages > 0 &&
    consumption.streamMonitoring.messagesPerSecond !== 0
    ? "nominal"
    : "idle";
}

export function streamMetricsEvent(
  consumption: ActiveFacadeConsumption,
  state: ConsumptionState,
  connectionName: string | null,
  sampledAt: string,
  sequence: number,
  monotonicNowMs: number,
): StreamMetricsChangedEvent | null {
  if (state === "unavailable" || connectionName === null) return null;
  const monitoring = consumption.streamMonitoring;
  const pressureReasons = currentPressure(consumption, state);
  return {
    event: "streamMetrics.changed",
    payload: {
      operationId: consumption.operationId,
      connectionName,
      delivery: {
        batchCount: monitoring.batchCount,
        batchSize: consumption.streamTuning.batchSize,
        publishedMessages: monitoring.publishedMessages,
        historySamples: consumption.streamTuning.historySamples,
        intervalMs: consumption.streamTuning.intervalMs,
        lastBatchMessages: monitoring.lastBatchMessages,
        messagesPerSecond: monitoring.messagesPerSecond,
        publicationDurationMs: monitoring.lastPublicationDurationMs,
        queueWaitMs: monitoring.lastQueueWaitMs,
        receivedMessages: consumption.receivedMessages,
        tuningSource: consumption.streamTuning.source,
        rateSampledAt: monitoring.rateSampledAt,
        rateWindowMs: monitoring.rateWindowMs,
        publicationSampledAt: monitoring.publicationSampledAt,
        queueWaitSampledAt: monitoring.queueWaitSampledAt,
      },
      queue: {
        capacityBytes: KAFKA_MESSAGE_LIMITS.queuedBytes,
        capacityMessages: consumption.streamTuning.queueDepth,
        currentBytes: consumption.queuedBytes,
        currentMessages: consumption.messages.length,
        droppedMessages: consumption.droppedMessages,
        droppedPerSecond: monitoring.droppedPerSecond,
        droppedSincePrevious: monitoring.droppedSincePrevious,
        peakBytes: Math.max(monitoring.peakQueuedBytes, consumption.queuedBytes),
        peakMessages: Math.max(monitoring.peakQueuedMessages, consumption.messages.length),
        oldestMessageAgeMs: streamQueueWaitMs(consumption, monotonicNowMs),
        pressureReasons,
        dropReasons: { ...monitoring.dropReasons },
      },
      request: consumption.request,
      sampledAt,
      state,
      status: streamMonitorStatus(consumption, state, pressureReasons),
    },
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

export function emitStreamMetrics(
  consumption: ActiveFacadeConsumption,
  state: ConsumptionState,
  context: StreamPublicationContext,
): void {
  if (state === "unavailable" || context.connectionName === null) return;
  const event = streamMetricsEvent(
    consumption,
    state,
    context.connectionName,
    context.sampledAt,
    context.nextSequence(),
    context.monotonicNowMs,
  );
  if (event !== null) context.publish(event);
}

export function emitConsumptionState(
  consumption: ActiveFacadeConsumption,
  state: ConsumptionState,
  error: HostError | undefined,
  ruleEvaluation: KafkaLiveRuleCapability,
  context: StreamPublicationContext,
): void {
  consumption.state = state;
  context.publish({
    event: "consumption.state",
    payload: {
      ...(consumption.coverage === undefined ? {} : { coverage: consumption.coverage }),
      ...(consumption.searchProgress === undefined
        ? {}
        : { searchProgress: consumption.searchProgress }),
      droppedMessages: consumption.droppedMessages,
      ...(error === undefined ? {} : { error }),
      receivedMessages: consumption.receivedMessages,
      request: consumption.request,
      ruleEvaluation,
      state,
    },
    sequence: context.nextSequence(),
    version: HOST_PROTOCOL_VERSION,
  });
  emitStreamMetrics(consumption, state, context);
}
