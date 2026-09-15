import {
  HOST_PROTOCOL_VERSION,
  KAFKA_MESSAGE_LIMITS,
  type ConsumptionState,
  type HostError,
  type HostEvent,
  type KafkaLiveRuleCapability,
  type KafkaStreamMonitorStatus,
} from "../contracts";

import type { ActiveFacadeConsumption } from "./facade-support";

type StreamMonitoring = ActiveFacadeConsumption["streamMonitoring"];
type StreamMetricsChangedEvent = Extract<HostEvent, { readonly event: "streamMetrics.changed" }>;

interface StreamFlushMeasurement {
  readonly batchCount: number;
  readonly completedAtMs: number;
  readonly deliveredMessages: number;
  readonly lastBatchMessages: number;
  readonly publicationDurationMs: number;
  readonly queueWaitMs: number | null;
}

interface StreamPublicationContext {
  readonly connectionName: string | null;
  readonly nextSequence: () => number;
  readonly publish: (event: HostEvent) => void;
  readonly sampledAt: string;
}

export function createStreamMonitoring(startedAtMs: number): StreamMonitoring {
  return {
    batchCount: 0,
    deliveredMessages: 0,
    droppedPerSecond: null,
    lastBatchMessages: 0,
    lastMeasuredAtMs: startedAtMs,
    lastPublicationDurationMs: null,
    lastQueueWaitMs: null,
    lastReportedDeliveredMessages: 0,
    lastReportedDroppedMessages: 0,
    messagesPerSecond: null,
    peakQueuedBytes: 0,
    peakQueuedMessages: 0,
    queueStartedAtMs: null,
  };
}

export function recordStreamQueueStart(
  consumption: ActiveFacadeConsumption,
  monotonicNow: () => number,
): void {
  if (consumption.messages.length === 0 && consumption.streamMonitoring.queueStartedAtMs === null) {
    consumption.streamMonitoring.queueStartedAtMs = monotonicNow();
  }
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
  startedAtMs: number,
): number | null {
  const queuedAtMs = consumption.streamMonitoring.queueStartedAtMs;
  return queuedAtMs === null ? null : Math.max(0, startedAtMs - queuedAtMs);
}

export function recordStreamFlush(
  consumption: ActiveFacadeConsumption,
  measurement: StreamFlushMeasurement,
): number {
  const monitoring = consumption.streamMonitoring;
  const elapsedMs = measurement.completedAtMs - monitoring.lastMeasuredAtMs;
  const deliveredSincePrevious =
    monitoring.deliveredMessages +
    measurement.deliveredMessages -
    monitoring.lastReportedDeliveredMessages;
  const droppedSincePrevious = consumption.droppedMessages - monitoring.lastReportedDroppedMessages;

  monitoring.deliveredMessages += measurement.deliveredMessages;
  monitoring.batchCount += measurement.batchCount;
  if (measurement.lastBatchMessages > 0) {
    monitoring.lastBatchMessages = measurement.lastBatchMessages;
  }
  monitoring.messagesPerSecond =
    elapsedMs > 0 ? (deliveredSincePrevious * 1_000) / elapsedMs : null;
  monitoring.droppedPerSecond = elapsedMs > 0 ? (droppedSincePrevious * 1_000) / elapsedMs : null;
  monitoring.lastPublicationDurationMs = measurement.publicationDurationMs;
  monitoring.lastQueueWaitMs = measurement.queueWaitMs;
  return droppedSincePrevious;
}

export function completeStreamMeasurement(
  consumption: ActiveFacadeConsumption,
  completedAtMs: number,
): void {
  const monitoring = consumption.streamMonitoring;
  monitoring.lastMeasuredAtMs = completedAtMs;
  monitoring.lastReportedDeliveredMessages = monitoring.deliveredMessages;
  monitoring.lastReportedDroppedMessages = consumption.droppedMessages;
  monitoring.queueStartedAtMs = null;
}

function streamMonitorStatus(
  consumption: ActiveFacadeConsumption,
  state: ConsumptionState,
): KafkaStreamMonitorStatus {
  if (state === "failed") {
    return "degraded";
  }
  if (consumption.droppedMessages > 0) {
    return "backpressure";
  }
  return consumption.streamMonitoring.deliveredMessages > 0 ? "nominal" : "idle";
}

export function streamMetricsEvent(
  consumption: ActiveFacadeConsumption,
  state: ConsumptionState,
  connectionName: string | null,
  sampledAt: string,
  sequence: number,
  droppedSincePrevious = Math.max(
    0,
    consumption.droppedMessages - consumption.streamMonitoring.lastReportedDroppedMessages,
  ),
): StreamMetricsChangedEvent | null {
  if (state === "unavailable" || connectionName === null) {
    return null;
  }
  const monitoring = consumption.streamMonitoring;
  return {
    event: "streamMetrics.changed",
    payload: {
      connectionName,
      delivery: {
        batchCount: monitoring.batchCount,
        batchSize: consumption.streamTuning.batchSize,
        deliveredMessages: monitoring.deliveredMessages,
        historySamples: consumption.streamTuning.historySamples,
        intervalMs: consumption.streamTuning.intervalMs,
        lastBatchMessages: monitoring.lastBatchMessages,
        messagesPerSecond: monitoring.messagesPerSecond,
        publicationDurationMs: monitoring.lastPublicationDurationMs,
        queueWaitMs: monitoring.lastQueueWaitMs,
        receivedMessages: consumption.receivedMessages,
        tuningSource: consumption.streamTuning.source,
      },
      queue: {
        capacityBytes: KAFKA_MESSAGE_LIMITS.queuedBytes,
        capacityMessages: consumption.streamTuning.queueDepth,
        currentBytes: consumption.queuedBytes,
        currentMessages: consumption.messages.length,
        droppedMessages: consumption.droppedMessages,
        droppedPerSecond: monitoring.droppedPerSecond,
        droppedSincePrevious,
        peakBytes: Math.max(monitoring.peakQueuedBytes, consumption.queuedBytes),
        peakMessages: Math.max(monitoring.peakQueuedMessages, consumption.messages.length),
      },
      request: consumption.request,
      sampledAt,
      state,
      status: streamMonitorStatus(consumption, state),
    },
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

export function emitStreamMetrics(
  consumption: ActiveFacadeConsumption,
  state: ConsumptionState,
  context: StreamPublicationContext,
  droppedSincePrevious?: number,
): void {
  if (state === "unavailable" || context.connectionName === null) {
    return;
  }
  const event = streamMetricsEvent(
    consumption,
    state,
    context.connectionName,
    context.sampledAt,
    context.nextSequence(),
    droppedSincePrevious,
  );
  if (event !== null) {
    context.publish(event);
  }
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
