import { KAFKA_MESSAGE_LIMITS, type KafkaFetchRequest } from "./types";
import { KAFKA_OPERATIONAL_PREFERENCE_LIMITS } from "./operational-preference-types";
import {
  KAFKA_STREAM_MONITOR_STATES,
  KAFKA_STREAM_MONITOR_STATUSES,
  KAFKA_STREAM_TUNING_SOURCES,
  type KafkaStreamDeliveryMetrics,
  type KafkaStreamMonitorSnapshot,
  type KafkaStreamQueueMetrics,
} from "./stream-monitor-types";
import { HostContractValidationError } from "./validation-error";
import {
  canonicalIsoTimestamp,
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  nullableText,
  record,
} from "./validation-primitives";

type FetchRequestParser = (value: unknown, path: string) => KafkaFetchRequest;

function nonNegativeFinite(value: unknown, path: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new HostContractValidationError(path, "must be a finite non-negative number");
  }
  return value;
}

function nullableNonNegativeFinite(value: unknown, path: string): number | null {
  return value === null ? null : nonNegativeFinite(value, path);
}

function boundedPreferenceInteger(
  value: unknown,
  path: string,
  limits: { readonly maximum: number; readonly minimum: number },
): number {
  const parsed = nonNegativeInteger(value, path);
  if (parsed < limits.minimum || parsed > limits.maximum) {
    throw new HostContractValidationError(
      path,
      `must be between ${String(limits.minimum)} and ${String(limits.maximum)}, inclusive`,
    );
  }
  return parsed;
}

function parseQueue(value: unknown, path: string): KafkaStreamQueueMetrics {
  const queue = record(value, path);
  exactKeys(
    queue,
    [
      "capacityBytes",
      "capacityMessages",
      "currentBytes",
      "currentMessages",
      "droppedMessages",
      "droppedPerSecond",
      "droppedSincePrevious",
      "peakBytes",
      "peakMessages",
    ],
    path,
  );
  const parsed: KafkaStreamQueueMetrics = {
    capacityBytes: nonNegativeInteger(queue.capacityBytes, `${path}.capacityBytes`),
    capacityMessages: nonNegativeInteger(queue.capacityMessages, `${path}.capacityMessages`),
    currentBytes: nonNegativeInteger(queue.currentBytes, `${path}.currentBytes`),
    currentMessages: nonNegativeInteger(queue.currentMessages, `${path}.currentMessages`),
    droppedMessages: nonNegativeInteger(queue.droppedMessages, `${path}.droppedMessages`),
    droppedPerSecond: nullableNonNegativeFinite(queue.droppedPerSecond, `${path}.droppedPerSecond`),
    droppedSincePrevious: nonNegativeInteger(
      queue.droppedSincePrevious,
      `${path}.droppedSincePrevious`,
    ),
    peakBytes: nonNegativeInteger(queue.peakBytes, `${path}.peakBytes`),
    peakMessages: nonNegativeInteger(queue.peakMessages, `${path}.peakMessages`),
  };

  if (
    parsed.capacityBytes !== KAFKA_MESSAGE_LIMITS.queuedBytes ||
    parsed.capacityMessages < KAFKA_OPERATIONAL_PREFERENCE_LIMITS.queueDepth.minimum ||
    parsed.capacityMessages > KAFKA_OPERATIONAL_PREFERENCE_LIMITS.queueDepth.maximum
  ) {
    throw new HostContractValidationError(
      path,
      "must use the canonical byte ceiling and bounded effective count capacity",
    );
  }
  if (
    parsed.currentBytes > parsed.peakBytes ||
    parsed.peakBytes > parsed.capacityBytes ||
    parsed.currentMessages > parsed.peakMessages ||
    parsed.peakMessages > parsed.capacityMessages
  ) {
    throw new HostContractValidationError(path, "contains inconsistent current, peak, or capacity");
  }
  if (parsed.droppedSincePrevious > parsed.droppedMessages) {
    throw new HostContractValidationError(
      `${path}.droppedSincePrevious`,
      "must not exceed cumulative dropped messages",
    );
  }
  return parsed;
}

function parseDelivery(value: unknown, path: string): KafkaStreamDeliveryMetrics {
  const delivery = record(value, path);
  exactKeys(
    delivery,
    [
      "batchCount",
      "batchSize",
      "deliveredMessages",
      "historySamples",
      "intervalMs",
      "lastBatchMessages",
      "messagesPerSecond",
      "publicationDurationMs",
      "queueWaitMs",
      "receivedMessages",
      "tuningSource",
    ],
    path,
  );
  const parsed: KafkaStreamDeliveryMetrics = {
    batchCount: nonNegativeInteger(delivery.batchCount, `${path}.batchCount`),
    batchSize: boundedPreferenceInteger(
      delivery.batchSize,
      `${path}.batchSize`,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.batchSize,
    ),
    deliveredMessages: nonNegativeInteger(delivery.deliveredMessages, `${path}.deliveredMessages`),
    historySamples: boundedPreferenceInteger(
      delivery.historySamples,
      `${path}.historySamples`,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.historySamples,
    ),
    intervalMs: boundedPreferenceInteger(
      delivery.intervalMs,
      `${path}.intervalMs`,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.intervalMs,
    ),
    lastBatchMessages: nonNegativeInteger(delivery.lastBatchMessages, `${path}.lastBatchMessages`),
    messagesPerSecond: nullableNonNegativeFinite(
      delivery.messagesPerSecond,
      `${path}.messagesPerSecond`,
    ),
    publicationDurationMs: nullableNonNegativeFinite(
      delivery.publicationDurationMs,
      `${path}.publicationDurationMs`,
    ),
    queueWaitMs: nullableNonNegativeFinite(delivery.queueWaitMs, `${path}.queueWaitMs`),
    receivedMessages: nonNegativeInteger(delivery.receivedMessages, `${path}.receivedMessages`),
    tuningSource: declaredValue(
      delivery.tuningSource,
      KAFKA_STREAM_TUNING_SOURCES,
      `${path}.tuningSource`,
    ),
  };

  if (
    parsed.lastBatchMessages > parsed.batchSize ||
    parsed.lastBatchMessages > parsed.deliveredMessages
  ) {
    throw new HostContractValidationError(
      `${path}.lastBatchMessages`,
      "must fit the canonical batch and delivered count",
    );
  }
  if (
    (parsed.batchCount === 0 &&
      (parsed.deliveredMessages !== 0 || parsed.lastBatchMessages !== 0)) ||
    (parsed.batchCount > 0 && (parsed.deliveredMessages === 0 || parsed.lastBatchMessages === 0))
  ) {
    throw new HostContractValidationError(path, "contains inconsistent batch evidence");
  }
  return parsed;
}

function requireCurrentEvidence(
  snapshot: KafkaStreamMonitorSnapshot,
  path: string,
): asserts snapshot is KafkaStreamMonitorSnapshot & {
  readonly connectionName: string;
  readonly delivery: KafkaStreamDeliveryMetrics;
  readonly queue: KafkaStreamQueueMetrics;
  readonly request: KafkaFetchRequest;
  readonly sampledAt: string;
} {
  if (
    snapshot.connectionName === null ||
    snapshot.delivery === null ||
    snapshot.queue === null ||
    snapshot.request === null ||
    snapshot.sampledAt === null
  ) {
    throw new HostContractValidationError(path, "must contain complete current evidence");
  }
}

function validateSnapshotConsistency(snapshot: KafkaStreamMonitorSnapshot, path: string): void {
  if (snapshot.state === "unavailable") {
    if (
      snapshot.status !== "unavailable" ||
      snapshot.connectionName !== null ||
      snapshot.delivery !== null ||
      snapshot.queue !== null ||
      snapshot.request !== null ||
      snapshot.sampledAt !== null
    ) {
      throw new HostContractValidationError(path, "contains inconsistent unavailable evidence");
    }
    return;
  }

  requireCurrentEvidence(snapshot, path);
  if (snapshot.delivery.batchSize > snapshot.queue.capacityMessages) {
    throw new HostContractValidationError(
      path,
      "effective batch size must not exceed effective queue capacity",
    );
  }
  const accountedMessages =
    snapshot.delivery.deliveredMessages +
    snapshot.queue.droppedMessages +
    snapshot.queue.currentMessages;
  if (accountedMessages !== snapshot.delivery.receivedMessages) {
    throw new HostContractValidationError(
      path,
      "received messages must equal delivered, dropped, and currently queued messages",
    );
  }

  if (snapshot.state === "stale") {
    if (snapshot.status !== "stale") {
      throw new HostContractValidationError(`${path}.status`, "must be stale");
    }
    return;
  }
  if (snapshot.state === "failed") {
    if (snapshot.status !== "degraded") {
      throw new HostContractValidationError(`${path}.status`, "must be degraded");
    }
    return;
  }
  if (
    snapshot.status === "unavailable" ||
    snapshot.status === "stale" ||
    snapshot.status === "degraded"
  ) {
    throw new HostContractValidationError(`${path}.status`, "does not match the current lifecycle");
  }
  if (snapshot.queue.droppedMessages > 0 && snapshot.status !== "backpressure") {
    throw new HostContractValidationError(
      `${path}.status`,
      "must report backpressure when host drops are confirmed",
    );
  }
  if (
    snapshot.status === "nominal" &&
    (snapshot.delivery.deliveredMessages === 0 || snapshot.queue.droppedMessages > 0)
  ) {
    throw new HostContractValidationError(
      `${path}.status`,
      "cannot be nominal without current loss-free delivery evidence",
    );
  }
}

export function parseKafkaStreamMonitorSnapshot(
  value: unknown,
  path: string,
  parseFetchRequest: FetchRequestParser,
): KafkaStreamMonitorSnapshot {
  const snapshot = record(value, path);
  exactKeys(
    snapshot,
    ["connectionName", "delivery", "queue", "request", "sampledAt", "state", "status"],
    path,
  );
  const parsed: KafkaStreamMonitorSnapshot = {
    connectionName: nullableText(snapshot.connectionName, `${path}.connectionName`, 256),
    delivery:
      snapshot.delivery === null ? null : parseDelivery(snapshot.delivery, `${path}.delivery`),
    queue: snapshot.queue === null ? null : parseQueue(snapshot.queue, `${path}.queue`),
    request:
      snapshot.request === null ? null : parseFetchRequest(snapshot.request, `${path}.request`),
    sampledAt:
      snapshot.sampledAt === null
        ? null
        : canonicalIsoTimestamp(snapshot.sampledAt, `${path}.sampledAt`),
    state: declaredValue(snapshot.state, KAFKA_STREAM_MONITOR_STATES, `${path}.state`),
    status: declaredValue(snapshot.status, KAFKA_STREAM_MONITOR_STATUSES, `${path}.status`),
  };
  validateSnapshotConsistency(parsed, path);
  return parsed;
}
