import { KAFKA_MESSAGE_LIMITS, type KafkaFetchRequest } from "./types";
import { KAFKA_OPERATIONAL_PREFERENCE_LIMITS } from "./operational-preference-types";
import {
  KAFKA_STREAM_MONITOR_STATES,
  KAFKA_STREAM_PRESSURE_REASONS,
  KAFKA_STREAM_QUEUE_BYTE_PRESSURE_THRESHOLD,
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

function nullableTimestamp(value: unknown, path: string): string | null {
  return value === null ? null : canonicalIsoTimestamp(value, path);
}

function parseDropReasons(value: unknown, path: string): KafkaStreamQueueMetrics["dropReasons"] {
  const reasons = record(value, path);
  exactKeys(reasons, ["countCapacity", "byteCapacity", "oversized", "terminalDiscarded"], path);
  return {
    countCapacity: nonNegativeInteger(reasons.countCapacity, `${path}.countCapacity`),
    byteCapacity: nonNegativeInteger(reasons.byteCapacity, `${path}.byteCapacity`),
    oversized: nonNegativeInteger(reasons.oversized, `${path}.oversized`),
    terminalDiscarded: nonNegativeInteger(reasons.terminalDiscarded, `${path}.terminalDiscarded`),
  };
}

function parsePressureReasons(
  value: unknown,
  path: string,
): KafkaStreamQueueMetrics["pressureReasons"] {
  if (
    !Array.isArray(value) ||
    value.length > KAFKA_STREAM_PRESSURE_REASONS.length ||
    new Set(value).size !== value.length
  ) {
    throw new HostContractValidationError(path, "must contain unique current pressure reasons");
  }
  return value.map((reason, index) =>
    declaredValue(reason, KAFKA_STREAM_PRESSURE_REASONS, `${path}[${index}]`),
  );
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
      "dropReasons",
      "oldestMessageAgeMs",
      "pressureReasons",
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
    dropReasons: parseDropReasons(queue.dropReasons, `${path}.dropReasons`),
    oldestMessageAgeMs: nullableNonNegativeFinite(
      queue.oldestMessageAgeMs,
      `${path}.oldestMessageAgeMs`,
    ),
    pressureReasons: parsePressureReasons(queue.pressureReasons, `${path}.pressureReasons`),
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
  if (
    parsed.dropReasons.countCapacity +
      parsed.dropReasons.byteCapacity +
      parsed.dropReasons.oversized +
      parsed.dropReasons.terminalDiscarded !==
    parsed.droppedMessages
  ) {
    throw new HostContractValidationError(
      `${path}.dropReasons`,
      "must sum to cumulative dropped messages",
    );
  }
  if ((parsed.currentMessages === 0) !== (parsed.oldestMessageAgeMs === null)) {
    throw new HostContractValidationError(
      `${path}.oldestMessageAgeMs`,
      "must describe the oldest currently queued record or be null for an empty queue",
    );
  }
  if (
    parsed.pressureReasons.includes("count-capacity") !==
      (parsed.currentMessages === parsed.capacityMessages) ||
    parsed.pressureReasons.includes("byte-capacity") !==
      parsed.currentBytes >= KAFKA_STREAM_QUEUE_BYTE_PRESSURE_THRESHOLD
  ) {
    throw new HostContractValidationError(
      `${path}.pressureReasons`,
      "must match current queue capacity pressure",
    );
  }
  return parsed;
}

function parseDelivery(value: unknown, path: string): KafkaStreamDeliveryMetrics {
  const delivery = record(value, path);
  exactKeys(
    delivery,
    [
      "rateSampledAt",
      "rateWindowMs",
      "publicationSampledAt",
      "queueWaitSampledAt",
      "batchCount",
      "batchSize",
      "publishedMessages",
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
    rateSampledAt: nullableTimestamp(delivery.rateSampledAt, `${path}.rateSampledAt`),
    rateWindowMs: nullableNonNegativeFinite(delivery.rateWindowMs, `${path}.rateWindowMs`),
    publicationSampledAt: nullableTimestamp(
      delivery.publicationSampledAt,
      `${path}.publicationSampledAt`,
    ),
    queueWaitSampledAt: nullableTimestamp(
      delivery.queueWaitSampledAt,
      `${path}.queueWaitSampledAt`,
    ),
    batchCount: nonNegativeInteger(delivery.batchCount, `${path}.batchCount`),
    batchSize: boundedPreferenceInteger(
      delivery.batchSize,
      `${path}.batchSize`,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.batchSize,
    ),
    publishedMessages: nonNegativeInteger(delivery.publishedMessages, `${path}.publishedMessages`),
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
    parsed.lastBatchMessages > parsed.publishedMessages
  ) {
    throw new HostContractValidationError(
      `${path}.lastBatchMessages`,
      "must fit the canonical batch and published count",
    );
  }
  if (
    (parsed.batchCount === 0 &&
      (parsed.publishedMessages !== 0 || parsed.lastBatchMessages !== 0)) ||
    (parsed.batchCount > 0 && (parsed.publishedMessages === 0 || parsed.lastBatchMessages === 0))
  ) {
    throw new HostContractValidationError(path, "contains inconsistent batch evidence");
  }
  if (
    (parsed.rateSampledAt === null) !== (parsed.rateWindowMs === null) ||
    (parsed.rateSampledAt === null) !== (parsed.messagesPerSecond === null) ||
    parsed.rateWindowMs === 0
  ) {
    throw new HostContractValidationError(
      path,
      "rates require a timestamp and a positive observation window",
    );
  }
  if (
    (parsed.publicationSampledAt === null) !== (parsed.publicationDurationMs === null) ||
    (parsed.queueWaitSampledAt === null) !== (parsed.queueWaitMs === null)
  ) {
    throw new HostContractValidationError(
      path,
      "measurements require their own observation timestamps",
    );
  }
  return parsed;
}

function requireCurrentEvidence(
  snapshot: KafkaStreamMonitorSnapshot,
  path: string,
): asserts snapshot is KafkaStreamMonitorSnapshot & {
  readonly connectionName: string;
  readonly operationId: string;
  readonly delivery: KafkaStreamDeliveryMetrics;
  readonly queue: KafkaStreamQueueMetrics;
  readonly request: KafkaFetchRequest;
  readonly sampledAt: string;
} {
  if (
    snapshot.operationId === null ||
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
      snapshot.operationId !== null ||
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
    snapshot.delivery.publishedMessages +
    snapshot.queue.droppedMessages +
    snapshot.queue.currentMessages;
  if (accountedMessages !== snapshot.delivery.receivedMessages) {
    throw new HostContractValidationError(
      path,
      "received messages must equal published, dropped, and currently queued messages",
    );
  }

  if ((snapshot.queue.droppedPerSecond === null) !== (snapshot.delivery.rateSampledAt === null)) {
    throw new HostContractValidationError(
      path,
      "drop and publication rates must share the same observation window",
    );
  }
  for (const observedAt of [
    snapshot.delivery.rateSampledAt,
    snapshot.delivery.publicationSampledAt,
    snapshot.delivery.queueWaitSampledAt,
  ]) {
    if (observedAt !== null && observedAt > snapshot.sampledAt) {
      throw new HostContractValidationError(
        path,
        "measurement timestamps must not follow the snapshot timestamp",
      );
    }
  }
  if (
    ["complete", "stopped", "failed"].includes(snapshot.state) &&
    (snapshot.queue.currentMessages !== 0 || snapshot.queue.pressureReasons.length !== 0)
  ) {
    throw new HostContractValidationError(
      path,
      "terminal evidence must account for the drained or discarded queue",
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
  if (snapshot.queue.pressureReasons.length > 0 !== (snapshot.status === "backpressure")) {
    throw new HostContractValidationError(
      `${path}.status`,
      "must describe current pressure independently of historical drops",
    );
  }
  if (snapshot.status === "nominal" && snapshot.delivery.publishedMessages === 0) {
    throw new HostContractValidationError(
      `${path}.status`,
      "cannot be nominal without host publication evidence",
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
    [
      "operationId",
      "connectionName",
      "delivery",
      "queue",
      "request",
      "sampledAt",
      "state",
      "status",
    ],
    path,
  );
  const parsed: KafkaStreamMonitorSnapshot = {
    operationId: nullableText(snapshot.operationId, `${path}.operationId`, 128),
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
