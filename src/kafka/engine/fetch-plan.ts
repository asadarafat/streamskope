import type { KafkaFetchRequest } from "../contracts";

export const KAFKA_EARLIEST_OFFSET_TIMESTAMP = -2n;
export const KAFKA_LATEST_OFFSET_TIMESTAMP = -1n;

const INITIAL_RECENT_WINDOW_MS = 60 * 60 * 1_000;
const MAX_RECENT_WINDOW_MS = 30 * 24 * 60 * 60 * 1_000;
const RECENT_TIMESTAMP_SEARCH_STEPS = 10;

export interface KafkaOffsetLookup {
  listTopicOffsets(topic: string, timestamp: bigint): Promise<readonly bigint[]>;
}

export interface KafkaFetchPlan {
  readonly continuous: boolean;
  readonly endOffsets: ReadonlyMap<number, bigint> | null;
  readonly maxMessages: number;
  readonly request: KafkaFetchRequest;
  readonly startOffsets: ReadonlyMap<number, bigint>;
}

interface TopicOffsetBounds {
  readonly high: readonly bigint[];
  readonly low: readonly bigint[];
}

function assertOffsetBounds(low: readonly bigint[], high: readonly bigint[]): void {
  if (low.length !== high.length) {
    throw new Error("Kafka returned inconsistent partition offset maps.");
  }
  for (let partition = 0; partition < low.length; partition += 1) {
    const lowOffset = low[partition];
    const highOffset = high[partition];
    if (
      lowOffset === undefined ||
      highOffset === undefined ||
      lowOffset < 0n ||
      highOffset < lowOffset
    ) {
      throw new Error(`Kafka returned inconsistent offsets for partition ${partition}.`);
    }
  }
}

function clampOffset(candidate: bigint, low: bigint, high: bigint): bigint {
  const normalized = candidate < 0n ? high : candidate;
  return normalized < low ? low : normalized > high ? high : normalized;
}

function offsetsMap(offsets: readonly bigint[]): ReadonlyMap<number, bigint> {
  return new Map(offsets.map((offset, partition) => [partition, offset]));
}

async function loadTopicBounds(
  lookup: KafkaOffsetLookup,
  topic: string,
): Promise<TopicOffsetBounds> {
  const [low, high] = await Promise.all([
    lookup.listTopicOffsets(topic, KAFKA_EARLIEST_OFFSET_TIMESTAMP),
    lookup.listTopicOffsets(topic, KAFKA_LATEST_OFFSET_TIMESTAMP),
  ]);
  assertOffsetBounds(low, high);
  return { high, low };
}

function boundedEndOffsets(
  start: readonly bigint[],
  high: readonly bigint[],
  maximum: number,
): readonly bigint[] {
  const span = BigInt(maximum);
  return start.map((offset, partition) => {
    const highOffset = high[partition];
    if (highOffset === undefined) {
      throw new Error(`Kafka returned inconsistent offsets for partition ${partition}.`);
    }
    const bounded = offset + span;
    return bounded < highOffset ? bounded : highOffset;
  });
}

function countCandidates(candidates: readonly bigint[], bounds: TopicOffsetBounds): number {
  let total = 0n;
  for (let partition = 0; partition < bounds.high.length; partition += 1) {
    const low = bounds.low[partition];
    const high = bounds.high[partition];
    if (low === undefined || high === undefined) {
      throw new Error(`Kafka returned inconsistent offsets for partition ${partition}.`);
    }
    const candidate = clampOffset(candidates[partition] ?? high, low, high);
    total += high - candidate;
  }
  return total > BigInt(Number.MAX_SAFE_INTEGER) ? Number.MAX_SAFE_INTEGER : Number(total);
}

function clampRecentOffsets(
  candidates: readonly bigint[],
  bounds: TopicOffsetBounds,
  maximum: number,
): readonly bigint[] {
  const span = BigInt(maximum);
  return bounds.high.map((high, partition) => {
    const low = bounds.low[partition];
    if (low === undefined) {
      throw new Error(`Kafka returned inconsistent offsets for partition ${partition}.`);
    }
    const candidate = clampOffset(candidates[partition] ?? high, low, high);
    const perPartitionFloor = high - low > span ? high - span : low;
    return candidate < perPartitionFloor ? perPartitionFloor : candidate;
  });
}

async function resolveRecentOffsets(
  lookup: KafkaOffsetLookup,
  request: KafkaFetchRequest,
  bounds: TopicOffsetBounds,
  nowMs: number,
): Promise<readonly bigint[]> {
  const available = bounds.high.reduce(
    (total, high, partition) => total + (high - (bounds.low[partition] ?? high)),
    0n,
  );
  if (available <= BigInt(request.maxMessages)) {
    return bounds.low;
  }

  let recentTimestamp = Math.max(0, nowMs - INITIAL_RECENT_WINDOW_MS);
  const initialRecentOffsets = await lookup.listTopicOffsets(
    request.topic,
    BigInt(recentTimestamp),
  );
  if (initialRecentOffsets.length !== bounds.high.length) {
    throw new Error("Kafka returned inconsistent partition offset maps.");
  }

  let olderTimestamp = recentTimestamp;
  let olderOffsets = initialRecentOffsets;
  let windowMs = INITIAL_RECENT_WINDOW_MS;
  while (
    countCandidates(olderOffsets, bounds) < request.maxMessages &&
    windowMs < MAX_RECENT_WINDOW_MS
  ) {
    recentTimestamp = olderTimestamp;
    windowMs = Math.min(windowMs * 2, MAX_RECENT_WINDOW_MS);
    olderTimestamp = Math.max(0, nowMs - windowMs);
    olderOffsets = await lookup.listTopicOffsets(request.topic, BigInt(olderTimestamp));
    if (olderOffsets.length !== bounds.high.length) {
      throw new Error("Kafka returned inconsistent partition offset maps.");
    }
  }

  if (countCandidates(olderOffsets, bounds) < request.maxMessages) {
    return clampRecentOffsets(bounds.low, bounds, request.maxMessages);
  }

  let selected = olderOffsets;
  for (let step = 0; step < RECENT_TIMESTAMP_SEARCH_STEPS; step += 1) {
    const midpoint = Math.floor((olderTimestamp + recentTimestamp) / 2);
    if (midpoint === olderTimestamp || midpoint === recentTimestamp) {
      break;
    }
    const midpointOffsets = await lookup.listTopicOffsets(request.topic, BigInt(midpoint));
    if (midpointOffsets.length !== bounds.high.length) {
      throw new Error("Kafka returned inconsistent partition offset maps.");
    }
    if (countCandidates(midpointOffsets, bounds) >= request.maxMessages) {
      olderTimestamp = midpoint;
      selected = midpointOffsets;
    } else {
      recentTimestamp = midpoint;
    }
  }
  return clampRecentOffsets(selected, bounds, request.maxMessages);
}

export async function resolveKafkaFetchPlan(
  lookup: KafkaOffsetLookup,
  request: KafkaFetchRequest,
  nowMs: number,
): Promise<KafkaFetchPlan> {
  const bounds = await loadTopicBounds(lookup, request.topic);

  if (request.mode === "earliest") {
    return {
      continuous: false,
      endOffsets: offsetsMap(boundedEndOffsets(bounds.low, bounds.high, request.maxMessages)),
      maxMessages: request.maxMessages,
      request,
      startOffsets: offsetsMap(bounds.low),
    };
  }

  if (request.mode === "time-window") {
    const [startCandidates, endCandidates] = await Promise.all([
      lookup.listTopicOffsets(request.topic, BigInt(request.startTimeMs)),
      lookup.listTopicOffsets(request.topic, BigInt(request.endTimeMs)),
    ]);
    if (
      startCandidates.length !== bounds.high.length ||
      endCandidates.length !== bounds.high.length
    ) {
      throw new Error("Kafka returned inconsistent partition offset maps.");
    }
    const start = bounds.high.map((high, partition) => {
      const low = bounds.low[partition];
      if (low === undefined) {
        throw new Error(`Kafka returned inconsistent offsets for partition ${partition}.`);
      }
      return clampOffset(startCandidates[partition] ?? high, low, high);
    });
    const windowEnd = bounds.high.map((high, partition) => {
      const low = bounds.low[partition];
      const startOffset = start[partition];
      if (low === undefined || startOffset === undefined) {
        throw new Error(`Kafka returned inconsistent offsets for partition ${partition}.`);
      }
      const end = clampOffset(endCandidates[partition] ?? high, low, high);
      return end < startOffset ? startOffset : end;
    });
    return {
      continuous: false,
      endOffsets: offsetsMap(boundedEndOffsets(start, windowEnd, request.maxMessages)),
      maxMessages: request.maxMessages,
      request,
      startOffsets: offsetsMap(start),
    };
  }

  const start = await resolveRecentOffsets(lookup, request, bounds, nowMs);
  return {
    continuous: request.mode === "tail",
    endOffsets: request.mode === "tail" ? null : offsetsMap(bounds.high),
    maxMessages: request.maxMessages,
    request,
    startOffsets: offsetsMap(start),
  };
}
