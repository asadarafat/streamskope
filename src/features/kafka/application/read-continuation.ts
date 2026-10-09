import type { KafkaFetchRequest, KafkaReadCoverage, KafkaSearchProgress } from "../contracts";
import { KAFKA_CONTINUATION_LIMITS, parseKafkaReadCoverage } from "../contracts/query-search";
import type { KafkaReadCheckpoint } from "./read-checkpoint";

export class KafkaContinuationError extends Error {
  readonly code = "QUERY_UNAVAILABLE" as const;
  readonly stage = "query" as const;
  readonly retryable = false;
  readonly recovery =
    "Start a new read. Continuations belong to one connection and unchanged record settings, and expire after 30 minutes.";
  constructor() {
    super("This read continuation is unavailable, expired or already used.");
  }
}

export interface KafkaReadContinuation {
  readonly request: KafkaFetchRequest;
  readonly binding: string;
  readonly checkpoint: KafkaReadCheckpoint;
  readonly progress: KafkaSearchProgress;
  readonly starts: ReadonlyMap<number, string>;
}

/** One bounded, single-use capability. Record bytes and credentials are never retained. */
export class KafkaReadContinuations {
  private current: { id: string; expiresAt: number; value: KafkaReadContinuation } | undefined;
  constructor(
    private readonly now = Date.now,
    private readonly id = (): string => crypto.randomUUID(),
  ) {}

  invalidate(): void {
    this.current = undefined;
  }

  take(id: string, binding: string): KafkaReadContinuation {
    const current = this.current;
    if (!current || current.id !== id) throw new KafkaContinuationError();
    this.current = undefined;
    if (current.expiresAt <= this.now() || current.value.binding !== binding)
      throw new KafkaContinuationError();
    return current.value;
  }

  progress(coverage: KafkaReadCoverage, previous?: KafkaReadContinuation): KafkaSearchProgress {
    return {
      pass: (previous?.progress.pass ?? 0) + 1,
      scannedRecords: (previous?.progress.scannedRecords ?? 0) + coverage.scannedRecords,
      scannedBytes: (previous?.progress.scannedBytes ?? 0) + coverage.scannedBytes,
      matchedRecords: (previous?.progress.matchedRecords ?? 0) + coverage.matchedRecords,
      unavailableRecords:
        (previous?.progress.unavailableRecords ?? 0) + coverage.unavailableRecords,
      continuation: null,
    };
  }

  coverage(coverage: KafkaReadCoverage, previous?: KafkaReadContinuation): KafkaReadCoverage {
    return {
      ...coverage,
      partitions: coverage.partitions.map((partition) => ({
        ...partition,
        startOffset: previous?.starts.get(partition.partition) ?? partition.startOffset,
      })),
    };
  }

  finish(
    request: KafkaFetchRequest,
    binding: string,
    checkpoint: KafkaReadCheckpoint,
    dropped: number,
    previous?: KafkaReadContinuation,
  ): KafkaSearchProgress {
    this.invalidate();
    const coverage = parseKafkaReadCoverage(checkpoint.coverage, "checkpoint.coverage");
    const progress = this.progress(coverage, previous);
    const advanced = coverage.partitions.some(
      (part) => BigInt(part.nextOffset) > BigInt(part.startOffset),
    );
    const remaining = coverage.partitions.some(
      (part) => BigInt(part.nextOffset) < BigInt(part.endOffset),
    );
    if (
      request.mode === "tail" ||
      dropped > 0 ||
      !advanced ||
      !remaining ||
      coverage.reason === "failed" ||
      coverage.reason === "reading" ||
      progress.pass >= KAFKA_CONTINUATION_LIMITS.passes
    )
      return progress;
    const id = this.id();
    const expiresAt = this.now() + KAFKA_CONTINUATION_LIMITS.lifetimeMs;
    const starts =
      previous?.starts ??
      new Map(coverage.partitions.map((part) => [part.partition, part.startOffset]));
    this.current = {
      id,
      expiresAt,
      value: {
        request: structuredClone(request),
        binding,
        checkpoint: structuredClone(checkpoint),
        progress,
        starts,
      },
    };
    return { ...progress, continuation: { id, expiresAt: new Date(expiresAt).toISOString() } };
  }
}
