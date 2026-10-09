import { KAFKA_FETCH_LIMITS, type KafkaFetchRequest, type KafkaMessage } from "../contracts";
import type { FiniteRecordInput, RecordReadSource } from "../contracts/finite-record-read";
import { KAFKA_QUERY_LIMITS, type KafkaReadCoverage } from "../contracts/query-search";

import type { RecordReadScope } from "./connection-scope";
import type { KafkaReadCheckpoint } from "./read-checkpoint";
import { ownedCleanupFailure } from "./session-lifecycle";
import type { KafkaMessageStream } from "./types";

export interface FiniteReadCounts {
  readonly passes: number;
  readonly scannedRecords: number;
  readonly scannedBytes: number;
  readonly acceptedRecords: number;
  readonly unavailableRecords: number;
  readonly decodeErrorRecords: number;
  readonly originalUnavailableRecords: number;
}
export interface FiniteReadProgress {
  readonly source: RecordReadSource;
  readonly counts: FiniteReadCounts;
  readonly coverage: KafkaReadCoverage | null;
}
export type FiniteReadReason =
  | "range-complete"
  | "records-unavailable"
  | "record-limit"
  | "scan-limit"
  | "pass-limit"
  | "deadline"
  | "cancelled"
  | "checkpoint-unavailable"
  | "consumer-limit";
export type FiniteReadResult = FiniteReadProgress & { readonly reason: FiniteReadReason };
export type RecordAcceptance = "committed" | "limit";
type StopReason = "cancelled" | "deadline" | "revoked" | "consumer-limit" | "record-limit";
type CleanupDebt = "reader-close" | "late-open-no-handle" | null;

export interface FiniteReadOptions {
  readonly scope: RecordReadScope;
  readonly input: FiniteRecordInput;
  readonly limits: {
    readonly scanRecords: number;
    readonly scanBytes: number;
    readonly passes: number;
  };
  readonly deadlineAt: number;
  readonly authority: AbortSignal;
  readonly assertCurrent: () => void;
  readonly changed: (progress: FiniteReadProgress) => void;
  readonly now?: () => number;
}

export class FiniteReadFailure extends Error {
  constructor(
    readonly kind: "read" | "consumer" | "revoked" | "cleanup",
    readonly cleanupDebt: CleanupDebt,
    readonly progress: FiniteReadProgress,
    override readonly cause?: unknown,
  ) {
    super(`Finite record read ${kind} failed.`);
    this.name = "FiniteReadFailure";
  }
}

/** One captured range, serial awaited acceptance, and original-reader cleanup ownership. */
export class FiniteRecordRead {
  private readonly now: () => number;
  private readonly input: FiniteRecordInput;
  private readonly limits: FiniteReadOptions["limits"];
  private readonly controller = new AbortController();
  private progress: FiniteReadProgress;
  private refreshCoverage: (() => void) | undefined;
  private reader: KafkaMessageStream | undefined;
  private closing: Promise<void> | undefined;
  private cleanupDebt: CleanupDebt = null;
  private task: Promise<FiniteReadResult> | undefined;
  private stopReason: StopReason | undefined;
  private settled = false;
  private readonly revoked = (): void => this.stop("revoked");

  constructor(private readonly options: FiniteReadOptions) {
    this.now = options.now ?? Date.now;
    this.input = structuredClone(options.input);
    this.limits = { ...options.limits };
    this.progress = {
      source: { connectionName: options.scope.connectionName, clusterId: null, topicId: null },
      counts: {
        passes: 0,
        scannedRecords: 0,
        scannedBytes: 0,
        acceptedRecords: 0,
        unavailableRecords: 0,
        decodeErrorRecords: 0,
        originalUnavailableRecords: 0,
      },
      coverage: null,
    };
  }

  snapshot(): FiniteReadProgress {
    // Explicit snapshots refresh coverage; the per-record acceptance path stays constant work.
    this.refreshCoverage?.();
    return structuredClone(this.progress);
  }

  run(accept: (record: KafkaMessage) => Promise<RecordAcceptance>): Promise<FiniteReadResult> {
    if (this.task !== undefined) throw new Error("A finite record reader can only run once.");
    this.options.authority.addEventListener("abort", this.revoked, { once: true });
    if (this.options.authority.aborted) this.revoked();
    this.task = Promise.resolve()
      .then(() => this.read(accept))
      .catch((error: unknown) => {
        if (this.cleanupDebt !== null) throw this.failure("cleanup", error);
        if (error instanceof FiniteReadFailure) throw error;
        throw this.failure("read", error);
      })
      .finally(() => {
        this.settled = true;
        this.options.authority.removeEventListener("abort", this.revoked);
      });
    return this.task;
  }

  stop(reason: "cancelled" | "deadline" | "revoked"): void {
    if (this.settled) return;
    if (reason === "revoked" || this.stopReason === undefined) this.stopReason = reason;
    this.controller.abort();
    void this.closeReader().catch(() => {
      // Joined by the iteration owner; an aborted signal never clears cleanup debt.
    });
  }

  async idle(): Promise<void> {
    await this.task?.catch(() => undefined);
    if (this.cleanupDebt !== null) throw this.failure("cleanup");
  }

  async retryCleanup(): Promise<void> {
    await this.task?.catch(() => undefined);
    await this.closeReader(true);
    if (this.cleanupDebt !== null) throw this.failure("cleanup");
  }

  private async read(
    accept: (record: KafkaMessage) => Promise<RecordAcceptance>,
  ): Promise<FiniteReadResult> {
    let checkpoint: KafkaReadCheckpoint | undefined;
    let initial: KafkaReadCoverage | undefined;
    while (true) {
      this.assertCurrent();
      if (this.stopReason !== undefined) return this.result(this.stopReason);
      if (this.now() >= this.options.deadlineAt) return this.result("deadline");
      const counts = this.progress.counts;
      if (counts.acceptedRecords >= this.input.maxRecords) return this.result("record-limit");
      if (counts.passes >= this.limits.passes) return this.result("pass-limit");
      if (
        counts.scannedRecords + KAFKA_QUERY_LIMITS.scanRecords > this.limits.scanRecords ||
        counts.scannedBytes + KAFKA_QUERY_LIMITS.scanBytes > this.limits.scanBytes
      )
        return this.result("scan-limit");
      const request: KafkaFetchRequest = {
        ...this.input.range,
        topic: this.input.topic,
        search: this.input.search,
        maxMessages: Math.min(
          KAFKA_FETCH_LIMITS.maxMessages,
          this.input.maxRecords - counts.acceptedRecords,
        ),
      };
      this.progress = { ...this.progress, counts: { ...counts, passes: counts.passes + 1 } };
      this.publish();
      let reader: KafkaMessageStream | undefined;
      let unsubscribe: (() => void) | undefined;
      let pending: { partition: number; offset: string } | undefined;
      const updateCoverage = (coverage: KafkaReadCoverage): void => {
        initial ??= coverage;
        const safe = reader?.checkpoint?.()?.coverage ?? coverage;
        this.progress = {
          ...this.progress,
          counts: {
            ...this.progress.counts,
            scannedRecords: counts.scannedRecords + coverage.scannedRecords,
            scannedBytes: counts.scannedBytes + coverage.scannedBytes,
            unavailableRecords: counts.unavailableRecords + coverage.unavailableRecords,
          },
          coverage: {
            ...safe,
            scannedRecords: counts.scannedRecords + coverage.scannedRecords,
            scannedBytes: counts.scannedBytes + coverage.scannedBytes,
            matchedRecords: this.progress.counts.acceptedRecords,
            unavailableRecords: counts.unavailableRecords + coverage.unavailableRecords,
            partitions: safe.partitions.map((partition) => ({
              ...partition,
              startOffset:
                initial!.partitions.find((first) => first.partition === partition.partition)
                  ?.startOffset ?? partition.startOffset,
              nextOffset:
                pending?.partition === partition.partition &&
                BigInt(pending.offset) < BigInt(partition.nextOffset)
                  ? pending.offset
                  : partition.nextOffset,
            })),
          },
        };
        if (
          this.progress.coverage?.reason === "range-complete" &&
          this.progress.coverage.partitions.some((part) => part.nextOffset !== part.endOffset)
        )
          this.progress = {
            ...this.progress,
            coverage: { ...this.progress.coverage, reason: "cancelled" },
          };
      };
      this.refreshCoverage = (): void => {
        const coverage = reader?.coverage?.();
        if (coverage !== undefined) updateCoverage(coverage);
      };
      let problem: unknown;
      try {
        reader = await this.options.scope.openMessageStream(
          request,
          this.controller.signal,
          checkpoint,
        );
        this.reader = reader;
        this.assertCurrent();
        const identity = reader.checkpoint?.();
        if (identity !== undefined)
          this.progress = {
            ...this.progress,
            source: {
              connectionName: this.options.scope.connectionName,
              clusterId: identity.clusterId,
              topicId: identity.topicId,
            },
          };
        const first = reader.coverage?.();
        if (first !== undefined) updateCoverage(first);
        unsubscribe = reader.subscribeCoverage?.((coverage) => {
          if (this.reader !== reader || this.options.authority.aborted) return;
          updateCoverage(coverage);
          this.publish();
        });
        if (this.stopReason === undefined) {
          for await (const message of reader) {
            this.assertCurrent();
            if (this.stopReason !== undefined) break;
            if (this.now() >= this.options.deadlineAt) {
              this.stop("deadline");
              break;
            }
            pending = message;
            let decision: RecordAcceptance;
            try {
              decision = await accept(message);
            } catch (error) {
              throw this.failure("consumer", error);
            }
            this.assertCurrent();
            if (decision === "limit") {
              this.stopReason ??= "consumer-limit";
              break;
            }
            this.progress = {
              ...this.progress,
              counts: {
                ...this.progress.counts,
                acceptedRecords: this.progress.counts.acceptedRecords + 1,
                decodeErrorRecords:
                  this.progress.counts.decodeErrorRecords +
                  (message.structured?.key.state === "error" ||
                  message.structured?.value.state === "error" ||
                  message.structured?.headers.some((header) => header.error !== null)
                    ? 1
                    : 0),
                originalUnavailableRecords:
                  this.progress.counts.originalUnavailableRecords +
                  (message.original?.state === "complete" ? 0 : 1),
              },
            };
            reader.acknowledge?.(message);
            pending = undefined;
            this.publish();
            if (this.stopReason !== undefined) break;
            if (this.progress.counts.acceptedRecords >= this.input.maxRecords) {
              this.stopReason = "record-limit";
              break;
            }
            // A pure accumulator may resolve synchronously for an entire buffered pass.
            // Yield to cancellation/deadline timers without releasing reader ownership.
            if (this.progress.counts.acceptedRecords % 128 === 0)
              await new Promise<void>((resolve) => setTimeout(resolve, 0));
          }
        }
      } catch (error) {
        if (ownedCleanupFailure(error) !== undefined) this.cleanupDebt = "late-open-no-handle";
        problem = error;
      } finally {
        try {
          unsubscribe?.();
        } catch (error) {
          problem ??= error;
        }
        try {
          await this.closeReader();
        } catch (error) {
          problem =
            problem === undefined
              ? error
              : new AggregateError([problem, error], "Finite reader cleanup failed.");
        }
        try {
          const coverage = reader?.coverage?.();
          if (coverage !== undefined) updateCoverage(coverage);
        } catch (error) {
          problem ??= error;
        }
        this.refreshCoverage = undefined;
        this.publish();
      }
      if (this.cleanupDebt !== null) throw this.failure("cleanup", problem);
      this.assertCurrent();
      if (
        problem !== undefined &&
        (problem instanceof FiniteReadFailure ||
          !(this.controller.signal.aborted && this.stopReason !== undefined))
      )
        throw problem instanceof FiniteReadFailure ? problem : this.failure("read", problem);
      const coverage = this.progress.coverage;
      if (coverage?.reason === "failed") throw this.failure("read");
      if (
        coverage?.reason === "range-complete" &&
        coverage.partitions.every((part) => part.nextOffset === part.endOffset)
      )
        return this.result(
          this.progress.counts.unavailableRecords > 0 ? "records-unavailable" : "range-complete",
        );
      if (this.stopReason !== undefined) return this.result(this.stopReason);
      const next = reader?.checkpoint?.();
      if (next === undefined) return this.result("checkpoint-unavailable");
      const previous = checkpoint?.coverage.partitions ?? initial?.partitions;
      if (
        previous !== undefined &&
        next.coverage.partitions.every(
          (part) =>
            part.nextOffset ===
            previous.find((before) => before.partition === part.partition)?.nextOffset,
        )
      )
        return this.result("checkpoint-unavailable");
      checkpoint = next;
    }
  }

  private async closeReader(retry = false): Promise<void> {
    const reader = this.reader;
    if (reader === undefined) return;
    if (retry) this.closing = undefined;
    this.closing ??= Promise.resolve().then(() => reader.close());
    try {
      await this.closing;
      if (this.reader === reader) this.reader = undefined;
      this.closing = undefined;
      if (this.cleanupDebt === "reader-close") this.cleanupDebt = null;
    } catch (error) {
      this.cleanupDebt ??= "reader-close";
      throw this.failure("cleanup", error);
    }
  }

  private assertCurrent(): void {
    if (
      this.options.authority.aborted ||
      this.stopReason === "revoked" ||
      !this.options.scope.isCurrent()
    )
      throw this.failure("revoked");
    try {
      this.options.assertCurrent();
    } catch (error) {
      throw this.failure("revoked", error);
    }
  }
  private result(reason: StopReason | FiniteReadReason): FiniteReadResult {
    if (reason === "revoked") throw this.failure("revoked");
    return { ...this.snapshot(), reason };
  }
  private failure(kind: FiniteReadFailure["kind"], cause?: unknown): FiniteReadFailure {
    return new FiniteReadFailure(kind, this.cleanupDebt, this.snapshot(), cause);
  }
  private publish(): void {
    try {
      this.options.changed(this.progress);
    } catch {
      /* Notifications cannot strand owned readers or retry accepted work. */
    }
  }
}
