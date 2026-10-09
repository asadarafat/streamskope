import { KAFKA_FETCH_LIMITS, type KafkaFetchRequest } from "../contracts";
import type { HostError } from "../contracts/host-errors";
import { KAFKA_QUERY_LIMITS, type KafkaReadCoverage } from "../contracts/query-search";
import {
  RECORD_EXPORT_LIMITS,
  type RecordExportInput,
  type RecordExportLimits,
  type RecordExportOperation,
  type RecordExportReason,
  type RecordExportSettings,
  type RecordExportSnapshot,
} from "../contracts/record-export";
import { parseRecordExportInput } from "../contracts/record-export-validation";

import type { RecordReadScope } from "./connection-scope";
import type { KafkaReadCheckpoint } from "./read-checkpoint";
import type { RecordExportArtifacts, RecordExportSink } from "./record-export-artifacts";
import { encodeRecordExportRow, recordExportHeader } from "./record-export-encoding";
import type { KafkaMessageStream } from "./types";
import { ownedCleanupFailure } from "./session-lifecycle";

interface ExportJob {
  operation: RecordExportOperation;
  readonly scope: RecordReadScope;
  readonly fingerprint: string;
  readonly readController: AbortController;
  readonly authority: AbortController;
  readonly started: number;
  previous: ExportJob | undefined;
  task: Promise<void>;
  settled: boolean;
  sink: RecordExportSink | undefined;
  reader: KafkaMessageStream | undefined;
  closing: Promise<void> | undefined;
  cleanupFailed: boolean;
  unconfirmedOpenCleanup: boolean;
  stop: RecordExportReason | undefined;
  deadline: ReturnType<typeof setTimeout> | undefined;
  expiry: ReturnType<typeof setTimeout> | undefined;
  lastPublished: number;
}

export interface RecordExportServiceOptions {
  readonly scope: () => RecordReadScope | null;
  readonly settings: () => RecordExportSettings;
  readonly artifacts?: RecordExportArtifacts;
  readonly changed: (snapshot: RecordExportSnapshot) => void;
  readonly now?: () => number;
  readonly id?: () => string;
  readonly limits?: Partial<RecordExportLimits>;
}

export class RecordExportOperationError extends Error {
  constructor(readonly error: HostError) {
    super(error.summary);
    this.name = "RecordExportOperationError";
  }
}

/** Owns one finite reader and one awaited sink; records never accumulate in this service. */
export class RecordExportService {
  private readonly now: () => number;
  private readonly id: () => string;
  private readonly limits: RecordExportLimits;
  private readonly scopeId: string;
  private current: ExportJob | undefined;
  private revision = 0;
  private orphanDrain: Promise<void> = Promise.resolve();
  private orphanCleanupFailed = false;

  constructor(private readonly options: RecordExportServiceOptions) {
    this.now = options.now ?? Date.now;
    this.id = options.id ?? ((): string => crypto.randomUUID());
    this.scopeId = this.id();
    this.limits = { ...RECORD_EXPORT_LIMITS, ...options.limits };
    for (const [key, value] of Object.entries(this.limits)) {
      if (
        !Number.isSafeInteger(value) ||
        value < 1 ||
        value > RECORD_EXPORT_LIMITS[key as keyof RecordExportLimits]
      )
        throw new Error("Export limits must be positive integers within the host limits.");
    }
  }

  snapshot(): RecordExportSnapshot {
    return structuredClone({
      scopeId: this.scopeId,
      revision: this.revision,
      available: this.options.artifacts !== undefined,
      operation: this.current?.operation ?? null,
    });
  }

  start(value: RecordExportInput): RecordExportSnapshot {
    const input = parseRecordExportInput(value, "export");
    const fingerprint = JSON.stringify(input);
    const previous = this.current;
    if (previous?.operation.input.requestId === input.requestId) {
      if (previous.fingerprint !== fingerprint)
        throw this.rejected(
          "This export request identity was already used with different options.",
        );
      return this.snapshot();
    }
    if (this.options.artifacts === undefined)
      throw this.rejected("Range export is unavailable in this host.");
    if (this.orphanCleanupFailed || previous?.cleanupFailed === true)
      throw this.rejected(
        "Export cleanup is not confirmed. Discard the previous export and retry.",
      );
    if (previous !== undefined && !previous.settled)
      throw this.rejected("An export is already running. Cancel it before starting another.");
    const scope = this.options.scope();
    if (scope === null || !scope.isCurrent())
      throw this.rejected("Connect to Kafka before exporting a range.");
    if (input.maxRecords > this.limits.records)
      throw this.rejected("The requested record limit exceeds this host's export limit.");
    const settings = structuredClone(this.options.settings());
    if (previous !== undefined) this.revoke(previous);
    const started = this.now();
    const job: ExportJob = {
      operation: {
        jobId: this.id(),
        state: "preparing",
        input: structuredClone(input),
        source: { connectionName: scope.connectionName, clusterId: null, topicId: null },
        settings,
        limits: { ...this.limits },
        startedAt: new Date(started).toISOString(),
        completedAt: null,
        counts: {
          passes: 0,
          scannedRecords: 0,
          scannedBytes: 0,
          writtenRecords: 0,
          writtenBytes: 0,
          unavailableRecords: 0,
          decodeErrorRecords: 0,
          originalUnavailableRecords: 0,
        },
        coverage: null,
        reason: null,
        artifact: null,
        error: null,
      },
      scope,
      fingerprint,
      readController: new AbortController(),
      authority: new AbortController(),
      started,
      previous,
      task: Promise.resolve(),
      settled: false,
      sink: undefined,
      reader: undefined,
      closing: undefined,
      cleanupFailed: false,
      unconfirmedOpenCleanup: false,
      stop: undefined,
      deadline: undefined,
      expiry: undefined,
      lastPublished: -Infinity,
    };
    this.current = job;
    // Reserve before publishing or opening asynchronous resources, including retries in the same turn.
    job.task = Promise.resolve().then(() => this.run(job));
    this.publish(job, true);
    return this.snapshot();
  }

  async cancel(jobId: string): Promise<RecordExportSnapshot> {
    const job = this.requireJob(jobId);
    if (!job.settled) this.stop(job, "cancelled");
    await job.task;
    if (job.cleanupFailed) throw this.failure(job, "cleanup-failed");
    return this.snapshot();
  }

  async discard(jobId: string): Promise<RecordExportSnapshot> {
    const job = this.requireJob(jobId);
    this.revoke(job);
    this.publish(job, true);
    await job.task;
    // Retry the actual owners, rather than erasing a previously rejected cleanup promise.
    await this.cleanup(job, true);
    if (this.current === job) this.current = undefined;
    this.revision += 1;
    this.notify();
    return this.snapshot();
  }

  invalidate(): void {
    const job = this.current;
    if (job === undefined) {
      this.options.artifacts?.revoke();
      this.orphanDrain = (this.options.artifacts?.drain() ?? Promise.resolve()).then(
        () => {
          this.orphanCleanupFailed = false;
        },
        () => {
          this.orphanCleanupFailed = true;
        },
      );
      return;
    }
    this.revoke(job);
    this.publish(job, true);
    if (job.settled) {
      job.settled = false;
      job.task = this.cleanup(job)
        .then(
          () => {
            this.terminal(job, "revoked");
          },
          () => {
            this.terminal(job, "cleanup-failed");
          },
        )
        .finally(() => {
          job.settled = true;
        });
    }
  }

  async idle(): Promise<void> {
    await this.orphanDrain;
    const job = this.current;
    await job?.task;
    if (this.orphanCleanupFailed || job?.cleanupFailed === true)
      throw this.failure(job, "cleanup-failed");
  }

  private async run(job: ExportJob): Promise<void> {
    let reason: RecordExportReason;
    let storage = false;
    job.deadline = setTimeout(() => this.stop(job, "deadline"), this.limits.durationMs);
    try {
      await this.orphanDrain;
      if (this.orphanCleanupFailed) throw this.failure(job, "cleanup-failed");
      if (job.previous !== undefined) {
        await this.cleanup(job.previous);
        job.previous = undefined;
      }
      this.assertCurrent(job);
      storage = true;
      job.sink = await this.options.artifacts!.create({
        jobId: job.operation.jobId,
        format: job.operation.input.format,
        maximumBytes: this.limits.bytes,
        lifetimeMs: this.limits.artifactLifetimeMs,
        signal: job.authority.signal,
        assertCurrent: (): void => this.assertCurrent(job),
      });
      this.assertCurrent(job);
      const header = recordExportHeader(job.operation.input.format);
      if (header.byteLength > this.limits.bytes) throw this.failure(job, "storage-failed");
      if (header.byteLength > 0) {
        await job.sink.write(header);
        this.assertCurrent(job);
        job.operation = {
          ...job.operation,
          counts: { ...job.operation.counts, writtenBytes: header.byteLength },
        };
      }
      storage = false;
      reason = await this.read(job);
      this.assertCurrent(job);
      storage = true;
      const completedAt = new Date(this.now()).toISOString();
      const artifact = await job.sink.seal({
        limits: job.operation.limits,
        jobId: job.operation.jobId,
        input: job.operation.input,
        source: job.operation.source,
        settings: job.operation.settings,
        startedAt: job.operation.startedAt,
        completedAt,
        outcome: reason === "range-complete" ? "complete" : "partial",
        reason,
        counts: job.operation.counts,
        coverage: job.operation.coverage,
      });
      this.assertCurrent(job);
      job.operation = {
        ...job.operation,
        state: reason === "range-complete" ? "completed" : "partial",
        completedAt,
        reason,
        artifact,
        error: null,
      };
      job.expiry = setTimeout(
        () => {
          if (this.current !== job || job.operation.artifact === null) return;
          this.revoke(job);
          job.operation = { ...job.operation, state: "expired", reason: "revoked", error: null };
          job.settled = false;
          job.task = this.cleanup(job)
            .catch(() => {
              this.terminal(job, "cleanup-failed");
            })
            .finally(() => {
              job.settled = true;
              this.publish(job, true);
            });
          this.publish(job, true);
        },
        Math.max(0, Date.parse(artifact.expiresAt) - this.now()),
      );
    } catch (error) {
      reason =
        job.authority.signal.aborted ||
        (error instanceof RecordExportOperationError && error.error.code === "CANCELLED")
          ? "revoked"
          : error instanceof RecordExportOperationError && error.error.stage === "storage"
            ? job.cleanupFailed
              ? "cleanup-failed"
              : "storage-failed"
            : storage
              ? "storage-failed"
              : "read-failed";
      try {
        await this.cleanup(job);
      } catch {
        reason = "cleanup-failed";
      }
      this.terminal(job, reason);
    } finally {
      if (job.deadline !== undefined) clearTimeout(job.deadline);
      job.deadline = undefined;
      job.settled = true;
      this.publish(job, true);
    }
  }

  private async read(job: ExportJob): Promise<RecordExportReason> {
    let checkpoint: KafkaReadCheckpoint | undefined;
    let initial: KafkaReadCoverage | undefined;
    while (true) {
      this.assertCurrent(job);
      if (job.stop !== undefined) return job.stop;
      if (this.now() - job.started >= this.limits.durationMs) return "deadline";
      const counts = job.operation.counts;
      if (counts.writtenRecords >= job.operation.input.maxRecords) return "record-limit";
      if (counts.passes >= this.limits.passes) return "pass-limit";
      // Reserve a complete bounded pass before opening: the reader's own caps are unchanged.
      if (
        counts.scannedRecords + KAFKA_QUERY_LIMITS.scanRecords > this.limits.scanRecords ||
        counts.scannedBytes + KAFKA_QUERY_LIMITS.scanBytes > this.limits.scanBytes
      )
        return "scan-limit";
      const input = job.operation.input;
      const request: KafkaFetchRequest = {
        ...input.range,
        topic: input.topic,
        search: input.search,
        maxMessages: Math.min(
          KAFKA_FETCH_LIMITS.maxMessages,
          input.maxRecords - counts.writtenRecords,
        ),
      };
      job.operation = {
        ...job.operation,
        state: "reading",
        counts: { ...counts, passes: counts.passes + 1 },
      };
      this.publish(job, true);
      let reader: KafkaMessageStream | undefined;
      let unsubscribe: (() => void) | undefined;
      let unacknowledged: { partition: number; offset: string } | undefined;
      const updateCoverage = (coverage: KafkaReadCoverage): void => {
        initial ??= coverage;
        const safe = reader?.checkpoint?.()?.coverage ?? coverage;
        job.operation = {
          ...job.operation,
          counts: {
            ...job.operation.counts,
            scannedRecords: counts.scannedRecords + coverage.scannedRecords,
            scannedBytes: counts.scannedBytes + coverage.scannedBytes,
            unavailableRecords: counts.unavailableRecords + coverage.unavailableRecords,
          },
          coverage: {
            ...safe,
            scannedRecords: counts.scannedRecords + coverage.scannedRecords,
            scannedBytes: counts.scannedBytes + coverage.scannedBytes,
            matchedRecords: job.operation.counts.writtenRecords,
            unavailableRecords: counts.unavailableRecords + coverage.unavailableRecords,
            partitions: safe.partitions.map((partition) => ({
              ...partition,
              startOffset:
                initial!.partitions.find((first) => first.partition === partition.partition)
                  ?.startOffset ?? partition.startOffset,
              nextOffset:
                unacknowledged?.partition === partition.partition &&
                BigInt(unacknowledged.offset) < BigInt(partition.nextOffset)
                  ? unacknowledged.offset
                  : partition.nextOffset,
            })),
          },
        };
        if (
          job.operation.coverage?.reason === "range-complete" &&
          job.operation.coverage.partitions.some((part) => part.nextOffset !== part.endOffset)
        )
          job.operation = {
            ...job.operation,
            coverage: { ...job.operation.coverage, reason: "cancelled" },
          };
      };
      let problem: unknown;
      try {
        reader = await job.scope.openMessageStream(request, job.readController.signal, checkpoint);
        job.reader = reader;
        this.assertCurrent(job);
        const identity = reader.checkpoint?.();
        if (identity !== undefined)
          job.operation = {
            ...job.operation,
            source: {
              connectionName: job.scope.connectionName,
              clusterId: identity.clusterId,
              topicId: identity.topicId,
            },
          };
        const first = reader.coverage?.();
        if (first !== undefined) updateCoverage(first);
        unsubscribe = reader.subscribeCoverage?.((coverage) => {
          if (job.reader !== reader || job.authority.signal.aborted) return;
          updateCoverage(coverage);
          this.publish(job);
        });
        if (job.stop === undefined) {
          for await (const message of reader) {
            this.assertCurrent(job);
            if (job.stop !== undefined) break;
            unacknowledged = message;
            const row = encodeRecordExportRow(message, input.format);
            if (job.operation.counts.writtenBytes + row.byteLength > this.limits.bytes) {
              job.stop = "byte-limit";
              break;
            }
            try {
              await job.sink!.write(row);
            } catch {
              throw this.failure(job, "storage-failed");
            }
            this.assertCurrent(job);
            job.operation = {
              ...job.operation,
              counts: {
                ...job.operation.counts,
                writtenRecords: job.operation.counts.writtenRecords + 1,
                writtenBytes: job.operation.counts.writtenBytes + row.byteLength,
                decodeErrorRecords:
                  job.operation.counts.decodeErrorRecords +
                  (message.structured?.key.state === "error" ||
                  message.structured?.value.state === "error" ||
                  message.structured?.headers.some((header) => header.error !== null)
                    ? 1
                    : 0),
                originalUnavailableRecords:
                  job.operation.counts.originalUnavailableRecords +
                  (message.original?.state === "complete" ? 0 : 1),
              },
            };
            reader.acknowledge?.(message);
            unacknowledged = undefined;
            this.publish(job);
            if (job.stop !== undefined) break;
            if (job.operation.counts.writtenRecords >= input.maxRecords) {
              job.stop = "record-limit";
              break;
            }
          }
        }
      } catch (error) {
        if (ownedCleanupFailure(error) !== undefined) {
          job.unconfirmedOpenCleanup = true;
          job.cleanupFailed = true;
        }
        problem = error;
      } finally {
        try {
          unsubscribe?.();
        } catch (error) {
          problem ??= error;
        }
        await this.closeReader(job);
        const coverage = reader?.coverage?.();
        if (coverage !== undefined) updateCoverage(coverage);
      }
      this.assertCurrent(job);
      if (job.unconfirmedOpenCleanup) throw this.failure(job, "cleanup-failed");
      if (
        problem !== undefined &&
        (problem instanceof RecordExportOperationError ||
          !(job.readController.signal.aborted && job.stop !== undefined))
      )
        throw problem instanceof Error ? problem : this.failure(job, "read-failed");
      const coverage = job.operation.coverage;
      if (coverage?.reason === "failed") throw this.failure(job, "read-failed");
      if (
        coverage?.reason === "range-complete" &&
        coverage.partitions.every((part) => part.nextOffset === part.endOffset)
      )
        return job.operation.counts.unavailableRecords > 0
          ? "records-unavailable"
          : "range-complete";
      if (job.stop !== undefined) return job.stop;
      const next = reader?.checkpoint?.();
      if (next === undefined) return "checkpoint-unavailable";
      const previousPositions = checkpoint?.coverage.partitions ?? initial?.partitions;
      if (
        previousPositions !== undefined &&
        next.coverage.partitions.every(
          (part) =>
            part.nextOffset ===
            previousPositions.find((previous) => previous.partition === part.partition)?.nextOffset,
        )
      )
        return "checkpoint-unavailable";
      checkpoint = next;
    }
  }

  private stop(job: ExportJob, reason: RecordExportReason): void {
    if (job.settled || job.stop !== undefined) return;
    job.stop = reason;
    job.readController.abort();
    job.operation = { ...job.operation, state: "stopping" };
    void this.closeReader(job).catch(() => {
      /* The owning task joins and records this debt. */
    });
    this.publish(job, true);
  }

  private revoke(job: ExportJob): void {
    if (job.expiry !== undefined) clearTimeout(job.expiry);
    job.expiry = undefined;
    job.authority.abort();
    job.stop = "revoked";
    job.readController.abort();
    this.options.artifacts?.revoke();
    job.operation = {
      ...job.operation,
      artifact: null,
      state: job.settled ? "failed" : "stopping",
      reason: "revoked",
    };
    void this.closeReader(job).catch(() => {
      /* Retained and joined by cleanup. */
    });
  }

  private async closeReader(job: ExportJob, retry = false): Promise<void> {
    const reader = job.reader;
    if (reader === undefined) return;
    if (retry) job.closing = undefined;
    job.closing ??= Promise.resolve().then(() => reader.close());
    try {
      await job.closing;
      if (job.reader === reader) job.reader = undefined;
      job.closing = undefined;
    } catch {
      job.cleanupFailed = true;
      throw this.failure(job, "cleanup-failed");
    }
  }

  private async cleanup(job: ExportJob, retry = false): Promise<void> {
    let failed = job.unconfirmedOpenCleanup;
    if (job.previous !== undefined) {
      try {
        await this.cleanup(job.previous, retry);
        job.previous = undefined;
      } catch {
        failed = true;
      }
    }
    try {
      await this.closeReader(job, retry);
    } catch {
      failed = true;
    }
    if (job.sink !== undefined) {
      try {
        await job.sink.discard();
        job.sink = undefined;
      } catch {
        failed = true;
      }
    }
    try {
      await this.options.artifacts?.drain();
    } catch {
      failed = true;
    }
    job.cleanupFailed = failed;
    if (failed) {
      this.terminal(job, "cleanup-failed");
      throw this.failure(job, "cleanup-failed");
    }
  }

  private terminal(job: ExportJob, reason: RecordExportReason): void {
    job.operation = {
      ...job.operation,
      state: "failed",
      artifact: null,
      reason,
      completedAt: new Date(this.now()).toISOString(),
      error: this.failure(job, reason).error,
    };
  }

  private assertCurrent(job: ExportJob): void {
    if (job.authority.signal.aborted || this.current !== job || !job.scope.isCurrent())
      throw this.failure(job, "revoked");
  }

  private requireJob(jobId: string): ExportJob {
    if (this.current?.operation.jobId !== jobId)
      throw this.rejected("This export is no longer available.");
    return this.current;
  }

  private publish(job: ExportJob, force = false): void {
    if (this.current !== job || (!force && this.now() - job.lastPublished < 500)) return;
    job.lastPublished = this.now();
    this.revision += 1;
    this.notify();
  }

  private notify(): void {
    try {
      this.options.changed(this.snapshot());
    } catch {
      // Notifications carry no resource authority. A broken transport must not strand the
      // owned reader or turn a successful write into a retry; snapshot() remains authoritative.
    }
  }

  private rejected(summary: string): RecordExportOperationError {
    return new RecordExportOperationError({
      code: "VALIDATION",
      stage: "validation",
      summary,
      recovery: "Check the export state and options, then retry.",
      correlationId: this.id(),
      retryable: false,
      activeStateChanged: false,
    });
  }

  private failure(
    job: ExportJob | undefined,
    reason: RecordExportReason,
  ): RecordExportOperationError {
    const cleanup = reason === "cleanup-failed";
    const storage = reason === "storage-failed" || cleanup;
    return new RecordExportOperationError({
      code: reason === "revoked" ? "CANCELLED" : "INTERNAL",
      stage: storage ? "storage" : "query",
      correlationId: job?.operation.jobId ?? this.id(),
      retryable: false,
      activeStateChanged: false,
      summary: cleanup
        ? "Export cleanup could not be confirmed."
        : storage
          ? "Export storage failed; no downloadable artifact was published."
          : reason === "revoked"
            ? "Export authority changed; the artifact was revoked."
            : "The export reader failed; no complete artifact was published.",
      recovery: cleanup
        ? job?.unconfirmedOpenCleanup === true
          ? "A reader opened during cancellation and did not confirm cleanup. Disconnect and restart the host before exporting again."
          : "Discard this export to retry cleanup before starting another."
        : reason === "revoked"
          ? "Start a new export from the current connection."
          : storage
            ? "Check available temporary storage and start a new export."
            : "Check the connection, topic permissions and retained offsets, then start a new export.",
    });
  }
}
