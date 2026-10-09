import type { HostError } from "../contracts/host-errors";
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
import { FiniteRecordRead, FiniteReadFailure, type FiniteReadProgress } from "./finite-record-read";
import type { RecordExportArtifacts, RecordExportSink } from "./record-export-artifacts";
import { encodeRecordExportRow, recordExportHeader } from "./record-export-encoding";

interface ExportJob {
  operation: RecordExportOperation;
  readonly scope: RecordReadScope;
  readonly fingerprint: string;
  readonly authority: AbortController;
  readonly started: number;
  previous: ExportJob | undefined;
  task: Promise<void>;
  settled: boolean;
  sink: RecordExportSink | undefined;
  reader: FiniteRecordRead | undefined;
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
    // Explicit status can refresh coverage between throttled events. Its delivery must
    // be newer even when no notification was published in the meantime.
    this.revision += 1;
    return this.cloneSnapshot();
  }

  private cloneSnapshot(): RecordExportSnapshot {
    const job = this.current;
    if (job?.reader !== undefined) this.updateReadProgress(job, job.reader.snapshot());
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
      authority: new AbortController(),
      started,
      previous,
      task: Promise.resolve(),
      settled: false,
      sink: undefined,
      reader: undefined,
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
    const update = (progress: FiniteReadProgress): void => {
      this.updateReadProgress(job, progress);
      this.publish(job);
    };
    const reader = new FiniteRecordRead({
      scope: job.scope,
      input: job.operation.input,
      limits: this.limits,
      deadlineAt: job.started + this.limits.durationMs,
      authority: job.authority.signal,
      assertCurrent: (): void => this.assertCurrent(job),
      changed: update,
      now: this.now,
    });
    job.reader = reader;
    job.operation = { ...job.operation, state: job.stop === undefined ? "reading" : "stopping" };
    if (job.stop === "cancelled" || job.stop === "deadline" || job.stop === "revoked")
      reader.stop(job.stop);
    this.publish(job, true);
    try {
      const result = await reader.run(async (message) => {
        const row = encodeRecordExportRow(message, job.operation.input.format);
        if (job.operation.counts.writtenBytes + row.byteLength > this.limits.bytes) {
          job.stop = "byte-limit";
          return "limit";
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
            writtenBytes: job.operation.counts.writtenBytes + row.byteLength,
          },
        };
        return "committed";
      });
      update(result);
      return result.reason === "consumer-limit" ? "byte-limit" : result.reason;
    } catch (error) {
      if (error instanceof FiniteReadFailure) {
        update(error.progress);
        job.cleanupFailed = error.cleanupDebt !== null;
        job.unconfirmedOpenCleanup = error.cleanupDebt === "late-open-no-handle";
        if (error.kind === "cleanup") throw this.failure(job, "cleanup-failed");
        if (error.kind === "revoked") throw this.failure(job, "revoked");
        if (error.kind === "consumer" && error.cause instanceof RecordExportOperationError)
          throw error.cause;
      }
      throw error;
    }
  }

  private updateReadProgress(job: ExportJob, progress: FiniteReadProgress): void {
    const { acceptedRecords, ...counts } = progress.counts;
    job.operation = {
      ...job.operation,
      source: progress.source,
      coverage: progress.coverage,
      counts: { ...job.operation.counts, ...counts, writtenRecords: acceptedRecords },
    };
  }

  private stop(job: ExportJob, reason: RecordExportReason): void {
    if (job.settled || job.stop !== undefined) return;
    job.stop = reason;
    job.operation = { ...job.operation, state: "stopping" };
    if (reason === "cancelled" || reason === "deadline" || reason === "revoked")
      job.reader?.stop(reason);
    this.publish(job, true);
  }

  private revoke(job: ExportJob): void {
    if (job.expiry !== undefined) clearTimeout(job.expiry);
    job.expiry = undefined;
    job.authority.abort();
    job.stop = "revoked";
    this.options.artifacts?.revoke();
    job.operation = {
      ...job.operation,
      artifact: null,
      state: job.settled ? "failed" : "stopping",
      reason: "revoked",
    };
    job.reader?.stop("revoked");
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
      if (retry) await job.reader?.retryCleanup();
      else await job.reader?.idle();
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
      this.options.changed(this.cloneSnapshot());
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
