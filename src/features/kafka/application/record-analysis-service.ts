import type { RecordReadSettings } from "../contracts/finite-record-read";
import type { HostError } from "../contracts/host-errors";
import { compileKafkaProjectionPath } from "../contracts/rule-expression-parser";
import {
  RECORD_ANALYSIS_LIMITS,
  type RecordAnalysisInput,
  type RecordAnalysisLimitReason,
  type RecordAnalysisLimits,
  type RecordAnalysisOperation,
  type RecordAnalysisReason,
  type RecordAnalysisSnapshot,
} from "../contracts/record-analysis";
import { parseRecordAnalysisInput } from "../contracts/record-analysis-validation";

import type { RecordReadScope } from "./connection-scope";
import { FiniteReadFailure, FiniteRecordRead, type FiniteReadProgress } from "./finite-record-read";
import { RecordAnalysisAccumulator } from "./record-analysis-accumulator";

interface AnalysisJob {
  operation: RecordAnalysisOperation;
  readonly fingerprint: string;
  readonly scope: RecordReadScope;
  readonly authority: AbortController;
  readonly started: number;
  accumulator: RecordAnalysisAccumulator | undefined;
  reader: FiniteRecordRead | undefined;
  task: Promise<void>;
  settled: boolean;
  cleanupFailed: boolean;
  unconfirmedOpenCleanup: boolean;
  stop: "cancelled" | "deadline" | "revoked" | undefined;
  deadline: ReturnType<typeof setTimeout> | undefined;
  lastPublished: number;
}
export interface RecordAnalysisServiceOptions {
  readonly scope: () => RecordReadScope | null;
  readonly settings: () => RecordReadSettings;
  readonly changed: (snapshot: RecordAnalysisSnapshot) => void;
  readonly now?: () => number;
  readonly id?: () => string;
  readonly limits?: Partial<RecordAnalysisLimits>;
}
export class RecordAnalysisOperationError extends Error {
  constructor(readonly error: HostError) {
    super(error.summary);
    this.name = "RecordAnalysisOperationError";
  }
}

/** Session-only, bounded analysis using the same protected finite traversal as export. */
export class RecordAnalysisService {
  private readonly now: () => number;
  private readonly id: () => string;
  private readonly limits: RecordAnalysisLimits;
  private readonly scopeId: string;
  private current: AnalysisJob | undefined;
  private revision = 0;

  constructor(private readonly options: RecordAnalysisServiceOptions) {
    this.now = options.now ?? Date.now;
    this.id = options.id ?? ((): string => crypto.randomUUID());
    this.scopeId = this.id();
    this.limits = { ...RECORD_ANALYSIS_LIMITS, ...options.limits };
    for (const [key, value] of Object.entries(this.limits)) {
      if (
        !Number.isSafeInteger(value) ||
        value < 1 ||
        value > RECORD_ANALYSIS_LIMITS[key as keyof RecordAnalysisLimits]
      )
        throw new Error("Analysis limits must be positive integers within the host limits.");
    }
  }

  snapshot(): RecordAnalysisSnapshot {
    const job = this.current;
    if (job?.reader !== undefined) this.applyProgress(job, job.reader.snapshot());
    // Status can include only an acknowledged working result. Acceptance may have committed
    // privately while the reader still owns the next microtask before its count/ACK update.
    const operation =
      job === undefined
        ? null
        : {
            ...job.operation,
            result:
              job.operation.result ??
              (!job.authority.signal.aborted &&
              !job.settled &&
              job.accumulator?.countedRecords === job.operation.counts.countedRecords
                ? job.accumulator.snapshot()
                : null),
          };
    // Explicit status includes a richer acknowledged projection than progress events.
    // Give every delivery a new revision so consumers can retain strict stale-response guards.
    this.revision += 1;
    return structuredClone({ scopeId: this.scopeId, revision: this.revision, operation });
  }

  start(value: RecordAnalysisInput): RecordAnalysisSnapshot {
    const input = parseRecordAnalysisInput(value, "analysis");
    const fingerprint = JSON.stringify(input);
    const previous = this.current;
    if (previous?.operation.input.requestId === input.requestId) {
      if (previous.fingerprint !== fingerprint)
        throw this.rejected(
          "This analysis request identity was already used with different options.",
        );
      return this.snapshot();
    }
    if (previous?.cleanupFailed === true)
      throw this.rejected(
        "Analysis cleanup is not confirmed. Discard the previous analysis and retry.",
      );
    if (previous !== undefined && !previous.settled)
      throw this.rejected("An analysis is already running. Cancel it before starting another.");
    const scope = this.options.scope();
    if (scope === null || !scope.isCurrent())
      throw this.rejected("Connect to Kafka before analyzing a range.");
    if (input.maxRecords > this.limits.records || input.columns.length > this.limits.columns)
      throw this.rejected("The requested analysis exceeds this host's limits.");
    if (
      input.columns.some(
        (column) =>
          column.path.length > this.limits.pathCharacters ||
          compileKafkaProjectionPath(column.path).segments.length > this.limits.pathSegments,
      )
    )
      throw this.rejected("The requested field path exceeds this host's analysis limits.");
    const settings = structuredClone(this.options.settings());
    const accumulator = new RecordAnalysisAccumulator(input, settings, this.limits);
    if (previous !== undefined) this.revoke(previous);
    const started = this.now();
    const job: AnalysisJob = {
      operation: {
        jobId: this.id(),
        input: structuredClone(input),
        state: "preparing",
        source: { connectionName: scope.connectionName, clusterId: null, topicId: null },
        settings,
        limits: { ...this.limits },
        startedAt: new Date(started).toISOString(),
        completedAt: null,
        counts: {
          passes: 0,
          scannedRecords: 0,
          scannedBytes: 0,
          countedRecords: 0,
          unavailableRecords: 0,
        },
        coverage: null,
        reason: null,
        result: null,
        error: null,
      },
      fingerprint,
      scope,
      authority: new AbortController(),
      started,
      accumulator,
      reader: undefined,
      task: Promise.resolve(),
      settled: false,
      cleanupFailed: false,
      unconfirmedOpenCleanup: false,
      stop: undefined,
      deadline: undefined,
      lastPublished: -Infinity,
    };
    this.current = job;
    job.task = Promise.resolve().then(() => this.run(job));
    this.publish(job, true);
    return this.snapshot();
  }

  async cancel(jobId: string): Promise<RecordAnalysisSnapshot> {
    const job = this.requireJob(jobId);
    if (!job.settled) this.stop(job, "cancelled");
    await job.task;
    if (job.cleanupFailed) throw this.failure(job, "cleanup-failed");
    return this.snapshot();
  }

  async discard(jobId: string): Promise<RecordAnalysisSnapshot> {
    const job = this.requireJob(jobId);
    this.revoke(job);
    this.publish(job, true);
    await job.task;
    await this.cleanup(job, true);
    if (this.current === job) this.current = undefined;
    this.revision++;
    this.notify();
    return this.snapshot();
  }

  invalidate(): void {
    const job = this.current;
    if (job === undefined) return;
    this.revoke(job);
    this.publish(job, true);
  }

  async idle(): Promise<void> {
    const job = this.current;
    await job?.task;
    if (job?.cleanupFailed === true) throw this.failure(job, "cleanup-failed");
  }

  private async run(job: AnalysisJob): Promise<void> {
    const deadlineAt = job.started + this.limits.durationMs;
    job.deadline = setTimeout(
      () => this.stop(job, "deadline"),
      Math.max(0, deadlineAt - this.now()),
    );
    try {
      this.assertCurrent(job);
      const reader = new FiniteRecordRead({
        scope: job.scope,
        input: job.operation.input,
        limits: this.limits,
        deadlineAt,
        authority: job.authority.signal,
        assertCurrent: (): void => this.assertCurrent(job),
        changed: (progress): void => this.update(job, progress),
        now: this.now,
      });
      job.reader = reader;
      job.operation = { ...job.operation, state: job.stop === undefined ? "reading" : "stopping" };
      if (job.stop !== undefined) reader.stop(job.stop);
      this.publish(job, true);
      let consumerLimit: RecordAnalysisLimitReason | undefined;
      const outcome = await reader.run((message) => {
        this.assertCurrent(job);
        const decision = job.accumulator!.accept(message);
        if (decision === "committed") return Promise.resolve("committed");
        consumerLimit = decision;
        return Promise.resolve("limit");
      });
      this.assertCurrent(job);
      this.update(job, outcome);
      if (job.accumulator!.countedRecords !== outcome.counts.acceptedRecords)
        throw this.failure(job, "analysis-failed");
      const reason = outcome.reason === "consumer-limit" ? consumerLimit : outcome.reason;
      if (reason === undefined) throw this.failure(job, "analysis-failed");
      job.operation = {
        ...job.operation,
        state: reason === "range-complete" ? "completed" : "partial",
        completedAt: new Date(this.now()).toISOString(),
        reason,
        result: job.accumulator!.snapshot(),
        error: null,
      };
      job.accumulator = undefined;
    } catch (error) {
      let reason: RecordAnalysisReason = job.authority.signal.aborted
        ? "revoked"
        : "analysis-failed";
      if (error instanceof FiniteReadFailure) {
        this.update(job, error.progress);
        job.cleanupFailed = error.cleanupDebt !== null;
        job.unconfirmedOpenCleanup = error.cleanupDebt === "late-open-no-handle";
        reason =
          error.kind === "revoked"
            ? "revoked"
            : error.kind === "cleanup"
              ? "cleanup-failed"
              : error.kind === "read"
                ? "read-failed"
                : "analysis-failed";
      } else if (error instanceof RecordAnalysisOperationError && error.error.code === "CANCELLED")
        reason = "revoked";
      try {
        await this.cleanup(job);
      } catch {
        reason = "cleanup-failed";
      }
      job.accumulator = undefined;
      job.operation = {
        ...job.operation,
        state: reason === "revoked" ? "revoked" : "failed",
        completedAt: new Date(this.now()).toISOString(),
        reason,
        result: null,
        error: this.failure(job, reason).error,
      };
    } finally {
      if (job.deadline !== undefined) clearTimeout(job.deadline);
      job.deadline = undefined;
      job.settled = true;
      this.publish(job, true);
    }
  }

  private update(job: AnalysisJob, progress: FiniteReadProgress): void {
    this.applyProgress(job, progress);
    this.publish(job);
  }

  private applyProgress(job: AnalysisJob, progress: FiniteReadProgress): void {
    job.operation = {
      ...job.operation,
      source: progress.source,
      coverage: progress.coverage,
      counts: {
        passes: progress.counts.passes,
        scannedRecords: progress.counts.scannedRecords,
        scannedBytes: progress.counts.scannedBytes,
        countedRecords: progress.counts.acceptedRecords,
        unavailableRecords: progress.counts.unavailableRecords,
      },
    };
  }

  private stop(job: AnalysisJob, reason: "cancelled" | "deadline"): void {
    if (job.settled || job.stop !== undefined) return;
    job.stop = reason;
    job.operation = { ...job.operation, state: "stopping" };
    job.reader?.stop(reason);
    this.publish(job, true);
  }

  private revoke(job: AnalysisJob): void {
    job.authority.abort();
    job.stop = "revoked";
    job.accumulator = undefined;
    job.reader?.stop("revoked");
    job.operation = {
      ...job.operation,
      result: null,
      state: job.cleanupFailed ? "failed" : job.settled ? "revoked" : "stopping",
      reason: job.cleanupFailed ? "cleanup-failed" : "revoked",
      error: job.cleanupFailed ? this.failure(job, "cleanup-failed").error : null,
    };
  }

  private async cleanup(job: AnalysisJob, retry = false): Promise<void> {
    try {
      if (retry) await job.reader?.retryCleanup();
      else await job.reader?.idle();
      job.cleanupFailed = job.unconfirmedOpenCleanup;
    } catch {
      job.cleanupFailed = true;
    }
    if (job.cleanupFailed) throw this.failure(job, "cleanup-failed");
  }

  private assertCurrent(job: AnalysisJob): void {
    if (job.authority.signal.aborted || this.current !== job || !job.scope.isCurrent())
      throw this.failure(job, "revoked");
  }
  private requireJob(jobId: string): AnalysisJob {
    if (this.current?.operation.jobId !== jobId)
      throw this.rejected("This analysis is no longer available.");
    return this.current;
  }
  private publish(job: AnalysisJob, force = false): void {
    if (this.current !== job || (!force && this.now() - job.lastPublished < 500)) return;
    job.lastPublished = this.now();
    this.revision++;
    this.notify();
  }
  private notify(): void {
    try {
      const job = this.current;
      if (job?.reader !== undefined) this.applyProgress(job, job.reader.snapshot());
      this.options.changed(
        structuredClone({
          scopeId: this.scopeId,
          revision: this.revision,
          operation: this.current?.operation ?? null,
        }),
      );
    } catch {
      /* Transport failure cannot strand the original reader or replay counted records. */
    }
  }
  private rejected(summary: string): RecordAnalysisOperationError {
    return new RecordAnalysisOperationError({
      code: "VALIDATION",
      stage: "validation",
      summary,
      recovery: "Check the analysis state and options, then retry.",
      correlationId: this.id(),
      retryable: false,
      activeStateChanged: false,
    });
  }
  private failure(job: AnalysisJob, reason: RecordAnalysisReason): RecordAnalysisOperationError {
    const cleanup = reason === "cleanup-failed";
    return new RecordAnalysisOperationError({
      code: reason === "revoked" ? "CANCELLED" : "INTERNAL",
      stage: "query",
      summary: cleanup
        ? "Analysis reader cleanup could not be confirmed."
        : reason === "revoked"
          ? "Analysis authority changed; its results were cleared."
          : reason === "read-failed"
            ? "The analysis reader failed; no complete result was published."
            : "The analysis failed; no complete result was published.",
      recovery: cleanup
        ? job.unconfirmedOpenCleanup
          ? "A reader opened during cancellation and did not confirm cleanup. Disconnect and restart the host before analyzing again."
          : "Discard this analysis to retry cleanup before starting another."
        : "Check the connection, retained offsets and analysis options, then start a new analysis.",
      correlationId: job.operation.jobId,
      retryable: false,
      activeStateChanged: false,
    });
  }
}
