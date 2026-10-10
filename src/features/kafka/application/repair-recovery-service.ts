import type {
  RepairArchiveInput,
  RepairContinuationInput,
  RepairContinuationReview,
  RepairFinding,
  RepairReconciliationInput,
} from "../contracts/repair-recovery";
import type { RepairJobSummary } from "../contracts/repair-jobs";

import type { RepairJournal } from "./repair-journal";
import type { RecordReplayService } from "./record-replay-service";
import type { RepairReconciliationReader } from "./repair-reconciliation-reader";

/** Coordinates recovery with the existing replay owner and its protected write-ahead journal. */
export class RepairRecoveryService {
  private pending: Promise<unknown> | undefined;
  private invalidating = false;
  private closing: Promise<void> | undefined;
  constructor(
    private readonly journal: RepairJournal,
    private readonly replay: RecordReplayService,
    private readonly reader: RepairReconciliationReader,
  ) {}
  available(): boolean {
    return (
      !this.invalidating &&
      !this.pending &&
      this.replay.recoveryAvailable() &&
      this.reader.available()
    );
  }
  replayAvailable(): boolean {
    return !this.invalidating && !this.pending && this.reader.available();
  }
  async list(): Promise<readonly RepairJobSummary[]> {
    const jobs = await this.journal.list();
    return jobs.map((j) => ({
      ...j,
      canContinue: j.canContinue && this.available(),
      canArchive: j.canArchive && this.available(),
    }));
  }
  review(input: RepairContinuationInput): Promise<RepairContinuationReview> {
    return this.run(async () => {
      const origin = await this.journal.continuation(input.jobId);
      this.assertCurrent();
      const review = await this.replay.review(
        {
          ...origin.parent.review.input,
          targetProfile: input.targetProfile,
          records: origin.parent.review.input.records.slice(origin.startIndex),
        },
        origin,
      );
      return {
        parentJobId: origin.parent.id,
        skipped: {
          acknowledged: origin.parent.outcomes.filter((o) => o.state === "acknowledged").length,
          rejected: origin.parent.outcomes.filter((o) => o.state === "rejected").length,
          uncertain:
            origin.parent.outcomes.filter((o) => o.state === "unknown").length +
            (origin.parent.pendingIndex === null ? 0 : 1),
        },
        review,
      };
    });
  }
  reconcile(input: RepairReconciliationInput): Promise<RepairFinding> {
    return this.run(async () => {
      const job = await this.journal.snapshot(input.jobId);
      this.assertCurrent();
      if (input.recordIndex >= job.review.batch.records.length)
        throw new Error("Record index is outside this attempt.");
      const result = await this.reader.observe(job, input);
      await this.journal.recordFinding(job, result);
      return result;
    });
  }
  archive(input: RepairArchiveInput): Promise<void> {
    return this.run(() => this.journal.archive(input));
  }
  invalidate(): Promise<void> {
    if (this.closing) return this.closing;
    this.invalidating = true;
    this.closing = Promise.resolve()
      .then(() => this.drain())
      .finally(() => {
        this.invalidating = false;
        this.closing = undefined;
      });
    return this.closing;
  }
  private async drain(): Promise<void> {
    const results = await Promise.allSettled([this.replay.invalidate(), this.reader.invalidate()]);
    await this.pending?.catch(() => undefined);
    if (results.some((r) => r.status === "rejected"))
      throw new Error("Repair cleanup could not be confirmed.");
  }
  private assertCurrent(): void {
    if (this.invalidating) throw new Error("Repair authority was revoked.");
  }
  private run<T>(operation: () => Promise<T>): Promise<T> {
    if (!this.available())
      return Promise.reject(new Error("Wait for repair and reader cleanup before recovery."));
    const task = Promise.resolve().then(() => {
      if (this.invalidating) throw new Error("Repair authority was revoked.");
      return operation();
    });
    this.pending = task;
    void task
      .finally(() => {
        if (this.pending === task) this.pending = undefined;
      })
      .catch(() => undefined);
    return task;
  }
}
