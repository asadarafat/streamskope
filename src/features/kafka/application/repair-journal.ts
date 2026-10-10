import {
  parseRepairJournalDocument,
  summarizeRepairJob,
  REPAIR_JOB_LIMITS,
  type RepairJournalDocument,
  type RepairJobSummary,
  type RepairJob,
} from "../contracts/repair-jobs";
import type { RecordReplayReview } from "../contracts/record-replay";
import type { KafkaWriteOutcome } from "../contracts/reviewed-writes";
import type { RepairArchiveInput, RepairFinding } from "../contracts/repair-recovery";
import { sameKafkaCompleteRecord } from "../contracts/record-bytes";

/** Host-private origin; the renderer cannot supply or alter this captured snapshot. */
export interface RepairContinuation {
  readonly parent: RepairJob;
  readonly startIndex: number;
}

export interface RepairJobStore {
  readonly durability: "durable" | "session" | "unavailable";
  load(): Promise<RepairJournalDocument>;
  commit(document: RepairJournalDocument): Promise<void>;
}
export class RepairJournalStorageError extends Error {
  constructor(message: string, cause?: unknown) {
    super(message, cause === undefined ? undefined : { cause });
  }
}
export class MemoryRepairJobStore implements RepairJobStore {
  readonly durability = "session" as const;
  private document: RepairJournalDocument = { schemaVersion: 3, jobs: [] };
  load(): Promise<RepairJournalDocument> {
    return Promise.resolve(structuredClone(this.document));
  }
  commit(document: RepairJournalDocument): Promise<void> {
    this.document = parseRepairJournalDocument(structuredClone(document));
    return Promise.resolve();
  }
}
export class UnavailableRepairJobStore implements RepairJobStore {
  readonly durability = "unavailable" as const;
  load(): Promise<RepairJournalDocument> {
    return Promise.reject(new Error("Protected repair storage is unavailable."));
  }
  commit(): Promise<void> {
    return Promise.reject(new Error("Protected repair storage is unavailable."));
  }
}
/** Serializes write-ahead transitions. A failed commit fences this owner until it is reopened. */
export class RepairJournal {
  private serial: Promise<unknown> = Promise.resolve();
  private failed = false;
  private readonly active = new Set<string>();
  constructor(
    readonly store: RepairJobStore,
    private readonly now = Date.now,
  ) {}
  async list(): Promise<readonly RepairJobSummary[]> {
    await this.serial.catch(() => undefined);
    let document: RepairJournalDocument;
    try {
      document = parseRepairJournalDocument(await this.store.load());
    } catch (cause) {
      throw new RepairJournalStorageError(
        "Protected repair history could not be read. Preserve the journal and complete vault backup.",
        cause,
      );
    }
    return document.jobs.map((j) => ({
      ...summarizeRepairJob(j),
      ...(this.active.has(j.id) && !this.failed ? { status: "running" as const } : {}),
      canContinue:
        !this.failed &&
        !this.active.has(j.id) &&
        j.continuationId === null &&
        this.nextIndex(j) < j.review.batch.records.length,
      canArchive:
        !this.failed &&
        j.parentJobId === null &&
        this.chain(document, j.id).every(
          (job) =>
            !this.active.has(job.id) &&
            job.pendingIndex === null &&
            job.outcomes.every((o) => o.state !== "unknown"),
        ),
    }));
  }
  async snapshot(id: string): Promise<RepairJob> {
    await this.serial;
    if (this.failed || this.store.durability === "unavailable")
      throw new RepairJournalStorageError(
        "Repair storage is unavailable. Reopen and inspect the retained journal.",
      );
    let document: RepairJournalDocument;
    try {
      document = parseRepairJournalDocument(await this.store.load());
    } catch (cause) {
      throw new RepairJournalStorageError(
        "Protected repair history could not be read. Preserve the complete backup.",
        cause,
      );
    }
    const job = document.jobs.find((j) => j.id === id);
    if (!job) throw new Error("Repair job is missing.");
    return structuredClone(job);
  }
  async continuation(id: string): Promise<RepairContinuation> {
    const parent = await this.snapshot(id),
      startIndex = this.nextIndex(parent);
    if (
      this.active.has(id) ||
      parent.continuationId !== null ||
      startIndex >= parent.review.batch.records.length
    )
      throw new Error(
        "This repair attempt has no available definitely unsent continuation. Refresh history.",
      );
    return { parent, startIndex };
  }
  begin(review: RecordReplayReview, continuation?: RepairContinuation): Promise<void> {
    return this.mutate((document) => {
      if (document.jobs.some((j) => j.id === review.planId))
        throw new Error("Repair job already exists.");
      if (document.jobs.length >= REPAIR_JOB_LIMITS.jobs)
        throw new RepairJournalStorageError(
          "Repair journal is full. Preserve and reconcile receipts before archiving jobs.",
        );
      const timestamp = new Date(this.now()).toISOString();
      if (continuation) {
        const current = document.jobs.find((j) => j.id === continuation.parent.id);
        if (
          !current ||
          this.active.has(current.id) ||
          current.continuationId !== null ||
          JSON.stringify(current) !== JSON.stringify(continuation.parent)
        )
          throw new Error("Repair history changed after review. Refresh and review again.");
        const next = this.nextIndex(current);
        if (
          next !== continuation.startIndex ||
          next >= current.review.batch.records.length ||
          review.destination.clusterId !== current.review.destination.clusterId ||
          review.destination.topicId !== current.review.destination.topicId ||
          review.destination.partitions !== current.review.destination.partitions ||
          review.input.topic !== current.review.input.topic ||
          review.input.partition !== current.review.input.partition ||
          review.batch.records.length !== current.review.batch.records.length - next ||
          !review.batch.records.every((value, index) =>
            sameKafkaCompleteRecord(value, current.review.batch.records[next + index]!),
          ) ||
          JSON.stringify(review.batch.timestamps) !==
            JSON.stringify(current.review.batch.timestamps?.slice(next)) ||
          JSON.stringify(review.input.records) !==
            JSON.stringify(current.review.input.records.slice(next)) ||
          JSON.stringify(review.encoding) !== JSON.stringify(current.review.encoding?.slice(next))
        )
          throw new Error(
            "Continuation differs from the frozen unsent records or original destination.",
          );
      }
      return {
        schemaVersion: 3,
        jobs: [
          ...document.jobs.map((job) =>
            job.id === continuation?.parent.id
              ? {
                  ...job,
                  continuationId: review.planId,
                  revision: job.revision + 1,
                  updatedAt: timestamp,
                }
              : job,
          ),
          {
            id: review.planId,
            createdAt: timestamp,
            updatedAt: timestamp,
            review: structuredClone(review),
            outcomes: [],
            pendingIndex: null,
            status: "running",
            cleanup: "pending",
            revision: 1,
            parentJobId: continuation?.parent.id ?? null,
            continuationId: null,
            findings: [],
          },
        ],
      };
    }).then(() => {
      this.active.add(review.planId);
    });
  }
  recordFinding(snapshot: RepairJob, finding: RepairFinding): Promise<void> {
    return this.update(snapshot.id, (job) => {
      if (this.active.has(job.id) || JSON.stringify(job) !== JSON.stringify(snapshot))
        throw new Error("Repair history changed during observation. Refresh before another check.");
      if (
        job.findings.length >= REPAIR_JOB_LIMITS.findings ||
        job.findings.some((f) => f.id === finding.id)
      )
        throw new Error("Repair observation history is full or this request was already used.");
      return { ...job, findings: [...job.findings, structuredClone(finding)] };
    });
  }
  archive(input: RepairArchiveInput): Promise<void> {
    return this.mutate((document) => {
      const root = document.jobs.find((j) => j.id === input.jobId);
      if (!root || root.parentJobId !== null || input.confirmation !== root.id)
        throw new Error("Confirm the exact root repair job to archive its chain.");
      const chain = this.chain(document, root.id);
      if (
        JSON.stringify(chain.map((j) => ({ id: j.id, revision: j.revision }))) !==
          JSON.stringify(input.chain) ||
        chain.some(
          (j) =>
            this.active.has(j.id) ||
            j.pendingIndex !== null ||
            j.outcomes.some((o) => o.state === "unknown"),
        )
      )
        throw new Error(
          "The repair chain changed or contains an active or uncertain write. Refresh and preserve its recovery state.",
        );
      const ids = new Set(chain.map((j) => j.id));
      return { schemaVersion: 3, jobs: document.jobs.filter((j) => !ids.has(j.id)) };
    });
  }
  intent(id: string, index: number): Promise<void> {
    return this.update(id, (j) => {
      if (j.pendingIndex !== null || index !== j.outcomes.length || j.status !== "running")
        throw new Error("Invalid repair dispatch transition.");
      return { ...j, pendingIndex: index };
    });
  }
  receipt(id: string, index: number, outcome: KafkaWriteOutcome | undefined): Promise<void> {
    return this.update(id, (j) => {
      if (j.pendingIndex !== index)
        throw new Error("Repair receipt has no matching dispatch intent.");
      return {
        ...j,
        pendingIndex: null,
        outcomes: outcome === undefined ? j.outcomes : [...j.outcomes, outcome],
      };
    });
  }
  finish(id: string, complete: boolean, cleanup: "complete" | "unavailable"): Promise<void> {
    return this.update(id, (j) => ({
      ...j,
      status: complete ? "complete" : "stopped",
      cleanup,
    })).finally(() => {
      this.active.delete(id);
    });
  }
  private update(id: string, change: (job: RepairJob) => RepairJob): Promise<void> {
    return this.mutate((document) => {
      if (!document.jobs.some((j) => j.id === id)) throw new Error("Repair job is missing.");
      return {
        schemaVersion: 3,
        jobs: document.jobs.map((j) =>
          j.id === id
            ? {
                ...change(j),
                updatedAt: new Date(this.now()).toISOString(),
                revision: j.revision + 1,
              }
            : j,
        ),
      };
    });
  }
  private nextIndex(job: RepairJob): number {
    return job.outcomes.length + (job.pendingIndex === null ? 0 : 1);
  }
  private chain(document: RepairJournalDocument, id: string): readonly RepairJob[] {
    const result: RepairJob[] = [];
    let current = document.jobs.find((j) => j.id === id);
    while (current) {
      result.push(current);
      current =
        current.continuationId === null
          ? undefined
          : document.jobs.find((j) => j.id === current!.continuationId);
    }
    return result;
  }
  private mutate(
    change: (document: RepairJournalDocument) => RepairJournalDocument,
  ): Promise<void> {
    const operation = this.serial.then(async () => {
      if (this.failed || this.store.durability === "unavailable")
        throw new RepairJournalStorageError(
          "Repair journal is unavailable. Reopen and inspect retained receipts before retrying.",
        );
      let document: RepairJournalDocument;
      try {
        document = parseRepairJournalDocument(await this.store.load());
      } catch (cause) {
        throw new RepairJournalStorageError(
          "Protected repair history could not be read. Preserve the journal and complete vault backup.",
          cause,
        );
      }
      // Validation/capacity failures occur before commit and do not poison an otherwise sound journal.
      const next = parseRepairJournalDocument(change(document));
      try {
        await this.store.commit(next);
      } catch (cause) {
        this.failed = true;
        throw new RepairJournalStorageError(
          "Repair journal commit is uncertain. No further writes are allowed through this owner.",
          cause,
        );
      }
    });
    this.serial = operation.catch(() => undefined);
    return operation;
  }
}
