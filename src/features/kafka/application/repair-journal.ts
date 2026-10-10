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
  private document: RepairJournalDocument = { schemaVersion: 1, jobs: [] };
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
    }));
  }
  begin(review: RecordReplayReview): Promise<void> {
    return this.mutate((document) => {
      if (document.jobs.some((j) => j.id === review.planId))
        throw new Error("Repair job already exists.");
      if (document.jobs.length >= REPAIR_JOB_LIMITS.jobs)
        throw new RepairJournalStorageError(
          "Repair journal is full. Preserve and reconcile receipts before archiving jobs.",
        );
      const timestamp = new Date(this.now()).toISOString();
      return {
        schemaVersion: 1,
        jobs: [
          ...document.jobs,
          {
            id: review.planId,
            createdAt: timestamp,
            updatedAt: timestamp,
            review: structuredClone(review),
            outcomes: [],
            pendingIndex: null,
            status: "running",
            cleanup: "pending",
          },
        ],
      };
    }).then(() => {
      this.active.add(review.planId);
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
        schemaVersion: 1,
        jobs: document.jobs.map((j) =>
          j.id === id
            ? {
                ...change(j),
                updatedAt: new Date(this.now()).toISOString(),
              }
            : j,
        ),
      };
    });
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
