import {
  parseKafkaSavedQuery,
  parseKafkaQueryLibraryDocument,
  type KafkaSavedQuery,
  type KafkaQueryLibrarySnapshot,
} from "../contracts";

export interface KafkaQueryStore {
  readonly durability: "session" | "durable";
  load(): Promise<readonly KafkaSavedQuery[]>;
  commit(queries: readonly KafkaSavedQuery[]): Promise<void>;
}

export class KafkaQueryLibraryError extends Error {
  readonly code = "QUERY_UNAVAILABLE" as const;
  readonly stage = "query" as const;
  readonly retryable = true;
  readonly target = undefined;
  readonly recovery =
    "Review the query name and capacity (100 queries). If storage is unreadable, preserve the queries file and restore a valid backup; then reopen Saved queries.";
}

export class InMemoryKafkaQueryStore implements KafkaQueryStore {
  readonly durability = "session" as const;
  private queries: readonly KafkaSavedQuery[] = [];
  load(): Promise<readonly KafkaSavedQuery[]> {
    return Promise.resolve(this.queries);
  }
  commit(queries: readonly KafkaSavedQuery[]): Promise<void> {
    this.queries = parseKafkaQueryLibraryDocument({ schemaVersion: 1, queries }).queries;
    return Promise.resolve();
  }
}

export class KafkaQueryLibrary {
  private pending: Promise<unknown> = Promise.resolve();
  constructor(private readonly store: KafkaQueryStore = new InMemoryKafkaQueryStore()) {}

  idle(): Promise<void> {
    return this.pending.then(() => undefined);
  }

  list(): Promise<KafkaQueryLibrarySnapshot> {
    return this.run();
  }
  put(query: KafkaSavedQuery): Promise<KafkaQueryLibrarySnapshot> {
    return this.run((queries) => {
      const validated = parseKafkaSavedQuery(query);
      const index = queries.findIndex((entry) => entry.id === validated.id);
      return index < 0
        ? [...queries, validated]
        : queries.map((entry) => (entry.id === validated.id ? validated : entry));
    });
  }
  delete(id: string): Promise<KafkaQueryLibrarySnapshot> {
    return this.run((queries) => queries.filter((entry) => entry.id !== id));
  }

  private run(
    change?: (queries: readonly KafkaSavedQuery[]) => readonly KafkaSavedQuery[],
  ): Promise<KafkaQueryLibrarySnapshot> {
    const result = this.pending.then(async () => {
      try {
        const original = parseKafkaQueryLibraryDocument({
          schemaVersion: 1,
          queries: await this.store.load(),
        }).queries;
        const queries =
          change === undefined
            ? original
            : parseKafkaQueryLibraryDocument({ schemaVersion: 1, queries: change(original) })
                .queries;
        if (change !== undefined && JSON.stringify(queries) !== JSON.stringify(original))
          await this.store.commit(queries);
        return { durability: this.store.durability, queries };
      } catch (error) {
        if (error instanceof KafkaQueryLibraryError) throw error;
        throw new KafkaQueryLibraryError(
          "The saved query could not be loaded or committed. Existing queries were preserved.",
        );
      }
    });
    this.pending = result.catch(() => undefined);
    return result;
  }
}
