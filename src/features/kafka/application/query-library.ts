import {
  parseKafkaSavedView,
  parseKafkaQueryLibraryDocument,
  type KafkaSavedView,
  type KafkaQueryLibrarySnapshot,
} from "../contracts";

export interface KafkaQueryStore {
  readonly durability: "session" | "durable";
  load(): Promise<readonly KafkaSavedView[]>;
  commit(queries: readonly KafkaSavedView[]): Promise<void>;
}

export class KafkaQueryLibraryError extends Error {
  readonly code = "QUERY_UNAVAILABLE" as const;
  readonly stage = "query" as const;
  readonly retryable = true;
  readonly target = undefined;
  readonly recovery =
    "Review the view name and capacity (100 views). Reopen Saved views to inspect current state before retrying; preserve unreadable storage and restore a verified backup if needed.";
}

export class InMemoryKafkaQueryStore implements KafkaQueryStore {
  readonly durability = "session" as const;
  private queries: readonly KafkaSavedView[] = [];
  load(): Promise<readonly KafkaSavedView[]> {
    return Promise.resolve(this.queries);
  }
  commit(queries: readonly KafkaSavedView[]): Promise<void> {
    this.queries = parseKafkaQueryLibraryDocument({ schemaVersion: 3, queries }).queries;
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
  put(query: KafkaSavedView, expected?: KafkaSavedView | null): Promise<KafkaQueryLibrarySnapshot> {
    return this.run((queries) => {
      const validated = parseKafkaSavedView(query);
      const index = queries.findIndex((entry) => entry.id === validated.id);
      if (expected !== undefined) {
        const previous = expected === null ? null : parseKafkaSavedView(expected);
        if (
          previous === null
            ? index >= 0
            : previous.id !== validated.id ||
              index < 0 ||
              JSON.stringify(previous) !== JSON.stringify(queries[index])
        )
          throw new KafkaQueryLibraryError(
            "This saved view changed or was deleted since it was opened. Refresh Saved views, review the current settings and bookmarks, then retry your change. Nothing was overwritten.",
          );
      }
      return index < 0
        ? [...queries, validated]
        : queries.map((entry) => (entry.id === validated.id ? validated : entry));
    });
  }
  delete(id: string): Promise<KafkaQueryLibrarySnapshot> {
    return this.run((queries) => queries.filter((entry) => entry.id !== id));
  }

  private run(
    change?: (queries: readonly KafkaSavedView[]) => readonly KafkaSavedView[],
  ): Promise<KafkaQueryLibrarySnapshot> {
    const result = this.pending.then(async () => {
      try {
        const original = parseKafkaQueryLibraryDocument({
          schemaVersion: 3,
          queries: await this.store.load(),
        }).queries;
        const queries =
          change === undefined
            ? original
            : parseKafkaQueryLibraryDocument({ schemaVersion: 3, queries: change(original) })
                .queries;
        if (change !== undefined && JSON.stringify(queries) !== JSON.stringify(original))
          await this.store.commit(queries);
        return { durability: this.store.durability, queries };
      } catch (error) {
        if (error instanceof KafkaQueryLibraryError) throw error;
        throw new KafkaQueryLibraryError(
          "The saved view could not be loaded or committed. Reopen Saved views to inspect current state before retrying.",
        );
      }
    });
    this.pending = result.catch(() => undefined);
    return result;
  }
}
