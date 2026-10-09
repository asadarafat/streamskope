import {
  parseKafkaSavedView,
  parseKafkaQueryLibraryDocument,
  serializeKafkaQueryLibraryDocument,
  KAFKA_QUERY_LIBRARY_LIMITS,
  type KafkaSavedView,
  type KafkaQueryLibrarySnapshot,
  type KafkaInvestigationLibraryState,
} from "../contracts/query-library";
import {
  parseKafkaTopicAnnotation,
  type KafkaTopicAnnotation,
  type KafkaTopicAnnotationSnapshot,
  type KafkaTopicCatalogSnapshot,
} from "../contracts/topic-catalog";
import {
  parseKafkaTopicIdentity,
  sameKafkaTopicIdentity,
  type KafkaTopicIdentity,
} from "../contracts/topic-identity";
import { utf8ByteLength } from "../contracts/message-limits";

export interface KafkaQueryStore {
  readonly durability: "session" | "durable";
  load(): Promise<KafkaInvestigationLibraryState>;
  commit(state: KafkaInvestigationLibraryState): Promise<void>;
}
export class KafkaQueryLibraryError extends Error {
  readonly code = "QUERY_UNAVAILABLE" as const;
  readonly stage = "query" as const;
  readonly retryable = true;
  readonly target = undefined;
  readonly recovery =
    "Review Saved views and Local notes (100 views, 256 annotated topics, 1 MiB shared storage). Refresh before retrying; preserve unreadable storage and restore a verified backup if needed.";
}
function canonical(state: KafkaInvestigationLibraryState): KafkaInvestigationLibraryState {
  const { queries, topics } = parseKafkaQueryLibraryDocument({ schemaVersion: 4, ...state });
  return { queries, topics };
}
function capacity(state: KafkaInvestigationLibraryState): void {
  if (
    utf8ByteLength(serializeKafkaQueryLibraryDocument(state)) > KAFKA_QUERY_LIBRARY_LIMITS.fileBytes
  )
    throw new KafkaQueryLibraryError(
      "Views and local notes exceed their shared 1 MiB storage limit. Remove unneeded entries and retry; nothing was pruned.",
    );
}
export class InMemoryKafkaQueryStore implements KafkaQueryStore {
  readonly durability = "session" as const;
  private state: KafkaInvestigationLibraryState = { queries: [], topics: [] };
  load(): Promise<KafkaInvestigationLibraryState> {
    return Promise.resolve(this.state);
  }
  commit(state: KafkaInvestigationLibraryState): Promise<void> {
    const next = canonical(state);
    capacity(next);
    this.state = next;
    return Promise.resolve();
  }
}

/** One queue owns the whole document; views and notes cannot drop each other's changes. */
export class KafkaQueryLibrary {
  private pending: Promise<unknown> = Promise.resolve();
  constructor(private readonly store: KafkaQueryStore = new InMemoryKafkaQueryStore()) {}
  idle(): Promise<void> {
    return this.pending.then(() => undefined);
  }
  list(): Promise<KafkaQueryLibrarySnapshot> {
    return this.run((state) => ({ durability: this.store.durability, queries: state.queries }));
  }
  put(query: KafkaSavedView, expected?: KafkaSavedView | null): Promise<KafkaQueryLibrarySnapshot> {
    return this.run(
      (state) => ({ durability: this.store.durability, queries: state.queries }),
      (state) => {
        const validated = parseKafkaSavedView(query);
        const previous = state.queries.find((entry) => entry.id === validated.id) ?? null;
        if (
          expected !== undefined &&
          JSON.stringify(expected === null ? null : parseKafkaSavedView(expected)) !==
            JSON.stringify(previous)
        )
          throw new KafkaQueryLibraryError(
            "This saved view changed or was deleted since it was opened. Refresh Saved views, review the current settings and bookmarks, then retry your change. Nothing was overwritten.",
          );
        return {
          ...state,
          queries:
            previous === null
              ? [...state.queries, validated]
              : state.queries.map((entry) => (entry.id === validated.id ? validated : entry)),
        };
      },
    );
  }
  delete(id: string): Promise<KafkaQueryLibrarySnapshot> {
    return this.run(
      (state) => ({ durability: this.store.durability, queries: state.queries }),
      (state) => ({ ...state, queries: state.queries.filter((entry) => entry.id !== id) }),
    );
  }
  listTopics(): Promise<KafkaTopicCatalogSnapshot> {
    return this.run((state) => ({ durability: this.store.durability, topics: state.topics }));
  }
  getTopic(identity: KafkaTopicIdentity): Promise<KafkaTopicAnnotationSnapshot> {
    const selected = parseKafkaTopicIdentity(identity);
    return this.run((state) => this.topicSnapshot(state, selected));
  }
  putTopic(
    annotation: KafkaTopicAnnotation,
    expected: KafkaTopicAnnotation | null,
    assertCurrent?: () => void,
  ): Promise<KafkaTopicAnnotationSnapshot> {
    const validated = parseKafkaTopicAnnotation(annotation);
    const previous = expected === null ? null : parseKafkaTopicAnnotation(expected);
    return this.run(
      (state) => this.topicSnapshot(state, validated.identity),
      (state) => {
        this.assertTopicExpected(state, validated.identity, previous);
        const exists = state.topics.some((entry) =>
          sameKafkaTopicIdentity(entry.identity, validated.identity),
        );
        return {
          ...state,
          topics: exists
            ? state.topics.map((entry) =>
                sameKafkaTopicIdentity(entry.identity, validated.identity) ? validated : entry,
              )
            : [...state.topics, validated],
        };
      },
      assertCurrent,
    );
  }
  deleteTopic(
    identity: KafkaTopicIdentity,
    expected: KafkaTopicAnnotation,
  ): Promise<KafkaTopicAnnotationSnapshot> {
    const selected = parseKafkaTopicIdentity(identity);
    const previous = parseKafkaTopicAnnotation(expected);
    return this.run(
      (state) => this.topicSnapshot(state, selected),
      (state) => {
        this.assertTopicExpected(state, selected, previous);
        return {
          ...state,
          topics: state.topics.filter((entry) => !sameKafkaTopicIdentity(entry.identity, selected)),
        };
      },
    );
  }
  private topicSnapshot(
    state: KafkaInvestigationLibraryState,
    identity: KafkaTopicIdentity,
  ): KafkaTopicAnnotationSnapshot {
    return {
      durability: this.store.durability,
      identity,
      annotation:
        state.topics.find((entry) => sameKafkaTopicIdentity(entry.identity, identity)) ?? null,
    };
  }
  private assertTopicExpected(
    state: KafkaInvestigationLibraryState,
    identity: KafkaTopicIdentity,
    expected: KafkaTopicAnnotation | null,
  ): void {
    const current = this.topicSnapshot(state, identity).annotation;
    if (
      (expected !== null && !sameKafkaTopicIdentity(expected.identity, identity)) ||
      JSON.stringify(current) !== JSON.stringify(expected)
    )
      throw new KafkaQueryLibraryError(
        "These local notes changed or were deleted since they were opened. Refresh and review the current notes before retrying. Nothing was overwritten.",
      );
  }
  private run<T>(
    select: (state: KafkaInvestigationLibraryState) => T,
    change?: (state: KafkaInvestigationLibraryState) => KafkaInvestigationLibraryState,
    assertCurrent?: () => void,
  ): Promise<T> {
    const result = this.pending.then(async () => {
      try {
        const original = canonical(await this.store.load());
        const next = change === undefined ? original : canonical(change(original));
        if (change !== undefined && JSON.stringify(next) !== JSON.stringify(original)) {
          capacity(next);
          // Authority is checked after queued reads/validation, before admitting
          // the local commit. Its eventual receipt belongs to this exact resource.
          assertCurrent?.();
          await this.store.commit(next);
        } else assertCurrent?.();
        return select(next);
      } catch (error) {
        if (error instanceof KafkaQueryLibraryError) throw error;
        throw new KafkaQueryLibraryError(
          "The investigation library could not be loaded or committed. Refresh Saved views or Local notes to inspect current state before retrying.",
        );
      }
    });
    this.pending = result.catch(() => undefined);
    return result;
  }
}
