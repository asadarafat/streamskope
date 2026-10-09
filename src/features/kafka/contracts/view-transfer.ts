import { parseKafkaInvestigationQuery, type KafkaInvestigationQuery } from "./investigation-query";
import { parseKafkaInvestigationView, type KafkaInvestigationView } from "./investigation-view";
import type { KafkaSavedView } from "./query-library";
import { KAFKA_QUERY_TRANSFER_LIMITS, parseKafkaQueryTransfer } from "./query-transfer";
import {
  KAFKA_RECORD_LOCATOR_LIMITS,
  parseKafkaSavedRecordContext,
  type KafkaRecordLocator,
} from "./record-locator";
import { HostContractValidationError } from "./validation-error";
import { exactKeys, record, text } from "./validation-primitives";

export const KAFKA_VIEW_TRANSFER_LIMITS = Object.freeze({ documentBytes: 131_072 });
export interface KafkaPortableView {
  readonly kind: "streamskope.kafka-view";
  readonly schemaVersion: 1;
  readonly suggestedName: string | null;
  readonly configuration: KafkaInvestigationQuery | null;
  readonly view: KafkaInvestigationView;
  readonly records: {
    readonly selected: KafkaRecordLocator | null;
    readonly comparison: KafkaRecordLocator | null;
    readonly bookmarks: readonly {
      readonly name: string;
      readonly locator: KafkaRecordLocator;
    }[];
  };
}
export type KafkaInvestigationTransfer =
  | { readonly kind: "view"; readonly view: KafkaPortableView }
  | { readonly kind: "query"; readonly query: KafkaInvestigationQuery };

function documentSize(value: string): void {
  if (
    value.length > KAFKA_VIEW_TRANSFER_LIMITS.documentBytes ||
    new TextEncoder().encode(value).length > KAFKA_VIEW_TRANSFER_LIMITS.documentBytes
  )
    throw new HostContractValidationError("view", "view documents are limited to 128 KiB");
}

/** Portable data never carries local IDs, content or connection authority. */
export function parseKafkaPortableView(value: unknown): KafkaPortableView {
  const input = record(value, "portableView");
  exactKeys(
    input,
    ["kind", "schemaVersion", "suggestedName", "configuration", "view", "records"],
    "portableView",
  );
  if (input.kind !== "streamskope.kafka-view" || input.schemaVersion !== 1)
    throw new HostContractValidationError(
      "portableView",
      "unsupported view document version or kind",
    );
  const suggestedName =
    input.suggestedName === null
      ? null
      : text(input.suggestedName, "portableView.suggestedName", 128).trim();
  if (suggestedName === "")
    throw new HostContractValidationError("portableView.suggestedName", "must not be blank");
  const configuration =
    input.configuration === null ? null : parseKafkaInvestigationQuery(input.configuration);
  const view = parseKafkaInvestigationView(input.view);
  if (view.destination.kind === "topic" && configuration === null)
    throw new HostContractValidationError(
      "portableView.configuration",
      "a topic view requires query settings",
    );
  const positions = record(input.records, "portableView.records");
  exactKeys(positions, ["selected", "comparison", "bookmarks"], "portableView.records");
  if (
    !Array.isArray(positions.bookmarks) ||
    positions.bookmarks.length > KAFKA_RECORD_LOCATOR_LIMITS.bookmarksPerView
  )
    throw new HostContractValidationError(
      "portableView.records.bookmarks",
      "at most 32 bookmarks fit in a view",
    );
  // Validation keys reuse the canonical cross-position constraints; they never leave this parser.
  const records = parseKafkaSavedRecordContext({
    selected: positions.selected,
    comparison: positions.comparison,
    bookmarks: positions.bookmarks.map((value: unknown, index: number) => {
      const path = `portableView.records.bookmarks[${String(index)}]`;
      const bookmark = record(value, path);
      exactKeys(bookmark, ["name", "locator"], path);
      return { id: String(index), name: bookmark.name, locator: bookmark.locator };
    }),
  });
  const parsed: KafkaPortableView = {
    kind: "streamskope.kafka-view",
    schemaVersion: 1,
    suggestedName,
    configuration,
    view,
    records: {
      selected: records.selected,
      comparison: records.comparison,
      bookmarks: records.bookmarks.map(({ name, locator }) => ({ name, locator })),
    },
  };
  documentSize(JSON.stringify(parsed));
  return parsed;
}

export function serializeKafkaPortableView(view: KafkaPortableView): string {
  const content = JSON.stringify(parseKafkaPortableView(view), null, 2) + "\n";
  documentSize(content);
  return content;
}

/** Explicit projection omits all source-local IDs and other saved-view fields. */
export function createKafkaPortableView(
  settings: Pick<KafkaSavedView, "configuration" | "view" | "records">,
  suggestedName: string | null,
): KafkaPortableView {
  return parseKafkaPortableView({
    kind: "streamskope.kafka-view",
    schemaVersion: 1,
    suggestedName,
    configuration: settings.configuration,
    view: settings.view,
    records: {
      selected: settings.records.selected,
      comparison: settings.records.comparison,
      bookmarks: settings.records.bookmarks.map(({ name, locator }) => ({ name, locator })),
    },
  });
}

/** Read/review only: no navigation, UUID allocation, host commands or persistence. */
export function parseKafkaInvestigationTransfer(input: string): KafkaInvestigationTransfer {
  try {
    documentSize(input);
    const content = input.trim();
    if (content.startsWith("{")) {
      const value: unknown = JSON.parse(content);
      if (value !== null && typeof value === "object" && Object.hasOwn(value, "kind"))
        return { kind: "view", view: parseKafkaPortableView(value) };
      // The shared file chooser accepts larger view files; legacy JSON keeps its original file bound.
      if (new TextEncoder().encode(input).length > KAFKA_QUERY_TRANSFER_LIMITS.documentBytes)
        throw new Error("Legacy query document exceeds its byte limit");
    }
    return { kind: "query", query: parseKafkaQueryTransfer(input) };
  } catch {
    throw new HostContractValidationError(
      "import",
      "invalid or unsupported import. Use a version 1 StreamSkope view JSON (maximum 128 KiB), a version 1 query JSON (maximum 32 KiB), or a StreamSkope query link",
    );
  }
}
