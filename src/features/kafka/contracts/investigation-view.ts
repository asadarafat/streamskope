import { KAFKA_CONSUMER_GROUP_LIMITS } from "./consumer-group-types";
import { HostContractValidationError } from "./validation-error";
import {
  declaredValue,
  exactKeys,
  positiveBoundedInteger,
  record,
  text,
  truth,
} from "./validation-primitives";

export const KAFKA_MESSAGE_VIEW_COLUMNS = [
  "timestamp",
  "key",
  "preview",
  "partition",
  "offset",
  "rules",
] as const;
export type KafkaMessageViewColumn = (typeof KAFKA_MESSAGE_VIEW_COLUMNS)[number];
export const KAFKA_TOPIC_VIEW_WORKSPACES = [
  "messages",
  "monitor",
  "latency",
  "rules",
  "configuration",
] as const;
export type KafkaTopicViewWorkspace = (typeof KAFKA_TOPIC_VIEW_WORKSPACES)[number];
export const KAFKA_MESSAGE_COLUMN_MIN_WIDTHS: Readonly<Record<KafkaMessageViewColumn, number>> =
  Object.freeze({ timestamp: 142, key: 92, preview: 130, partition: 50, offset: 50, rules: 106 });
export const KAFKA_INVESTIGATION_VIEW_LIMITS = Object.freeze({
  columnWidthMaximum: 960,
  inspectorWidth: Object.freeze({ minimum: 292, maximum: 560, default: 320 }),
});
export type KafkaViewDestination =
  | { readonly kind: "topic"; readonly workspace: KafkaTopicViewWorkspace }
  | { readonly kind: "consumer-group"; readonly groupId: string };
export interface KafkaMessageViewPresentation {
  readonly visibleColumns: readonly KafkaMessageViewColumn[];
  readonly columnWidths: readonly {
    readonly column: KafkaMessageViewColumn;
    readonly pixels: number;
  }[];
  readonly inspectorWidth: number;
  readonly filtersOpen: boolean;
}
export interface KafkaInvestigationView {
  readonly schemaVersion: 1;
  readonly destination: KafkaViewDestination;
  readonly messages: KafkaMessageViewPresentation;
}

/**
 * Fixed schema-1 defaults, also used to expand compact disk format 2. Changing
 * these requires a format migration; responsive presentation is never stored here.
 */
export function createDefaultKafkaInvestigationView(
  destination: KafkaViewDestination = { kind: "topic", workspace: "messages" },
): KafkaInvestigationView {
  return {
    schemaVersion: 1,
    destination: { ...destination },
    messages: {
      visibleColumns: [...KAFKA_MESSAGE_VIEW_COLUMNS],
      columnWidths: [],
      inspectorWidth: KAFKA_INVESTIGATION_VIEW_LIMITS.inspectorWidth.default,
      filtersOpen: false,
    },
  };
}

function parseDestination(value: unknown, path: string): KafkaViewDestination {
  const input = record(value, path);
  const kind = declaredValue(input.kind, ["topic", "consumer-group"], `${path}.kind`);
  if (kind === "topic") {
    exactKeys(input, ["kind", "workspace"], path);
    return {
      kind,
      workspace: declaredValue(input.workspace, KAFKA_TOPIC_VIEW_WORKSPACES, `${path}.workspace`),
    };
  }
  exactKeys(input, ["kind", "groupId"], path);
  return {
    kind,
    groupId: text(input.groupId, `${path}.groupId`, KAFKA_CONSUMER_GROUP_LIMITS.groupIdCharacters),
  };
}
function boundedColumns(value: unknown, path: string, minimum: number): unknown[] {
  if (
    !Array.isArray(value) ||
    value.length < minimum ||
    value.length > KAFKA_MESSAGE_VIEW_COLUMNS.length
  )
    throw new HostContractValidationError(
      path,
      `must contain ${String(minimum)} to ${String(KAFKA_MESSAGE_VIEW_COLUMNS.length)} columns`,
    );
  return value;
}
export function parseKafkaMessageViewPresentation(
  value: unknown,
  path = "messageView",
): KafkaMessageViewPresentation {
  const input = record(value, path);
  exactKeys(input, ["visibleColumns", "columnWidths", "inspectorWidth", "filtersOpen"], path);
  const visible = boundedColumns(input.visibleColumns, `${path}.visibleColumns`, 1).map(
    (column, index) =>
      declaredValue(column, KAFKA_MESSAGE_VIEW_COLUMNS, `${path}.visibleColumns[${String(index)}]`),
  );
  if (new Set(visible).size !== visible.length)
    throw new HostContractValidationError(`${path}.visibleColumns`, "must not repeat columns");
  const widths = boundedColumns(input.columnWidths, `${path}.columnWidths`, 0).map(
    (value, index) => {
      const at = `${path}.columnWidths[${String(index)}]`;
      const width = record(value, at);
      exactKeys(width, ["column", "pixels"], at);
      const column = declaredValue(width.column, KAFKA_MESSAGE_VIEW_COLUMNS, `${at}.column`);
      const pixels = positiveBoundedInteger(
        width.pixels,
        `${at}.pixels`,
        KAFKA_INVESTIGATION_VIEW_LIMITS.columnWidthMaximum,
      );
      if (pixels < KAFKA_MESSAGE_COLUMN_MIN_WIDTHS[column])
        throw new HostContractValidationError(`${at}.pixels`, "is below the column minimum");
      return { column, pixels };
    },
  );
  if (new Set(widths.map((width) => width.column)).size !== widths.length)
    throw new HostContractValidationError(`${path}.columnWidths`, "must not repeat columns");
  const inspectorWidth = positiveBoundedInteger(
    input.inspectorWidth,
    `${path}.inspectorWidth`,
    KAFKA_INVESTIGATION_VIEW_LIMITS.inspectorWidth.maximum,
  );
  if (inspectorWidth < KAFKA_INVESTIGATION_VIEW_LIMITS.inspectorWidth.minimum)
    throw new HostContractValidationError(
      `${path}.inspectorWidth`,
      "is below the inspector minimum",
    );
  return {
    visibleColumns: KAFKA_MESSAGE_VIEW_COLUMNS.filter((column) => visible.includes(column)),
    columnWidths: KAFKA_MESSAGE_VIEW_COLUMNS.flatMap((column) =>
      widths.filter((width) => width.column === column),
    ),
    inspectorWidth,
    filtersOpen: truth(input.filtersOpen, `${path}.filtersOpen`),
  };
}
export function parseKafkaInvestigationView(
  value: unknown,
  path = "investigationView",
): KafkaInvestigationView {
  const input = record(value, path);
  exactKeys(input, ["schemaVersion", "destination", "messages"], path);
  if (input.schemaVersion !== 1)
    throw new HostContractValidationError(
      `${path}.schemaVersion`,
      "must use saved-view descriptor version 1",
    );
  return {
    schemaVersion: 1,
    destination: parseDestination(input.destination, `${path}.destination`),
    messages: parseKafkaMessageViewPresentation(input.messages, `${path}.messages`),
  };
}
