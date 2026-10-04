import {
  KAFKA_MESSAGE_LIMITS,
  compileKafkaSearchFilter,
  KAFKA_RULE_LIMITS,
  KafkaRuleWorkBudget,
  DESKTOP_TEXT_DOCUMENT_LIMITS,
  utf8ByteLength,
  type HostTextDocument,
  type KafkaExploredMessage,
  type KafkaOriginalRecord,
} from "../contracts";

export const KAFKA_MESSAGE_OPERATION_LIMITS = {
  exportBytes: DESKTOP_TEXT_DOCUMENT_LIMITS.bytes,
  exportContentBytes: 8 * 1_048_576,
  exportMessages: KAFKA_MESSAGE_LIMITS.retainedMessages,
  exportTopicCharacters: 512,
  fileNameCharacters: DESKTOP_TEXT_DOCUMENT_LIMITS.fileNameCharacters,
  filterCharacters: 256,
} as const;

export const KAFKA_MESSAGE_TEXT_FILTER_FIELDS = [
  "timestamp",
  "offset",
  "key",
  "value",
  "expression",
] as const;

export type KafkaMessageTextFilterField = (typeof KAFKA_MESSAGE_TEXT_FILTER_FIELDS)[number];

export interface KafkaMessageFilters {
  readonly expression?: string;
  readonly activeRuleMatchesOnly: boolean;
  readonly key: string;
  readonly offset: string;
  readonly offsetExact?: string;
  readonly partition: number | null;
  readonly timestamp: string;
  readonly value: string;
}

export const KAFKA_MESSAGE_OPERATION_ERROR_CODES = [
  "NO_MESSAGES",
  "INVALID_EXPORT",
  "EXPORT_TOO_LARGE",
] as const;

export type KafkaMessageOperationErrorCode = (typeof KAFKA_MESSAGE_OPERATION_ERROR_CODES)[number];

export class KafkaMessageOperationError extends Error {
  constructor(
    readonly code: KafkaMessageOperationErrorCode,
    message: string,
    readonly recovery: string,
  ) {
    super(message);
    this.name = "KafkaMessageOperationError";
  }
}

export interface KafkaMessageExportInput {
  readonly filters: KafkaMessageFilters;
  readonly messages: readonly KafkaExploredMessage[];
  readonly retainedMessageCount: number;
  readonly stale: boolean;
  readonly topic: string;
}

export interface KafkaMessageExportRecord {
  readonly original: KafkaOriginalRecord;
  readonly headers: Readonly<Record<string, string>>;
  readonly key: string | null;
  readonly offset: string;
  readonly originalByteSize: number;
  readonly partition: number;
  readonly payload: string | null;
  readonly payloadTruncated: boolean;
  readonly preview: string;
  readonly timestamp: string;
  readonly truncated: boolean;
}

export interface KafkaMessageExportSnapshot {
  readonly exportedMessageCount: number;
  readonly filters: KafkaMessageFilters;
  readonly messages: readonly KafkaMessageExportRecord[];
  readonly retainedMessageCount: number;
  readonly schemaVersion: 2;
  readonly stale: boolean;
  readonly topic: string;
}

export const initialKafkaMessageFilters: KafkaMessageFilters = Object.freeze({
  activeRuleMatchesOnly: false,
  key: "",
  offset: "",
  partition: null,
  timestamp: "",
  value: "",
});

export function countActiveKafkaMessageFilters(filters: KafkaMessageFilters): number {
  return (
    Number(filters.activeRuleMatchesOnly) +
    Number(filters.partition !== null) +
    Number(filters.offsetExact !== undefined) +
    KAFKA_MESSAGE_TEXT_FILTER_FIELDS.reduce(
      (count, field) => count + Number((filters[field]?.trim().length ?? 0) > 0),
      0,
    )
  );
}

export function selectKafkaQueryMessages(
  messages: readonly KafkaExploredMessage[],
  filters: KafkaMessageFilters,
): { readonly messages: readonly KafkaExploredMessage[]; readonly unavailable: number } {
  if (countActiveKafkaMessageFilters(filters) === 0) return { messages, unavailable: 0 };
  let predicate: ReturnType<typeof compileKafkaSearchFilter>;
  try {
    predicate = compileKafkaSearchFilter(
      filters,
      new KafkaRuleWorkBudget(2 * KAFKA_RULE_LIMITS.evaluationWork),
    );
  } catch {
    return { messages: [], unavailable: messages.length };
  }
  let unavailable = 0;
  const selected = messages.filter((message) => {
    if (filters.activeRuleMatchesOnly && message.ruleEvaluation.activeMatchCount === 0)
      return false;
    const outcome = predicate({
      ...message,
      payload: message.payload ?? (message.truncated ? message.preview : null),
    });
    if (outcome === "unavailable") unavailable += 1;
    return outcome === "matched";
  });
  return { messages: selected, unavailable };
}

export function selectFilteredKafkaMessages(
  messages: readonly KafkaExploredMessage[],
  filters: KafkaMessageFilters,
): readonly KafkaExploredMessage[] {
  return selectKafkaQueryMessages(messages, filters).messages;
}

export function withKafkaMessageTextFilter(
  filters: KafkaMessageFilters,
  field: KafkaMessageTextFilterField,
  value: string,
): KafkaMessageFilters {
  const bounded = value.slice(
    0,
    field === "expression"
      ? KAFKA_RULE_LIMITS.expressionCharacters
      : KAFKA_MESSAGE_OPERATION_LIMITS.filterCharacters,
  );
  if (bounded === filters[field] && !(field === "offset" && filters.offsetExact !== undefined))
    return filters;
  const next = { ...filters, [field]: bounded };
  if (field === "offset") delete next.offsetExact;
  return next;
}

function operationFailure(
  code: KafkaMessageOperationErrorCode,
  message: string,
  recovery: string,
): never {
  throw new KafkaMessageOperationError(code, message, recovery);
}

function validateExportInput(input: KafkaMessageExportInput): void {
  if (input.messages.length === 0) {
    operationFailure(
      "NO_MESSAGES",
      "No filtered messages are available to export.",
      "Clear or change the filters, then retry.",
    );
  }
  if (
    input.messages.length > KAFKA_MESSAGE_OPERATION_LIMITS.exportMessages ||
    input.retainedMessageCount > KAFKA_MESSAGE_LIMITS.retainedMessages
  ) {
    operationFailure(
      "EXPORT_TOO_LARGE",
      `Message export is limited to ${KAFKA_MESSAGE_OPERATION_LIMITS.exportMessages.toLocaleString()} records.`,
      "Narrow the filters and retry.",
    );
  }
  if (
    !Number.isSafeInteger(input.retainedMessageCount) ||
    input.retainedMessageCount < input.messages.length
  ) {
    operationFailure(
      "INVALID_EXPORT",
      "The retained message count is inconsistent.",
      "Refresh the message view and retry.",
    );
  }
  if (
    input.topic.length === 0 ||
    input.topic.length > KAFKA_MESSAGE_OPERATION_LIMITS.exportTopicCharacters ||
    input.messages.some((message) => message.topic !== input.topic)
  ) {
    operationFailure(
      "INVALID_EXPORT",
      "The filtered messages do not belong to one valid topic.",
      "Select one topic and retry.",
    );
  }
  if (
    KAFKA_MESSAGE_TEXT_FILTER_FIELDS.some(
      (field) =>
        (input.filters[field]?.length ?? 0) >
        (field === "expression"
          ? KAFKA_RULE_LIMITS.expressionCharacters
          : KAFKA_MESSAGE_OPERATION_LIMITS.filterCharacters),
    ) ||
    (input.filters.partition !== null &&
      (!Number.isSafeInteger(input.filters.partition) || input.filters.partition < 0)) ||
    (input.filters.offsetExact !== undefined &&
      (!/^(0|[1-9]\d*)$/u.test(input.filters.offsetExact) ||
        input.filters.offsetExact.length > 20 ||
        BigInt(input.filters.offsetExact) > 9_223_372_036_854_775_807n))
  ) {
    operationFailure(
      "INVALID_EXPORT",
      "The message filters exceed their declared bounds.",
      "Clear or shorten the filters and retry.",
    );
  }
}

function orderedHeaders(
  headers: Readonly<Record<string, string>>,
): Readonly<Record<string, string>> {
  return Object.fromEntries(
    Object.entries(headers).sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0)),
  );
}

function exportRecord(message: KafkaExploredMessage): KafkaMessageExportRecord {
  return {
    timestamp: message.timestamp,
    partition: message.partition,
    offset: message.offset,
    key: message.key,
    headers: orderedHeaders(message.headers),
    payload: message.payload,
    payloadTruncated:
      message.payloadTruncated ??
      (message.truncated && message.originalByteSize > KAFKA_MESSAGE_LIMITS.messageBytes),
    preview: message.preview,
    truncated: message.truncated,
    originalByteSize: message.originalByteSize,
    original: message.original ?? { state: "unavailable", reason: "not-captured" },
  };
}

function exportFilters(filters: KafkaMessageFilters): KafkaMessageFilters {
  return {
    ...(filters.expression === undefined ? {} : { expression: filters.expression }),
    ...(filters.offsetExact === undefined ? {} : { offsetExact: filters.offsetExact }),
    timestamp: filters.timestamp,
    partition: filters.partition,
    offset: filters.offset,
    key: filters.key,
    value: filters.value,
    activeRuleMatchesOnly: filters.activeRuleMatchesOnly,
  };
}

function safeTopicSegment(topic: string): string {
  const segment = topic
    .replace(/[^A-Za-z0-9._-]+/gu, "-")
    .replace(/^[._-]+|[._-]+$/gu, "")
    .slice(0, 160)
    .replace(/[._-]+$/gu, "");
  return segment.length === 0 ? "topic" : segment;
}

export function createKafkaMessageExportDocument(input: KafkaMessageExportInput): HostTextDocument {
  validateExportInput(input);
  const messages = input.messages.map(exportRecord);
  let contentBytes = 0;
  for (const message of messages) {
    contentBytes += utf8ByteLength(JSON.stringify(message));
    if (contentBytes > KAFKA_MESSAGE_OPERATION_LIMITS.exportContentBytes) {
      operationFailure(
        "EXPORT_TOO_LARGE",
        `Filtered message content exceeds ${KAFKA_MESSAGE_OPERATION_LIMITS.exportContentBytes.toLocaleString()} UTF-8 bytes.`,
        "Narrow the filters and retry.",
      );
    }
  }
  const snapshot: KafkaMessageExportSnapshot = {
    schemaVersion: 2,
    topic: input.topic,
    filters: exportFilters(input.filters),
    retainedMessageCount: input.retainedMessageCount,
    exportedMessageCount: messages.length,
    stale: input.stale,
    messages,
  };
  const content = `${JSON.stringify(snapshot, null, 2)}\n`;
  const byteSize = utf8ByteLength(content);
  if (byteSize > KAFKA_MESSAGE_OPERATION_LIMITS.exportBytes) {
    operationFailure(
      "EXPORT_TOO_LARGE",
      `The JSON document exceeds ${KAFKA_MESSAGE_OPERATION_LIMITS.exportBytes.toLocaleString()} UTF-8 bytes.`,
      "Narrow the filters and retry.",
    );
  }
  const fileName = `streamskope-${safeTopicSegment(input.topic)}-messages.json`;
  if (fileName.length > KAFKA_MESSAGE_OPERATION_LIMITS.fileNameCharacters) {
    operationFailure(
      "INVALID_EXPORT",
      "The message export filename exceeds its declared bound.",
      "Select a topic with a shorter name and retry.",
    );
  }
  return {
    byteSize,
    content,
    fileName,
    mediaType: "application/json",
  };
}
