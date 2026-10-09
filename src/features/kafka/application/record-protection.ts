import {
  KAFKA_MESSAGE_LIMITS,
  kafkaRawMessageRetainedBytes,
  type KafkaMessage,
  type KafkaRecordProtection,
} from "../contracts";
import type { RecordField } from "../contracts/structured-record";

import { projectStructuredRecord } from "./structured-record-service";

export const MASKED_RECORD_TEXT = "[MASKED]";

export function hasRecordMasking(policy: KafkaRecordProtection): boolean {
  return policy.maskKey || policy.maskHeaders.length > 0 || policy.valuePaths.length > 0;
}

/** JSON Pointer traversal uses own properties only, including arrays and escaped names. */
function maskPointer(root: unknown, pointer: string): unknown {
  if (pointer === "") return MASKED_RECORD_TEXT;
  const segments = pointer
    .slice(1)
    .split("/")
    .map((part) => part.replace(/~1/gu, "/").replace(/~0/gu, "~"));
  let parent = root;
  for (let index = 0; index < segments.length; index += 1) {
    const segment = segments[index];
    if (
      segment === undefined ||
      parent === null ||
      typeof parent !== "object" ||
      (Array.isArray(parent) && !/^(?:0|[1-9]\d*)$/u.test(segment)) ||
      !Object.hasOwn(parent, segment)
    )
      return root;
    const record = parent as Record<string, unknown>;
    if (index === segments.length - 1) {
      // Define rather than assign so a JSON property named __proto__ stays data.
      Object.defineProperty(record, segment, {
        value: MASKED_RECORD_TEXT,
        enumerable: true,
        configurable: true,
        writable: true,
      });
    } else parent = record[segment];
  }
  return root;
}

export function protectKafkaRecord(
  message: KafkaMessage,
  policy: KafkaRecordProtection,
): KafkaMessage {
  if (!hasRecordMasking(policy)) return message;
  if (message.structured) {
    const structured = message.structured;
    const masked = (field: RecordField): RecordField =>
      field.state === "null"
        ? field
        : { state: "masked", codec: field.codec, writerSchema: field.writerSchema };
    let value = structured.value;
    if (policy.valuePaths.length && value.state !== "null") {
      if (value.state === "decoded" && value.json !== null) {
        try {
          let json: unknown = JSON.parse(value.json);
          for (const path of policy.valuePaths) json = maskPointer(json, path);
          const projection = JSON.stringify(json);
          value = { ...value, text: projection, json: projection };
        } catch {
          value = masked(value);
        }
      } else value = masked(value);
    }
    const protectedRecord = projectStructuredRecord(
      { ...message, original: { state: "unavailable", reason: "masked" } },
      {
        ...structured,
        key: policy.maskKey ? masked(structured.key) : structured.key,
        value,
        headers: structured.headers.map((header) =>
          policy.maskHeaders.includes(header.key) ||
          (policy.maskHeaders.length > 0 && header.error !== null)
            ? {
                ...header,
                value: header.value === null && header.error === null ? null : MASKED_RECORD_TEXT,
                error: header.error,
              }
            : header,
        ),
        protection: "masked",
      },
    );
    if (kafkaRawMessageRetainedBytes(protectedRecord) <= KAFKA_MESSAGE_LIMITS.messageBytes)
      return protectedRecord;
    return projectStructuredRecord(
      { ...protectedRecord, payloadTruncated: true, truncated: true },
      { ...protectedRecord.structured!, value: masked(value) },
    );
  }
  const headers = Object.fromEntries(
    Object.entries(message.headers).map(([key, value]) => [
      key,
      policy.maskHeaders.includes(key) ? MASKED_RECORD_TEXT : value,
    ]),
  );
  let payload = message.payload;
  let preview = message.preview;
  if (
    policy.valuePaths.length > 0 &&
    (payload !== null || message.payloadTruncated === true || message.preview.length > 0)
  ) {
    // Incomplete, binary or non-JSON values cannot be selectively proven safe.
    try {
      if (payload === null || message.payloadTruncated === true) throw new Error("incomplete");
      let value: unknown = JSON.parse(payload);
      for (const path of policy.valuePaths) value = maskPointer(value, path);
      payload = JSON.stringify(value);
      preview = payload.slice(0, KAFKA_MESSAGE_LIMITS.previewBytes / 4);
    } catch {
      payload = MASKED_RECORD_TEXT;
      preview = MASKED_RECORD_TEXT;
    }
  }
  const protectedRecord: KafkaMessage = {
    ...message,
    headers,
    key: policy.maskKey && message.key !== null ? MASKED_RECORD_TEXT : message.key,
    payload,
    preview,
    // Never provide an unmasked Base64 side channel, including fields omitted from previews.
    original: { state: "unavailable", reason: "masked" },
  };
  if (kafkaRawMessageRetainedBytes(protectedRecord) <= KAFKA_MESSAGE_LIMITS.messageBytes)
    return protectedRecord;
  return {
    ...protectedRecord,
    payload: MASKED_RECORD_TEXT,
    preview: MASKED_RECORD_TEXT,
    payloadTruncated: true,
    truncated: true,
  };
}
