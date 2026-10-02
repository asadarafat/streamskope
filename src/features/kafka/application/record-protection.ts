import {
  KAFKA_MESSAGE_LIMITS,
  kafkaRawMessageRetainedBytes,
  type KafkaMessage,
  type KafkaRecordProtection,
} from "../contracts";

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
