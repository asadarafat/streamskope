import { KAFKA_MESSAGE_LIMITS, type KafkaExploredMessage, type KafkaMessage } from "./types";
import { parseKafkaRecordProvenance } from "./record-locator";
import { parseStructuredRecord, recordFieldText } from "./structured-record";
import { parseKafkaOriginalRecord } from "./record-bytes";
import { parseKafkaLiveRuleEvaluation } from "./live-rule-validation";
import { kafkaRawMessageRetainedBytes } from "./message-limits";
import { HostContractValidationError } from "./validation-error";
import {
  record,
  exactKeys,
  boundedUtf8Text,
  nullableBoundedUtf8Text,
  text,
  nonNegativeInteger,
  truth,
} from "./validation-primitives";

function parseHeaders(value: unknown, path: string): Readonly<Record<string, string>> {
  const headers = record(value, path);
  if (Object.keys(headers).length > KAFKA_MESSAGE_LIMITS.headerCount) {
    throw new HostContractValidationError(
      path,
      `must contain at most ${KAFKA_MESSAGE_LIMITS.headerCount} headers`,
    );
  }
  return Object.fromEntries(
    Object.entries(headers).map(([key, headerValue]) => [
      boundedUtf8Text(key, `${path}.key`, KAFKA_MESSAGE_LIMITS.headerKeyBytes),
      boundedUtf8Text(headerValue, `${path}.${key}`, KAFKA_MESSAGE_LIMITS.headerValueBytes),
    ]),
  );
}

export function parseKafkaMessage(value: unknown, path: string): KafkaMessage {
  const message = record(value, path);
  exactKeys(
    message,
    [
      "headers",
      "id",
      "key",
      "offset",
      "originalByteSize",
      "recordByteSize",
      "original",
      "structured",
      "provenance",
      "partition",
      "payload",
      "payloadTruncated",
      "preview",
      "timestamp",
      "topic",
      "truncated",
    ],
    path,
  );
  const parsed: KafkaMessage = {
    ...(message.provenance === undefined
      ? {}
      : {
          provenance: parseKafkaRecordProvenance(message.provenance, `${path}.provenance`),
        }),
    ...(message.structured === undefined
      ? {}
      : { structured: parseStructuredRecord(message.structured, `${path}.structured`) }),
    ...(message.original === undefined
      ? {}
      : { original: parseKafkaOriginalRecord(message.original, `${path}.original`) }),
    headers: parseHeaders(message.headers, `${path}.headers`),
    id: text(message.id, `${path}.id`, 256),
    key: nullableBoundedUtf8Text(message.key, `${path}.key`, KAFKA_MESSAGE_LIMITS.messageBytes),
    offset: text(message.offset, `${path}.offset`, 128),
    originalByteSize: nonNegativeInteger(message.originalByteSize, `${path}.originalByteSize`),
    ...(message.recordByteSize === undefined
      ? {}
      : { recordByteSize: nonNegativeInteger(message.recordByteSize, `${path}.recordByteSize`) }),
    partition: nonNegativeInteger(message.partition, `${path}.partition`),
    payload: nullableBoundedUtf8Text(
      message.payload,
      `${path}.payload`,
      KAFKA_MESSAGE_LIMITS.messageBytes,
    ),
    ...(message.payloadTruncated === undefined
      ? {}
      : { payloadTruncated: truth(message.payloadTruncated, `${path}.payloadTruncated`) }),
    preview: boundedUtf8Text(message.preview, `${path}.preview`, KAFKA_MESSAGE_LIMITS.previewBytes),
    timestamp: text(message.timestamp, `${path}.timestamp`, 128),
    topic: text(message.topic, `${path}.topic`, 512),
    truncated: truth(message.truncated, `${path}.truncated`),
  };
  if (parsed.recordByteSize !== undefined && parsed.recordByteSize < parsed.originalByteSize)
    throw new HostContractValidationError(path, "full wire bytes must include key and value bytes");
  if (parsed.structured !== undefined) {
    if (
      parsed.key !== recordFieldText(parsed.structured.key) ||
      parsed.payload !== recordFieldText(parsed.structured.value)
    )
      throw new HostContractValidationError(
        path,
        "record aliases must match the shared protected projection",
      );
    const headers = Object.fromEntries(
      parsed.structured.headers.map((h) => [h.key, h.value ?? "(null)"]),
    );
    if (
      parsed.preview !== (parsed.payload ?? "").slice(0, KAFKA_MESSAGE_LIMITS.previewBytes / 4) ||
      Object.keys(parsed.headers).length !== Object.keys(headers).length ||
      Object.entries(headers).some(([key, value]) => parsed.headers[key] !== value)
    )
      throw new HostContractValidationError(
        path,
        "preview and headers must match the shared protected projection",
      );
    if (
      parsed.structured.protection === "masked" &&
      (parsed.original?.state !== "unavailable" || parsed.original.reason !== "masked")
    )
      throw new HostContractValidationError(
        path,
        "protected records must withhold original bytes explicitly",
      );
    if (
      parsed.original?.state === "unavailable" &&
      parsed.original.reason === "masked" &&
      parsed.structured.protection !== "masked"
    )
      throw new HostContractValidationError(
        path,
        "masked original bytes require protected projection metadata",
      );
  }
  if (kafkaRawMessageRetainedBytes(parsed) > KAFKA_MESSAGE_LIMITS.messageBytes) {
    throw new HostContractValidationError(
      path,
      `retained record data must total at most ${KAFKA_MESSAGE_LIMITS.messageBytes} UTF-8 bytes`,
    );
  }
  return parsed;
}

export function parseKafkaExploredMessage(value: unknown, path: string): KafkaExploredMessage {
  const message = record(value, path);
  const { ruleEvaluation, ...base } = message;
  return {
    ...parseKafkaMessage(base, path),
    ruleEvaluation: parseKafkaLiveRuleEvaluation(ruleEvaluation, `${path}.ruleEvaluation`),
  };
}
