import { NATS_LIMITS, type NatsRecord, type NatsSubscriptionCounters } from "./types";
import {
  NatsContractValidationError,
  natsArray,
  natsBoolean,
  natsCanonicalBase64,
  natsEnum,
  natsExactKeys,
  natsHasAsciiControl,
  natsIdentifier,
  natsInteger,
  natsObject,
  natsText,
  natsTimestamp,
  natsUtf8Bytes,
} from "./validation-primitives";

export function parseNatsSubject(value: unknown, allowWildcards = true): string {
  const subject = natsText(value, NATS_LIMITS.subjectBytes);
  if (/\s/u.test(subject) || natsHasAsciiControl(subject)) throw new NatsContractValidationError();
  const parts = subject.split(".");
  if (
    parts.some(
      (part, index) =>
        part.length === 0 ||
        (part.includes("*") && (!allowWildcards || part !== "*")) ||
        (part.includes(">") && (!allowWildcards || part !== ">" || index !== parts.length - 1)),
    )
  )
    throw new NatsContractValidationError();
  return subject;
}
export function parseNatsCopiedMessage(value: unknown): Omit<NatsRecord, "id" | "generation"> {
  const input = natsObject(value);
  natsExactKeys(
    input,
    [
      "subject",
      "headers",
      "headersTruncated",
      "payload",
      "payloadBytes",
      "preview",
      "receivedAt",
      "timestampProvenance",
    ],
    ["reply"],
  );
  let headerValues = 0;
  let headerBytes = 0;
  const headers = natsArray(input.headers, NATS_LIMITS.headerEntries).map((entry) => {
    const header = natsObject(entry);
    natsExactKeys(header, ["name", "values"]);
    const name = natsText(header.name, NATS_LIMITS.headerNameBytes);
    if (name.includes("\r") || name.includes("\n") || name.includes("\0"))
      throw new NatsContractValidationError();
    const values = natsArray(header.values, NATS_LIMITS.headerValues).map((item) =>
      natsText(item, NATS_LIMITS.headerValueBytes, true),
    );
    headerValues += values.length;
    headerBytes +=
      natsUtf8Bytes(name) + values.reduce((bytes, item) => bytes + natsUtf8Bytes(item), 0);
    return { name, values };
  });
  if (headerValues > NATS_LIMITS.headerValues || headerBytes > NATS_LIMITS.headerBytes)
    throw new NatsContractValidationError();
  const payload = natsObject(input.payload);
  natsExactKeys(payload, ["encoding", "data"]);
  const encoding = natsEnum(payload.encoding, ["utf8", "base64"] as const);
  const decoded =
    encoding === "utf8"
      ? {
          data: natsText(payload.data, NATS_LIMITS.payloadBytes, true),
          bytes: natsUtf8Bytes(natsText(payload.data, NATS_LIMITS.payloadBytes, true)),
        }
      : natsCanonicalBase64(payload.data, NATS_LIMITS.payloadBytes);
  const payloadBytes = natsInteger(input.payloadBytes, 0, NATS_LIMITS.payloadBytes);
  if (decoded.bytes !== payloadBytes) throw new NatsContractValidationError();
  const reply = Object.hasOwn(input, "reply") ? parseNatsSubject(input.reply, false) : undefined;
  return {
    subject: parseNatsSubject(input.subject, false),
    ...(reply === undefined ? {} : { reply }),
    headers,
    headersTruncated: natsBoolean(input.headersTruncated),
    payload: { encoding, data: decoded.data },
    payloadBytes,
    preview: natsText(input.preview, NATS_LIMITS.previewBytes, true),
    receivedAt: natsTimestamp(input.receivedAt),
    timestampProvenance: natsEnum(input.timestampProvenance, ["host-received"] as const),
  };
}
export function parseNatsRecord(value: unknown): NatsRecord {
  const input = natsObject(value);
  natsExactKeys(
    input,
    [
      "id",
      "generation",
      "subject",
      "headers",
      "headersTruncated",
      "payload",
      "payloadBytes",
      "preview",
      "receivedAt",
      "timestampProvenance",
    ],
    ["reply"],
  );
  const { id, generation, ...message } = input;
  return {
    ...parseNatsCopiedMessage(message),
    id: natsIdentifier(id),
    generation: natsIdentifier(generation),
  };
}
export function natsRecordRetainedBytes(record: NatsRecord): number {
  return natsUtf8Bytes(JSON.stringify(record));
}
export function parseNatsSubscriptionCounters(value: unknown): NatsSubscriptionCounters {
  const input = natsObject(value);
  const keys = [
    "receivedRecords",
    "applicationOmittedRecords",
    "publishedRecords",
    "queuedRecords",
    "queuedBytes",
    "transportOmittedRecords",
  ] as const;
  natsExactKeys(input, keys);
  const parsed = {
    receivedRecords: natsInteger(input.receivedRecords),
    applicationOmittedRecords: natsInteger(input.applicationOmittedRecords),
    publishedRecords: natsInteger(input.publishedRecords),
    queuedRecords: natsInteger(input.queuedRecords, 0, NATS_LIMITS.queuedRecords),
    queuedBytes: natsInteger(input.queuedBytes, 0, NATS_LIMITS.queuedBytes),
    transportOmittedRecords: natsInteger(input.transportOmittedRecords),
  };
  const accounted =
    parsed.applicationOmittedRecords + parsed.publishedRecords + parsed.queuedRecords;
  if (
    !Number.isSafeInteger(accounted) ||
    parsed.applicationOmittedRecords > parsed.receivedRecords ||
    parsed.publishedRecords > parsed.receivedRecords ||
    parsed.queuedRecords > parsed.receivedRecords ||
    accounted > parsed.receivedRecords
  )
    throw new NatsContractValidationError();
  return parsed;
}
