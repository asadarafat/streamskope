import { Buffer } from "node:buffer";

import type { NatsMessageReceipt } from "../application/engine-port";
import { NATS_LIMITS, parseNatsSubject } from "../contracts";

import type { NatsSdkMessage } from "./sdk-types";

const encoder = new TextEncoder();

function bytes(value: string): number {
  return encoder.encode(value).byteLength;
}

function preview(value: string): string {
  const encoded = encoder.encode(value);
  if (encoded.byteLength <= NATS_LIMITS.previewBytes) return value;
  // Streaming decoding omits an incomplete final UTF-8 code point.
  return new TextDecoder("utf-8", { ignoreBOM: true }).decode(
    encoded.subarray(0, NATS_LIMITS.previewBytes),
    {
      stream: true,
    },
  );
}

/** Copy while the SDK callback owns the packet; never retain its bytes/header arrays. */
export function copyNatsMessage(message: NatsSdkMessage, now: Date): NatsMessageReceipt {
  const payloadBytes = message.data.byteLength;
  const reply = message.reply ?? "";
  if (payloadBytes > NATS_LIMITS.payloadBytes)
    return { kind: "omitted", reason: "payload-limit", payloadBytes };
  if (
    message.subject.length > NATS_LIMITS.subjectBytes ||
    reply.length > NATS_LIMITS.replyBytes ||
    bytes(message.subject) > NATS_LIMITS.subjectBytes ||
    bytes(reply) > NATS_LIMITS.replyBytes
  )
    return { kind: "omitted", reason: "metadata-limit", payloadBytes };
  try {
    parseNatsSubject(message.subject, false);
    if (reply !== "") parseNatsSubject(reply, false);
  } catch {
    return { kind: "omitted", reason: "metadata-limit", payloadBytes };
  }

  const copied = Uint8Array.from(message.data);
  let payload: { readonly encoding: "utf8" | "base64"; readonly data: string };
  try {
    payload = {
      encoding: "utf8",
      data: new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(copied),
    };
  } catch {
    payload = { encoding: "base64", data: Buffer.from(copied).toString("base64") };
  }
  const headers: { readonly name: string; readonly values: readonly string[] }[] = [];
  let headerBytes = 0;
  let headerValues = 0;
  let inspectedEntries = 0;
  let headersTruncated = false;
  if (message.headers !== undefined) {
    outer: for (const [name, values] of message.headers) {
      if (inspectedEntries >= NATS_LIMITS.headerEntries) {
        headersTruncated = true;
        break;
      }
      inspectedEntries += 1;
      const nameBytes =
        name.length > NATS_LIMITS.headerNameBytes ? NATS_LIMITS.headerNameBytes + 1 : bytes(name);
      if (
        headers.length >= NATS_LIMITS.headerEntries ||
        name.length === 0 ||
        name.includes("\0") ||
        /[\r\n]/u.test(name) ||
        nameBytes > NATS_LIMITS.headerNameBytes ||
        headerBytes + nameBytes > NATS_LIMITS.headerBytes
      ) {
        headersTruncated = true;
        break;
      }
      const copiedValues: string[] = [];
      headerBytes += nameBytes;
      for (const value of values) {
        const valueBytes =
          value.length > NATS_LIMITS.headerValueBytes
            ? NATS_LIMITS.headerValueBytes + 1
            : bytes(value);
        if (
          headerValues >= NATS_LIMITS.headerValues ||
          valueBytes > NATS_LIMITS.headerValueBytes ||
          headerBytes + valueBytes > NATS_LIMITS.headerBytes
        ) {
          headersTruncated = true;
          if (copiedValues.length > 0) headers.push({ name, values: copiedValues });
          break outer;
        }
        copiedValues.push(value);
        headerBytes += valueBytes;
        headerValues += 1;
      }
      if (copiedValues.length > 0) headers.push({ name, values: copiedValues });
    }
  }
  return {
    kind: "record",
    record: {
      subject: message.subject,
      ...(reply === "" ? {} : { reply }),
      headers,
      headersTruncated,
      payload,
      payloadBytes,
      preview:
        payload.encoding === "utf8"
          ? preview(payload.data)
          : `Binary payload (${payloadBytes} bytes)`,
      receivedAt: now.toISOString(),
      timestampProvenance: "host-received",
    },
  };
}
