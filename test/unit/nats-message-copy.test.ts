import { Buffer } from "node:buffer";

import { describe, expect, it } from "vitest";

import { NATS_LIMITS, parseNatsCopiedMessage } from "../../src/features/nats/contracts";
import { copyNatsMessage } from "../../src/features/nats/engine/message-copy";
import { natsMessage } from "../support/nats-engine-fixture";

const receivedAt = new Date("2026-10-05T12:00:00.000Z");

describe("NATS callback-owned message copies", () => {
  it("copies binary backing bytes and multi-value header arrays with host-time provenance", () => {
    const data = Uint8Array.from([0, 255, 128, 1]);
    const values = ["first", "second"];
    const receipt = copyNatsMessage(
      {
        subject: "qualification.binary",
        reply: "reply.test",
        data,
        headers: [
          ["Trace", values],
          ["trace", ["case-sensitive"]],
        ],
      },
      receivedAt,
    );
    data.fill(3);
    values[0] = "changed";
    expect(receipt).toEqual({
      kind: "record",
      record: {
        subject: "qualification.binary",
        reply: "reply.test",
        headers: [
          { name: "Trace", values: ["first", "second"] },
          { name: "trace", values: ["case-sensitive"] },
        ],
        headersTruncated: false,
        payload: { encoding: "base64", data: "AP+AAQ==" },
        payloadBytes: 4,
        preview: "Binary payload (4 bytes)",
        receivedAt: receivedAt.toISOString(),
        timestampProvenance: "host-received",
      },
    });
    if (receipt.kind === "record")
      expect(parseNatsCopiedMessage(receipt.record)).toEqual(receipt.record);
  });

  it("preserves empty payload and UTF-8 BOM bytes instead of silently removing them", () => {
    const empty = copyNatsMessage(natsMessage(""), receivedAt);
    const bom = copyNatsMessage(
      { ...natsMessage(), data: Uint8Array.from([239, 187, 191, 65]) },
      receivedAt,
    );
    expect(empty).toMatchObject({
      kind: "record",
      record: { payload: { encoding: "utf8", data: "" }, payloadBytes: 0 },
    });
    expect(
      copyNatsMessage({ subject: "qualification.no-reply", data: new Uint8Array() }, receivedAt),
    ).toMatchObject({
      kind: "record",
      record: { subject: "qualification.no-reply", payloadBytes: 0 },
    });
    expect(bom).toMatchObject({
      kind: "record",
      record: { payload: { encoding: "utf8", data: "\uFEFFA" }, payloadBytes: 4 },
    });
    if (bom.kind === "record") expect(parseNatsCopiedMessage(bom.record)).toEqual(bom.record);
  });

  it("retains exact boundary payload, bounds UTF-8 preview and discloses whole oversized omission", () => {
    const content = "€".repeat(Math.floor(NATS_LIMITS.payloadBytes / 3));
    const receipt = copyNatsMessage(natsMessage(content), receivedAt);
    expect(receipt.kind).toBe("record");
    if (receipt.kind !== "record") throw new Error("Expected bounded record.");
    expect(receipt.record.payload.data).toBe(content);
    expect(Buffer.byteLength(receipt.record.preview)).toBeLessThanOrEqual(NATS_LIMITS.previewBytes);
    expect(receipt.record.preview).not.toContain("�");
    expect(parseNatsCopiedMessage(receipt.record)).toEqual(receipt.record);
    expect(
      copyNatsMessage(
        { ...natsMessage(), data: new Uint8Array(NATS_LIMITS.payloadBytes + 1) },
        receivedAt,
      ),
    ).toEqual({
      kind: "omitted",
      reason: "payload-limit",
      payloadBytes: NATS_LIMITS.payloadBytes + 1,
    });
  });

  it("bounds total header values and reports truncation without inventing omitted counts", () => {
    const receipt = copyNatsMessage(
      {
        ...natsMessage(),
        headers: [
          [
            "trace",
            Array.from({ length: NATS_LIMITS.headerValues + 1 }, (_, index) => String(index)),
          ],
        ],
      },
      receivedAt,
    );
    expect(receipt.kind).toBe("record");
    if (receipt.kind !== "record") throw new Error("Expected bounded record.");
    expect(receipt.record.headersTruncated).toBe(true);
    expect(receipt.record.headers[0]?.values).toHaveLength(NATS_LIMITS.headerValues);
    expect(parseNatsCopiedMessage(receipt.record)).toEqual(receipt.record);
    expect(receipt.record).not.toHaveProperty("omittedHeaders");
  });

  it("bounds inspected header entries even when an SDK iterable supplies empty value arrays", () => {
    let inspected = 0;
    const headers: Iterable<[string, string[]]> = {
      *[Symbol.iterator](): Generator<[string, string[]]> {
        for (let index = 0; index < 1_000; index += 1) {
          inspected += 1;
          yield [`h${index}`, []];
        }
      },
    };
    const receipt = copyNatsMessage({ ...natsMessage(), headers }, receivedAt);
    expect(inspected).toBe(NATS_LIMITS.headerEntries + 1);
    expect(receipt).toMatchObject({
      kind: "record",
      record: { headers: [], headersTruncated: true },
    });
    if (receipt.kind === "record")
      expect(parseNatsCopiedMessage(receipt.record)).toEqual(receipt.record);
  });

  it("bounds aggregate header bytes and individual names/values with truthful truncation", () => {
    const headers: [string, string[]][] = Array.from({ length: 12 }, (_, index) => [
      `h${index}`,
      ["x".repeat(NATS_LIMITS.headerValueBytes)],
    ]);
    const receipt = copyNatsMessage({ ...natsMessage(), headers }, receivedAt);
    expect(receipt.kind).toBe("record");
    if (receipt.kind !== "record") throw new Error("Expected bounded record.");
    expect(receipt.record.headersTruncated).toBe(true);
    expect(
      receipt.record.headers.reduce(
        (sum, header) =>
          sum +
          Buffer.byteLength(header.name) +
          header.values.reduce((count, value) => count + Buffer.byteLength(value), 0),
        0,
      ),
    ).toBeLessThanOrEqual(NATS_LIMITS.headerBytes);
    expect(parseNatsCopiedMessage(receipt.record)).toEqual(receipt.record);
    for (const bad of [
      ["n".repeat(NATS_LIMITS.headerNameBytes + 1), ["v"]],
      ["n", ["v".repeat(NATS_LIMITS.headerValueBytes + 1)]],
    ] satisfies [string, string[]][]) {
      expect(copyNatsMessage({ ...natsMessage(), headers: [bad] }, receivedAt)).toMatchObject({
        kind: "record",
        record: { headers: [], headersTruncated: true },
      });
    }
  });

  it("omits invalid or oversized routing metadata without truncating it into a false subject", () => {
    for (const subject of [
      "x".repeat(NATS_LIMITS.subjectBytes + 1),
      "bad..subject",
      "bad.*",
      "bad subject",
    ])
      expect(copyNatsMessage(natsMessage("payload", subject), receivedAt)).toEqual({
        kind: "omitted",
        reason: "metadata-limit",
        payloadBytes: 7,
      });
    expect(
      copyNatsMessage(
        { ...natsMessage(), reply: "x".repeat(NATS_LIMITS.replyBytes + 1) },
        receivedAt,
      ),
    ).toMatchObject({ kind: "omitted", reason: "metadata-limit" });
  });
});
