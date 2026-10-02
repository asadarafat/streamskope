import { describe, expect, it } from "vitest";

import {
  KAFKA_MESSAGE_LIMITS,
  kafkaRawMessageRetainedBytes,
  HOST_PROTOCOL_VERSION,
  parseHostEvent,
} from "../../src/features/kafka/contracts";
import { parseKafkaOriginalRecord } from "../../src/features/kafka/contracts/record-bytes";
import { translateKafkaRecord } from "../../src/features/kafka/engine/message-record";
import type { KafkaRawMessage } from "../../src/features/kafka/engine/types";

it("counts the complete Base64 envelope without underestimating its serialized retention", () => {
  for (const source of [
    raw(),
    raw({
      key: Buffer.alloc(0),
      value: Buffer.from([255, 0]),
      headerEntries: [
        [Buffer.from("a"), null],
        [Buffer.from("a"), Buffer.from([1])],
      ],
    }),
    raw({ value: Buffer.alloc(300_000) }),
  ]) {
    const message = translateKafkaRecord(source, "events");
    const { original, ...withoutOriginal } = message;
    const bytesWithoutOriginal = kafkaRawMessageRetainedBytes(withoutOriginal);
    expect(kafkaRawMessageRetainedBytes(message) - bytesWithoutOriginal).toBe(
      Buffer.byteLength(JSON.stringify(original)),
    );
  }
});

function raw(overrides: Partial<KafkaRawMessage> = {}): KafkaRawMessage {
  return {
    headers: new Map(),
    offset: 42n,
    partition: 1,
    timestamp: 1_700_000_000_000n,
    topic: "events",
    ...overrides,
  };
}

describe("original Kafka record fidelity", () => {
  it("preserves binary keys/values, repeated ordered headers and null header values", () => {
    const source = raw({
      key: Buffer.from([0xff, 0, 0xc3]),
      value: Buffer.from([0x80, 0, 0xfe]),
      headers: new Map([[Buffer.from("same"), Buffer.from("last")]]),
      headerEntries: [
        [Buffer.from("same"), Buffer.from([0xff])],
        [Buffer.from("empty"), Buffer.alloc(0)],
        [Buffer.from("same"), null],
        [Buffer.from("same"), Buffer.from("last")],
      ],
    });
    const message = translateKafkaRecord(source, "events");
    expect(message.original).toEqual({
      state: "complete",
      encoding: "base64",
      key: "/wDD",
      value: "gAD+",
      headers: [
        { key: "c2FtZQ==", value: "/w==" },
        { key: "ZW1wdHk=", value: "" },
        { key: "c2FtZQ==", value: null },
        { key: "c2FtZQ==", value: "bGFzdA==" },
      ],
    });
    expect(parseKafkaOriginalRecord(JSON.parse(JSON.stringify(message.original)))).toEqual(
      message.original,
    );
    expect(message.originalByteSize).toBe(6);
    expect(message.payload).toBe("�\u0000�");
  });

  it("distinguishes tombstones and null keys from empty bytes", () => {
    expect(translateKafkaRecord(raw(), "events").original).toMatchObject({
      key: null,
      value: null,
    });
    expect(
      translateKafkaRecord(raw({ key: Buffer.alloc(0), value: Buffer.alloc(0) }), "events")
        .original,
    ).toMatchObject({ key: "", value: "" });
  });

  it("preserves the original envelope through a real serialized host event", () => {
    const message = translateKafkaRecord(raw({ value: Buffer.from([255, 254]) }), "events");
    const wire = {
      event: "messages.batch",
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
      payload: {
        topic: "events",
        droppedMessages: 0,
        messages: [
          {
            ...message,
            ruleEvaluation: {
              state: "unavailable",
              reason: "payload-malformed",
              activeMatchCount: 0,
              activeMatches: [],
              durationMicros: 0,
              errorCount: 0,
              errors: [],
              evaluatedRules: 0,
              omittedEvidence: 0,
              omittedRules: 0,
              suppressedMatchCount: 0,
              suppressedMatches: [],
            },
          },
        ],
      },
    };
    expect(parseHostEvent(JSON.parse(JSON.stringify(wire)))).toEqual(wire);
  });

  it("marks unavailable originals explicitly rather than saving a misleading prefix", () => {
    const message = translateKafkaRecord(raw({ value: Buffer.alloc(262_145, 65) }), "events");
    expect(message.original).toEqual({ state: "unavailable", reason: "size-limit" });
    expect(message.payload?.length).toBe(262_145);
    expect(JSON.stringify(message.original)).not.toContain("QQ");
  });

  it("bounds combined binary envelope, decoded replacement text and header previews", () => {
    const message = translateKafkaRecord(
      raw({
        value: Buffer.alloc(220_000, 0xff),
        headerEntries: Array.from({ length: 128 }, (_, i) => [
          Buffer.from(String(i)),
          Buffer.alloc(8_192, 0xff),
        ]),
      }),
      "events",
    );
    expect(kafkaRawMessageRetainedBytes(message)).toBeLessThanOrEqual(
      KAFKA_MESSAGE_LIMITS.messageBytes,
    );
    expect(message.truncated).toBe(true);
    expect(message.original).toEqual({ state: "unavailable", reason: "size-limit" });
  });

  it.each(["%%%", "YQ", "YQ===", "YR==", "YQ==\n"])(
    "rejects malformed/noncanonical Base64: %s",
    (value) => {
      expect(() =>
        parseKafkaOriginalRecord({
          state: "complete",
          encoding: "base64",
          key: null,
          value,
          headers: [],
        }),
      ).toThrow();
    },
  );

  it("rejects oversized envelopes and extra fields on unavailable originals", () => {
    expect(() =>
      parseKafkaOriginalRecord({
        state: "complete",
        encoding: "base64",
        key: null,
        value: Buffer.alloc(262_145).toString("base64"),
        headers: [],
      }),
    ).toThrow();
    expect(() =>
      parseKafkaOriginalRecord({ state: "unavailable", reason: "size-limit", value: "hidden" }),
    ).toThrow();
  });
});
