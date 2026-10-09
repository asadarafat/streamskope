import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostEvent,
  type KafkaExploredMessage,
} from "../../src/features/kafka/contracts";
import {
  parseRecordField,
  parseStructuredRecord,
  type RecordField,
} from "../../src/features/kafka/contracts/structured-record";
import { parseKafkaOriginalRecord } from "../../src/features/kafka/contracts/record-bytes";

const field: RecordField = {
  state: "decoded",
  codec: "avro",
  text: '{"id":"9007199254740993"}',
  json: '{"id":"9007199254740993"}',
  writerSchema: {
    id: 27,
    format: "avro",
    messageType: null,
    registry: "https://registry.example.test/",
  },
};
function message(): KafkaExploredMessage {
  return {
    id: "events:0:1",
    topic: "events",
    partition: 0,
    offset: "1",
    timestamp: "2026-10-09T00:00:00.000Z",
    originalByteSize: 23,
    truncated: false,
    key: null,
    payload: field.state === "decoded" ? field.text : null,
    preview: field.state === "decoded" ? field.text : "",
    headers: { duplicate: "(null)" },
    original: { state: "unavailable", reason: "masked" },
    structured: {
      version: 1,
      key: { state: "null", codec: "auto", writerSchema: null },
      value: field,
      headersState: "complete",
      headers: [
        { key: "duplicate", value: "first", error: null },
        { key: "duplicate", value: null, error: null },
      ],
      protection: "masked",
    },
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
  };
}
function event(value: KafkaExploredMessage): unknown {
  return {
    event: "messages.batch",
    sequence: 1,
    version: HOST_PROTOCOL_VERSION,
    payload: { topic: "events", droppedMessages: 0, messages: [value] },
  };
}

describe("structured record trust boundary", () => {
  it("rejects masked fields disguised as unprotected projections and unexplained missing originals", () => {
    const source = message();
    expect(() =>
      parseStructuredRecord({
        ...source.structured,
        protection: "none",
        key: { state: "masked", codec: "utf8", writerSchema: null },
      }),
    ).toThrow(/protected projection/);
    expect(() =>
      parseHostEvent(
        event({ ...source, original: { state: "unavailable", reason: "not-captured" } }),
      ),
    ).toThrow(/original bytes/);
    expect(() =>
      parseStructuredRecord({ ...source.structured, headersState: "unavailable" }),
    ).toThrow(/inventory/);
  });
  it("preserves writer identity, ordered duplicate headers and protected projections through serialization", () => {
    const wire = event(message());
    expect(parseHostEvent(JSON.parse(JSON.stringify(wire)))).toEqual(wire);
  });
  it.each(["key", "payload", "preview", "headers"] as const)(
    "rejects a stale %s alias rather than rendering an unprotected alternative",
    (part) => {
      const input = message();
      expect(() =>
        parseHostEvent(
          event({
            ...input,
            [part]: part === "headers" ? { duplicate: "unprotected-secret" } : "unprotected-secret",
          }),
        ),
      ).toThrow(/projection/);
    },
  );
  it("rejects an original-byte side channel on a protected record", () => {
    expect(() =>
      parseHostEvent(
        event({
          ...message(),
          original: {
            state: "complete",
            encoding: "base64",
            key: null,
            value: "c2VjcmV0",
            headers: [],
          },
        }),
      ),
    ).toThrow(/original bytes/);
  });
  it.each([
    null,
    {
      id: 27,
      format: "protobuf",
      messageType: ".example.Message",
      registry: "https://registry.example.test/",
    },
    { id: 27, format: "avro", messageType: null, registry: null },
  ])("rejects contradictory successful writer identity %j", (writerSchema) => {
    expect(() => parseRecordField({ ...field, writerSchema }, "field")).toThrow();
  });
  it("retains a framed writer ID when lookup fails without inventing a resolved format", () => {
    expect(
      parseRecordField(
        {
          state: "error",
          codec: "auto",
          writerSchema: { id: 999, format: "unknown", messageType: null, registry: null },
          code: "schema-unavailable",
          detail: "Configure the Registry.",
        },
        "field",
      ),
    ).toMatchObject({ state: "error", writerSchema: { id: 999, format: "unknown" } });
  });
  it.each([
    "https://user:password@registry.test",
    "https://registry.test?token=private",
    "https://registry.test#secret",
  ])("rejects credential-bearing writer identity", (registry) => {
    expect(() =>
      parseRecordField({ ...field, writerSchema: { id: 27, format: "avro", registry } }, "field"),
    ).toThrow();
  });
  it("rejects malformed projected JSON and unknown record versions", () => {
    expect(() => parseRecordField({ ...field, text: "{", json: "{" }, "field")).toThrow();
    expect(() => parseStructuredRecord({ ...message().structured, version: 2 })).toThrow();
  });
  it("detaches and freezes the original bytes and ordered header entries", () => {
    const input = {
      state: "complete",
      encoding: "base64",
      key: "",
      value: null,
      headers: [{ key: "aA==", value: "dg==" }],
    };
    const captured = parseKafkaOriginalRecord(input);
    input.headers[0]!.value = "Y2hhbmdlZA==";
    expect(captured).toMatchObject({ headers: [{ value: "dg==" }] });
    expect(Object.isFrozen(captured)).toBe(true);
    if (captured.state === "complete") {
      expect(Object.isFrozen(captured.headers)).toBe(true);
      expect(Object.isFrozen(captured.headers[0])).toBe(true);
    }
  });
});
