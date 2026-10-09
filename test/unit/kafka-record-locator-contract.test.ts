import { describe, expect, it } from "vitest";

import {
  createEmptyKafkaSavedRecordContext,
  kafkaRecordLocator,
  parseKafkaRecordLocator,
  parseKafkaSavedRecordContext,
} from "../../src/features/kafka/contracts/record-locator";
import {
  HOST_PROTOCOL_VERSION,
  createDefaultKafkaInvestigationView,
  inspectKafkaQueryLibraryDocument,
  parseHostCommand,
  parseHostCommandResponse,
  parseCorrelatedHostResponse,
  parseKafkaSavedView,
  serializeKafkaQueryLibraryDocument,
} from "../../src/features/kafka/contracts";
import { parseKafkaRecordLocatorOutcome } from "../../src/features/kafka/contracts/record-locator-protocol";
import { message } from "../support/kafka-backend-facade-fixture";

const locator = {
  schemaVersion: 1,
  clusterId: "test-cluster",
  topicId: "27c1c482-b9e0-43f2-abd0-ae257fd6a6df",
  topic: "orders",
  partition: 2,
  offset: "9007199254740993",
  leaderEpoch: 7,
} as const;
const empty = { selected: null, comparison: null, bookmarks: [] };
const legacy = {
  id: "incident",
  name: "Incident",
  configuration: {
    schemaVersion: 1,
    request: { topic: "orders", mode: "earliest", maxMessages: 10 },
  },
} as const;

describe("durable record positions", () => {
  it("keeps exact offsets above JavaScript's safe integer range without payloads", () => {
    expect(parseKafkaRecordLocator(locator)).toEqual(locator);
    expect(createEmptyKafkaSavedRecordContext()).toEqual(empty);
  });
  it.each([
    { schemaVersion: 2 },
    { clusterId: " " },
    { clusterId: "cluster\nsecret" },
    { topicId: "00000000-0000-0000-0000-000000000000" },
    { topicId: "orders" },
    { topic: ".." },
    { topic: "orders/new" },
    { partition: -1 },
    { partition: 2 ** 31 },
    { offset: "01" },
    { offset: "-1" },
    { offset: "9223372036854775808" },
    { offset: 3 },
    { leaderEpoch: -1 },
    { leaderEpoch: 2 ** 31 },
    { value: "secret" },
    { original: { value: "c2VjcmV0" } },
    { continuation: "token" },
    { fingerprint: "payload-digest" },
  ])("rejects invalid or undeclared locator fields %j", (change) => {
    expect(() => parseKafkaRecordLocator({ ...locator, ...change })).toThrow();
  });
  it("derives only from a record's admitted provenance, never a topic/profile name", () => {
    const message = {
      topic: locator.topic,
      partition: locator.partition,
      offset: locator.offset,
      provenance: {
        clusterId: locator.clusterId,
        topicId: locator.topicId,
        leaderEpoch: locator.leaderEpoch,
      },
    };
    expect(kafkaRecordLocator(message)).toEqual(locator);
    expect(kafkaRecordLocator({ ...message, provenance: undefined })).toBeNull();
  });
  it("requires one cluster per view while allowing a comparison across topics", () => {
    expect(
      parseKafkaSavedRecordContext({
        ...empty,
        selected: locator,
        comparison: { ...locator, topic: "dead-letter" },
      }).comparison?.topic,
    ).toBe("dead-letter");
    expect(() =>
      parseKafkaSavedRecordContext({
        ...empty,
        selected: locator,
        comparison: { ...locator, clusterId: "other" },
      }),
    ).toThrow(/cluster/u);
  });
  it("rejects duplicate names, IDs and positions, with a bounded bookmark count", () => {
    const bookmark = { id: "one", name: "First", locator };
    for (const other of [
      { ...bookmark, name: "Second", locator: { ...locator, offset: "1" } },
      { ...bookmark, id: "two", name: "first", locator: { ...locator, offset: "1" } },
      { ...bookmark, id: "two", name: "Second" },
    ])
      expect(() =>
        parseKafkaSavedRecordContext({ ...empty, bookmarks: [bookmark, other] }),
      ).toThrow();
    expect(() =>
      parseKafkaSavedRecordContext({
        ...empty,
        bookmarks: Array.from({ length: 33 }, (_, i) => ({
          id: String(i),
          name: `Record ${String(i)}`,
          locator: { ...locator, offset: String(i) },
        })),
      }),
    ).toThrow();
  });
  it("converts actual v1/v2 files purely but writes compact v3 only", () => {
    const view = createDefaultKafkaInvestigationView();
    for (const doc of [
      { schemaVersion: 1, queries: [legacy] },
      { schemaVersion: 2, queries: [{ ...legacy, view }] },
    ]) {
      const before = JSON.stringify(doc);
      expect(inspectKafkaQueryLibraryDocument(doc)).toEqual({
        schemaVersion: doc.schemaVersion,
        queries: [{ ...legacy, view, records: empty }],
      });
      expect(JSON.stringify(doc)).toBe(before);
    }
    expect(
      JSON.parse(serializeKafkaQueryLibraryDocument([{ ...legacy, view, records: empty }])),
    ).toEqual({
      schemaVersion: 3,
      queries: [legacy],
    });
    expect(() =>
      inspectKafkaQueryLibraryDocument({
        schemaVersion: 2,
        queries: [{ ...legacy, view, records: empty }],
      }),
    ).toThrow();
    expect(() => parseKafkaSavedView({ ...legacy, view })).toThrow(/records/u);
  });
});

describe("record reload boundary", () => {
  const requestId = "1640c2a9-f5c3-437b-b1ba-eaf50fc83653";
  const record = {
    ...message(locator.offset),
    topic: locator.topic,
    partition: locator.partition,
    provenance: {
      clusterId: locator.clusterId,
      topicId: locator.topicId,
      leaderEpoch: locator.leaderEpoch,
    },
  };
  const outcome = { requestId, locator, state: "loaded", message: record };
  it("admits an exact proven record and rejects substituted positions or history", () => {
    expect(parseKafkaRecordLocatorOutcome(outcome)).toEqual(outcome);
    for (const change of [
      { offset: "9007199254740992" },
      { partition: 1 },
      { topic: "other" },
      { provenance: undefined },
      { provenance: { ...record.provenance, clusterId: "other" } },
      { provenance: { ...record.provenance, topicId: "11111111-1111-1111-1111-111111111111" } },
      { provenance: { ...record.provenance, leaderEpoch: 8 } },
      { ruleEvaluation: { status: "matched" } },
      { secret: "must-not-cross" },
    ])
      expect(() =>
        parseKafkaRecordLocatorOutcome({ ...outcome, message: { ...record, ...change } }),
      ).toThrow();
  });
  it("keeps unavailable results payload-free and requires affirmative cleanup confirmation", () => {
    const unavailable = {
      requestId,
      locator,
      state: "expired",
      detail: "Retention removed this position.",
    };
    expect(parseKafkaRecordLocatorOutcome(unavailable)).toEqual(unavailable);
    expect(() => parseKafkaRecordLocatorOutcome({ ...unavailable, message: record })).toThrow();
    expect(() =>
      parseKafkaRecordLocatorOutcome({ ...unavailable, detail: "x".repeat(513) }),
    ).toThrow();
    const response = {
      command: "records.locator.cancel",
      id: "cancel",
      version: HOST_PROTOCOL_VERSION,
      ok: true,
      result: { correlationId: "safe-correlation", requestId, stopped: true },
    };
    expect(parseHostCommandResponse(response)).toEqual(response);
    for (const stopped of [false, undefined, "true"])
      expect(() =>
        parseHostCommandResponse({ ...response, result: { ...response.result, stopped } }),
      ).toThrow();
  });
  it("never imports a hidden payload through locator commands or optimistic view updates", () => {
    const command = {
      command: "records.locator.load",
      id: "load",
      version: HOST_PROTOCOL_VERSION,
      payload: { requestId, locator },
    };
    expect(parseHostCommand(command)).toEqual(command);
    expect(() =>
      parseHostCommand({ ...command, payload: { ...command.payload, message: record } }),
    ).toThrow();
    const query = { ...legacy, view: createDefaultKafkaInvestigationView(), records: empty };
    const put = {
      command: "queries.put",
      id: "save",
      version: HOST_PROTOCOL_VERSION,
      payload: { query, expected: null },
    };
    expect(parseHostCommand(put)).toEqual(put);
    expect(parseHostCommand({ ...put, payload: { query, expected: query } })).toEqual({
      ...put,
      payload: { query, expected: query },
    });
    expect(() =>
      parseHostCommand({ ...put, payload: { query, expected: { ...query, payload: "hidden" } } }),
    ).toThrow();
  });
  it("correlates loaded and unavailable positions and exact cancel identities to their requests", () => {
    const command = parseHostCommand({
      command: "records.locator.load",
      id: "load",
      version: HOST_PROTOCOL_VERSION,
      payload: { requestId, locator },
    });
    const response = {
      command: command.command,
      id: command.id,
      version: HOST_PROTOCOL_VERSION,
      ok: true,
      result: { correlationId: "safe", outcome },
    };
    expect(parseCorrelatedHostResponse(response, command)).toEqual(response);
    for (const replacement of [
      { ...outcome, requestId: "de14253f-48b3-4140-aeef-1f57d3e178c8" },
      { requestId, locator: { ...locator, offset: "3" }, state: "expired", detail: "Expired." },
    ])
      expect(() =>
        parseCorrelatedHostResponse(
          { ...response, result: { ...response.result, outcome: replacement } },
          command,
        ),
      ).toThrow(/submitted/u);
    const stop = parseHostCommand({
      command: "records.locator.cancel",
      id: "stop",
      version: HOST_PROTOCOL_VERSION,
      payload: { requestId },
    });
    expect(() =>
      parseCorrelatedHostResponse(
        {
          command: stop.command,
          id: stop.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: {
            correlationId: "safe",
            requestId: "de14253f-48b3-4140-aeef-1f57d3e178c8",
            stopped: true,
          },
        },
        stop,
      ),
    ).toThrow(/submitted/u);
  });
});
