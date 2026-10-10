import avro from "avsc";
import protobuf from "protobufjs";
import { afterEach, expect, it, vi } from "vitest";

import { createHostRecordCodec } from "../../src/platform/node/record-codec";
import { StructuredReplayService } from "../../src/features/kafka/application/structured-replay-service";
import { applyReplayJsonPatches } from "../../src/features/kafka/application/replay-json-patches";
import { RecordCodecService } from "../../src/features/kafka/application/record-codec-service";
import { RecordReplayService } from "../../src/features/kafka/application/record-replay-service";
import {
  MemoryRepairJobStore,
  RepairJournal,
} from "../../src/features/kafka/application/repair-journal";
import type {
  SchemaRegistryReviewScope,
  ReviewedWriteScope,
} from "../../src/features/kafka/application/connection-scope";
import type { KafkaClusterServiceContext } from "../../src/features/kafka/application/types";
import type {
  RegisteredSchema,
  SchemaLookupPort,
} from "../../src/features/kafka/application/record-codec-types";
import { parseStructuredReplayTransform } from "../../src/features/kafka/contracts/structured-replay";
import {
  parseRecordReplayReview,
  parseRecordReplayInput,
  replayConfirmation,
  UNCHANGED_REPLAY_TRANSFORM,
  type RecordReplayInput,
} from "../../src/features/kafka/contracts/record-replay";
import { parseRepairJournalDocument } from "../../src/features/kafka/contracts/repair-jobs";

const signal = (): AbortSignal => AbortSignal.timeout(15_000);
const json = (value: unknown): string => Buffer.from(JSON.stringify(value)).toString("base64");
const frame = (id: number, payload: Buffer): string => {
  const header = Buffer.alloc(5);
  header.writeUInt32BE(id, 1);
  return Buffer.concat([header, payload]).toString("base64");
};
const schema: RegisteredSchema = {
  id: 1,
  schemaType: "AVRO",
  references: [],
  schema: '{"type":"record","name":"R","fields":[{"name":"name","type":"string"}]}',
};
const type = avro.Type.forSchema(JSON.parse(schema.schema) as avro.Schema);
const targetSchema = { ...schema, id: 7 };
const selection = { subject: "target-value", version: 1, messageType: "" };
const sourceContext: KafkaClusterServiceContext = {
  baseUrl: "http://source.test",
  authorization: (): Promise<undefined> => Promise.resolve(undefined),
};
const targetContext: KafkaClusterServiceContext = {
  baseUrl: "http://target.test",
  authorization: (): Promise<undefined> => Promise.resolve(undefined),
};
function scope(context: KafkaClusterServiceContext): SchemaRegistryReviewScope {
  return {
    connectionName: context.baseUrl,
    isCurrent: () => true,
    read: async <T>(
      run: (context: KafkaClusterServiceContext, signal: AbortSignal) => Promise<T>,
      passed: AbortSignal,
    ): Promise<T> => {
      passed.throwIfAborted();
      const result = await run(context, passed);
      passed.throwIfAborted();
      return result;
    },
    tryDispatch: () => ({ started: false }),
  };
}
const lookup: SchemaLookupPort = {
  byId: (context, id) =>
    Promise.resolve(context === sourceContext && id === 1 ? schema : { ...targetSchema, id }),
  byVersion: (): Promise<RegisteredSchema> => Promise.resolve(targetSchema),
};
function input(): RecordReplayInput {
  return parseRecordReplayInput({
    targetProfile: { id: "target", revision: 1 },
    topic: "destination",
    partition: 0,
    ratePerSecond: 10,
    records: [frame(1, type.toBuffer({ name: "before" })), json({ name: "before" }), null].map(
      (value, i) => ({
        topic: "source",
        partition: 0,
        offset: String(i),
        timestampMs: "1000",
        original: {
          state: "complete",
          encoding: "base64",
          key: "",
          value,
          headers: [
            { key: "aA==", value: null },
            { key: "aA==", value: "" },
          ],
        },
      }),
    ),
    transform: {
      ...UNCHANGED_REPLAY_TRANSFORM,
      structured: {
        key: null,
        value: {
          codec: "auto",
          patches: [{ op: "set", path: "/name", json: '"after"' }],
          mappings: [
            { format: "avro", sourceId: 1, target: selection },
            { format: "json", sourceId: null, target: null },
          ],
        },
      },
    },
  });
}
function encoding(port: SchemaLookupPort = lookup): StructuredReplayService {
  const worker = createHostRecordCodec();
  return new StructuredReplayService(() => scope(sourceContext), worker, port, worker);
}
const services: RecordReplayService[] = [];
afterEach(async () => {
  for (const s of services.splice(0)) await s.invalidate();
});
it("translates mixed writers from original bytes, independently decodes the destination and preserves tombstones/empty keys/duplicate headers/timestamps", async () => {
  const original = input(),
    before = structuredClone(original);
  const prepared = await encoding().prepare(original, scope(targetContext), signal());
  const wire = Buffer.from(prepared.batch.records[0]!.value!, "base64");
  expect(wire.readUInt32BE(1)).toBe(7);
  expect(type.fromBuffer(wire.subarray(5))).toEqual({ name: "after" });
  expect(JSON.parse(Buffer.from(prepared.batch.records[1]!.value!, "base64").toString())).toEqual({
    name: "after",
  });
  expect(prepared.batch.records[2]!.value).toBeNull();
  expect(prepared.batch.records.map((r) => r.key)).toEqual(["", "", ""]);
  expect(prepared.batch.records.map((r) => r.headers)).toEqual(
    before.records.map((r) => r.original.headers),
  );
  expect(prepared.batch.timestamps).toEqual(["1000", "1000", "1000"]);
  expect(prepared.encoding).toMatchObject([
    {
      value: {
        source: { format: "avro", id: 1 },
        target: { id: 7, subject: "target-value", version: 1 },
      },
    },
    { value: { source: { format: "json", id: null }, target: null } },
    { value: null },
  ]);
  expect(original).toEqual(before);
  expect(await prepared.revalidate(0)).toBe(true);
  expect(await prepared.revalidate(100)).toBe(false);
});
it("encodes a declared Protobuf message with destination references and exact decimal int64", async () => {
  const detail: RegisteredSchema = {
    id: 2,
    schemaType: "PROTOBUF",
    references: [],
    schema: 'syntax="proto3"; package p; message Detail { string name=1; }',
  };
  const root: RegisteredSchema = {
    id: 9,
    schemaType: "PROTOBUF",
    schema:
      'syntax="proto3"; package p; import "detail.proto"; message Other { bool ignored=1; } message Event { int64 id=1; Detail detail=2; }',
    references: [{ name: "detail.proto", subject: "detail", version: 1 }],
  };
  const f = input();
  const prepared = await encoding({
    byId: () => Promise.resolve(root),
    byVersion: (_c, subject) => Promise.resolve(subject === "detail" ? detail : root),
  }).prepare(
    {
      ...f,
      records: [
        {
          ...f.records[1]!,
          original: {
            ...f.records[1]!.original,
            value: json({ id: "9223372036854775807", detail: { name: "before" } }),
          },
        },
      ],
      transform: {
        ...UNCHANGED_REPLAY_TRANSFORM,
        structured: {
          key: null,
          value: {
            codec: "json",
            patches: [{ op: "set", path: "/detail/name", json: '"after"' }],
            mappings: [
              { format: "json", sourceId: null, target: { ...selection, messageType: "p.Event" } },
            ],
          },
        },
      },
    },
    scope(targetContext),
    signal(),
  );
  const wire = Buffer.from(prepared.batch.records[0]!.value!, "base64");
  expect(wire.readUInt32BE(1)).toBe(9);
  expect(wire.subarray(5, 7).toString("hex")).toBe("0202");
  const independently = protobuf.parse(root.schema, { keepCase: true }).root;
  protobuf.parse(detail.schema, independently, { keepCase: true });
  const event = independently.lookupType("p.Event");
  expect(event.toObject(event.decode(wire.subarray(7)), { longs: String })).toEqual({
    id: "9223372036854775807",
    detail: { name: "after" },
  });
});
it.each(["changed", "deleted", "inconsistent"] as const)(
  "freshly refuses a %s destination writer without reusing source cache",
  async (change) => {
    let current = targetSchema;
    const byVersion = vi.fn<SchemaLookupPort["byVersion"]>(() =>
      change === "deleted" && current.id === 99
        ? Promise.reject(new Error("deleted private-sentinel"))
        : Promise.resolve(current),
    );
    const byId = vi.fn<SchemaLookupPort["byId"]>((context, id) =>
      Promise.resolve(
        context === sourceContext
          ? schema
          : change === "inconsistent" && current.id === 99
            ? { ...current, schema: schema.schema }
            : { ...current, id },
      ),
    );
    const prepared = await encoding({ byVersion, byId }).prepare(
      input(),
      scope(targetContext),
      signal(),
    );
    current = {
      ...targetSchema,
      id: 99,
      schema:
        '{"type":"record","name":"R","fields":[{"name":"name","type":"string"},{"name":"changed","type":"boolean","default":false}]}',
    };
    expect(await prepared.revalidate(0)).toBe(false);
    expect(byVersion).toHaveBeenCalledTimes(2);
  },
);
it("refuses stale referenced content even when the root ID and schema remain unchanged", async () => {
  let child: RegisteredSchema = {
    id: 2,
    schemaType: "AVRO",
    references: [],
    schema: '{"type":"record","name":"Detail","fields":[{"name":"name","type":"string"}]}',
  };
  const root: RegisteredSchema = {
    id: 7,
    schemaType: "AVRO",
    schema: '{"type":"record","name":"R","fields":[{"name":"detail","type":"Detail"}]}',
    references: [{ name: "Detail", subject: "detail", version: 1 }],
  };
  const f = input(),
    prepared = await encoding({
      byId: () => Promise.resolve(root),
      byVersion: (_c, subject) => Promise.resolve(subject === "detail" ? child : root),
    }).prepare(
      {
        ...f,
        records: [
          {
            ...f.records[1]!,
            original: { ...f.records[1]!.original, value: json({ detail: { name: "x" } }) },
          },
        ],
        transform: {
          ...UNCHANGED_REPLAY_TRANSFORM,
          structured: {
            key: null,
            value: {
              codec: "json",
              patches: [],
              mappings: [{ format: "json", sourceId: null, target: selection }],
            },
          },
        },
      },
      scope(targetContext),
      signal(),
    );
  child = {
    ...child,
    schema:
      '{"type":"record","name":"Detail","fields":[{"name":"name","type":"string"},{"name":"extra","type":"int","default":0}]}',
  };
  expect(await prepared.revalidate(0)).toBe(false);
});
it("rejects opaque cross-profile frames, malformed bytes, missing mappings and invalid destination payloads", async () => {
  const f = input();
  await expect(
    encoding().prepare(
      { ...f, transform: UNCHANGED_REPLAY_TRANSFORM },
      scope(targetContext),
      signal(),
    ),
  ).rejects.toThrow("mapping");
  for (const value of ["{", "not json", "\u0000\u0000"])
    await expect(
      encoding().prepare(
        {
          ...f,
          records: [
            {
              ...f.records[0]!,
              original: { ...f.records[0]!.original, value: Buffer.from(value).toString("base64") },
            },
          ],
        },
        scope(targetContext),
        signal(),
      ),
    ).rejects.toThrow();
  const fields = f.transform.structured!;
  await expect(
    encoding().prepare(
      {
        ...f,
        transform: {
          ...f.transform,
          structured: {
            ...fields,
            value: {
              ...fields.value!,
              mappings: [{ format: "json", sourceId: null, target: null }],
            },
          },
        },
      },
      scope(targetContext),
      signal(),
    ),
  ).rejects.toThrow("mapping");
  await expect(
    encoding().prepare(
      {
        ...f,
        transform: {
          ...f.transform,
          structured: {
            ...fields,
            value: { ...fields.value!, patches: [{ op: "set", path: "/name", json: "true" }] },
          },
        },
      },
      scope(targetContext),
      signal(),
    ),
  ).rejects.toThrow("validate");
});
it("applies bounded escaped JSON Pointer edits without prototypes, implicit parents or imprecise numbers", () => {
  const original = '{"a/b":{"~name":[1,2]},"remove":true}';
  expect(
    applyReplayJsonPatches(original, [
      { op: "set", path: "/a~1b/~0name/0", json: '"9223372036854775807"' },
      { op: "remove", path: "/a~1b/~0name/1" },
      { op: "remove", path: "/remove" },
    ]),
  ).toEqual({ "a/b": { "~name": ["9223372036854775807"] } });
  for (const path of [
    "/__proto__/x",
    "/constructor/x",
    "/missing/x",
    "/a~1b/~0name/00",
    "/a~2b",
    "/a~1b/~0name/2",
  ])
    expect(() => applyReplayJsonPatches(original, [{ op: "set", path, json: "1" }])).toThrow();
  expect(() =>
    applyReplayJsonPatches(original, [{ op: "set", path: "/x", json: "9007199254740993" }]),
  ).toThrow("decimal strings");
  expect(() =>
    parseStructuredReplayTransform({
      key: null,
      value: {
        codec: "auto",
        patches: Array(17).fill({ op: "set", path: "", json: "1" }),
        mappings: [{ format: "json", sourceId: null, target: null }],
      },
    }),
  ).toThrow();
  expect(JSON.parse(original)).toEqual({ "a/b": { "~name": [1, 2] }, remove: true });
});
it("parses only evidence consistent with the selected mapping and actual source/output frames; legacy formats refuse structured jobs", async () => {
  const f = input(),
    prepared = await encoding().prepare(f, scope(targetContext), signal());
  const review = {
    planId: "mapped",
    sourceName: "Source",
    targetName: "Target",
    expiresAt: "2026-10-10T12:00:00Z",
    input: f,
    batch: prepared.batch,
    encoding: prepared.encoding!,
    destination: { clusterId: "c", topicId: "t", partitions: 1 },
  };
  expect(parseRecordReplayReview(review)).toEqual(review);
  for (const patch of [
    { encoding: [] },
    { batch: { ...review.batch, records: [...review.batch.records].reverse() } },
    {
      encoding: review.encoding.map((e) =>
        e.value?.target
          ? { ...e, value: { ...e.value, target: { ...e.value.target, subject: "other" } } }
          : e,
      ),
    },
  ])
    expect(() => parseRecordReplayReview({ ...review, ...patch })).toThrow();
  const journal = new RepairJournal(new MemoryRepairJobStore());
  await journal.begin(review);
  const document = await journal.store.load();
  expect(parseRepairJournalDocument(document)).toEqual(document);
  expect(() => parseRepairJournalDocument({ ...document, schemaVersion: 2 })).toThrow();
});
it("continues frozen output bytes without the old source Registry or another authoring call", async () => {
  const f = input(),
    service = encoding(),
    prepared = await service.prepare(f, scope(targetContext), signal());
  const review = {
    planId: "original",
    sourceName: "Source",
    targetName: "Target",
    expiresAt: "2026-10-10T12:00:00Z",
    input: f,
    batch: prepared.batch,
    encoding: prepared.encoding!,
    destination: { clusterId: "c", topicId: "t", partitions: 1 },
  };
  const worker = createHostRecordCodec(),
    author = vi.spyOn(worker, "author");
  const frozen = await new StructuredReplayService(
    () => {
      throw new Error("Source Registry offline");
    },
    worker,
    lookup,
    worker,
  ).prepare({ ...f, records: f.records.slice(1) }, scope(targetContext), signal(), {
    review,
    startIndex: 1,
  });
  expect(frozen.batch.records).toEqual(prepared.batch.records.slice(1));
  expect(frozen.encoding).toEqual(prepared.encoding!.slice(1));
  expect(author).not.toHaveBeenCalled();
  expect(await frozen.revalidate(0)).toBe(true);
});
it("rechecks the destination graph between actual sends, retaining the first receipt and remaining unsent count", async () => {
  let current = targetSchema;
  const port: SchemaLookupPort = {
    byId: (context, id) => Promise.resolve(context === sourceContext ? schema : { ...current, id }),
    byVersion: () => Promise.resolve(current),
  };
  const send = vi.fn(() => {
    current = { ...current, id: 99 };
    return Promise.resolve({
      state: "acknowledged" as const,
      receipt: { topic: "destination", partition: 0, offset: "0" },
      verification: "verified" as const,
      detail: "Accepted",
    });
  });
  const write: ReviewedWriteScope = {
    connectionName: "Target",
    isCurrent: () => true,
    reviewWrite: () => Promise.resolve({ clusterId: "c", topicId: "t", partitions: 1 }),
    tryDispatchWrite: () => ({ started: true, result: send() }),
  };
  const journal = new RepairJournal(new MemoryRepairJobStore());
  const replay = new RecordReplayService(
    () => write,
    {
      openReviewed: (): Promise<
        import("../../src/features/kafka/application/replay-destination").ReviewedReplayDestination
      > =>
        Promise.resolve({
          scope: write,
          registryScope: scope(targetContext),
          close: (): Promise<void> => Promise.resolve(),
        }),
    },
    undefined,
    journal,
    encoding(port),
  );
  services.push(replay);
  const f = input(),
    review = await replay.review({
      ...f,
      records: [f.records[0]!, { ...f.records[0]!, offset: "10" }],
    });
  expect(send).not.toHaveBeenCalled();
  const result = await replay.apply(review.planId, replayConfirmation(review));
  expect(result).toMatchObject({
    stopReason: "destination-changed",
    unsent: 1,
    outcomes: [{ state: "acknowledged" }],
  });
  expect(send).toHaveBeenCalledTimes(1);
  expect(await journal.list()).toMatchObject([
    { unsent: 1, outcomes: [{ state: "acknowledged" }] },
  ]);
});
it("rejects a mismatched subject-version response instead of treating the numeric ID as identity", async () => {
  const worker = createHostRecordCodec(),
    resolver = new RecordCodecService(
      {
        byVersion: (): Promise<RegisteredSchema> => Promise.resolve(targetSchema),
        byId: (): Promise<RegisteredSchema> =>
          Promise.resolve({ ...targetSchema, schema: '"string"' }),
      },
      worker,
    );
  await expect(resolver.resolveVersion(targetContext, "writer", 1, signal())).rejects.toThrow(
    "schema-unavailable",
  );
});
