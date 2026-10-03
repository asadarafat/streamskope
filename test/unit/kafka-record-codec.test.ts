import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
} from "../../src/features/kafka/contracts";
import { parseStructuredRecord } from "../../src/features/kafka/engine/record-codec-parser";
import { RecordCodecService } from "../../src/features/kafka/application/record-codec-service";
import type {
  CodecSchemaBundle,
  RecordCodecPort,
  RegisteredSchema,
  SchemaLookupPort,
} from "../../src/features/kafka/application/record-codec-types";
import type {
  RecordDecodeResult,
  RecordFormat,
} from "../../src/features/kafka/contracts/record-codec";
import { createHostRecordCodec } from "../../src/platform/node/record-codec";
import { BoundedRecordCodec } from "../../src/features/kafka/engine/record-codec";

const context = {
  baseUrl: "https://registry.test",
  authorization: (): Promise<undefined> => Promise.resolve(undefined),
};
const signal = (): AbortSignal => new AbortController().signal;
const avroSchema: RegisteredSchema = {
  id: 7,
  references: [],
  schemaType: "AVRO",
  schema: JSON.stringify({
    type: "record",
    name: "Event",
    fields: [
      { name: "id", type: "long" },
      { name: "name", type: "string" },
    ],
  }),
};
const bundle = (root: RegisteredSchema): CodecSchemaBundle => ({ root, dependencies: [] });
function json(result: RecordDecodeResult): unknown {
  expect(result.state).toBe("decoded");
  return result.state === "decoded" ? (JSON.parse(result.json) as unknown) : undefined;
}
const parse = (format: RecordFormat, hex: string, schema: CodecSchemaBundle): RecordDecodeResult =>
  parseStructuredRecord({
    input: { format, bytes: Buffer.from(hex, "hex").toString("base64") },
    bundle: schema,
  });

describe("explicit structured record decoding", () => {
  it("bounds concurrent workers and terminates an unresponsive decoder", async () => {
    const directory = await mkdtemp(join(tmpdir(), "streamskope-codec-deadline-"));
    try {
      const script = join(directory, "unresponsive.cjs");
      await writeFile(script, "setInterval(() => {}, 1000);");
      const codec = new BoundedRecordCodec({ script, execArgv: [] });
      const controller = new AbortController();
      const first = codec.decode({ format: "json", bytes: "e30=" }, null, controller.signal);
      const firstOutcome = expect(first).rejects.toThrow(/cancelled/u);
      const second = codec.decode({ format: "json", bytes: "e30=" }, null, signal());
      const secondOutcome = expect(second).rejects.toThrow(/deadline/u);
      await expect(codec.decode({ format: "json", bytes: "e30=" }, null, signal())).rejects.toThrow(
        /capacity/u,
      );
      controller.abort();
      await firstOutcome;
      await secondOutcome;
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
  it("decodes independent Avro wire bytes without losing int64 precision or changing originals", () => {
    const wire = "0000000007feffffffffffffffff01046f6b";
    expect(json(parse("avro", wire, bundle(avroSchema)))).toEqual({
      id: "9223372036854775807",
      name: "ok",
    });
    expect(parse("avro", wire + "00", bundle(avroSchema))).toMatchObject({
      state: "error",
      code: "malformed",
    });
  });
  it("preserves JSON null, false and unsafe integer text and rejects invalid UTF-8", () => {
    const decode = (bytes: string | null): RecordDecodeResult =>
      parseStructuredRecord({ input: { format: "json", bytes }, bundle: null });
    expect(decode(null)).toEqual({ state: "null", format: "json" });
    expect(
      json(
        decode(
          Buffer.from('{"id":9223372036854775807,"empty":null,"ok":false}').toString("base64"),
        ),
      ),
    ).toEqual({ id: "9223372036854775807", empty: null, ok: false });
    expect(decode("/w==")).toMatchObject({ state: "error", code: "malformed" });
    expect(decode("")).toMatchObject({ state: "error", code: "malformed" });
  });
  it("handles Avro references, union branches and Confluent top-level bytes", () => {
    const referenced: CodecSchemaBundle = {
      root: {
        id: 7,
        schemaType: "AVRO",
        references: [{ name: "Address", subject: "address", version: 1 }],
        schema:
          '{"type":"record","name":"Person","fields":[{"name":"address","type":"Address"},{"name":"optional","type":["null","string"]}]}',
      },
      dependencies: [
        {
          name: "Address",
          schema: {
            id: 6,
            schemaType: "AVRO",
            references: [],
            schema: '{"type":"record","name":"Address","fields":[{"name":"city","type":"string"}]}',
          },
        },
      ],
    };
    expect(json(parse("avro", "0000000007046f6b020278", referenced))).toEqual({
      address: { city: "ok" },
      optional: { string: "x" },
    });
    expect(
      json(
        parse(
          "avro",
          "000000000700ff",
          bundle({ id: 7, schemaType: "AVRO", references: [], schema: '"bytes"' }),
        ),
      ),
    ).toBe("\u0000ÿ");
  });
  it("decodes independent Protobuf frames with imported types and string int64", () => {
    const root: RegisteredSchema = {
      id: 7,
      schemaType: "PROTOBUF",
      references: [{ name: "common.proto", subject: "common", version: 2 }],
      schema:
        'syntax="proto3"; package fixture; import "common.proto"; message Event { int64 id=1; Detail detail=2; bytes data=3; }',
    };
    const decoded = parse("protobuf", "00000000070008ffffffffffffffff7f12040a026f6b1a0200ff", {
      root,
      dependencies: [
        {
          name: "common.proto",
          schema: {
            id: 8,
            schemaType: "PROTOBUF",
            references: [],
            schema: 'syntax="proto3"; package fixture; message Detail { string name=1; }',
          },
        },
      ],
    });
    expect(json(decoded)).toEqual({
      id: "9223372036854775807",
      detail: { name: "ok" },
      data: "AP8=",
    });
    expect(decoded).toMatchObject({ messageType: ".fixture.Event" });
  });
  it("honors nested message indexes, not the first message or an imported type", () => {
    const schema = bundle({
      id: 7,
      schemaType: "PROTOBUF",
      references: [],
      schema:
        'syntax="proto3"; package fixture; message First { int32 wrong=1; } message Outer { message Inner { string name=1; } }',
    });
    expect(json(parse("protobuf", "00000000070402000a026f6b", schema))).toEqual({ name: "ok" });
    for (const prefix of ["02", "01", "04020080", "02fe01", "808080808080"])
      expect(parse("protobuf", "0000000007" + prefix, schema)).toMatchObject({ state: "error" });
  });
  it("does not resolve arbitrary filesystem/URL imports and bounds decoded structures", () => {
    expect(
      parse(
        "protobuf",
        "000000000700",
        bundle({
          id: 7,
          schemaType: "PROTOBUF",
          references: [],
          schema: 'syntax="proto3"; import "/tmp/credentials.proto"; message Event {}',
        }),
      ),
    ).toMatchObject({ state: "error" });
    const bytes = Buffer.from("[".repeat(34) + "0" + "]".repeat(34)).toString("base64");
    expect(parseStructuredRecord({ input: { format: "json", bytes }, bundle: null })).toMatchObject(
      { state: "error", code: "limit" },
    );
  });
  it("requires explicit bounded canonical bytes and a structured host response", () => {
    const command = {
      command: "records.decode",
      id: "decode",
      version: HOST_PROTOCOL_VERSION,
      payload: { format: "avro", bytes: "AA==" },
    };
    expect(parseHostCommand(command)).toEqual(command);
    expect(() =>
      parseHostCommand({ ...command, payload: { ...command.payload, bytes: "AA" } }),
    ).toThrow();
    expect(() =>
      parseHostCommand({
        ...command,
        payload: { ...command.payload, bytes: Buffer.alloc(256 * 1024 + 1).toString("base64") },
      }),
    ).toThrow();
    expect(() =>
      parseHostCommandResponse({
        command: "records.decode",
        id: "decode",
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId: "c" },
      }),
    ).toThrow();
  });
  it("runs the production isolated worker and cancels a requested decode", async () => {
    const codec = createHostRecordCodec();
    expect(
      json(
        await codec.decode(
          { format: "avro", bytes: Buffer.from("000000000702046f6b", "hex").toString("base64") },
          bundle(avroSchema),
          signal(),
        ),
      ),
    ).toEqual({ id: "1", name: "ok" });
    const controller = new AbortController();
    const pending = codec.decode({ format: "json", bytes: "e30=" }, null, controller.signal);
    controller.abort();
    await expect(pending).rejects.toThrow(/cancelled/u);
  });
});

describe("bounded writer schema resolution", () => {
  const codec: RecordCodecPort = {
    decode: (input, resolved) =>
      Promise.resolve(parseStructuredRecord({ input, bundle: resolved })),
  };
  const input = {
    format: "avro" as const,
    bytes: Buffer.from("000000000702046f6b", "hex").toString("base64"),
  };
  it("caches schemas for one connection, clears on invalidation and preserves failures per record", async () => {
    const byId = vi.fn(() => Promise.resolve(avroSchema));
    const lookup: SchemaLookupPort = {
      byId,
      byVersion: vi.fn(),
    };
    const service = new RecordCodecService(lookup, codec);
    expect(json(await service.decode(input, context, signal()))).toEqual({ id: "1", name: "ok" });
    await service.decode(input, context, signal());
    expect(byId).toHaveBeenCalledTimes(1);
    service.clear();
    await service.decode(input, context, signal());
    expect(byId).toHaveBeenCalledTimes(2);
    expect(await service.decode(input, null, signal())).toMatchObject({
      code: "schema-unavailable",
    });
    expect(await service.decode({ ...input, bytes: "AA==" }, context, signal())).toMatchObject({
      code: "malformed",
    });
    expect(await service.decode({ ...input, bytes: null }, null, signal())).toEqual({
      state: "null",
      format: "avro",
    });
  });
  it("detects cycles and missing or oversized schemas without calling the decoder", async () => {
    const read = vi.fn(() =>
      Promise.resolve({
        ...avroSchema,
        references: [{ name: "Event", subject: "event", version: 1 }],
      }),
    );
    const decode = vi.fn(codec.decode.bind(codec));
    const service = new RecordCodecService({ byId: read, byVersion: read }, { decode });
    expect(await service.decode(input, context, signal())).toMatchObject({ code: "reference" });
    expect(decode).not.toHaveBeenCalled();
    const missing = new RecordCodecService(
      {
        byId: (): Promise<RegisteredSchema> => Promise.reject(new Error("404 secret")),
        byVersion: vi.fn(),
      },
      { decode },
    );
    const result = await missing.decode(input, context, signal());
    expect(result).toMatchObject({ code: "schema-unavailable" });
    expect(JSON.stringify(result)).not.toContain("secret");
    const oversized = new RecordCodecService(
      {
        byId: (): Promise<RegisteredSchema> =>
          Promise.resolve({ ...avroSchema, schema: "a".repeat(262145) }),
        byVersion: vi.fn(),
      },
      { decode },
    );
    expect(await oversized.decode(input, context, signal())).toMatchObject({ code: "limit" });
  });
  it("does not cache a schema fetched after cancellation", async () => {
    const controller = new AbortController();
    let complete!: (value: RegisteredSchema) => void;
    const byId = vi.fn(
      () =>
        new Promise<RegisteredSchema>((resolve) => {
          complete = resolve;
        }),
    );
    const service = new RecordCodecService({ byId, byVersion: vi.fn() }, codec);
    const pending = service.decode(input, context, controller.signal);
    controller.abort();
    service.clear();
    complete(avroSchema);
    expect(await pending).toMatchObject({ code: "cancelled" });
    byId.mockResolvedValue(avroSchema);
    await service.decode(input, context, signal());
    expect(byId).toHaveBeenCalledTimes(2);
  });
});
