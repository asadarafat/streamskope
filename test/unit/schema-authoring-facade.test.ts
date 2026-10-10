import { expect, it, vi, type Mock } from "vitest";

import type {
  KafkaApplicationSession,
  KafkaClusterServiceContext,
} from "../../src/features/kafka/application";
import type {
  RegisteredSchema,
  SchemaAuthoringPort,
  SchemaLookupPort,
  SchemaSamplePort,
} from "../../src/features/kafka/application/record-codec-types";
import type { SchemaAuthoringResult } from "../../src/features/kafka/contracts/schema-authoring";
import { HOST_PROTOCOL_VERSION, type HostCommand } from "../../src/features/kafka/contracts";
import { SchemaSamplesFacade } from "../../src/features/kafka/facade/schema-samples-facade";
import { authorSchemaRecord } from "../../src/features/kafka/engine/schema-record-encoder";

const command = {
  command: "schemas.author",
  version: HOST_PROTOCOL_VERSION,
  id: "author",
  payload: { subject: "events", version: 1, schemaId: 7, messageType: "", payload: '"edited"' },
} as const satisfies HostCommand;
function fixture(): {
  facade: SchemaSamplesFacade;
  byVersion: Mock<SchemaLookupPort["byVersion"]>;
  author: Mock<SchemaAuthoringPort["author"]>;
  authority: AbortController;
  replace(): void;
} {
  const authority = new AbortController();
  let id = 7;
  const schema = (): RegisteredSchema => ({
    id,
    schemaType: "AVRO",
    schema: '"string"',
    references: [],
  });
  const byVersion = vi.fn<SchemaLookupPort["byVersion"]>(() => Promise.resolve(schema()));
  const lookup: SchemaLookupPort = { byVersion, byId: () => Promise.resolve(schema()) };
  const session = {
    clusterServiceContext: (): KafkaClusterServiceContext => ({
      baseUrl: "http://registry",
      authorization: (): Promise<undefined> => Promise.resolve(undefined),
      signal: authority.signal,
    }),
    snapshot: (): { state: "connected" } => ({ state: "connected" }),
    reviewedWriteScope: (): never => {
      throw new Error("Validation must not access a write scope.");
    },
  } as unknown as KafkaApplicationSession;
  const author = vi.fn<SchemaAuthoringPort["author"]>((input, bundle) =>
    Promise.resolve(authorSchemaRecord({ kind: "author", input, bundle })),
  );
  const generator: SchemaSamplePort & SchemaAuthoringPort = {
    generate: () => Promise.reject(new Error("Unused generator")),
    author,
  };
  const facade = new SchemaSamplesFacade(
    session,
    { decode: (): Promise<never> => Promise.reject(new Error("Unused decoder")) },
    lookup,
    generator,
  );
  return {
    facade,
    byVersion,
    author,
    authority,
    replace: (): void => {
      id = 8;
    },
  };
}

it("resolves registered writer identity again for every edited validation", async () => {
  const f = fixture();
  expect(await f.facade.execute(command, "first")).toMatchObject({
    ok: true,
    result: { authoring: { state: "valid", writer: { id: 7 } } },
  });
  f.replace();
  expect(await f.facade.execute(command, "second")).toMatchObject({
    ok: true,
    result: { authoring: { state: "invalid", issues: [{ code: "schema" }] } },
  });
  expect(f.byVersion).toHaveBeenCalledTimes(2);
});

it.each(["connection", "invalidate"] as const)(
  "rejects late worker validation after %s authority ends",
  async (mode) => {
    const f = fixture();
    let finish!: () => void;
    f.author.mockImplementation(
      (input, bundle) =>
        new Promise<SchemaAuthoringResult>((resolve) => {
          finish = (): void => resolve(authorSchemaRecord({ kind: "author", input, bundle }));
        }),
    );
    const pending = f.facade.execute(command, "late");
    await vi.waitFor(() => expect(f.author).toHaveBeenCalledTimes(1));
    if (mode === "connection") f.authority.abort();
    else f.facade.invalidate();
    finish();
    expect(await pending).toMatchObject({ ok: false, error: { activeStateChanged: false } });
  },
);

it("bounds concurrent validation and does not reflect a compiler or Registry diagnostic", async () => {
  const f = fixture();
  const releases: (() => void)[] = [];
  f.author.mockImplementation(
    (input, bundle) =>
      new Promise<SchemaAuthoringResult>((resolve) =>
        releases.push(() => resolve(authorSchemaRecord({ kind: "author", input, bundle }))),
      ),
  );
  const first = f.facade.execute(command, "one"),
    second = f.facade.execute(command, "two");
  await vi.waitFor(() => expect(releases).toHaveLength(2));
  expect(await f.facade.execute(command, "three")).toMatchObject({ ok: false });
  releases.forEach((release) => release());
  await Promise.all([first, second]);
  f.author.mockImplementation(() => Promise.reject(new Error("private-compiler-diagnostic")));
  const failed = await f.facade.execute(command, "safe");
  expect(failed).toMatchObject({ ok: false });
  expect(JSON.stringify(failed)).not.toContain("private-compiler-diagnostic");
});
