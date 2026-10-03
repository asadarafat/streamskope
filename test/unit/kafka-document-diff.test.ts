import { expect, it } from "vitest";

import { compareDocuments } from "../../src/features/kafka/contracts/document-diff";
import { inspectSchema } from "../../src/features/kafka/application/schema-inspection";
import {
  parseSchemaInspection,
  parseSchemaInspectionInput,
} from "../../src/features/kafka/contracts/schema-inspection";
import type { RegisteredSchema } from "../../src/features/kafka/application/record-codec-types";

it("compares values, ordered arrays, additions, removals and null without confusing missing properties", () => {
  expect(
    compareDocuments(
      '{"empty":null,"a/b":1,"array":[1,2]}',
      '{"added":null,"a/b":2,"array":[2,1]}',
      "json",
    ),
  ).toEqual({
    limited: false,
    rows: [
      { path: "/a~1b", kind: "changed", before: "1", after: "2" },
      { path: "/added", kind: "added", before: "(missing)", after: "null" },
      { path: "/array/0", kind: "changed", before: "1", after: "2" },
      { path: "/array/1", kind: "changed", before: "2", after: "1" },
      { path: "/empty", kind: "removed", before: "null", after: "(missing)" },
    ],
  });
  expect(compareDocuments('{"b":2,"a":1}', '{"a":1,"b":2}', "json").rows).toEqual([]);
});
it("keeps unsafe numeric differences exact and distinguishes a number from a matching-looking object", () => {
  const result = compareDocuments(
    '{"id":9223372036854775806}',
    '{"id":9223372036854775807}',
    "json",
  );
  expect(result.rows).toHaveLength(1);
  expect(result.rows[0]?.before).toContain("9223372036854775806");
  expect(result.rows[0]?.after).toContain("9223372036854775807");
  expect(
    compareDocuments("9223372036854775807", '{"value":"9223372036854775807"}', "json").rows,
  ).toHaveLength(1);
});
it("reports bounded and invalid input and supports uninterpreted line comparison", () => {
  expect(compareDocuments("not JSON", "[]", "json").error).toBeTruthy();
  expect(
    compareDocuments("one\ntwo", "one\nthree\nfour", "text").rows.map((row) => row.kind),
  ).toEqual(["changed", "added"]);
  expect(compareDocuments("x".repeat(524289), "", "text")).toMatchObject({ limited: true });
  expect(
    compareDocuments(
      JSON.stringify(Array(600).fill(1)),
      JSON.stringify(Array(600).fill(2)),
      "json",
    ),
  ).toMatchObject({ limited: true });
  expect(
    compareDocuments(
      "[".repeat(40) + "0" + "]".repeat(40),
      "[".repeat(40) + "1" + "]".repeat(40),
      "json",
    ).limited,
  ).toBe(true);
});
const context = {
  baseUrl: "https://registry.invalid",
  authorization: (): Promise<undefined> => Promise.resolve(undefined),
};
const schema = (id: number, references: RegisteredSchema["references"] = []): RegisteredSchema => ({
  id,
  schemaType: "AVRO",
  schema: '"string"',
  references,
});
it("inspects only exact schema versions and retains missing, repeated and cyclic edge evidence", async () => {
  const calls: string[] = [];
  const result = await inspectSchema(
    { subject: "A", version: 2 },
    {
      byId: (): Promise<never> => Promise.reject(new Error("unused")),
      byVersion: (_context, subject, version): Promise<RegisteredSchema> => {
        calls.push(`${subject}@${String(version)}`);
        if (subject === "missing") return Promise.reject(new Error("404 private details"));
        return Promise.resolve(
          subject === "A"
            ? schema(10, [
                { name: "b", subject: "B", version: 1 },
                { name: "missing1", subject: "missing", version: 1 },
                { name: "missing2", subject: "missing", version: 1 },
              ])
            : schema(11, [{ name: "a", subject: "A", version: 2 }]),
        );
      },
    },
    context,
    new AbortController().signal,
  );
  expect(result.root).toMatchObject({ subject: "A", version: 2, id: 10 });
  expect(result.edges.map((edge) => edge.state)).toEqual([
    "resolved",
    "cycle",
    "unavailable",
    "unavailable",
  ]);
  expect(calls).toEqual(["A@2", "B@1", "missing@1"]);
  expect(JSON.stringify(result)).not.toContain("private details");
  expect(parseSchemaInspection(result)).toEqual(result);
  expect(() => parseSchemaInspectionInput({ subject: "A", version: "latest" })).toThrow(/exact/u);
});
it("bounds graph traversal and rejects late results after cancellation", async () => {
  let calls = 0;
  const controller = new AbortController();
  const lookup = {
    byId: (): Promise<never> => Promise.reject(new Error("unused")),
    byVersion: (): Promise<RegisteredSchema> => {
      calls++;
      return Promise.resolve(
        schema(calls, [
          { name: `level${String(calls)}`, subject: `s${String(calls)}`, version: 1 },
        ]),
      );
    },
  };
  const result = await inspectSchema(
    { subject: "root", version: 1 },
    lookup,
    context,
    controller.signal,
  );
  expect(result.limited).toBe(true);
  expect(calls).toBe(9);
  expect(result.edges.at(-1)?.state).toBe("limit");
  controller.abort();
  await expect(
    inspectSchema({ subject: "root", version: 1 }, lookup, context, controller.signal),
  ).rejects.toThrow();
});
