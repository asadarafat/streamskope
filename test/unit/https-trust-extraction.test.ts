import { describe, expect, it } from "vitest";

import {
  extractHttpsTrustMaterial,
  extractHttpsTrustPassword,
} from "../../src/main/https-trust-extraction";

const bytes = (text: string): Uint8Array => Buffer.from(text, "utf8");

describe("bounded HTTPS trust extraction", () => {
  it("preserves raw binary bytes without decoding them as text", () => {
    const input = Uint8Array.of(0, 255, 128, 10);
    expect(extractHttpsTrustMaterial(input, { mode: "raw" })).toEqual(input);
  });

  it("resolves root, escaped object keys and canonical array indexes", () => {
    expect(
      extractHttpsTrustMaterial(bytes('"PEM fixture"'), { mode: "json-pem", pointer: "" }),
    ).toEqual(bytes("PEM fixture"));
    expect(
      extractHttpsTrustMaterial(bytes('{"a/b":{"~key":["PEM fixture"]}}'), {
        mode: "json-pem",
        pointer: "/a~1b/~0key/0",
      }),
    ).toEqual(bytes("PEM fixture"));
    expect(
      extractHttpsTrustMaterial(bytes('{"":"empty key"}'), { mode: "json-pem", pointer: "/" }),
    ).toEqual(bytes("empty key"));
  });

  it("strictly decodes canonical base64", () => {
    expect([
      ...extractHttpsTrustMaterial(bytes('{"value":"AP+A"}'), {
        mode: "json-base64",
        pointer: "/value",
      }),
    ]).toEqual([0, 255, 128]);
    expect(
      extractHttpsTrustMaterial(bytes('"YQ=="'), { mode: "json-base64", pointer: "" }),
    ).toEqual(bytes("a"));
  });

  it.each(["YQ", "YQ=", "YR==", "YQ===", "YQ==junk", "YQ==\n", "-_==", ""])(
    "rejects noncanonical base64 without echoing it: %#",
    (value) => {
      expect(() =>
        extractHttpsTrustMaterial(bytes(JSON.stringify(value)), {
          mode: "json-base64",
          pointer: "",
        }),
      ).toThrow("material.extraction");
    },
  );

  it.each([
    "missing",
    "/~2",
    "/~",
    "/constructor",
    "/toString",
    "/__proto__/x",
    "/list/01",
    "/list/-",
    "/list/length",
    "/absent",
    "/object",
    "/number",
    "/boolean",
    "/nil",
  ])("rejects malformed/missing/non-string paths: %s", (pointer) => {
    const document = '{"list":["x"],"object":{},"number":4,"boolean":true,"nil":null}';
    expect(() => extractHttpsTrustMaterial(bytes(document), { mode: "json-pem", pointer })).toThrow(
      "material.extraction",
    );
  });

  it("uses own properties without prototype traversal", () => {
    expect(
      extractHttpsTrustMaterial(bytes('{"__proto__":"literal"}'), {
        mode: "json-pem",
        pointer: "/__proto__",
      }),
    ).toEqual(bytes("literal"));
  });

  it("rejects malformed UTF-8, JSON and unpaired surrogates without response disclosure", () => {
    for (const body of [
      Uint8Array.of(0xff),
      bytes('{"secret":"sensitive-body"'),
      bytes('"\u005cud800"'),
    ]) {
      expect(() => extractHttpsTrustMaterial(body, { mode: "json-pem", pointer: "" })).toThrow(
        "material.extraction",
      );
      try {
        extractHttpsTrustMaterial(body, { mode: "json-pem", pointer: "" });
      } catch (error) {
        expect(String(error)).not.toContain("sensitive-body");
      }
    }
  });

  it("enforces nesting, pointer, wire and decoded limits before decoder handoff", () => {
    const nested = "[".repeat(17) + '"x"' + "]".repeat(17);
    expect(() =>
      extractHttpsTrustMaterial(bytes(nested), { mode: "json-pem", pointer: "" }),
    ).toThrow("material.extraction");
    for (const pointer of ["/x".repeat(33), "/" + "x".repeat(1024)])
      expect(() => extractHttpsTrustMaterial(bytes('"x"'), { mode: "json-pem", pointer })).toThrow(
        "material.extraction",
      );
    expect(() =>
      extractHttpsTrustMaterial(new Uint8Array(8 * 1024 * 1024 + 1), { mode: "raw" }),
    ).toThrow("material.extraction");
    expect(() =>
      extractHttpsTrustMaterial(bytes(JSON.stringify("x".repeat(8 * 1024 * 1024 + 1))), {
        mode: "json-pem",
        pointer: "",
      }),
    ).toThrow("material.extraction");
    expect(() =>
      extractHttpsTrustMaterial(new Uint8Array(12 * 1024 * 1024 + 1), {
        mode: "json-pem",
        pointer: "",
      }),
    ).toThrow("material.extraction");
    expect(() =>
      extractHttpsTrustMaterial(
        bytes(JSON.stringify(Buffer.alloc(8 * 1024 * 1024 + 1).toString("base64"))),
        { mode: "json-base64", pointer: "" },
      ),
    ).toThrow("material.extraction");
  });

  it("allows sixteen nested levels and does not count brackets inside strings", () => {
    const body = "[".repeat(16) + JSON.stringify('["{x}"]') + "]".repeat(16);
    expect(
      extractHttpsTrustMaterial(bytes(body), { mode: "json-pem", pointer: "/0".repeat(16) }),
    ).toEqual(bytes('["{x}"]'));
  });

  it("extracts passwords with only surrounding CR/LF removed", () => {
    expect(extractHttpsTrustPassword(bytes("\r\n  pass word \t\r\n"), { mode: "text" })).toBe(
      "  pass word \t",
    );
    expect(
      extractHttpsTrustPassword(bytes('{"value":"\\n secret \\r"}'), {
        mode: "json",
        pointer: "/value",
      }),
    ).toBe(" secret ");
    for (const body of [
      bytes("\r\n"),
      bytes("x".repeat(4097)),
      new Uint8Array(65537),
      Uint8Array.of(0xff),
    ])
      expect(() => extractHttpsTrustPassword(body, { mode: "text" })).toThrow(
        "password.extraction",
      );
  });
});
