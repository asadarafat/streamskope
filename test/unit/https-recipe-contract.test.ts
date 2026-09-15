import { describe, expect, it } from "vitest";

import {
  parseTrustAcquisitionRecipeInput,
  parseTrustAcquisitionRecipeDocument,
  parseTrustAcquisitionRecipeImport,
} from "../../src/kafka/contracts";
import { resolveHttpsGet } from "../../src/kafka/contracts/https-trust-validation";

const request = {
  url: "https://{{host}}:8443/cert/{{path}}",
  headers: [{ name: "X-Api-Key", value: "{{token}}" }],
  query: [{ name: "type", value: "{{path}}" }],
  extraction: { mode: "raw" },
};
const recipe = {
  name: "Certificate API",
  kind: "pem",
  syntax: "named-v1",
  method: "https",
  https: { authentication: "bearer", material: request, password: { source: "none" } },
  parameters: [
    { key: "path", label: "Path", type: "text", required: true },
    { key: "token", label: "Key", type: "secret", required: true },
  ],
  timeoutSeconds: 30,
};

describe("HTTPS recipe canonical contract", () => {
  it("encodes runtime values as data and confines secrets to validated headers", () => {
    const parsed = parseTrustAcquisitionRecipeInput(recipe);
    const result = resolveHttpsGet(
      request,
      parsed.parameters,
      new Map([
        ["host", "localhost"],
        ["path", "a/b?x=y&z=#"],
        ["token", "secret-value"],
      ]),
    );
    const url = new URL(result.url);
    expect(url.origin).toBe("https://localhost:8443");
    expect(url.pathname).toBe("/cert/a%2Fb%3Fx%3Dy%26z%3D%23");
    expect(url.searchParams.get("type")).toBe("a/b?x=y&z=#");
    expect(result.url).not.toContain("secret-value");
    expect(result.headers).toEqual([{ name: "X-Api-Key", value: "secret-value" }]);
    expect(() =>
      resolveHttpsGet(
        request,
        parsed.parameters,
        new Map([
          ["host", "localhost"],
          ["path", "safe"],
          ["token", "x\r\nAuthorization: stolen"],
        ]),
      ),
    ).toThrow();
  });
  it("requires a runtime host when the URL uses the reserved host token", () => {
    const parsed = parseTrustAcquisitionRecipeInput(recipe);
    expect(() =>
      resolveHttpsGet(
        request,
        parsed.parameters,
        new Map([
          ["path", "safe"],
          ["token", "safe"],
        ]),
      ),
    ).toThrow();
  });
  it("accepts generic HTTPS settings without requiring SSH fields", () => {
    expect(parseTrustAcquisitionRecipeInput(recipe)).toEqual(recipe);
    expect(
      parseTrustAcquisitionRecipeDocument({
        version: 1,
        recipes: [{ ...recipe, id: "https", revision: 1 }],
      }).recipes,
    ).toHaveLength(1);
    expect(
      parseTrustAcquisitionRecipeImport(
        JSON.stringify({ format: "streamskope-trust-recipe", version: 1, recipe }),
      ),
    ).toEqual(recipe);
  });
  it("preserves valid inactive settings without requiring the inactive password kind", () => {
    const ssh = { source: "file", value: "/ca.pem", password: { source: "none" } };
    const binary = {
      ...recipe,
      kind: "jks",
      ssh,
      https: { ...recipe.https, password: { source: "ask" } },
    };
    expect(parseTrustAcquisitionRecipeInput(binary)).toEqual(binary);
  });
  it.each([
    "http://host/cert",
    "https://u:p@host/cert",
    "https://host/cert#x",
    "https://{{path}}/cert",
    "https://host:{{path}}/cert",
    "https://host/{{token}}",
    "https://host/?secret={{token}}",
    "https://host/{{missing}}",
    "https://host/\ncert",
    "https://host/{{path",
  ])("rejects unsafe URL %s", (url) => {
    expect(() =>
      parseTrustAcquisitionRecipeInput({
        ...recipe,
        https: { ...recipe.https, material: { ...request, url } },
      }),
    ).toThrow();
  });
  it.each([
    "Authorization",
    "Host",
    "COOKIE",
    "Content-Length",
    "Connection",
    "Transfer-Encoding",
    "Proxy-Authorization",
    "Sec-Fetch-Site",
    "x\r\ny",
  ])("rejects reserved header %s", (name) => {
    expect(() =>
      parseTrustAcquisitionRecipeInput({
        ...recipe,
        https: { ...recipe.https, material: { ...request, headers: [{ name, value: "x" }] } },
      }),
    ).toThrow();
  });
  it("rejects duplicate rows, forbidden query secrets and unsafe values", () => {
    for (const patch of [
      {
        headers: [
          { name: "X-Key", value: "a" },
          { name: "x-key", value: "b" },
        ],
      },
      { headers: [{ name: "x", value: "a\r\nb" }] },
      { query: [{ name: "x", value: "{{token}}" }] },
      {
        query: [
          { name: "x", value: "a" },
          { name: "x", value: "b" },
        ],
      },
      { headers: Array.from({ length: 33 }, (_, i) => ({ name: `x-${i}`, value: "x" })) },
      { extraction: { mode: "json-pem", pointer: "/~2" } },
      { extraction: { mode: "json-base64", pointer: "/x".repeat(33) } },
    ])
      expect(() =>
        parseTrustAcquisitionRecipeInput({
          ...recipe,
          https: { ...recipe.https, material: { ...request, ...patch } },
        }),
      ).toThrow();
  });
  it("requires matching same-origin password GET and an explicit supported method", () => {
    const password = {
      source: "https",
      request: { ...request, url: "https://{{host}}:8443/password", extraction: { mode: "text" } },
    };
    const binary = { ...recipe, kind: "jks", https: { ...recipe.https, password } };
    expect(parseTrustAcquisitionRecipeInput(binary)).toEqual(binary);
    expect(() =>
      parseTrustAcquisitionRecipeInput({
        ...binary,
        https: {
          ...binary.https,
          password: {
            ...password,
            request: { ...password.request, url: "https://elsewhere/password" },
          },
        },
      }),
    ).toThrow();
    for (const patch of [
      { method: "http" },
      { syntax: "legacy-v1" },
      { https: { ...recipe.https, authentication: "oauth" } },
      { https: { ...recipe.https, caPem: "private" } },
      { https: { ...recipe.https, password } },
    ])
      expect(() => parseTrustAcquisitionRecipeInput({ ...recipe, ...patch })).toThrow();
  });
});
