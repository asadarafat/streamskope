import { readFile } from "node:fs/promises";
import { join } from "node:path";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import type { HttpsTrustGetDefinition } from "../../src/features/kafka/contracts/https-trust-types";
import type { HttpsTrustAcquisitionRequest } from "../../src/features/kafka/application/https-trust-port";
import { NodeHttpsTrustAcquisition } from "../../src/platform/electron/main/https-trust-acquisition";
import { createHostTrustMaterialDecoder } from "../../src/platform/electron/main/trust-material-decoder";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";

describe("HTTPS material and password stages", () => {
  let fixture: Awaited<ReturnType<typeof createHttpsTrustFixture>>;
  let jks: Uint8Array;
  beforeAll(async () => {
    jks = await readFile(join(process.cwd(), "node_modules/jks-js/examples/assets/truststore.jks"));
    fixture = await createHttpsTrustFixture((request, response) => {
      if (request.url === "/jks") response.end(jks);
      else if (request.url === "/pkcs12") response.end(fixture.pkcs12);
      else if (request.url === "/password") response.end('{"value":"\\n keep spaces \\r\\n"}');
      else if (request.url === "/empty") response.end("");
      else if (request.url === "/failed") {
        response.writeHead(503);
        response.end("secret-body");
      } else response.end(fixture.caPem);
    });
  });
  afterAll(async () => fixture.close());
  const get = (path: string): HttpsTrustGetDefinition => ({
    url: fixture.origin + path,
    headers: [],
    query: [],
  });
  const input = (): HttpsTrustAcquisitionRequest => ({
    definition: {
      authentication: "none" as const,
      material: { ...get("/pem"), extraction: { mode: "raw" as const } },
      password: { source: "none" as const },
    },
    parameters: [],
    values: new Map<string, string>(),
    authentication: { mode: "none" as const },
    tls: { mode: "custom" as const, caPem: fixture.caPem },
    signal: AbortSignal.timeout(2000),
  });
  it("fetches PEM without a password request", async () => {
    const before = fixture.requests.length;
    const result = await new NodeHttpsTrustAcquisition().fetch(input());
    expect(Buffer.from(result.bytes).toString()).toBe(fixture.caPem);
    expect(result.password).toBeUndefined();
    expect(fixture.requests.slice(before).map(({ url }) => url)).toEqual(["/pem"]);
  });
  it.each(["jks", "pkcs12"] as const)(
    "preserves real %s bytes through retrieval and shared worker decoding",
    async (kind) => {
      const request = input();
      const result = await new NodeHttpsTrustAcquisition().fetch({
        ...request,
        password: "password",
        definition: {
          ...request.definition,
          material: { ...get(`/${kind}`), extraction: { mode: "raw" } },
          password: { source: "ask" },
        },
      });
      expect(Buffer.from(result.bytes)).toEqual(Buffer.from(kind === "jks" ? jks : fixture.pkcs12));
      const decoded = await createHostTrustMaterialDecoder().decode({
        kind,
        material: Buffer.from(result.bytes).toString("base64"),
        password: result.password ?? "",
      });
      expect(decoded.kind).toBe(kind);
      expect(decoded.evidence?.count).toBe(1);
      expect(decoded.caPem).toContain("-----BEGIN CERTIFICATE-----");
    },
  );
  it("fetches the same-origin password first and preserves meaningful spaces", async () => {
    const request = input();
    const before = fixture.requests.length;
    const result = await new NodeHttpsTrustAcquisition().fetch({
      ...request,
      definition: {
        ...request.definition,
        password: {
          source: "https",
          request: { ...get("/password"), extraction: { mode: "json", pointer: "/value" } },
        },
      },
    });
    expect(result.password).toBe(" keep spaces ");
    expect(fixture.requests.slice(before).map(({ url }) => url)).toEqual(["/password", "/pem"]);
  });
  it("does not fetch material after a malformed password response", async () => {
    const request = input();
    const before = fixture.requests.length;
    await expect(
      new NodeHttpsTrustAcquisition().fetch({
        ...request,
        definition: {
          ...request.definition,
          password: {
            source: "https",
            request: { ...get("/empty"), extraction: { mode: "text" } },
          },
        },
      }),
    ).rejects.toMatchObject({
      category: "extraction",
      target: fixture.origin,
      message: "HTTPS password retrieval failed (extraction). Existing trust is unchanged.",
    });
    expect(fixture.requests.slice(before).map(({ url }) => url)).toEqual(["/empty"]);
  });
  it("rejects a cross-origin password and missing Ask input before any request", async () => {
    const request = input();
    const before = fixture.requests.length;
    await expect(
      new NodeHttpsTrustAcquisition().fetch({
        ...request,
        definition: {
          ...request.definition,
          password: {
            source: "https",
            request: {
              ...get("/password"),
              url: "https://elsewhere.invalid/password",
              extraction: { mode: "text" },
            },
          },
        },
      }),
    ).rejects.toThrow();
    await expect(
      new NodeHttpsTrustAcquisition().fetch({
        ...request,
        definition: { ...request.definition, password: { source: "ask" } },
      }),
    ).rejects.toThrow();
    expect(fixture.requests.length).toBe(before);
  });
  it("returns no partial result when material fails after password retrieval", async () => {
    const request = input();
    await expect(
      new NodeHttpsTrustAcquisition().fetch({
        ...request,
        definition: {
          ...request.definition,
          material: { ...request.definition.material, url: fixture.origin + "/failed" },
          password: {
            source: "https",
            request: { ...get("/password"), extraction: { mode: "json", pointer: "/value" } },
          },
        },
      }),
    ).rejects.toMatchObject({
      category: "status",
      message: "HTTPS material retrieval failed (status). Existing trust is unchanged.",
    });
  });
});
