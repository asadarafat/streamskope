import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";

import { beforeAll, describe, expect, it } from "vitest";

import { parseTrustMaterial } from "../../src/kafka/engine";
import { loadFixtureConnection } from "../support/kafka-fixture";

let fixtureTruststore: string;

beforeAll(async () => {
  const fixture = await loadFixtureConnection();
  fixtureTruststore = (
    await readFile(join(dirname(fixture.caPath), "kafka.truststore.jks"))
  ).toString("base64");
});

describe("owned Kafka fixture trust material", () => {
  it("matches the fixture's JKS declaration and .jks filename", () => {
    const result = parseTrustMaterial({
      kind: "jks",
      material: fixtureTruststore,
      password: "password",
    });

    expect(result.kind).toBe("jks");
    expect(result.caPem).toContain("-----BEGIN CERTIFICATE-----");
    expect(result.caPem).not.toMatch(/PRIVATE KEY/);
  });
});
