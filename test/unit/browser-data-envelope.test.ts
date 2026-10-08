import { describe, expect, it } from "vitest";

import { inspectKafkaProfileEnvelope } from "../../src/platform/node/kafka-profile-file-store";
import { inspectNatsProfileEnvelope } from "../../src/platform/node/nats-profile-file-store";
import { parsePluginNetworkSettingsDocument } from "../../src/platform/node/plugins/network-settings";
import {
  assertVaultValueEnvelope,
  decryptVaultValue,
  encryptVaultValue,
} from "../../src/platform/node/vault/vault-crypto";

const bytes = Buffer.from("opaque ciphertext");
const kafka = {
  version: 3,
  profiles: [
    {
      id: "fixture",
      name: "Fixture",
      brokers: ["broker.invalid:9092"],
      transport: "plaintext",
      createdAt: "2026-01-01",
      updatedAt: "2026-01-01",
      protectedValue: bytes.toString("base64"),
    },
  ],
};
const nats = {
  version: 1,
  profiles: [{ id: "fixture", revision: 1, protectedValue: bytes.toString("base64") }],
};

describe("production outer envelope inspection", () => {
  it("retains opaque bytes and outer version without attempting provider-specific decryption", () => {
    expect(
      inspectKafkaProfileEnvelope(Buffer.from(JSON.stringify(kafka))).profiles[0]?.protectedBytes,
    ).toEqual(bytes);
    expect(
      inspectNatsProfileEnvelope(Buffer.from(JSON.stringify(nats))).profiles[0]?.protectedBytes,
    ).toEqual(bytes);
    for (const version of [1, 2, 3])
      expect(
        inspectKafkaProfileEnvelope(Buffer.from(JSON.stringify({ version, profiles: [] }))).version,
      ).toBe(version);
  });
  it.each([inspectKafkaProfileEnvelope, inspectNatsProfileEnvelope])(
    "refuses unknown versions, malformed UTF8 and invalid canonical base64",
    (inspect) => {
      const value = inspect === inspectKafkaProfileEnvelope ? kafka : nats;
      for (const document of [
        { ...value, version: 99 },
        { ...value, profiles: [{ ...value.profiles[0], protectedValue: "YQ= " }] },
        { ...value, privateUnexpectedField: true },
      ])
        expect(() => inspect(Buffer.from(JSON.stringify(document)))).toThrow();
      expect(() => inspect(Buffer.from([0xff]))).toThrow();
    },
  );
  it("distinguishes structural browser protection from successful authentication", () => {
    const key = Buffer.alloc(32, 7);
    const context = Buffer.from("fixture");
    const encrypted = encryptVaultValue(key, context, "secret");
    encrypted[20] = encrypted[20]! ^ 1;
    expect(() => assertVaultValueEnvelope(encrypted)).not.toThrow();
    expect(() => decryptVaultValue(key, context, encrypted)).toThrow();
    expect(() => assertVaultValueEnvelope(Buffer.from("desktop opaque ciphertext"))).toThrow();
    expect(() => assertVaultValueEnvelope(Buffer.from("SKV1"))).toThrow();
  });
  it("parses durable proxy metadata without credentials use and rejects malformed protection", () => {
    const input = {
      formatVersion: 1,
      revision: 2,
      configuration: { mode: "custom", proxyUrl: "http://proxy.invalid:3128", offline: false },
      protectedCredentials: bytes.toString("base64"),
    };
    expect(parsePluginNetworkSettingsDocument(input)).toEqual(input);
    for (const change of [
      { formatVersion: 99 },
      { revision: -1 },
      { protectedCredentials: "" },
      { protectedCredentials: "YQ= " },
      { unknown: "secret" },
    ])
      expect(() => parsePluginNetworkSettingsDocument({ ...input, ...change })).toThrow();
  });
});
