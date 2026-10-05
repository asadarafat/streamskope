import { generateKeyPairSync } from "node:crypto";

import { describe, expect, it } from "vitest";

import type { PluginManifest } from "../../src/plugins/contracts";
import {
  encodePluginPackage,
  MAX_PLUGIN_RESOURCE_BYTES,
  MAX_PLUGIN_ARCHIVE_BYTES,
  MAX_PLUGIN_PACKAGE_BYTES,
  parsePluginPackage,
  parsePortablePluginPackage,
  pluginPackagePayloadBytes,
  pluginPackageSha256,
  signPortablePluginPackage,
} from "../../src/platform/node/plugins/package";
import { TRUSTED_PLUGIN_PUBLISHERS } from "../../src/platform/node/plugins/publishers";
import { pluginPublisherFixture } from "../support/plugin-publisher-fixture";

const manifest: PluginManifest = {
  id: "streamskope.eda",
  name: "EDA Capture",
  version: "26.8.2",
  targetEdaVersion: "26.8.2",
  apiVersion: 2,
  backend: "backend.cjs",
  renderer: "renderer.js",
};

function envelope(): {
  formatVersion: number;
  manifest: Record<string, unknown>;
  files: { path: string; content: string }[];
} {
  return {
    formatVersion: 1,
    manifest: { ...manifest },
    files: [
      {
        path: "backend.cjs",
        content: Buffer.from("exports.activate = () => ({});").toString("base64"),
      },
      { path: "renderer.js", content: Buffer.from("export default {};").toString("base64") },
    ],
  };
}

function resourceEnvelope(): ReturnType<typeof envelope> {
  const value = envelope();
  const resource = Buffer.from("version: '2.0'\nfixture: {}\n");
  value.manifest = {
    ...manifest,
    targetEdaVersion: undefined,
    apiVersion: 3,
    version: "v0.1.0+build.5--eda-26.8.2-26.8.2--r1",
    revision: 1,
    compatibility: {
      streamskope: { minimum: "v0.1.0+build.5" },
      target: { system: "eda", minimum: "26.8.2", maximum: "26.8.2" },
    },
    resources: [{ path: "capture.workflow.yaml", sha256: pluginPackageSha256(resource) }],
  };
  delete value.manifest.targetEdaVersion;
  value.files.push({ path: "capture.workflow.yaml", content: resource.toString("base64") });
  return value;
}

describe("desktop plugin packages", () => {
  it("verifies portable Ed25519 provenance without changing its exact primary content identity", () => {
    const fixture = pluginPublisherFixture();
    const primary = Buffer.from(JSON.stringify(envelope()));
    const keyId = fixture.publishers[0]!.keyId;
    const portable = signPortablePluginPackage(
      primary,
      keyId,
      Buffer.from(fixture.encodedKey, "base64").toString(),
    );
    const parsed = parsePortablePluginPackage(
      portable,
      pluginPackageSha256(portable),
      fixture.publishers,
    );
    expect(parsed.manifest).toEqual(manifest);
    expect(parsed.publisher).toEqual({ keyId, name: "Test release publisher" });
    expect(parsed.sha256).toBe(pluginPackageSha256(portable));
    expect(parsed.contentSha256).toBe(pluginPackageSha256(primary));
    expect(parsed.sha256).not.toBe(parsed.contentSha256);
    expect(parsePluginPackage(primary).contentSha256).toBe(parsed.contentSha256);
    expect(parsePluginPackage(primary).publisher).toBeUndefined();
    expect(pluginPackagePayloadBytes(portable, fixture.publishers)).toEqual(primary);
    expect(pluginPackagePayloadBytes(primary)).toEqual(primary);
    expect(() => parsePortablePluginPackage(primary, undefined, fixture.publishers)).toThrow(
      /signed portable/u,
    );
    expect(() => parsePortablePluginPackage(portable)).toThrow(/not trusted/u);
  });

  it("binds key identity and exact payload bytes, including when another trusted ID uses the same public key", () => {
    const fixture = pluginPublisherFixture();
    const primary = Buffer.from(JSON.stringify(envelope()));
    const portable = signPortablePluginPackage(
      primary,
      fixture.publishers[0]!.keyId,
      Buffer.from(fixture.encodedKey, "base64").toString(),
    );
    const outer = JSON.parse(Buffer.from(portable).toString()) as Record<string, unknown>;
    const aliases = [
      ...fixture.publishers,
      { ...fixture.publishers[0]!, keyId: "same-public-key-alias" },
    ];
    expect(() =>
      parsePortablePluginPackage(
        Buffer.from(JSON.stringify({ ...outer, keyId: "same-public-key-alias" })),
        undefined,
        aliases,
      ),
    ).toThrow(/signature/u);
    const changed = {
      ...outer,
      payload: Buffer.from("not JSON executable content").toString("base64"),
    };
    expect(() =>
      parsePortablePluginPackage(
        Buffer.from(JSON.stringify(changed)),
        undefined,
        fixture.publishers,
      ),
    ).toThrow(/signature/u);
    const reserializedPrimary = Buffer.from(JSON.stringify(envelope(), null, 2));
    const wrapped = signPortablePluginPackage(
      reserializedPrimary,
      fixture.publishers[0]!.keyId,
      Buffer.from(fixture.encodedKey, "base64").toString(),
    );
    expect(
      parsePortablePluginPackage(wrapped, undefined, fixture.publishers).contentSha256,
    ).not.toBe(parsePluginPackage(primary).contentSha256);
  });

  it("restricts publisher permissions to trusted host IDs and never trusts package-supplied public keys", () => {
    const fixture = pluginPublisherFixture(["streamskope.nsp"]);
    const portable = signPortablePluginPackage(
      Buffer.from(JSON.stringify(envelope())),
      fixture.publishers[0]!.keyId,
      Buffer.from(fixture.encodedKey, "base64").toString(),
    );
    expect(() => parsePortablePluginPackage(portable, undefined, fixture.publishers)).toThrow(
      /not authorized/u,
    );
    const outer = JSON.parse(Buffer.from(portable).toString()) as Record<string, unknown>;
    expect(() =>
      parsePortablePluginPackage(
        Buffer.from(JSON.stringify({ ...outer, publicKey: fixture.publishers[0]!.publicKey })),
        undefined,
        fixture.publishers,
      ),
    ).toThrow(/fields/u);
    expect(Object.isFrozen(TRUSTED_PLUGIN_PUBLISHERS)).toBe(true);
    expect(
      TRUSTED_PLUGIN_PUBLISHERS.every(
        (publisher) => Object.isFrozen(publisher) && Object.isFrozen(publisher.pluginIds),
      ),
    ).toBe(true);
    expect(TRUSTED_PLUGIN_PUBLISHERS[0]?.pluginIds).toEqual(["streamskope.eda", "streamskope.nsp"]);
  });

  it("rejects malformed portable envelopes, noncanonical data, invalid keys and oversized archives", () => {
    const fixture = pluginPublisherFixture();
    const primary = Buffer.from(JSON.stringify(envelope()));
    const privateKey = Buffer.from(fixture.encodedKey, "base64").toString();
    const portable = signPortablePluginPackage(primary, fixture.publishers[0]!.keyId, privateKey);
    const outer = JSON.parse(Buffer.from(portable).toString()) as Record<string, unknown>;
    for (const overrides of [
      { payload: "" },
      { payload: "Y Q==" },
      { signature: "Y Q==" },
      { signature: Buffer.alloc(63).toString("base64") },
      { keyId: "../publisher" },
      { keyId: "unknown-publisher" },
      { formatVersion: 99 },
    ])
      expect(() =>
        parsePortablePluginPackage(
          Buffer.from(JSON.stringify({ ...outer, ...overrides })),
          undefined,
          fixture.publishers,
        ),
      ).toThrow();
    expect(() => parsePortablePluginPackage(portable, "0".repeat(64), fixture.publishers)).toThrow(
      /SHA256/u,
    );
    expect(() =>
      signPortablePluginPackage(portable, fixture.publishers[0]!.keyId, privateKey),
    ).toThrow();
    const rsa = generateKeyPairSync("rsa", { modulusLength: 1024 });
    expect(() =>
      signPortablePluginPackage(primary, fixture.publishers[0]!.keyId, rsa.privateKey),
    ).toThrow(/Ed25519/u);
    expect(() =>
      parsePortablePluginPackage(portable, undefined, [
        {
          ...fixture.publishers[0]!,
          publicKey: rsa.publicKey.export({ type: "spki", format: "pem" }).toString(),
        },
      ]),
    ).toThrow(/Ed25519/u);
    expect(() => parsePluginPackage(Buffer.alloc(MAX_PLUGIN_ARCHIVE_BYTES + 1))).toThrow(/size/u);
    expect(() =>
      signPortablePluginPackage(
        Buffer.alloc(MAX_PLUGIN_PACKAGE_BYTES + 1),
        fixture.publishers[0]!.keyId,
        privateKey,
      ),
    ).toThrow(/size/u);
  });

  it("verifies declared resource bytes while retaining format 1 legacy packages", () => {
    const source = resourceEnvelope();
    const parsed = parsePluginPackage(Buffer.from(JSON.stringify(source)));
    expect(parsed.files.get("capture.workflow.yaml")).toEqual(
      Buffer.from("version: '2.0'\nfixture: {}\n"),
    );
    expect(parsePluginPackage(Buffer.from(JSON.stringify(envelope()))).manifest).toEqual(manifest);
    expect(() =>
      encodePluginPackage({ ...parsed.manifest, version: "26.8.2" }, parsed.files),
    ).toThrow();
  });

  it.each([
    [
      "resource digest mismatch",
      (value: ReturnType<typeof envelope>): void => {
        value.files[2]!.content = Buffer.from("changed workflow").toString("base64");
      },
    ],
    [
      "missing resource",
      (value: ReturnType<typeof envelope>): void => {
        value.files.pop();
      },
    ],
    [
      "undeclared resource",
      (value: ReturnType<typeof envelope>): void => {
        delete value.manifest.resources;
      },
    ],
    [
      "resource traversal",
      (value: ReturnType<typeof envelope>): void => {
        value.manifest.resources = [{ path: "../capture.workflow.yaml", sha256: "a".repeat(64) }];
        value.files[2]!.path = "../capture.workflow.yaml";
      },
    ],
    [
      "duplicate resource",
      (value: ReturnType<typeof envelope>): void => {
        value.files[0]!.path = "capture.workflow.yaml";
      },
    ],
    [
      "oversized resource with a matching digest",
      (value: ReturnType<typeof envelope>): void => {
        const bytes = Buffer.alloc(MAX_PLUGIN_RESOURCE_BYTES + 1, "x");
        value.manifest.resources = [
          { path: "capture.workflow.yaml", sha256: pluginPackageSha256(bytes) },
        ];
        value.files[2]!.content = bytes.toString("base64");
      },
    ],
  ] as const)("rejects %s before writing package files", (_name, mutate) => {
    const value = resourceEnvelope();
    mutate(value);
    expect(() => parsePluginPackage(Buffer.from(JSON.stringify(value)))).toThrow();
  });

  it("encodes deterministic packages and verifies the full downloaded bytes", () => {
    const files = new Map([
      ["renderer.js", Buffer.from("export default {};")],
      ["backend.cjs", Buffer.from("exports.activate = () => ({});")],
    ]);
    const bytes = encodePluginPackage(manifest, files);
    expect(bytes).toEqual(encodePluginPackage(manifest, new Map([...files].reverse())));
    const parsed = parsePluginPackage(bytes, pluginPackageSha256(bytes));
    expect(parsed.manifest).toEqual(manifest);
    expect(parsed.files.get("backend.cjs")).toEqual(files.get("backend.cjs"));
    expect(() => parsePluginPackage(bytes, "0".repeat(64))).toThrow("SHA256");
    expect(() => parsePluginPackage(bytes, "not-a-digest")).toThrow("SHA256");
  });

  it.each([
    [
      "path traversal",
      (value: ReturnType<typeof envelope>): void => {
        value.files[0]!.path = "../backend.cjs";
      },
    ],
    [
      "absolute path",
      (value: ReturnType<typeof envelope>): void => {
        value.files[0]!.path = "/backend.cjs";
      },
    ],
    [
      "duplicate files",
      (value: ReturnType<typeof envelope>): void => {
        value.files[1]!.path = "backend.cjs";
      },
    ],
    [
      "extra files",
      (value: ReturnType<typeof envelope>): void => {
        value.files.push({ path: "installer.sh", content: "YWJj" });
      },
    ],
    [
      "incompatible API",
      (value: ReturnType<typeof envelope>): void => {
        value.manifest.apiVersion = 1;
      },
    ],
    [
      "incorrect entrypoint",
      (value: ReturnType<typeof envelope>): void => {
        value.manifest.backend = "elsewhere.cjs";
      },
    ],
    [
      "unknown package format",
      (value: ReturnType<typeof envelope>): void => {
        value.formatVersion = 2;
      },
    ],
    [
      "malformed base64",
      (value: ReturnType<typeof envelope>): void => {
        value.files[0]!.content = "%%%";
      },
    ],
    [
      "noncanonical base64",
      (value: ReturnType<typeof envelope>): void => {
        value.files[0]!.content = "Y Q==";
      },
    ],
    [
      "empty module",
      (value: ReturnType<typeof envelope>): void => {
        value.files[0]!.content = "";
      },
    ],
  ] as const)("rejects %s before any file is written", (_name, mutate) => {
    const value = envelope();
    mutate(value);
    expect(() => parsePluginPackage(Buffer.from(JSON.stringify(value)))).toThrow();
  });

  it("rejects undecodable text and unexpected archive metadata", () => {
    expect(() => parsePluginPackage(Buffer.from([0xff]))).toThrow();
    expect(() =>
      parsePluginPackage(Buffer.from(JSON.stringify({ ...envelope(), symlinks: [] }))),
    ).toThrow("fields");
  });
});
