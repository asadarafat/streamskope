import { describe, expect, it } from "vitest";

import type { PluginManifest } from "../../src/plugins/contracts";
import {
  encodePluginPackage,
  MAX_PLUGIN_RESOURCE_BYTES,
  parsePluginPackage,
  pluginPackageSha256,
} from "../../src/platform/node/plugins/package";

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
