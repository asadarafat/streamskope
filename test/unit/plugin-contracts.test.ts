import { describe, expect, it } from "vitest";

import {
  comparePluginVersions,
  parsePluginJson,
  parsePluginManifest,
  parsePluginProfileSource,
} from "../../src/plugins/validation";

const manifest = {
  id: "example.capture",
  name: "Example capture",
  version: "1.0.0",
  apiVersion: 2,
  backend: "backend.cjs",
  renderer: "renderer.js",
};

describe("desktop plugin boundary", () => {
  it("orders semantic versions numerically, handles prereleases, and ignores build metadata", () => {
    const ordered = [
      "1.0.0-alpha",
      "1.0.0-alpha.2",
      "1.0.0-alpha.10",
      "1.0.0-beta",
      "1.0.0",
      "1.0.1",
      "1.2.0",
      "1.10.0",
      "2.0.0",
    ];
    for (let index = 1; index < ordered.length; index += 1) {
      expect(comparePluginVersions(ordered[index - 1]!, ordered[index]!)).toBeLessThan(0);
      expect(comparePluginVersions(ordered[index]!, ordered[index - 1]!)).toBeGreaterThan(0);
    }
    expect(comparePluginVersions("26.8.2+desktop", "26.8.2+plugin")).toBe(0);
    expect(comparePluginVersions("1.0.0-1", "1.0.0-alpha")).toBeLessThan(0);
    expect(comparePluginVersions("1.0.0-alpha", "1.0.0-alpha.1")).toBeLessThan(0);
    expect(comparePluginVersions("9007199254740993.0.0", "9007199254740992.0.0")).toBeGreaterThan(
      0,
    );
    for (const invalid of ["v1.0.0", "1.0.0-01", "1.0.0-alpha..1", "1.0.0+build..1"]) {
      expect(() => comparePluginVersions(invalid, "1.0.0")).toThrow("semantic version");
    }
  });
  it("rejects incompatible packages before any executable entrypoint is accepted", () => {
    expect(parsePluginManifest(manifest)).toEqual(manifest);
    expect(() => parsePluginManifest({ ...manifest, apiVersion: 99 })).toThrow(/different.*API/u);
    expect(() => parsePluginManifest({ ...manifest, backend: "../outside.cjs" })).toThrow(
      /entrypoints/u,
    );
    expect(() => parsePluginManifest({ ...manifest, id: "../outside" })).toThrow(/identifier/u);
    expect(() => parsePluginManifest({ ...manifest, installScript: "run me" })).toThrow(
      /unsupported/u,
    );
  });

  it("retains bounded opaque profile metadata without requiring its plugin", () => {
    const source = {
      kind: "plugin",
      pluginId: "example.capture",
      version: 1,
      data: { session: "recoverable", topics: ["events"], nested: { keep: true } },
    };
    expect(parsePluginProfileSource(source)).toEqual(source);
    expect(() => parsePluginProfileSource({ ...source, data: ["not an object"] })).toThrow(
      /object/u,
    );
  });

  it("rejects executable, cyclic, prototype-polluting and excessive input", () => {
    expect(() => parsePluginJson({ callback: (): void => undefined })).toThrow(/JSON/u);
    const cyclic: { self?: unknown } = {};
    cyclic.self = cyclic;
    expect(() => parsePluginJson(cyclic)).toThrow(/acyclic/u);
    expect(() => parsePluginJson(JSON.parse('{"__proto__":{"polluted":true}}'))).toThrow(/key/u);
    expect(() => parsePluginJson({ text: "x".repeat(262_145) })).toThrow(/limit/u);
    expect(() => parsePluginJson({ number: Number.NaN })).toThrow(/JSON/u);
    let nested: unknown = {};
    for (let index = 0; index < 30; index += 1) nested = { nested };
    expect(() => parsePluginJson(nested)).toThrow(/structural/u);
  });
});
