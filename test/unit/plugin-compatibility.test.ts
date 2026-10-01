import { describe, expect, it } from "vitest";

import packageMetadata from "../../package.json";
import type { PluginCompatibility } from "../../src/plugins/contracts";
import { STREAMSKOPE_RELEASE } from "../../src/plugins/host-release";
import {
  compareDesktopReleases,
  comparePluginVersions,
  formatPluginVersion,
  isPluginCompatibleWithHost,
  isSupportedPluginApiVersion,
  isTargetVersionCompatible,
  parsePluginManifest,
} from "../../src/plugins/validation";

const compatibility: PluginCompatibility = {
  streamskope: { minimum: "v0.1.0+build.5" },
  target: { system: "eda", minimum: "26.8.2", maximum: "27.4.1" },
};
const manifest = {
  id: "example.capture",
  name: "Example capture",
  version: "v0.1.0+build.5--eda-26.8.2-27.4.1--r1",
  apiVersion: 3,
  compatibility,
  revision: 1,
  backend: "backend.cjs",
  renderer: "renderer.js",
};
const resource = { path: "nsp-capture.workflow.yaml", sha256: "a".repeat(64) };

describe("plugin compatibility declarations", () => {
  it("round-trips the canonical host, target interval, revision and resource declarations", () => {
    expect(formatPluginVersion(compatibility, 1)).toBe(manifest.version);
    expect(parsePluginManifest(manifest)).toEqual(manifest);
    expect(parsePluginManifest({ ...manifest, resources: [resource] }).resources).toEqual([
      resource,
    ]);
    expect(isSupportedPluginApiVersion(2)).toBe(true);
    expect(isSupportedPluginApiVersion(3)).toBe(true);
    expect(isSupportedPluginApiVersion(4)).toBe(false);
    expect(isSupportedPluginApiVersion("3")).toBe(false);
  });

  it.each([
    { version: "1.0.0" },
    { version: "v0.1.0+build.5--eda-26.8.2-27.4.1--r2" },
    { revision: 0 },
    { revision: 1.5 },
    { revision: Number.MAX_SAFE_INTEGER + 1 },
    { revision: "1" },
    { revision: undefined },
    { compatibility: undefined },
    { targetEdaVersion: "26.8.2" },
  ])("rejects incomplete or contradictory API 3 metadata: %j", (patch) => {
    expect(() => parsePluginManifest({ ...manifest, ...patch })).toThrow();
  });

  it.each([
    { streamskope: { minimum: "0.1.0" } },
    { streamskope: { minimum: "v0.1.0+build.05" } },
    { streamskope: { minimum: "v0.1.0+build.0" } },
    { streamskope: { minimum: "v0.1.0", maximum: "v1.0.0" } },
    { target: { system: "eda", minimum: "26.8.x", maximum: "27.4.1" } },
    { target: { system: "eda", minimum: "26.8", maximum: "27.4.1" } },
    { target: { system: "eda", minimum: "26.8.2", maximum: "26.8.1" } },
    { target: { system: "../eda", minimum: "26.8.2", maximum: "27.4.1" } },
    { target: { system: "EDA", minimum: "26.8.2", maximum: "27.4.1" } },
    { target: { system: "eda", minimum: "26.8.2+build.1", maximum: "27.4.1" } },
    { target: { system: "eda", minimum: "26.8.2", maximum: "27.4.1", inferred: true } },
    { arbitrary: true },
  ])("rejects unsafe or unbounded compatibility declarations: %j", (patch) => {
    expect(() =>
      parsePluginManifest({ ...manifest, compatibility: { ...compatibility, ...patch } }),
    ).toThrow();
  });

  it("enforces inclusive target boundaries without assuming unqualified releases", () => {
    const parsed = parsePluginManifest(manifest);
    for (const version of ["26.8.2", "26.8.10", "26.10.0", "27.4.1"]) {
      expect(isTargetVersionCompatible(parsed, version)).toBe(true);
    }
    for (const version of [
      "26.8.1",
      "27.4.2",
      "27.10.0",
      "26.8.x",
      "26.8.02",
      "26.8.2\n",
      "26.8.2+build.1",
      "unknown",
    ]) {
      expect(isTargetVersionCompatible(parsed, version)).toBe(false);
    }
  });

  it("orders desktop builds numerically and applies the minimum host inclusively", () => {
    expect(compareDesktopReleases("v0.1.0+build.10", "v0.1.0+build.2")).toBeGreaterThan(0);
    expect(compareDesktopReleases("v0.1.1", "v0.1.0+build.99")).toBeGreaterThan(0);
    expect(compareDesktopReleases("v0.1.0", "v0.1.0+build.1")).toBeLessThan(0);
    expect(() => compareDesktopReleases("v0.1.0", "v0.1.0+build.0")).toThrow(/release/u);
    expect(() =>
      comparePluginVersions("v0.1.0+build.0--eda-26.8.2-27.4.1--r1", manifest.version),
    ).toThrow();
    const parsed = parsePluginManifest(manifest);
    expect(isPluginCompatibleWithHost(parsed, "v0.1.0+build.4")).toBe(false);
    expect(isPluginCompatibleWithHost(parsed, "v0.1.0+build.5")).toBe(true);
    expect(isPluginCompatibleWithHost(parsed, "v0.1.0+build.10")).toBe(true);
    expect(isPluginCompatibleWithHost(parsed, "v0.1.1")).toBe(true);
    expect(() => compareDesktopReleases("v0.1.0+build.x", "v0.1.0")).toThrow(/release/u);
    expect(() => compareDesktopReleases("v0.1.0+build.5\n", "v0.1.0")).toThrow(/release/u);
    expect(() => comparePluginVersions("26.8.2\n", "26.8.2")).toThrow(/semantic/u);
    expect(() => comparePluginVersions(`${manifest.version}\n`, manifest.version)).toThrow();
    expect(STREAMSKOPE_RELEASE).toBe(packageMetadata.streamskopeRelease);
    expect(STREAMSKOPE_RELEASE.split("+")[0]).toBe(`v${packageMetadata.version}`);
  });

  it("orders new packages by monotonically increasing revision instead of host or target versions", () => {
    const older = formatPluginVersion(compatibility, 2);
    const newer = formatPluginVersion(compatibility, 10);
    expect(comparePluginVersions(newer, older)).toBeGreaterThan(0);
    expect(comparePluginVersions(older, newer)).toBeLessThan(0);
    expect(comparePluginVersions(newer, newer)).toBe(0);
    const changed = { ...compatibility, streamskope: { minimum: "v0.1.0+build.10" } };
    expect(comparePluginVersions(formatPluginVersion(changed, 11), newer)).toBeGreaterThan(0);
    expect(() => comparePluginVersions(formatPluginVersion(changed, 10), newer)).toThrow(
      /conflicting/u,
    );
    const otherSystem = { ...compatibility, target: { ...compatibility.target, system: "nsp" } };
    expect(() => comparePluginVersions(formatPluginVersion(otherSystem, 11), newer)).toThrow(
      /different target/u,
    );
    expect(comparePluginVersions(newer, "26.8.999")).toBeGreaterThan(0);
    expect(comparePluginVersions("999.0.0", newer)).toBeLessThan(0);
  });

  it("retains strict legacy API 2 parsing and target declarations", () => {
    const legacy = {
      id: "example.capture",
      name: "Legacy capture",
      version: "26.8.2",
      apiVersion: 2,
      backend: "backend.cjs",
      renderer: "renderer.js",
      targetEdaVersion: "26.8.2",
    };
    expect(parsePluginManifest(legacy)).toEqual(legacy);
    expect(isPluginCompatibleWithHost(parsePluginManifest(legacy), "v0.1.0+build.5")).toBe(true);
    expect(isTargetVersionCompatible(parsePluginManifest(legacy), "26.8.2")).toBe(true);
    expect(isTargetVersionCompatible(parsePluginManifest(legacy), "26.8.3")).toBe(false);
    expect(
      isTargetVersionCompatible(
        parsePluginManifest({ ...legacy, targetEdaVersion: undefined }),
        "26.8.2",
      ),
    ).toBe(false);
    expect(() => parsePluginManifest({ ...legacy, compatibility })).toThrow(/unsupported/u);
    expect(() => parsePluginManifest({ ...legacy, resources: [resource] })).toThrow(/unsupported/u);
  });

  it.each([
    "../nsp.yaml",
    "/nsp.yaml",
    "nsp/helper.yaml",
    "nsp\\helper.yaml",
    ".helper.yaml",
    "Nsp.yaml",
    "nsp..yaml",
    "helper.cjs",
    "renderer.js",
    "manifest.json",
    "nsp%2ejson",
    "helper.yaml\n",
  ])("rejects resource path %s", (path) => {
    expect(() => parsePluginManifest({ ...manifest, resources: [{ ...resource, path }] })).toThrow(
      /filenames/u,
    );
  });

  it("bounds and authenticates the resource index", () => {
    expect(() => parsePluginManifest({ ...manifest, resources: [resource, resource] })).toThrow(
      /unique/u,
    );
    expect(() =>
      parsePluginManifest({
        ...manifest,
        resources: Array.from({ length: 9 }, (_, index) => ({
          ...resource,
          path: `resource-${index}.yaml`,
        })),
      }),
    ).toThrow(/eight/u);
    expect(() =>
      parsePluginManifest({ ...manifest, resources: [{ ...resource, sha256: "a".repeat(63) }] }),
    ).toThrow(/SHA-256/u);
    expect(() =>
      parsePluginManifest({ ...manifest, resources: [{ ...resource, sha256: "A".repeat(64) }] }),
    ).toThrow(/SHA-256/u);
    expect(() =>
      parsePluginManifest({ ...manifest, resources: [{ ...resource, script: "run" }] }),
    ).toThrow(/unsupported/u);
  });
});
