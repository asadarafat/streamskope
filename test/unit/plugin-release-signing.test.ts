import { generateKeyPairSync } from "node:crypto";

import { expect, it } from "vitest";

import type { PluginManifest } from "../../src/plugins/contracts";
import { OFFICIAL_PLUGINS, officialPluginAssets } from "../../src/platform/node/plugins/official";
import {
  encodePluginPackage,
  parsePortablePluginPackage,
  pluginPackagePayloadBytes,
} from "../../src/platform/node/plugins/package";
import { createPortablePluginRelease } from "../../tools/package/plugin";
import { pluginPublisherFixture } from "../support/plugin-publisher-fixture";

function primaryPackage(component: "eda" | "nsp" = "eda"): Uint8Array {
  const manifest: PluginManifest = {
    id: `streamskope.${component}`,
    name: `${component.toUpperCase()} Capture`,
    version: "0.2.0",
    apiVersion: 4,
    backend: "backend.cjs",
    renderer: "renderer.js",
    compatibility: {
      streamskope: { minimum: "0.4.0", maximumExclusive: "0.8.0" },
      target: { system: component, minimum: "26.4.0", maximum: "26.8.2" },
    },
  };
  return encodePluginPackage(
    manifest,
    new Map([
      ["backend.cjs", Buffer.from("exports.activate = () => ({});")],
      ["renderer.js", Buffer.from("export default {};")],
    ]),
  );
}

it.each(["eda", "nsp"] as const)(
  "signs and re-verifies the identical %s primary payload",
  (component) => {
    const primary = primaryPackage(component);
    const fixture = pluginPublisherFixture();
    const portable = createPortablePluginRelease(primary, fixture.encodedKey, fixture.publishers);
    const verified = parsePortablePluginPackage(portable, undefined, fixture.publishers);
    expect(verified.manifest.id).toBe(`streamskope.${component}`);
    expect(verified.publisher).toMatchObject({ keyId: fixture.publishers[0]!.keyId });
    expect(Buffer.from(pluginPackagePayloadBytes(portable, fixture.publishers))).toEqual(
      Buffer.from(primary),
    );
    const names = officialPluginAssets(
      OFFICIAL_PLUGINS.find((entry) => entry.directory === component)!,
      "0.2.0",
    );
    expect(names.packageAsset).toBe(`streamskope-${component}-v0.2.0.skope-plugin`);
    expect(names.portablePackageAsset).toBe(
      `streamskope-${component}-portable-v0.2.0.skope-plugin`,
    );
    expect(names.manifestAsset).toBe(`streamskope-${component}-v0.2.0-plugin.json`);
  },
);

it.each([undefined, "", "not canonical base64", "A".repeat(16_385)])(
  "refuses a missing or malformed release signing key",
  (key) => {
    expect(() => createPortablePluginRelease(primaryPackage(), key)).toThrow(/signing|SIGNING/u);
  },
);

it("rejects non-Ed25519 private keys and keys outside the shipped publisher registry", () => {
  const { privateKey } = generateKeyPairSync("ec", { namedCurve: "prime256v1" });
  const wrongType = Buffer.from(privateKey.export({ format: "pem", type: "pkcs8" })).toString(
    "base64",
  );
  expect(() => createPortablePluginRelease(primaryPackage(), wrongType)).toThrow("Ed25519");
  const fixture = pluginPublisherFixture();
  expect(() => createPortablePluginRelease(primaryPackage(), fixture.encodedKey)).toThrow(
    "trusted publisher",
  );
  expect(() =>
    createPortablePluginRelease(
      primaryPackage(),
      fixture.encodedKey,
      pluginPublisherFixture().publishers,
    ),
  ).toThrow("trusted publisher");
});

it("refuses a matching signing key whose trusted publisher does not own the plugin identity", () => {
  const fixture = pluginPublisherFixture(["streamskope.nsp"]);
  expect(() =>
    createPortablePluginRelease(primaryPackage("eda"), fixture.encodedKey, fixture.publishers),
  ).toThrow("trusted publisher");
});
