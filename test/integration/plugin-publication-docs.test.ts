import { expect, it, vi, type Mock } from "vitest";

import type { PluginManifest } from "../../src/plugins/contracts";
import type { OfficialPluginEntry } from "../../src/platform/node/plugins/catalog";
import { OFFICIAL_PLUGINS, officialPluginAssets } from "../../src/platform/node/plugins/official";
import { encodePluginPackage, pluginPackageSha256 } from "../../src/platform/node/plugins/package";
import {
  verifyPluginPublications,
  capturePluginPublications,
} from "../../tools/docs/plugin-publications";
import { createPortablePluginRelease } from "../../tools/package/plugin";
import { pluginPublisherFixture } from "../support/plugin-publisher-fixture";

const repository = "https://github.com/asadarafat/streamskope/releases";
const api = "https://api.github.com/repos/asadarafat/streamskope/releases/assets";

interface PublicationFixture {
  readonly trust: ReturnType<typeof pluginPublisherFixture>;
  readonly manifest: PluginManifest;
  readonly files: Map<string, Buffer>;
  readonly primary: Uint8Array;
  readonly portable: Uint8Array;
  readonly downloads: Map<string, Uint8Array>;
  readonly catalog: {
    downloadPinned: Mock<
      (entry: OfficialPluginEntry) => Promise<{ bytes: Uint8Array; sha256: string }>
    >;
  };
  readonly entry: OfficialPluginEntry;
  readonly release: {
    tag_name: string;
    html_url: string;
    draft: boolean;
    published_at: string;
    assets: {
      id: number;
      name: string;
      digest: string;
      size: number;
      state: string;
      browser_download_url: string;
    }[];
  };
}

function fixture(component: "eda" | "nsp" = "eda"): PublicationFixture {
  const trust = pluginPublisherFixture();
  const manifest: PluginManifest = {
    id: `streamskope.${component}`,
    name: `${component.toUpperCase()} Capture`,
    version: "0.1.0",
    apiVersion: 4,
    backend: "backend.cjs",
    renderer: "renderer.js",
    compatibility: {
      streamskope: { minimum: "0.4.0", maximumExclusive: "1.0.0" },
      target: { system: component, minimum: "26.8.2", maximum: "26.8.2" },
    },
  };
  const files = new Map([
    ["backend.cjs", Buffer.from("exports.activate = () => ({});")],
    ["renderer.js", Buffer.from("export default {};")],
  ]);
  const primary = encodePluginPackage(manifest, files);
  const portable = createPortablePluginRelease(primary, trust.encodedKey, trust.publishers);
  const names = officialPluginAssets(
    OFFICIAL_PLUGINS.find((plugin) => plugin.directory === component)!,
    manifest.version,
  );
  const tag = `plugins/${component}/v0.1.0`;
  const assets = [
    {
      id: 1,
      name: names.packageAsset,
      digest: `sha256:${pluginPackageSha256(primary)}`,
      size: primary.byteLength,
      state: "uploaded",
      browser_download_url: `${repository}/download/${tag}/${names.packageAsset}`,
    },
    {
      id: 2,
      name: names.portablePackageAsset,
      digest: `sha256:${pluginPackageSha256(portable)}`,
      size: portable.byteLength,
      state: "uploaded",
      browser_download_url: `${repository}/download/${tag}/${names.portablePackageAsset}`,
    },
  ];
  const downloads = new Map([
    [`${api}/1`, primary],
    [`${api}/2`, portable],
  ]);
  const catalog = {
    downloadPinned: vi.fn((entry: OfficialPluginEntry) => {
      const bytes = downloads.get(entry.downloadUrl);
      if (!bytes) return Promise.reject(new Error("Missing fixture bytes"));
      return Promise.resolve({ bytes, sha256: pluginPackageSha256(bytes) });
    }),
  };
  const entry: OfficialPluginEntry = {
    manifest,
    sha256: pluginPackageSha256(primary),
    downloadUrl: `${api}/1`,
  };
  const release = {
    tag_name: tag,
    html_url: `${repository}/tag/${tag}`,
    draft: false,
    published_at: "2026-10-06T12:00:00Z",
    assets,
  };
  return { trust, manifest, files, primary, portable, downloads, catalog, entry, release };
}

it.each(["eda", "nsp"] as const)(
  "records only the verified %s signed portable and exact primary payload",
  async (component) => {
    const f = fixture(component);
    const [result] = await verifyPluginPublications(
      [f.entry],
      [f.release],
      f.catalog,
      f.trust.publishers,
    );
    expect(result).toMatchObject({
      id: f.manifest.id,
      version: "0.1.0",
      api: 4,
      release_url: f.release.html_url,
      compatibility: f.manifest.compatibility,
      portable: {
        url: f.release.assets[1]!.browser_download_url,
        sha256: pluginPackageSha256(f.portable),
        publisher: "Test release publisher",
        publisher_key_id: "ephemeral-release-fixture",
      },
    });
    expect(f.catalog.downloadPinned.mock.calls.map(([entry]) => entry.downloadUrl)).toEqual([
      `${api}/1`,
      `${api}/2`,
    ]);
  },
);

it("keeps a published catalog package distinct from absent portable delivery", async () => {
  const f = fixture();
  f.release.assets.pop();
  const [result] = await verifyPluginPublications(
    [f.entry],
    [f.release],
    f.catalog,
    f.trust.publishers,
  );
  expect(result!.portable).toBeNull();
  expect(result!.version).toBe("0.1.0");
  expect(f.catalog.downloadPinned).not.toHaveBeenCalled();
});

it("rejects renamed unsigned catalog bytes as a portable download", async () => {
  const f = fixture();
  f.downloads.set(`${api}/2`, f.primary);
  Object.assign(f.release.assets[1]!, {
    digest: `sha256:${pluginPackageSha256(f.primary)}`,
    size: f.primary.byteLength,
  });
  await expect(
    verifyPluginPublications([f.entry], [f.release], f.catalog, f.trust.publishers),
  ).rejects.toThrow("signed portable");
});

it("production trust rejects an unknown publisher even with a valid envelope and GitHub digest", async () => {
  const f = fixture();
  await expect(verifyPluginPublications([f.entry], [f.release], f.catalog)).rejects.toThrow(
    "publisher is not trusted",
  );
});

it("checks cryptographic authenticity independently of GitHub asset integrity", async () => {
  const f = fixture();
  const envelope = JSON.parse(Buffer.from(f.portable).toString("utf8")) as { signature: string };
  const signature = Buffer.from(envelope.signature, "base64");
  signature[0] = signature[0]! ^ 1;
  envelope.signature = signature.toString("base64");
  const changed = Buffer.from(JSON.stringify(envelope));
  f.downloads.set(`${api}/2`, changed);
  Object.assign(f.release.assets[1]!, {
    digest: `sha256:${pluginPackageSha256(changed)}`,
    size: changed.byteLength,
  });
  await expect(
    verifyPluginPublications([f.entry], [f.release], f.catalog, f.trust.publishers),
  ).rejects.toThrow("signature");
});

it.each(["payload", "manifest"] as const)(
  "rejects a legitimately signed portable with different %s",
  async (difference) => {
    const f = fixture();
    const changedFiles = new Map(f.files);
    if (difference === "payload")
      changedFiles.set("renderer.js", Buffer.from("export default {changed: true};"));
    const changedManifest =
      difference === "manifest" ? { ...f.manifest, version: "0.2.0" } : f.manifest;
    const changed = createPortablePluginRelease(
      encodePluginPackage(changedManifest, changedFiles),
      f.trust.encodedKey,
      f.trust.publishers,
    );
    f.downloads.set(`${api}/2`, changed);
    Object.assign(f.release.assets[1]!, {
      digest: `sha256:${pluginPackageSha256(changed)}`,
      size: changed.byteLength,
    });
    await expect(
      verifyPluginPublications([f.entry], [f.release], f.catalog, f.trust.publishers),
    ).rejects.toThrow("differs from its catalog");
  },
);

it.each([
  { size: 0 },
  { size: 1 },
  { state: "new" },
  { digest: "sha256:invalid" },
  { browser_download_url: "https://example.com/plugin.skope-plugin" },
])("fails qualification rather than concealing invalid portable metadata %j", async (change) => {
  const f = fixture();
  Object.assign(f.release.assets[1]!, change);
  await expect(
    verifyPluginPublications([f.entry], [f.release], f.catalog, f.trust.publishers),
  ).rejects.toThrow();
});

it("refuses ambiguous release or portable identities", async () => {
  const f = fixture();
  await expect(
    verifyPluginPublications([f.entry], [f.release, f.release], f.catalog, f.trust.publishers),
  ).rejects.toThrow("exactly one");
  f.release.assets.push({ ...f.release.assets[1]! });
  await expect(
    verifyPluginPublications([f.entry], [f.release], f.catalog, f.trust.publishers),
  ).rejects.toThrow("duplicate portable");
});

it("refuses draft publications and links to another release", async () => {
  const f = fixture();
  f.release.draft = true;
  await expect(
    verifyPluginPublications([f.entry], [f.release], f.catalog, f.trust.publishers),
  ).rejects.toThrow("not published");
  f.release.draft = false;
  f.release.html_url = `${repository}/tag/plugins/eda/v0.2.0`;
  await expect(
    verifyPluginPublications([f.entry], [f.release], f.catalog, f.trust.publishers),
  ).rejects.toThrow("outside its official");
});

it("network errors cannot become an empty publication snapshot", async () => {
  const fetcher: typeof fetch = () => Promise.resolve(new Response(null, { status: 503 }));
  await expect(capturePluginPublications("v0.9.1", fetcher)).rejects.toThrow("HTTP 503");
});
