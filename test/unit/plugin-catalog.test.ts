import { describe, expect, it, vi } from "vitest";

import type { PluginManifest } from "../../src/plugins/contracts";
import {
  OfficialPluginCatalog,
  PLUGIN_MANIFEST_ASSET,
  PLUGIN_PACKAGE_ASSET,
} from "../../src/platform/node/plugins/catalog";
import {
  encodePluginPackage,
  parsePluginPackage,
  pluginPackageSha256,
} from "../../src/platform/node/plugins/package";
import { OFFICIAL_PLUGINS, officialPluginAssets } from "../../src/platform/node/plugins/official";
import { formatPluginVersion } from "../../src/plugins/validation";

const manifest: PluginManifest = {
  id: "streamskope.eda",
  name: "EDA Capture",
  version: "26.8.2",
  targetEdaVersion: "26.8.2",
  apiVersion: 2,
  backend: "backend.cjs",
  renderer: "renderer.js",
};
const bytes = encodePluginPackage(
  manifest,
  new Map([
    ["backend.cjs", Buffer.from("exports.activate=()=>({});")],
    ["renderer.js", Buffer.from("export default {};")],
  ]),
);
const manifestBytes = Buffer.from(JSON.stringify(manifest));

const semanticManifest: PluginManifest = {
  id: manifest.id,
  name: manifest.name,
  apiVersion: 4,
  version: "0.1.0",
  compatibility: {
    streamskope: { minimum: "0.2.0", maximumExclusive: "0.3.0" },
    target: { system: "eda", minimum: "26.8.2", maximum: "26.8.2" },
  },
  backend: "backend.cjs",
  renderer: "renderer.js",
};

function versionedCatalog(
  entries: readonly PluginManifest[],
  hostRelease = "v0.2.0",
  altered = false,
): OfficialPluginCatalog {
  const downloads = new Map<string, Uint8Array>();
  const releases = entries.map((entry, index) => {
    const names =
      entry.apiVersion === 2
        ? OFFICIAL_PLUGINS[0]
        : officialPluginAssets(OFFICIAL_PLUGINS[0], entry.version);
    return {
      ...release(),
      // GitHub's prerelease flag is separate from the plugin version's release channel.
      prerelease: true,
      tag_name: index === 0 ? "v0.2.0" : `plugins/eda/v${entry.version}`,
      published_at: new Date(Date.UTC(2026, 9, index + 1)).toISOString(),
      assets: [
        Buffer.from(JSON.stringify(entry)),
        encodePluginPackage(
          entry,
          new Map([
            [
              "backend.cjs",
              Buffer.from(`exports.activate=()=>({});${altered && index === 1 ? "//changed" : ""}`),
            ],
            ["renderer.js", Buffer.from("export default {};")],
          ]),
        ),
      ].map((content, offset) => {
        const id = index * 2 + offset + 1;
        downloads.set(`/assets/${id}`, content);
        return {
          id,
          name: offset === 0 ? names.manifestAsset : names.packageAsset,
          size: content.byteLength,
          state: "uploaded",
          digest: `sha256:${pluginPackageSha256(content)}`,
        };
      }),
    };
  });
  return new OfficialPluginCatalog(
    vi.fn<typeof fetch>((input) => {
      const url = requestUrl(input);
      if (url.endsWith("/releases?per_page=100")) return Promise.resolve(Response.json(releases));
      const content = downloads.get(/\/assets\/\d+$/u.exec(url)![0]);
      return Promise.resolve(new Response(Buffer.from(content!)));
    }),
    hostRelease,
  );
}

function release(): {
  draft: boolean;
  tag_name: string;
  published_at: string;
  assets: { id: number; name: string; size: number; state: string; digest: string }[];
} {
  return {
    draft: false,
    tag_name: "plugins/eda/v26.8.2",
    published_at: "2026-09-30T00:00:00Z",
    assets: [
      {
        id: 1,
        name: PLUGIN_MANIFEST_ASSET,
        size: manifestBytes.byteLength,
        state: "uploaded",
        digest: `sha256:${pluginPackageSha256(manifestBytes)}`,
      },
      {
        id: 2,
        name: PLUGIN_PACKAGE_ASSET,
        size: bytes.byteLength,
        state: "uploaded",
        digest: `sha256:${pluginPackageSha256(bytes)}`,
      },
    ],
  };
}

function requestUrl(input: Parameters<typeof fetch>[0]): string {
  return typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
}

function fixture(
  metadata: unknown = [release()],
  download: Uint8Array = bytes,
): { catalog: OfficialPluginCatalog; fetcher: ReturnType<typeof vi.fn<typeof fetch>> } {
  const fetcher = vi.fn<typeof fetch>((input) => {
    const url = requestUrl(input);
    if (url.endsWith("/releases?per_page=100")) return Promise.resolve(Response.json(metadata));
    if (url.endsWith("/assets/1")) return Promise.resolve(new Response(manifestBytes));
    if (url.endsWith("/assets/2")) return Promise.resolve(new Response(Buffer.from(download)));
    throw new Error(`Unexpected URL: ${url}`);
  });
  return { catalog: new OfficialPluginCatalog(fetcher, "v0.2.0"), fetcher };
}

function multipleReleases(
  entries: readonly {
    readonly version: string;
    readonly tag: string;
    readonly published: string;
    readonly apiVersion?: number;
    readonly plugin?: "eda" | "nsp";
  }[],
): {
  catalog: OfficialPluginCatalog;
  fetcher: ReturnType<typeof vi.fn<typeof fetch>>;
  metadata: ReturnType<typeof release>[];
} {
  const downloads = new Map<string, Uint8Array>();
  const metadata = entries.map((entry, index) => {
    const descriptor = OFFICIAL_PLUGINS.find(
      (candidate) => candidate.directory === (entry.plugin ?? "eda"),
    )!;
    const entryManifest: PluginManifest = {
      ...manifest,
      id: descriptor.id,
      name: entry.plugin === "nsp" ? "NSP Capture" : manifest.name,
      version: entry.version,
    };
    const manifestAsset = Buffer.from(
      JSON.stringify({ ...entryManifest, apiVersion: entry.apiVersion ?? 2 }),
    );
    const packageAsset = encodePluginPackage(
      entryManifest,
      new Map([
        ["backend.cjs", Buffer.from("exports.activate=()=>({});")],
        ["renderer.js", Buffer.from("export default {};")],
      ]),
    );
    const item = { ...release(), tag_name: entry.tag, published_at: entry.published };
    item.assets = [manifestAsset, packageAsset].map((content, offset) => {
      const id = 2 * index + offset + 1;
      downloads.set(`/assets/${id}`, content);
      return {
        id,
        name: offset === 0 ? descriptor.manifestAsset : descriptor.packageAsset,
        size: content.byteLength,
        state: "uploaded",
        digest: `sha256:${pluginPackageSha256(content)}`,
      };
    });
    return item;
  });
  const fetcher = vi.fn<typeof fetch>((input) => {
    const url = requestUrl(input);
    if (url.endsWith("/releases?per_page=100")) return Promise.resolve(Response.json(metadata));
    const assetPath = /\/assets\/\d+$/u.exec(url)?.[0];
    const content = assetPath === undefined ? undefined : downloads.get(assetPath);
    if (!content) throw new Error(`Unexpected URL: ${url}`);
    return Promise.resolve(new Response(Buffer.from(content)));
  });
  return { catalog: new OfficialPluginCatalog(fetcher, "v0.2.0"), fetcher, metadata };
}

describe("official plugin downloads", () => {
  it("keeps development builds separate from published plugin compatibility", async () => {
    const development = { ...semanticManifest, version: "0.0.0-dev.1790928000000" };
    const update = { ...development, version: "0.0.0-dev.1790928000001" };
    const entries = [semanticManifest, development, update, manifest];
    expect(await versionedCatalog(entries, "v0.0.0-dev").list()).toMatchObject([
      { manifest: update },
    ]);
    expect(await versionedCatalog(entries, "v0.2.0").list()).toMatchObject([
      { manifest: semanticManifest },
    ]);
    expect(await versionedCatalog([semanticManifest, manifest], "v0.0.0-dev").list()).toEqual([]);
  });

  it("selects independently versioned API 4 updates over retained API 2/3 packages and enforces host bounds", async () => {
    const compatibility = {
      streamskope: { minimum: "v0.1.0+build.1" },
      target: semanticManifest.compatibility!.target,
    };
    const legacy = {
      ...semanticManifest,
      apiVersion: 3 as const,
      compatibility,
      revision: 99,
      version: formatPluginVersion(compatibility, 99),
    };
    const entries = [
      semanticManifest,
      { ...semanticManifest, version: "0.1.10" },
      legacy,
      manifest,
      { ...semanticManifest, version: "0.1.2" },
    ];
    expect(officialPluginAssets(OFFICIAL_PLUGINS[0], "0.1.0").packageAsset).toBe(
      "streamskope-eda-v0.1.0.skope-plugin",
    );
    const catalog = versionedCatalog(entries);
    expect(await catalog.list()).toMatchObject([
      { manifest: { apiVersion: 4, version: "0.1.10" } },
    ]);
    expect(parsePluginPackage((await catalog.download(manifest.id)).bytes).manifest.version).toBe(
      "0.1.10",
    );
    expect(await versionedCatalog(entries, "v0.1.0+build.1").list()).toMatchObject([
      { manifest: legacy },
    ]);
    expect(await versionedCatalog([semanticManifest], "v0.3.0").list()).toEqual([]);
  });

  it("keeps prerelease plugin updates out of stable hosts while preview hosts can select them within declared bounds", async () => {
    const qualified = {
      ...semanticManifest,
      compatibility: {
        ...semanticManifest.compatibility!,
        streamskope: { minimum: "0.2.0-rc.1", maximumExclusive: "0.3.0" },
      },
    };
    const entries = [
      qualified,
      { ...qualified, version: "0.2.0-rc.1" },
      { ...qualified, version: "0.2.0-rc.10" },
      { ...qualified, version: "0.2.0-rc.2" },
    ];
    expect(await versionedCatalog(entries).list()).toMatchObject([{ manifest: qualified }]);
    expect(await versionedCatalog(entries, "v0.2.0-rc.2").list()).toMatchObject([
      { manifest: { version: "0.2.0-rc.10" } },
    ]);
    for (const host of ["v0.2.0-rc.0", "v0.2.1-rc.1", "v0.3.0-rc.1"]) {
      expect(await versionedCatalog(entries, host).list()).toEqual([]);
    }
  });

  it("rejects republished API 4 content but accepts identical package copies across desktop/plugin releases", async () => {
    expect(await versionedCatalog([semanticManifest, semanticManifest]).list()).toMatchObject([
      { manifest: semanticManifest },
    ]);
    await expect(
      versionedCatalog([semanticManifest, semanticManifest], "v0.2.0", true).list(),
    ).rejects.toThrow("conflicting package content");
    const incompatible = {
      ...semanticManifest,
      compatibility: {
        ...semanticManifest.compatibility!,
        streamskope: { minimum: "0.3.0", maximumExclusive: "0.4.0" },
      },
    };
    await expect(versionedCatalog([semanticManifest, incompatible]).list()).rejects.toThrow(
      "conflicting package content",
    );
  });

  it("selects versioned packages by revision only after checking the exact minimum desktop build", async () => {
    const manifests: PluginManifest[] = [
      manifest,
      ...[5, 10].map((build, index) => {
        const compatibility = {
          streamskope: { minimum: `v0.1.0+build.${build}` },
          target: { system: "eda", minimum: "26.8.2", maximum: "26.8.2" },
        };
        return {
          id: manifest.id,
          name: manifest.name,
          backend: "backend.cjs" as const,
          renderer: "renderer.js" as const,
          apiVersion: 3 as const,
          compatibility,
          revision: index + 1,
          version: formatPluginVersion(compatibility, index + 1),
        };
      }),
    ];
    const downloads = new Map<string, Uint8Array>();
    const releases = manifests.map((entry, index) => {
      const names =
        entry.apiVersion === 2
          ? OFFICIAL_PLUGINS[0]
          : officialPluginAssets(OFFICIAL_PLUGINS[0], entry.version);
      const contents = [
        Buffer.from(JSON.stringify(entry)),
        encodePluginPackage(
          entry,
          new Map([
            ["backend.cjs", Buffer.from("exports.activate=()=>({});")],
            ["renderer.js", Buffer.from("export default {};")],
          ]),
        ),
      ];
      return {
        ...release(),
        tag_name: `v0.1.0+build.${index + 4}`,
        published_at: new Date(Date.UTC(2026, 9, index + 1)).toISOString(),
        assets: contents.map((content, offset) => {
          const id = index * 2 + offset + 1;
          downloads.set(`/assets/${id}`, content);
          return {
            id,
            name: offset === 0 ? names.manifestAsset : names.packageAsset,
            size: content.byteLength,
            state: "uploaded",
            digest: `sha256:${pluginPackageSha256(content)}`,
          };
        }),
      };
    });
    const fetcher = vi.fn<typeof fetch>((input) => {
      const url = requestUrl(input);
      if (url.endsWith("/releases?per_page=100")) return Promise.resolve(Response.json(releases));
      const content = downloads.get(/\/assets\/\d+$/u.exec(url)![0]);
      return Promise.resolve(new Response(Buffer.from(content!)));
    });
    for (const [build, selected] of [
      [4, 0],
      [5, 1],
      [10, 2],
    ] as const) {
      const catalog = new OfficialPluginCatalog(fetcher, `v0.1.0+build.${build}`);
      expect(await catalog.list()).toMatchObject([{ manifest: manifests[selected] }]);
      const downloaded = await catalog.download(manifest.id);
      expect(parsePluginPackage(downloaded.bytes).manifest.version).toBe(
        manifests[selected]!.version,
      );
    }
    // A digest-verified manifest still cannot advertise different requirements in its filename.
    for (const asset of releases[2]!.assets) asset.name = asset.name.replace("--r2", "--r3");
    await expect(new OfficialPluginCatalog(fetcher, "v0.1.0+build.10").list()).rejects.toThrow(
      "filename",
    );
  });

  it.each([false, true])(
    "enforces immutable API 3 content across releases (changed bytes: %s)",
    async (changed) => {
      const compatibility = {
        streamskope: { minimum: "v0.1.0+build.5" },
        target: { system: "eda", minimum: "26.8.2", maximum: "26.8.2" },
      };
      const current: PluginManifest = {
        id: manifest.id,
        name: manifest.name,
        apiVersion: 3,
        backend: "backend.cjs",
        renderer: "renderer.js",
        compatibility,
        revision: 1,
        version: formatPluginVersion(compatibility, 1),
      };
      const names = officialPluginAssets(OFFICIAL_PLUGINS[0], current.version);
      const downloads = new Map<string, Uint8Array>();
      const releases = [false, changed].map((altered, index) => ({
        ...release(),
        tag_name: `v0.1.0+build.${index + 5}`,
        published_at: new Date(Date.UTC(2026, 9, index + 1)).toISOString(),
        assets: [
          Buffer.from(JSON.stringify(current)),
          encodePluginPackage(
            current,
            new Map([
              [
                "backend.cjs",
                Buffer.from(`exports.activate=()=>({});${altered ? "// changed" : ""}`),
              ],
              ["renderer.js", Buffer.from("export default {};")],
            ]),
          ),
        ].map((content, offset) => {
          const id = index * 2 + offset + 1;
          downloads.set(`/assets/${id}`, content);
          return {
            id,
            name: offset === 0 ? names.manifestAsset : names.packageAsset,
            size: content.byteLength,
            state: "uploaded",
            digest: `sha256:${pluginPackageSha256(content)}`,
          };
        }),
      }));
      const fetcher = vi.fn<typeof fetch>((input) => {
        const url = requestUrl(input);
        if (url.endsWith("/releases?per_page=100")) return Promise.resolve(Response.json(releases));
        const content = downloads.get(/\/assets\/\d+$/u.exec(url)![0]);
        return Promise.resolve(new Response(Buffer.from(content!)));
      });
      const catalog = new OfficialPluginCatalog(fetcher, compatibility.streamskope.minimum);
      if (changed) {
        await expect(catalog.list()).rejects.toThrow("conflicting package content");
        await expect(catalog.download(manifest.id)).rejects.toThrow("conflicting package content");
      } else {
        expect(await catalog.list()).toMatchObject([{ manifest: current }]);
        expect(parsePluginPackage((await catalog.download(manifest.id)).bytes).manifest).toEqual(
          current,
        );
      }
    },
  );

  it("lists verified manifest metadata and downloads only a digest-matched package", async () => {
    const { catalog, fetcher } = fixture();
    expect(await catalog.list()).toMatchObject([{ manifest, sha256: pluginPackageSha256(bytes) }]);
    expect(await catalog.download(manifest.id)).toEqual({
      bytes: Buffer.from(bytes),
      sha256: pluginPackageSha256(bytes),
    });
    expect(
      fetcher.mock.calls.every(([url]) =>
        requestUrl(url).startsWith("https://api.github.com/repos/asadarafat/streamskope/"),
      ),
    ).toBe(true);
    await expect(catalog.download("third-party.code")).rejects.toThrow("official catalog");
  });

  it("handles no published plugin and ignores drafts", async () => {
    const { catalog } = fixture([{ ...release(), draft: true }]);
    expect(await catalog.list()).toEqual([]);
    await expect(catalog.download(manifest.id)).rejects.toThrow("not been published");
  });

  it("rejects missing GitHub digests and changed downloaded package bytes", async () => {
    const metadata = release();
    metadata.assets[1]!.digest = "";
    await expect(fixture([metadata]).catalog.list()).rejects.toThrow("SHA256");
    await expect(
      fixture([release()], Buffer.from("tampered")).catalog.download(manifest.id),
    ).rejects.toThrow("SHA256");
  });

  it("skips a verified incompatible manifest API and rejects mismatched package metadata", async () => {
    const incompatible = Buffer.from(JSON.stringify({ ...manifest, apiVersion: 99 }));
    const metadata = release();
    metadata.assets[0]!.size = incompatible.length;
    metadata.assets[0]!.digest = `sha256:${pluginPackageSha256(incompatible)}`;
    const { fetcher } = fixture([metadata]);
    fetcher.mockImplementation((url) =>
      Promise.resolve(
        requestUrl(url).endsWith("/assets/1")
          ? new Response(incompatible)
          : Response.json([metadata]),
      ),
    );
    expect(await new OfficialPluginCatalog(fetcher).list()).toEqual([]);
    const other = encodePluginPackage(
      { ...manifest, version: "26.8.3" },
      new Map([
        ["backend.cjs", Buffer.from("code")],
        ["renderer.js", Buffer.from("code")],
      ]),
    );
    const changed = release();
    changed.assets[1]!.size = other.byteLength;
    changed.assets[1]!.digest = `sha256:${pluginPackageSha256(other)}`;
    await expect(fixture([changed], other).catalog.download(manifest.id)).rejects.toThrow(
      "published manifest",
    );
  });

  it("selects the highest compatible plugin version when a newer desktop release bundles an older plugin", async () => {
    const { catalog } = multipleReleases([
      { version: "26.8.2", tag: "v0.2.0", published: "2026-10-03T00:00:00Z" },
      { version: "26.8.10", tag: "plugins/eda/v26.8.10", published: "2026-10-01T00:00:00Z" },
      {
        version: "27.1.0",
        tag: "plugins/eda/v27.1.0",
        published: "2026-10-04T00:00:00Z",
        apiVersion: 99,
      },
    ]);
    expect(await catalog.list()).toMatchObject([
      { manifest: { version: "26.8.10", apiVersion: 2 } },
    ]);
    const downloaded = await catalog.download(manifest.id);
    expect(JSON.parse(Buffer.from(downloaded.bytes).toString("utf8"))).toMatchObject({
      manifest: { version: "26.8.10" },
    });
  });

  it("fails closed on invalid asset digests even when another compatible candidate is available", async () => {
    const { catalog, metadata } = multipleReleases([
      { version: "26.8.2", tag: "v0.2.0", published: "2026-10-03T00:00:00Z" },
      { version: "26.8.10", tag: "plugins/eda/v26.8.10", published: "2026-10-01T00:00:00Z" },
    ]);
    metadata[0]!.assets[0]!.digest = `sha256:${"0".repeat(64)}`;
    await expect(catalog.list()).rejects.toThrow("SHA256");
  });

  it("selects EDA and NSP independently across desktop and plugin releases", async () => {
    const { catalog, metadata } = multipleReleases([
      { version: "26.8.2", tag: "v0.2.0", published: "2026-10-03T00:00:00Z" },
      { plugin: "nsp", version: "0.1.0", tag: "v0.2.0", published: "2026-10-03T00:00:00Z" },
      { version: "26.8.10", tag: "plugins/eda/v26.8.10", published: "2026-10-01T00:00:00Z" },
      {
        plugin: "nsp",
        version: "0.2.0",
        tag: "plugins/nsp/v0.2.0",
        published: "2026-10-02T00:00:00Z",
      },
      {
        plugin: "nsp",
        version: "0.3.0",
        tag: "plugins/nsp/v0.3.0",
        published: "2026-10-04T00:00:00Z",
        apiVersion: 99,
      },
    ]);
    // A desktop release contains both plugin assets in the same GitHub release.
    metadata[0]!.assets.push(...metadata[1]!.assets);
    metadata.splice(1, 1);
    expect(await catalog.list()).toMatchObject([
      { manifest: { id: "streamskope.eda", version: "26.8.10" } },
      { manifest: { id: "streamskope.nsp", version: "0.2.0" } },
    ]);
    for (const [id, version] of [
      ["streamskope.eda", "26.8.10"],
      ["streamskope.nsp", "0.2.0"],
    ] as const) {
      const downloaded = await catalog.download(id);
      expect(JSON.parse(Buffer.from(downloaded.bytes).toString("utf8"))).toMatchObject({
        manifest: { id, version },
      });
    }
  });

  it("does not let another plugin's releases consume its candidate budget", async () => {
    const { catalog } = multipleReleases([
      { version: "26.8.2", tag: "plugins/eda/v26.8.2", published: "2026-09-01T00:00:00Z" },
      ...Array.from({ length: 20 }, (_, index) => ({
        plugin: "nsp" as const,
        version: `0.1.${index}`,
        tag: `plugins/nsp/v0.1.${index}`,
        published: new Date(Date.UTC(2026, 9, index + 1)).toISOString(),
      })),
    ]);
    expect(await catalog.list()).toMatchObject([
      { manifest: { id: "streamskope.eda", version: "26.8.2" } },
      { manifest: { id: "streamskope.nsp", version: "0.1.19" } },
    ]);
  });

  it("rejects a plugin identity hidden behind another official plugin's asset names", async () => {
    const { catalog, metadata } = multipleReleases([
      { plugin: "nsp", version: "0.1.0", tag: "v0.2.0", published: "2026-10-03T00:00:00Z" },
    ]);
    metadata[0]!.assets[0]!.name = PLUGIN_MANIFEST_ASSET;
    metadata[0]!.assets[1]!.name = PLUGIN_PACKAGE_ASSET;
    await expect(catalog.list()).rejects.toThrow("wrong identity");
  });

  it("bounds candidate manifest requests and shares one deadline across catalog and package downloads", async () => {
    const { catalog, fetcher, metadata } = multipleReleases(
      Array.from({ length: 30 }, (_, index) => ({
        version: `26.8.${index}`,
        tag: `plugins/eda/v26.8.${index}`,
        published: new Date(Date.UTC(2026, 9, index + 1)).toISOString(),
      })),
    );
    // An old malformed asset outside the 16-candidate window must not break discovery.
    metadata[0]!.assets[0]!.digest = "invalid-old-digest";
    expect(await catalog.list()).toMatchObject([{ manifest: { version: "26.8.29" } }]);
    expect(fetcher).toHaveBeenCalledTimes(17);
    expect(fetcher.mock.calls[0]?.[1]?.signal).toBeInstanceOf(AbortSignal);
    expect(new Set(fetcher.mock.calls.map(([, options]) => options?.signal)).size).toBe(1);
    fetcher.mockClear();
    await catalog.download(manifest.id);
    expect(fetcher).toHaveBeenCalledTimes(18);
    expect(new Set(fetcher.mock.calls.map(([, options]) => options?.signal)).size).toBe(1);
  });

  it("refuses untrusted redirects before connecting to the target", async () => {
    const fetcher = vi.fn<typeof fetch>(() =>
      Promise.resolve(
        new Response(null, {
          status: 302,
          headers: { location: "https://attacker.example/package" },
        }),
      ),
    );
    await expect(new OfficialPluginCatalog(fetcher).list()).rejects.toThrow("outside GitHub");
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("follows official release CDN redirects and bounds streamed responses", async () => {
    const fetcher = vi.fn<typeof fetch>();
    fetcher.mockResolvedValueOnce(
      new Response(null, {
        status: 302,
        headers: { location: "https://release-assets.githubusercontent.com/asset" },
      }),
    );
    fetcher.mockResolvedValueOnce(Response.json([]));
    expect(await new OfficialPluginCatalog(fetcher).list()).toEqual([]);
    const oversized = vi.fn<typeof fetch>(() =>
      Promise.resolve(new Response("[]", { headers: { "content-length": "9999999999" } })),
    );
    await expect(new OfficialPluginCatalog(oversized).list()).rejects.toThrow("size limit");
  });
});
