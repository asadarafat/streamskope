import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { gzipSync } from "node:zlib";

import { Header } from "tar";
import { afterEach, expect, it } from "vitest";

import {
  browserReleaseNames,
  browserReleaseTopology,
  prepareBrowserReleaseAssets,
  validateBrowserReleaseAssets,
  validateBrowserImageArchive,
} from "../../tools/package/browser-release";
import { prepareUnsignedRelease } from "../../tools/package/release-policy";

const version = "0.10.0-rc.1";
const commit = "a".repeat(40);
const directories: string[] = [];
const execute = promisify(execFile);
type Mutation =
  | "tag"
  | "platform"
  | "source"
  | "digest"
  | "layer"
  | "truncated"
  | "unsafe"
  | "duplicate"
  | "link"
  | "not-gzip";

afterEach(async () => {
  for (const directory of directories.splice(0))
    await rm(directory, { recursive: true, force: true });
});

function tarEntry(path: string, content: Buffer, type: "File" | "SymbolicLink" = "File"): Buffer {
  const header = new Header({
    path,
    type,
    mode: 0o644,
    size: content.length,
    ...(type === "SymbolicLink" ? { linkpath: "config.json" } : {}),
  });
  header.encode();
  return Buffer.concat([
    header.block!,
    content,
    Buffer.alloc((512 - (content.length % 512)) % 512),
  ]);
}

async function fixture(
  mutation?: Mutation,
): Promise<{ root: string; staging: string; output: string; topology: string }> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-browser-release-"));
  directories.push(root);
  const staging = join(root, "staging");
  const output = join(root, "container-package");
  const topology = browserReleaseTopology(await readFile("streamskope.clab.yml", "utf8"), version);
  for (const arch of ["amd64", "arm64"] as const) {
    const directory = join(staging, `browser-linux-${arch}`);
    await mkdir(directory, { recursive: true });
    const change = arch === "arm64" ? mutation : undefined;
    const config = Buffer.from(
      JSON.stringify({
        os: "linux",
        architecture: change === "platform" ? "amd64" : arch,
        config: {
          Labels: {
            "org.opencontainers.image.version": version,
            "org.opencontainers.image.revision": change === "source" ? "b".repeat(40) : commit,
          },
        },
      }),
    );
    const digest = createHash("sha256").update(config).digest("hex");
    const configName = `blobs/sha256/${digest}`;
    const layerName = "blobs/sha256/layer";
    const manifest = Buffer.from(
      JSON.stringify([
        {
          Config: configName,
          RepoTags: [change === "tag" ? "streamskope:latest" : `streamskope:${version}`],
          Layers: [change === "layer" ? "missing-layer.tar" : layerName],
        },
      ]),
    );
    const entries = [
      tarEntry(configName, config),
      tarEntry(layerName, Buffer.from("layer bytes")),
      tarEntry("manifest.json", manifest),
    ];
    if (change === "unsafe") entries.push(tarEntry("../escape", Buffer.from("bad")));
    if (change === "duplicate") entries.push(tarEntry(configName, config));
    if (change === "link") entries.push(tarEntry("shortcut", Buffer.alloc(0), "SymbolicLink"));
    const tar = Buffer.concat([...entries, Buffer.alloc(1024)]);
    const gzip = gzipSync(tar);
    const archive = `StreamSkope-${version}-container-linux-${arch}.tar.gz`;
    await writeFile(
      join(directory, archive),
      change === "truncated"
        ? gzip.subarray(0, gzip.length - 8)
        : change === "not-gzip"
          ? tar
          : gzip,
    );
    await writeFile(
      join(directory, `container-linux-${arch}.json`),
      JSON.stringify({
        version,
        sourceRevision: commit,
        image: `streamskope:${version}`,
        imageId: `sha256:${change === "digest" ? "b".repeat(64) : digest}`,
        platform: `linux/${arch}`,
        archive,
      }),
    );
    await writeFile(join(directory, `streamskope-${version}.clab.yml`), topology);
  }
  return { root, staging, output, topology };
}

it("assembles both independently inspected Docker save archives, one topology and bounded metadata", async () => {
  const { staging, output, topology } = await fixture();
  await prepareBrowserReleaseAssets(staging, output, version, commit);
  expect(await validateBrowserReleaseAssets(output, version, commit)).toEqual(
    browserReleaseNames(version),
  );
  const manifest = JSON.parse(
    await readFile(join(output, `streamskope-${version}-container.json`), "utf8"),
  ) as {
    format: string;
    archives: { platform: string; sha256: string; bytes: number; archive: string }[];
  };
  expect(manifest.format).toBe("docker-save-gzip");
  expect(manifest.archives.map((record) => record.platform)).toEqual([
    "linux/amd64",
    "linux/arm64",
  ]);
  for (const record of manifest.archives) {
    const bytes = await readFile(join(output, record.archive));
    expect(record.sha256).toBe(createHash("sha256").update(bytes).digest("hex"));
    expect(record.bytes).toBe(bytes.length);
  }
  expect(await readFile(join(output, `streamskope-${version}.clab.yml`), "utf8")).toBe(topology);
  await expect(prepareBrowserReleaseAssets(staging, output, version, commit)).rejects.toThrow(
    /already exists/u,
  );
});

it("qualifies one actual native archive locally while retaining the development publication guard", async () => {
  const { staging } = await fixture();
  const directory = join(staging, "browser-linux-arm64");
  const metadata: unknown = JSON.parse(
    await readFile(join(directory, "container-linux-arm64.json"), "utf8"),
  );
  const archive = join(directory, `StreamSkope-${version}-container-linux-arm64.tar.gz`);
  await expect(
    validateBrowserImageArchive(archive, metadata, version, commit, "arm64"),
  ).resolves.toMatchObject({ platform: "linux/arm64" });
  await expect(
    validateBrowserImageArchive(archive, metadata, "0.0.0-dev", commit, "arm64"),
  ).rejects.toThrow(/publication version/u);
});

it.each<Mutation>([
  "tag",
  "platform",
  "source",
  "digest",
  "layer",
  "truncated",
  "unsafe",
  "duplicate",
  "link",
  "not-gzip",
])("rejects actual %s archive corruption before creating public assets", async (mutation) => {
  const { staging, output } = await fixture(mutation);
  await expect(prepareBrowserReleaseAssets(staging, output, version, commit)).rejects.toThrow();
  await expect(
    readFile(join(output, `streamskope-${version}-container.json`)),
  ).rejects.toMatchObject({ code: "ENOENT" });
});

it.each(["missing-arch", "extra", "topology", "metadata", "symlink"])(
  "rejects %s native build staging",
  async (kind) => {
    const { staging, output } = await fixture();
    const arm = join(staging, "browser-linux-arm64");
    const topology = join(arm, `streamskope-${version}.clab.yml`);
    if (kind === "missing-arch") await rm(arm, { recursive: true });
    if (kind === "extra") await writeFile(join(arm, "unqualified.txt"), "extra");
    if (kind === "topology") await writeFile(topology, "privileged: true\n");
    if (kind === "metadata") await writeFile(join(arm, "container-linux-arm64.json"), "{}");
    if (kind === "symlink") {
      await rm(topology);
      await symlink(
        join(staging, "browser-linux-amd64", `streamskope-${version}.clab.yml`),
        topology,
      );
    }
    await expect(prepareBrowserReleaseAssets(staging, output, version, commit)).rejects.toThrow();
  },
);

it("retains exactly three installers and hashes all seven public delivery files", async () => {
  const { root, staging, output } = await fixture();
  await prepareBrowserReleaseAssets(staging, output, version, commit);
  const installers = join(root, "installers");
  await mkdir(installers);
  const native = [
    `StreamSkope-${version}-darwin-arm64.dmg`,
    `StreamSkope-${version}-linux-x64.AppImage`,
    `StreamSkope-${version}-win32-x64-Setup.exe`,
  ];
  for (const name of native) await writeFile(join(installers, name), "native");
  const notes = await prepareUnsignedRelease(installers, version, commit, `v${version}`, output);
  const checksums = await readFile(join(installers, "SHA256SUMS"), "utf8");
  expect(checksums.trim().split("\n")).toHaveLength(7);
  for (const name of [...native, ...browserReleaseNames(version)])
    expect(checksums).toContain(`  ${name}\n`);
  expect(notes).toContain("## Browser with Containerlab");
  expect(notes).toContain("gzip-compressed Docker save archive");
  expect(notes).toContain("No image registry or Apple Developer ID certificate is needed.");
  expect(notes).toContain("not notarized");
});

it("rejects changed final archive bytes before writing installer checksums", async () => {
  const { root, staging, output } = await fixture();
  await prepareBrowserReleaseAssets(staging, output, version, commit);
  await writeFile(join(output, `StreamSkope-${version}-container-linux-arm64.tar.gz`), "corrupted");
  const installers = join(root, "installers");
  await mkdir(installers);
  for (const suffix of ["darwin-arm64.dmg", "linux-x64.AppImage", "win32-x64-Setup.exe"])
    await writeFile(join(installers, `StreamSkope-${version}-${suffix}`), "native");
  await expect(
    prepareUnsignedRelease(installers, version, commit, `v${version}`, output),
  ).rejects.toThrow();
  await expect(readFile(join(installers, "SHA256SUMS"))).rejects.toMatchObject({ code: "ENOENT" });
});

it.each([true, false])(
  "aggregates native browser jobs through the package release command with changelog=%s",
  async (includeChangelog) => {
    const { root, staging } = await fixture();
    const installers = join(root, "installers");
    await mkdir(installers);
    for (const suffix of ["darwin-arm64.dmg", "linux-x64.AppImage", "win32-x64-Setup.exe"])
      await writeFile(join(installers, `StreamSkope-${version}-${suffix}`), "native");
    const source = join(root, "reviewed.md");
    const changelog = join(root, "changelog.md");
    const notes = join(root, "notes.md");
    await writeFile(
      source,
      `---\nrelease_version: ${version}\nrelease_tag: v${version}\n---\n# Reviewed release\n\nOperator notes.\n`,
    );
    const markdown = "## Changes\n\nQualified change.\n";
    await writeFile(changelog, markdown);
    await writeFile(
      `${changelog}.json`,
      JSON.stringify({
        schemaVersion: 1,
        component: "desktop",
        version,
        tag: `v${version}`,
        sourceSha: commit,
        markdownSha256: createHash("sha256").update(markdown).digest("hex"),
      }),
    );
    await execute(
      process.execPath,
      [
        "--import",
        "tsx",
        "tools/package.ts",
        "release",
        installers,
        version,
        commit,
        source,
        notes,
        `v${version}`,
        ...(includeChangelog ? [changelog] : []),
        "--containers",
        staging,
      ],
      { timeout: 15_000 },
    );
    const content = await readFile(notes, "utf8");
    expect(content).toContain("## Browser with Containerlab");
    if (includeChangelog) expect(content).toContain("Qualified change.");
    expect(
      (await readFile(join(installers, "SHA256SUMS"), "utf8")).trim().split("\n"),
    ).toHaveLength(7);
  },
);
