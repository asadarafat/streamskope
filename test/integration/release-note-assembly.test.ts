import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

import { afterEach, expect, it } from "vitest";

import {
  preparePluginReleaseNotes,
  readReleaseChangelog,
  validatePluginReleaseAssets,
} from "../../tools/package/release";
import { parsePluginManifest } from "../../src/plugins/validation";
import { OFFICIAL_PLUGINS, officialPluginAssets } from "../../src/platform/node/plugins/official";
import { encodePluginPackage, parsePluginPackage } from "../../src/platform/node/plugins/package";
import type { TrustedPluginPublisher } from "../../src/platform/node/plugins/publishers";
import { createPortablePluginRelease } from "../../tools/package/plugin";
import { pluginPublisherFixture } from "../support/plugin-publisher-fixture";

const execute = promisify(execFile);
const directories: string[] = [];
const commit = "a".repeat(40);
const changes =
  "## Changes\n\n### Fixes\n\n- Recover a disconnected capture ([#12](https://github.com/example/project/pull/12)).\n";

afterEach(async () => {
  for (const directory of directories.splice(0))
    await rm(directory, { recursive: true, force: true });
});

async function fixture(component: "desktop" | "eda" | "nsp" = "eda"): Promise<{
  root: string;
  directory: string;
  changelog: string;
  reviewed: string;
  output: string;
  tag: string;
  evidence: Record<string, unknown>;
  publishers: readonly TrustedPluginPublisher[];
  encodedKey: string;
}> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-release-notes-"));
  directories.push(root);
  const directory = join(root, "assets");
  await mkdir(directory);
  const changelog = join(root, "changelog.md");
  const reviewed = join(root, "reviewed.md");
  const output = join(root, "notes.md");
  const tag = component === "desktop" ? "v0.2.0" : `plugins/${component}/v0.2.0`;
  const publisher = pluginPublisherFixture();
  const evidence = {
    schemaVersion: 1,
    repository: "example/project",
    component,
    version: "0.2.0",
    tag,
    sourceSha: commit,
    baseline: null,
    markdownSha256: createHash("sha256").update(changes).digest("hex"),
  };
  await writeFile(changelog, changes);
  await writeFile(`${changelog}.json`, JSON.stringify(evidence));
  await writeFile(
    reviewed,
    "## Upgrade\n\nBack up saved profiles.\n\n## Known limitations\n\nLive tests are unverified.\n",
  );
  if (component !== "desktop") {
    const source = parsePluginManifest({
      ...(JSON.parse(await readFile(`plugins/${component}/manifest.json`, "utf8")) as Record<
        string,
        unknown
      >),
      version: "0.2.0",
    });
    const assets = officialPluginAssets(
      OFFICIAL_PLUGINS.find((entry) => entry.directory === component)!,
      source.version,
    );
    const contents = new Map<string, Uint8Array>([
      ["backend.cjs", Buffer.from("exports.activate = () => ({});")],
      ["renderer.js", Buffer.from("export default {};")],
    ]);
    for (const resource of source.resources ?? []) {
      const bytes = await readFile(`plugins/${component}/resources/${resource.path}`);
      contents.set(resource.path, bytes);
      await writeFile(join(directory, `${assets.prefix}-${resource.path}`), bytes);
    }
    const primary = encodePluginPackage(source, contents);
    await writeFile(join(directory, assets.manifestAsset), JSON.stringify(source));
    await writeFile(join(directory, assets.packageAsset), primary);
    await writeFile(
      join(directory, assets.portablePackageAsset),
      createPortablePluginRelease(primary, publisher.encodedKey, publisher.publishers),
    );
  }
  return { root, directory, changelog, reviewed, output, tag, evidence, ...publisher };
}

it.each(["eda", "nsp"] as const)(
  "assembles %s guidance and compatibility only after verifying matching primary and signed portable artifacts",
  async (component) => {
    const files = await fixture(component);
    const args = [
      "--import",
      "tsx",
      "tools/package.ts",
      "release",
      "plugin",
      files.directory,
      component,
      "0.2.0",
      commit,
      files.reviewed,
      files.changelog,
      files.output,
    ];
    const notes = await preparePluginReleaseNotes(
      files.directory,
      component,
      "0.2.0",
      commit,
      files.reviewed,
      files.changelog,
      files.publishers,
    );
    expect(notes).toContain(`# ${component.toUpperCase()} Capture 0.2.0`);
    expect(notes).toContain("Back up saved profiles.");
    expect(notes).toContain("Live tests are unverified.");
    expect(notes).toContain("Requires StreamSkope >=0.9.0 and <0.10.0.");
    expect(notes).toContain(
      `Supports ${component.toUpperCase()} ${component === "eda" ? "26.8.2 through 26.8.2" : "26.4.0 through 26.4.0"}`,
    );
    expect(notes).toContain(changes.trim());
    expect(notes).toContain(`Source commit: ${commit} (tag ${files.tag}).`);
    expect(notes).toContain("portable download is publisher-signed");
    if (component === "nsp")
      expect(notes).toContain("Included resource: nsp-capture.workflow.yaml (SHA256");
    // The CLI has no environment mechanism to trust an ephemeral fixture signer.
    await expect(execute(process.execPath, args, { timeout: 15000 })).rejects.toThrow(
      /publisher|signature/u,
    );
    await expect(readFile(files.output)).rejects.toMatchObject({ code: "ENOENT" });
  },
);

it.each(["component", "version", "sourceSha", "tag", "schemaVersion", "markdown"])(
  "rejects mismatched changelog %s before desktop checksums or notes are written",
  async (field) => {
    const files = await fixture("desktop");
    for (const name of [
      "StreamSkope-0.2.0-darwin-arm64.dmg",
      "StreamSkope-0.2.0-linux-x64.AppImage",
      "StreamSkope-0.2.0-win32-x64-Setup.exe",
    ])
      await writeFile(join(files.directory, name), "test installer");
    await writeFile(
      files.reviewed,
      "---\nrelease_version: 0.2.0\nrelease_tag: v0.2.0\n---\n# Reviewed notes\n\nKnown limitations.\n",
    );
    if (field === "markdown") await writeFile(files.changelog, changes + "Unverified added text\n");
    else
      await writeFile(
        `${files.changelog}.json`,
        JSON.stringify({ ...files.evidence, [field]: "wrong" }),
      );
    await expect(
      execute(
        process.execPath,
        [
          "--import",
          "tsx",
          "tools/package.ts",
          "release",
          files.directory,
          "0.2.0",
          commit,
          files.reviewed,
          files.output,
          files.tag,
          files.changelog,
        ],
        { timeout: 15000 },
      ),
    ).rejects.toThrow("Generated release notes do not match");
    await expect(readFile(files.output)).rejects.toMatchObject({ code: "ENOENT" });
    await expect(readFile(join(files.directory, "SHA256SUMS"))).rejects.toMatchObject({
      code: "ENOENT",
    });
  },
);

it("includes generated changes alongside the reviewed desktop summary and installation notices", async () => {
  const files = await fixture("desktop");
  for (const name of [
    "StreamSkope-0.2.0-darwin-arm64.dmg",
    "StreamSkope-0.2.0-linux-x64.AppImage",
    "StreamSkope-0.2.0-win32-x64-Setup.exe",
  ])
    await writeFile(join(files.directory, name), "test installer");
  await writeFile(
    files.reviewed,
    "---\nrelease_version: 0.2.0\nrelease_tag: v0.2.0\n---\n# Reviewed notes\n\nKnown limitations.\n",
  );
  await execute(
    process.execPath,
    [
      "--import",
      "tsx",
      "tools/package.ts",
      "release",
      files.directory,
      "0.2.0",
      commit,
      files.reviewed,
      files.output,
      files.tag,
      files.changelog,
    ],
    { timeout: 15000 },
  );
  const notes = await readFile(files.output, "utf8");
  expect(notes).toContain("Known limitations.");
  expect(notes).toContain("These downloads are unsigned.");
  expect(notes).toContain(changes.trim());
});

it("refuses a wrong plugin artifact, ambiguous manifests, missing evidence and empty reviewed guidance", async () => {
  const files = await fixture();
  await expect(
    preparePluginReleaseNotes(
      files.directory,
      "nsp",
      "0.2.0",
      commit,
      files.reviewed,
      files.changelog,
      files.publishers,
    ),
  ).rejects.toThrow("Packaged plugin");
  await writeFile(join(files.directory, "extra-plugin.json"), "{}");
  await expect(
    preparePluginReleaseNotes(
      files.directory,
      "eda",
      "0.2.0",
      commit,
      files.reviewed,
      files.changelog,
      files.publishers,
    ),
  ).rejects.toThrow("exactly one");
  await rm(join(files.directory, "extra-plugin.json"));
  await writeFile(files.reviewed, "");
  await expect(
    preparePluginReleaseNotes(
      files.directory,
      "eda",
      "0.2.0",
      commit,
      files.reviewed,
      files.changelog,
      files.publishers,
    ),
  ).rejects.toThrow("reviewed upgrade");
  await rm(`${files.changelog}.json`);
  await expect(readReleaseChangelog(files.changelog, "eda", "0.2.0", commit)).rejects.toMatchObject(
    { code: "ENOENT" },
  );
});

it.each(["primary", "portable"] as const)(
  "refuses plugin notes when the %s package is missing",
  async (missing) => {
    const files = await fixture();
    const assets = officialPluginAssets(OFFICIAL_PLUGINS[0], "0.2.0");
    await rm(
      join(
        files.directory,
        missing === "primary" ? assets.packageAsset : assets.portablePackageAsset,
      ),
    );
    await expect(
      validatePluginReleaseAssets(files.directory, "eda", "0.2.0", files.publishers),
    ).rejects.toThrow("primary, signed portable");
  },
);

it("refuses unsigned portable packages and different code even when signed by the trusted fixture publisher", async () => {
  const files = await fixture();
  const assets = officialPluginAssets(OFFICIAL_PLUGINS[0], "0.2.0");
  const primary = await readFile(join(files.directory, assets.packageAsset));
  await writeFile(join(files.directory, assets.portablePackageAsset), primary);
  await expect(
    validatePluginReleaseAssets(files.directory, "eda", "0.2.0", files.publishers),
  ).rejects.toThrow(/signed|portable|format/u);
  const parsed = parsePluginPackage(primary);
  const changed = new Map(parsed.files);
  changed.set("backend.cjs", Buffer.from("exports.activate = () => ({ changed: true });"));
  const other = encodePluginPackage(parsed.manifest, changed);
  await writeFile(
    join(files.directory, assets.portablePackageAsset),
    createPortablePluginRelease(other, files.encodedKey, files.publishers),
  );
  await expect(
    validatePluginReleaseAssets(files.directory, "eda", "0.2.0", files.publishers),
  ).rejects.toThrow("identical shared release manifest and payload");
});

it("refuses an altered shared manifest and a second portable manifest", async () => {
  const files = await fixture();
  const assets = officialPluginAssets(OFFICIAL_PLUGINS[0], "0.2.0");
  const source = JSON.parse(
    await readFile(join(files.directory, assets.manifestAsset), "utf8"),
  ) as Record<string, unknown>;
  await writeFile(
    join(files.directory, assets.manifestAsset),
    JSON.stringify({ ...source, description: "An unverified artifact description" }),
  );
  await expect(
    validatePluginReleaseAssets(files.directory, "eda", "0.2.0", files.publishers),
  ).rejects.toThrow("identical shared release manifest and payload");
  await writeFile(
    join(files.directory, "streamskope-eda-portable-v0.2.0-plugin.json"),
    JSON.stringify(source),
  );
  await expect(
    validatePluginReleaseAssets(files.directory, "eda", "0.2.0", files.publishers),
  ).rejects.toThrow("exactly one");
});

it("refuses an exported NSP workflow whose bytes differ from the signed bundled workflow", async () => {
  const files = await fixture("nsp");
  const assets = officialPluginAssets(OFFICIAL_PLUGINS[1], "0.2.0");
  await writeFile(
    join(files.directory, `${assets.prefix}-nsp-capture.workflow.yaml`),
    "changed workflow",
  );
  await expect(
    validatePluginReleaseAssets(files.directory, "nsp", "0.2.0", files.publishers),
  ).rejects.toThrow("exported plugin resource");
});
