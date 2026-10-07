import { createHash } from "node:crypto";
import { readFile, readdir, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { PLUGIN_API_VERSION, type PluginManifest } from "../../src/plugins/contracts";
import { parsePluginManifest } from "../../src/plugins/validation";
import { readBoundedFile } from "../../src/platform/node/bounded-file";
import { OFFICIAL_PLUGINS, officialPluginAssets } from "../../src/platform/node/plugins/official";
import {
  MAX_PLUGIN_ARCHIVE_BYTES,
  MAX_PLUGIN_PACKAGE_BYTES,
  MAX_PLUGIN_RESOURCE_BYTES,
  parsePluginPackage,
  parsePortablePluginPackage,
  pluginPackagePayloadBytes,
} from "../../src/platform/node/plugins/package";
import {
  TRUSTED_PLUGIN_PUBLISHERS,
  type TrustedPluginPublisher,
} from "../../src/platform/node/plugins/publishers";

import { prepareUnsignedRelease, releaseNotesBody } from "./release-policy";
import { prepareBrowserReleaseAssets } from "./browser-release";
import { releaseIdentity, type ReleaseComponent } from "./release-version";

/** Refuse changes collected for another component, source, or release identity. */
export async function readReleaseChangelog(
  path: string,
  component: ReleaseComponent,
  version: string,
  commit: string,
): Promise<string> {
  const identity = releaseIdentity(component, version);
  const markdown = await readFile(path, "utf8");
  const evidence: unknown = JSON.parse(await readFile(`${path}.json`, "utf8"));
  if (
    evidence === null ||
    typeof evidence !== "object" ||
    !("schemaVersion" in evidence) ||
    evidence.schemaVersion !== 1 ||
    !("component" in evidence) ||
    evidence.component !== component ||
    !("version" in evidence) ||
    evidence.version !== version ||
    !("tag" in evidence) ||
    evidence.tag !== identity.tag ||
    !("sourceSha" in evidence) ||
    evidence.sourceSha !== commit ||
    !("markdownSha256" in evidence) ||
    evidence.markdownSha256 !== createHash("sha256").update(markdown).digest("hex") ||
    !markdown.trim()
  )
    throw new Error("Generated release notes do not match this release and source commit.");
  return markdown;
}

/** Signed portable delivery and the old-client package must describe identical code. */
export async function validatePluginReleaseAssets(
  directory: string,
  component: string,
  version: string,
  publishers: readonly TrustedPluginPublisher[] = TRUSTED_PLUGIN_PUBLISHERS,
): Promise<PluginManifest> {
  const names = await readdir(directory);
  const manifests = names.filter((name) => name.endsWith("-plugin.json"));
  if (manifests.length !== 1) throw new Error("A plugin release must contain exactly one plugin.");
  const manifest = parsePluginManifest(
    JSON.parse(
      (
        await readBoundedFile(join(directory, manifests[0]!), 64 * 1024, { rejectSymlinks: true })
      ).toString("utf8"),
    ),
  );
  if (
    manifest.id !== `streamskope.${component}` ||
    manifest.version !== version ||
    manifest.apiVersion !== PLUGIN_API_VERSION ||
    manifest.compatibility?.target.system !== component
  )
    throw new Error("Packaged plugin does not match the requested release.");
  const plugin = OFFICIAL_PLUGINS.find((entry) => entry.directory === component);
  if (plugin === undefined)
    throw new Error("Packaged plugin does not match the requested release.");
  const assets = officialPluginAssets(plugin, version);
  if (manifests[0] !== assets.manifestAsset)
    throw new Error("Packaged plugin manifest filename does not match the requested release.");
  const allowed = new Set([
    assets.packageAsset,
    assets.portablePackageAsset,
    assets.manifestAsset,
    ...(manifest.resources ?? []).map((resource) => `${assets.prefix}-${resource.path}`),
  ]);
  if (names.some((name) => !allowed.has(name)) || names.length !== allowed.size)
    throw new Error(
      "Plugin release must include exactly its primary, signed portable, shared manifest and declared resources.",
    );
  const primaryBytes = await readBoundedFile(
    join(directory, assets.packageAsset),
    MAX_PLUGIN_PACKAGE_BYTES,
    { rejectSymlinks: true },
  );
  const portableBytes = await readBoundedFile(
    join(directory, assets.portablePackageAsset),
    MAX_PLUGIN_ARCHIVE_BYTES,
    { rejectSymlinks: true },
  );
  const primary = parsePluginPackage(primaryBytes, undefined, publishers);
  const portable = parsePortablePluginPackage(portableBytes, undefined, publishers);
  if (
    JSON.stringify(primary.manifest) !== JSON.stringify(manifest) ||
    JSON.stringify(portable.manifest) !== JSON.stringify(manifest) ||
    primary.contentSha256 !== portable.contentSha256 ||
    !Buffer.from(pluginPackagePayloadBytes(portableBytes, publishers)).equals(primaryBytes)
  )
    throw new Error(
      "Primary and signed portable packages must contain the identical shared release manifest and payload.",
    );
  for (const resource of manifest.resources ?? []) {
    const exported = await readBoundedFile(
      join(directory, `${assets.prefix}-${resource.path}`),
      MAX_PLUGIN_RESOURCE_BYTES,
      { rejectSymlinks: true },
    );
    if (!exported.equals(Buffer.from(primary.files.get(resource.path)!)))
      throw new Error("An exported plugin resource does not match the signed package payload.");
  }
  return manifest;
}

export async function preparePluginReleaseNotes(
  directory: string,
  component: string,
  version: string,
  commit: string,
  reviewedPath: string,
  changelogPath: string,
  publishers: readonly TrustedPluginPublisher[] = TRUSTED_PLUGIN_PUBLISHERS,
): Promise<string> {
  const identity = releaseIdentity(component, version);
  if (identity.component === "desktop" || !/^[a-f0-9]{40}$/u.test(commit))
    throw new Error("Plugin notes require a plugin component and exact source commit.");
  const manifest = await validatePluginReleaseAssets(directory, component, version, publishers);
  const reviewed = await readFile(reviewedPath, "utf8");
  if (!reviewed.trim() || !/^##\s+\S/mu.test(reviewed))
    throw new Error("Plugin release needs reviewed upgrade and limitation notes.");
  const changelog = await readReleaseChangelog(changelogPath, identity.component, version, commit);
  const compatibility = manifest.compatibility!;
  return (
    [
      `# ${manifest.name} ${version}`,
      reviewed.trim(),
      "## Installation and compatibility",
      `Install from StreamSkope Preferences > Plugins; activation is immediate. Requires plugin API ${manifest.apiVersion}.`,
      `The primary package remains compatible with existing clients. The separate portable download is publisher-signed and requires a StreamSkope build with signed portable package support; it contains the identical plugin code and resources.`,
      `Requires StreamSkope >=${compatibility.streamskope.minimum} and <${compatibility.streamskope.maximumExclusive}.`,
      `Supports ${compatibility.target.system.toUpperCase()} ${compatibility.target.minimum} through ${compatibility.target.maximum} (inclusive).`,
      ...(component === "eda"
        ? ["The EDA cluster application is installed separately through the capture workflow."]
        : []),
      ...(manifest.resources ?? []).map(
        (resource) =>
          `Included resource: ${resource.path} (SHA256 ${resource.sha256}). The identical resource is available as a separate download.`,
      ),
      `Source commit: ${commit} (tag ${identity.tag}).`,
      changelog.trim(),
    ].join("\n\n") + "\n"
  );
}

async function main(): Promise<void> {
  if (process.argv[2] === "plugin") {
    const [directory, component, version, commit, reviewed, changelog, output, ...extra] =
      process.argv.slice(3);
    if (
      !directory ||
      !component ||
      !version ||
      !commit ||
      !reviewed ||
      !changelog ||
      !output ||
      extra.length
    )
      throw new Error(
        "Usage: release.ts plugin <assets-directory> <component> <version> <commit> <reviewed-notes> <changelog> <notes-output>",
      );
    const notes = await preparePluginReleaseNotes(
      directory,
      component,
      version,
      commit,
      reviewed,
      changelog,
    );
    await writeFile(output, notes, { flag: "wx", mode: 0o600 });
    process.stdout.write("Prepared reviewed plugin notes, compatibility and generated changes.\n");
    return;
  }
  const arguments_ = process.argv.slice(2);
  const containerOption = arguments_.indexOf("--containers");
  const containerStaging = containerOption === -1 ? undefined : arguments_[containerOption + 1];
  if (containerOption !== -1 && (containerOption !== arguments_.length - 2 || !containerStaging))
    throw new Error(
      "--containers requires one native-build staging directory as the final argument.",
    );
  const [directory, version, commit, source, output, tag, changelog, ...extra] =
    containerOption === -1 ? arguments_ : arguments_.slice(0, containerOption);
  if (!directory || !version || !commit || !source || !output || extra.length) {
    throw new Error(
      "Usage: npm run package -- release <assets-directory> <version> <commit> <source-page> <notes-output> [tag] [changelog] [--containers <native-build-staging>]",
    );
  }
  const body = releaseNotesBody(await readFile(source, "utf8"), version, tag);
  const changes = changelog
    ? await readReleaseChangelog(changelog, "desktop", version, commit)
    : "";
  const containerDirectory =
    containerStaging === undefined
      ? undefined
      : join(resolve(directory), "..", "container-package");
  if (containerStaging !== undefined && containerDirectory !== undefined)
    await prepareBrowserReleaseAssets(containerStaging, containerDirectory, version, commit);
  const downloads = (
    await prepareUnsignedRelease(directory, version, commit, tag, containerDirectory)
  ).replace(/^# StreamSkope [^\n]+\n/u, "## Distribution\n");
  await writeFile(output, `${body.trimEnd()}\n\n${downloads}${changes ? `\n${changes}` : ""}`, {
    flag: "wx",
    mode: 0o600,
  });
  process.stdout.write("Prepared reviewed release notes, download notices and SHA256SUMS.\n");
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  void main().catch((error: unknown) => {
    process.stderr.write(
      `Release preparation failed: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  });
