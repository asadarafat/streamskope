import { createHash } from "node:crypto";
import { readFile, readdir, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { PLUGIN_API_VERSION } from "../../src/plugins/contracts";
import { parsePluginManifest } from "../../src/plugins/validation";

import { prepareUnsignedRelease, releaseNotesBody } from "./release-policy";
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

export async function preparePluginReleaseNotes(
  directory: string,
  component: string,
  version: string,
  commit: string,
  reviewedPath: string,
  changelogPath: string,
): Promise<string> {
  const identity = releaseIdentity(component, version);
  if (identity.component === "desktop" || !/^[a-f0-9]{40}$/u.test(commit))
    throw new Error("Plugin notes require a plugin component and exact source commit.");
  const manifests = (await readdir(directory)).filter((name) => name.endsWith("-plugin.json"));
  if (manifests.length !== 1) throw new Error("A plugin release must contain exactly one plugin.");
  const manifest = parsePluginManifest(
    JSON.parse(await readFile(join(directory, manifests[0]!), "utf8")),
  );
  if (
    manifest.id !== `streamskope.${component}` ||
    manifest.version !== version ||
    manifest.apiVersion !== PLUGIN_API_VERSION ||
    manifest.compatibility?.target.system !== component
  )
    throw new Error("Packaged plugin does not match the requested release.");
  const reviewed = await readFile(reviewedPath, "utf8");
  if (!reviewed.trim() || !/^##\s+\S/mu.test(reviewed))
    throw new Error("Plugin release needs reviewed upgrade and limitation notes.");
  const changelog = await readReleaseChangelog(changelogPath, identity.component, version, commit);
  const compatibility = manifest.compatibility;
  return (
    [
      `# ${manifest.name} ${version}`,
      reviewed.trim(),
      "## Installation and compatibility",
      `Install from StreamSkope Preferences > Plugins; activation is immediate. Requires plugin API ${manifest.apiVersion}.`,
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
  const [directory, version, commit, source, output, tag, changelog, ...extra] =
    process.argv.slice(2);
  if (!directory || !version || !commit || !source || !output || extra.length) {
    throw new Error(
      "Usage: npm run package -- release <assets-directory> <version> <commit> <source-page> <notes-output> [tag] [changelog]",
    );
  }
  const body = releaseNotesBody(await readFile(source, "utf8"), version, tag);
  const changes = changelog
    ? await readReleaseChangelog(changelog, "desktop", version, commit)
    : "";
  const downloads = (await prepareUnsignedRelease(directory, version, commit, tag)).replace(
    /^# StreamSkope [^\n]+\n/u,
    "## Distribution\n",
  );
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
