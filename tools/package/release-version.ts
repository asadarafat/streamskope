import { appendFile, readFile, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { DEVELOPMENT_VERSION } from "../../src/plugins/compatibility";
import { PLUGIN_API_VERSION } from "../../src/plugins/contracts";
import { parsePluginManifest } from "../../src/plugins/validation";

import { parsePublicationVersion, releaseNotesBody } from "./release-policy";

export type ReleaseComponent = "desktop" | "eda" | "nsp";

export interface ReleaseIdentity {
  readonly component: ReleaseComponent;
  readonly version: string;
  readonly tag: string;
  readonly prerelease: boolean;
}

export function releaseIdentity(component: string, version: string): ReleaseIdentity {
  if (component !== "desktop" && component !== "eda" && component !== "nsp")
    throw new Error("Choose desktop, eda or nsp as the release component.");
  parsePublicationVersion(version);
  return {
    component,
    version,
    tag: component === "desktop" ? `v${version}` : `plugins/${component}/v${version}`,
    prerelease: version.includes("-"),
  };
}

function object(value: unknown, name: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error(`Invalid ${name}.`);
  return value as Record<string, unknown>;
}

function sourceVersion(value: unknown, version: string): void {
  if (value !== DEVELOPMENT_VERSION && value !== version)
    throw new Error("Release stamping requires development source or the same selected version.");
}

/** Turn reviewed, unversioned changes into the notes for this one release build. */
export function stampReleaseNotes(source: string, identity: ReleaseIdentity): string {
  const match = /^---\r?\n([\s\S]*?)\r?\n---\r?\n([\s\S]*)$/u.exec(source);
  if (
    identity.component !== "desktop" ||
    !match?.[1] ||
    (match[1].match(/^unreleased:\s*true\s*$/gmu) ?? []).length !== 1 ||
    /^release_(?:version|tag):/mu.test(match[1]) ||
    !match[2] ||
    (match[2].match(/^# .+$/gmu) ?? []).length !== 1
  )
    throw new Error("Desktop release needs reviewed unversioned notes with unreleased: true.");
  const body = match[2].replace(/^# .+$/mu, `# StreamSkope ${identity.tag}`);
  const notes = `---\ntitle: StreamSkope ${identity.tag}\nrelease_version: ${identity.version}\nrelease_tag: ${identity.tag}\n---\n${body}`;
  releaseNotesBody(notes, identity.version, identity.tag);
  return notes;
}

/** Validate every input before mutating only the selected product's build metadata. */
export async function prepareReleaseVersion(
  root: string,
  component: string,
  version: string,
  stamp = false,
): Promise<ReleaseIdentity> {
  const identity = releaseIdentity(component, version);
  const writes = new Map<string, string>();
  if (identity.component === "desktop") {
    const packageFile = join(root, "package.json");
    const lockFile = join(root, "package-lock.json");
    const project = object(JSON.parse(await readFile(packageFile, "utf8")), "package manifest");
    const lock = object(JSON.parse(await readFile(lockFile, "utf8")), "package lock");
    const packages = object(lock.packages, "locked packages");
    const lockedRoot = object(packages[""], "locked root package");
    for (const manifest of [project, lock, lockedRoot]) sourceVersion(manifest.version, version);
    const notes = stampReleaseNotes(
      await readFile(join(root, "website/docs/releases/unreleased.md"), "utf8"),
      identity,
    );
    const notesFile = join(root, "website/docs/releases", `${identity.tag}.md`);
    try {
      const existing = await readFile(notesFile, "utf8");
      if (existing !== notes) throw new Error("Refusing to overwrite different release notes.");
    } catch (error) {
      if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
    }
    project.version = version;
    lock.version = version;
    lockedRoot.version = version;
    writes.set(packageFile, `${JSON.stringify(project, null, 2)}\n`);
    writes.set(lockFile, `${JSON.stringify(lock, null, 2)}\n`);
    writes.set(notesFile, notes);
  } else {
    const manifestFile = join(root, "plugins", identity.component, "manifest.json");
    const manifest = object(JSON.parse(await readFile(manifestFile, "utf8")), "plugin manifest");
    sourceVersion(manifest.version, version);
    const parsed = parsePluginManifest({ ...manifest, version });
    if (
      parsed.id !== `streamskope.${identity.component}` ||
      parsed.apiVersion !== PLUGIN_API_VERSION ||
      parsed.compatibility?.target.system !== identity.component
    )
      throw new Error("Release component must match its current-API plugin manifest and target.");
    manifest.version = version;
    writes.set(manifestFile, `${JSON.stringify(manifest, null, 2)}\n`);
  }
  if (stamp) for (const [file, contents] of writes) await writeFile(file, contents);
  return identity;
}

async function main(): Promise<void> {
  const [component, version, option, ...extra] = process.argv.slice(2);
  if (!component || !version || (option !== undefined && option !== "--stamp") || extra.length)
    throw new Error("Usage: release-version.ts <desktop|eda|nsp> <version> [--stamp]");
  const identity = await prepareReleaseVersion(
    process.cwd(),
    component,
    version,
    option === "--stamp",
  );
  if (process.env.GITHUB_OUTPUT !== undefined)
    await appendFile(
      process.env.GITHUB_OUTPUT,
      Object.entries(identity)
        .map(([key, value]) => `${key}=${String(value)}\n`)
        .join(""),
      "utf8",
    );
  process.stdout.write(
    `${option === "--stamp" ? "Stamped build checkout for" : "Validated"} ${identity.tag}.\n`,
  );
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  void main().catch((error: unknown) => {
    process.stderr.write(
      `Release version failed: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  });
