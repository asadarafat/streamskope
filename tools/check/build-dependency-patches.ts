import { createHash, randomUUID } from "node:crypto";
import { lstat, readFile, realpath, rename, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { BUILD_DEPENDENCY_PATCHES } from "./build-dependency-patch-data";

export interface VerifiedBuildPatch {
  readonly name: string;
  readonly advisoryUrl: string;
  readonly paths: readonly string[];
}
interface PatchFile {
  readonly path: string;
  readonly source: string;
  readonly patched: string;
}
function record(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Invalid dependency metadata");
  return value as Record<string, unknown>;
}
const digest = (source: string): string => createHash("sha256").update(source).digest("hex");
async function regularFile(path: string): Promise<void> {
  if (!(await lstat(path)).isFile() || (await realpath(path)) !== path)
    throw new Error(`Refusing a linked/non-regular dependency file: ${path}`);
}
function safePath(path: string): boolean {
  return (
    path.split("/").every((p) => p !== "." && p !== "..") &&
    /^(?:node_modules\/(?:@[a-zA-Z0-9._-]+\/)?[a-zA-Z0-9._-]+\/)*node_modules\/(?:@[a-zA-Z0-9._-]+\/)?[a-zA-Z0-9._-]+$/u.test(
      path,
    )
  );
}

async function inspect(
  root: string,
  allowOriginal: boolean,
): Promise<{ verified: VerifiedBuildPatch[]; files: PatchFile[] }> {
  const directory = await realpath(root);
  const lock = record(JSON.parse(await readFile(join(directory, "package-lock.json"), "utf8")));
  if (lock.lockfileVersion !== 3) throw new Error("Dependency mitigation requires lockfile v3");
  const packages = record(lock.packages);
  const verified: VerifiedBuildPatch[] = [];
  const files: PatchFile[] = [];
  for (const patch of BUILD_DEPENDENCY_PATCHES) {
    const paths: string[] = [];
    for (const [path, metadata] of Object.entries(packages)) {
      const entry = record(metadata);
      const matches =
        path.endsWith(`/node_modules/${patch.name}`) || path === `node_modules/${patch.name}`;
      if (entry.name === patch.name && !matches)
        throw new Error(`Aliased dependency is unreviewed: ${path}`);
      if (!matches) continue;
      if (
        !safePath(path) ||
        entry.dev !== true ||
        entry.version !== patch.version ||
        entry.resolved !== patch.registryUrl ||
        entry.integrity !== patch.integrity
      )
        throw new Error(`Unreviewed build dependency identity or runtime reachability: ${path}`);
      const manifestPath = join(directory, path, "package.json");
      await regularFile(manifestPath);
      const installed = record(JSON.parse(await readFile(manifestPath, "utf8")));
      if (installed.name !== patch.name || installed.version !== patch.version)
        throw new Error(`Unreviewed installed dependency: ${path}`);
      for (const file of patch.files) {
        const filename = join(directory, path, file.file);
        await regularFile(filename);
        const source = await readFile(filename, "utf8");
        const hash = digest(source);
        if (hash === file.patchedSha256) continue;
        if (!allowOriginal || hash !== file.originalSha256)
          throw new Error(`Missing or unreviewed dependency mitigation: ${filename}`);
        let replacement = source;
        for (const hunk of file.replacements) {
          if (replacement.split(hunk.before).length !== 2)
            throw new Error(`Dependency patch hunk mismatch: ${filename}`);
          replacement = replacement.replace(hunk.before, hunk.after);
        }
        if (digest(replacement) !== file.patchedSha256)
          throw new Error(`Dependency patch digest mismatch: ${filename}`);
        files.push({ path: filename, source, patched: replacement });
      }
      paths.push(path);
    }
    if (!paths.length) throw new Error(`No reviewed ${patch.name} installation in the lockfile`);
    const declared = new Set(paths);
    // Verify actual resolution from each installed consumer, including hidden nested copies.
    for (const [path, metadata] of Object.entries(packages)) {
      const entry = record(metadata);
      const dependencies = entry.dependencies === undefined ? {} : record(entry.dependencies);
      if (!(patch.name in dependencies)) continue;
      if (path !== "" && !safePath(path)) throw new Error(`Unreviewed consumer path: ${path}`);
      const consumer = join(directory, path, "package.json");
      try {
        await regularFile(consumer);
      } catch (error) {
        if (entry.dev === true && (error as NodeJS.ErrnoException).code === "ENOENT") continue;
        throw error;
      }
      const require = createRequire(consumer);
      for (const search of require.resolve.paths(patch.name) ?? []) {
        const candidate = join(search, patch.name, "package.json");
        try {
          await lstat(candidate);
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code === "ENOENT") continue;
          throw error;
        }
        const found = relative(directory, dirname(await realpath(candidate))).replaceAll("\\", "/");
        if (!declared.has(found)) throw new Error(`Unlisted dependency resolution: ${found}`);
        break;
      }
      const found = relative(
        directory,
        dirname(await realpath(require.resolve(`${patch.name}/package.json`))),
      ).replaceAll("\\", "/");
      if (!declared.has(found)) throw new Error(`Unlisted dependency resolution: ${found}`);
    }
    verified.push({ name: patch.name, advisoryUrl: patch.advisoryUrl, paths: paths.sort() });
  }
  return { verified, files };
}

export async function verifyBuildDependencyPatches(
  root: string,
): Promise<readonly VerifiedBuildPatch[]> {
  return (await inspect(root, false)).verified;
}
export async function applyBuildDependencyPatches(
  root: string,
): Promise<readonly VerifiedBuildPatch[]> {
  const result = await inspect(root, true);
  for (const file of result.files) {
    if ((await readFile(file.path, "utf8")) !== file.source)
      throw new Error(`Dependency changed during mitigation: ${file.path}`);
    const temporary = `${file.path}.streamskope-${randomUUID()}.tmp`;
    try {
      await writeFile(temporary, file.patched, { flag: "wx", mode: 0o644 });
      await rename(temporary, file.path);
    } finally {
      await rm(temporary, { force: true });
    }
  }
  return verifyBuildDependencyPatches(root);
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [mode, root = process.cwd(), ...extra] = process.argv.slice(2);
  void (async (): Promise<void> => {
    if ((mode !== "--apply" && mode !== "--verify") || extra.length)
      throw new Error("Use --apply or --verify [root]");
    const verified = await (mode === "--apply"
      ? applyBuildDependencyPatches(root)
      : verifyBuildDependencyPatches(root));
    process.stdout.write(
      `Verified exact-source build dependency mitigations: ${verified.map((p) => p.name).join(", ")}.\n`,
    );
  })().catch((error: unknown) => {
    process.stderr.write(
      `${error instanceof Error ? error.message : "Dependency mitigation failed"}\n`,
    );
    process.exitCode = 1;
  });
}
