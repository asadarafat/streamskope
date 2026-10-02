import { createHash, randomUUID } from "node:crypto";
import { lstat, readFile, realpath, rename, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// Temporary, exact-source backport of the upstream proposal, not a released
// upstream fix. Remove this helper and its audit mitigation when upgrading to a
// reviewed release containing the fix. The npm dependency identity stays intact.
export const FORGE_BACKPORT = Object.freeze({
  version: "1.4.0",
  advisoryUrl: "https://github.com/advisories/GHSA-86w9-cpqp-85rv",
  upstreamUrl: "https://github.com/digitalbazaar/forge/pull/1152",
  upstreamCommit: "ceba34402e329f0365134f23fe19898756527d65",
  registryUrl: "https://registry.npmjs.org/node-forge/-/node-forge-1.4.0.tgz",
  integrity:
    "sha512-LarFH0+6VfriEhqMMcLX2F7SwSXeWwnEAJEsYm5QKWchiVYVvJyV9v7UDvUv+w5HO23ZpQTXDv/GxdDdMyOuoQ==",
  originalSha256: "fd4740238145ec26470eb3f06a627c72039538ce1307dbdce40521f94dfd0a50",
  patchedSha256: "acc22e5d36e27832c34e02dd3933aad7977d45b047eead5016520735efedc9c5",
});

const original = `          // validate DigestInfo structure and element count
          var capture = {};
          var errors = [];
          if(!asn1.validate(obj, digestInfoValidator, capture, errors) ||
            obj.value.length !== 2) {`;

const patched = `          // validate DigestInfo structure and element counts (outer DigestInfo
          // and nested DigestAlgorithm). asn1.validate ignores extra children,
          // so length must be checked explicitly at each nesting level to
          // prevent low-exponent PKCS#1 v1.5 signature forgery (CVE-2026-85393).
          var capture = {};
          var errors = [];
          if(!asn1.validate(obj, digestInfoValidator, capture, errors) ||
            obj.value.length !== 2 ||
            obj.value[0].value.length !==
              (('parameters' in capture) ? 2 : 1)) {`;

interface ForgeInstallation {
  readonly path: string;
  readonly rsa: string;
  readonly source: string;
  readonly patched: boolean;
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error(`${label} must be an object.`);
  return value as Record<string, unknown>;
}

function digest(source: string): string {
  return createHash("sha256").update(source).digest("hex");
}

async function regularFile(path: string): Promise<void> {
  if (!(await lstat(path)).isFile() || (await realpath(path)) !== path)
    throw new Error(`Forge mitigation refuses linked or non-regular files: ${path}`);
}

function safeLockPath(path: string): boolean {
  return (
    path.split("/").every((part) => part !== "." && part !== "..") &&
    /^(?:node_modules\/(?:@[a-zA-Z0-9._-]+\/)?[a-zA-Z0-9._-]+\/)*node_modules\/(?:@[a-zA-Z0-9._-]+\/)?[a-zA-Z0-9._-]+$/u.test(
      path,
    )
  );
}

async function installations(root: string, allowOriginal: boolean): Promise<ForgeInstallation[]> {
  const directory = await realpath(root);
  const manifest = record(
    JSON.parse(await readFile(join(directory, "package.json"), "utf8")),
    "Package manifest",
  );
  const lock = record(
    JSON.parse(await readFile(join(directory, "package-lock.json"), "utf8")),
    "Package lock",
  );
  if (lock.lockfileVersion !== 3) throw new Error("Forge mitigation requires lockfile version 3.");
  const packages = record(lock.packages, "Locked packages");
  const rootPackage = record(packages[""], "Locked root package");
  for (const [label, value] of [
    ["manifest", manifest],
    ["lock", rootPackage],
  ] as const) {
    if (
      record(value.dependencies, `${label} dependencies`)["node-forge"] !== FORGE_BACKPORT.version
    )
      throw new Error(`Forge mitigation requires ${label} node-forge ${FORGE_BACKPORT.version}.`);
  }

  const entries: ForgeInstallation[] = [];
  for (const [path, value] of Object.entries(packages)) {
    const metadata = record(value, `Locked ${path}`);
    const forgePath = /(?:^|\/)node_modules\/node-forge$/u.test(path);
    if (metadata.name === "node-forge" && !forgePath)
      throw new Error(`Forge mitigation refuses an aliased node-forge package: ${path}`);
    if (!forgePath) continue;
    if (
      !safeLockPath(path) ||
      metadata.version !== FORGE_BACKPORT.version ||
      metadata.resolved !== FORGE_BACKPORT.registryUrl ||
      metadata.integrity !== FORGE_BACKPORT.integrity
    )
      throw new Error(`Unreviewed locked node-forge metadata: ${path}`);
    const packagePath = join(directory, path, "package.json");
    const rsa = join(directory, path, "lib/rsa.js");
    await regularFile(packagePath);
    await regularFile(rsa);
    const installed = record(JSON.parse(await readFile(packagePath, "utf8")), `Installed ${path}`);
    if (
      installed.name !== "node-forge" ||
      installed.version !== FORGE_BACKPORT.version ||
      installed.main !== "lib/index.js"
    )
      throw new Error(`Unreviewed installed node-forge metadata: ${path}`);
    const source = await readFile(rsa, "utf8");
    const hash = digest(source);
    const isPatched = hash === FORGE_BACKPORT.patchedSha256;
    if (!isPatched && (!allowOriginal || hash !== FORGE_BACKPORT.originalSha256))
      throw new Error(
        `Node-forge mitigation is missing or source differs from the reviewed backport: ${path}`,
      );
    entries.push({ path, rsa, source, patched: isPatched });
  }
  if (entries.length === 0)
    throw new Error("The lockfile contains no reviewed node-forge installation.");

  // Check actual Node resolution, including jks-js, rather than assuming a
  // deduplicated dependency. An undeclared nested copy must not escape the fix.
  const lockedPaths = new Set(entries.map(({ path }) => path));
  for (const [path, value] of Object.entries(packages)) {
    const metadata = record(value, `Locked ${path}`);
    const dependencies =
      metadata.dependencies === undefined
        ? {}
        : record(metadata.dependencies, `Locked ${path} dependencies`);
    if (!("node-forge" in dependencies)) continue;
    if (path !== "" && !safeLockPath(path))
      throw new Error(`Invalid locked consumer path: ${path}`);
    const consumer = join(directory, path, "package.json");
    try {
      await regularFile(consumer);
    } catch (error) {
      // Production packaging deliberately omits development dependencies.
      if (
        path !== "" &&
        metadata.dev === true &&
        (error as NodeJS.ErrnoException).code === "ENOENT"
      )
        continue;
      throw error;
    }
    const require = createRequire(consumer);
    // require.resolve caches previous results. Inspect its search directories as
    // well, so a nested copy added since an earlier verification cannot hide.
    for (const lookup of require.resolve.paths("node-forge") ?? []) {
      const candidate = join(lookup, "node-forge", "package.json");
      try {
        await lstat(candidate);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") continue;
        throw error;
      }
      const candidatePath = relative(directory, dirname(await realpath(candidate))).replaceAll(
        "\\",
        "/",
      );
      if (!lockedPaths.has(candidatePath))
        throw new Error(`Unlisted node-forge resolution from ${path || "root"}: ${candidatePath}`);
      break;
    }
    const resolved = await realpath(require.resolve("node-forge/package.json"));
    const resolvedPath = relative(directory, dirname(resolved)).replaceAll("\\", "/");
    if (!lockedPaths.has(resolvedPath))
      throw new Error(`Unlisted node-forge resolution from ${path || "root"}: ${resolvedPath}`);
    const entry = entries.find(({ path: installedPath }) => installedPath === resolvedPath)!;
    if ((await realpath(require.resolve("node-forge/lib/rsa.js"))) !== entry.rsa)
      throw new Error(`Unexpected node-forge RSA resolution from ${path || "root"}.`);
  }
  return entries.sort((left, right) => left.path.localeCompare(right.path));
}

/** Verifies the complete backport without modifying the installed dependency tree. */
export async function verifyForgePatch(root: string): Promise<readonly string[]> {
  return Object.freeze((await installations(root, false)).map(({ path }) => path));
}

/** Applies the pinned hunk to verified npm source; repeated calls leave it unchanged. */
export async function applyForgePatch(root: string): Promise<readonly string[]> {
  const entries = await installations(root, true);
  for (const entry of entries) {
    if (entry.patched) continue;
    const replacement = entry.source.replace(original, patched);
    if (digest(replacement) !== FORGE_BACKPORT.patchedSha256)
      throw new Error(`Forge backport does not produce the reviewed digest: ${entry.path}`);
    const temporary = `${entry.rsa}.streamskope-${randomUUID()}.tmp`;
    try {
      await writeFile(temporary, replacement, { flag: "wx", mode: 0o644 });
      await rename(temporary, entry.rsa);
    } finally {
      await rm(temporary, { force: true });
    }
  }
  return verifyForgePatch(root);
}

// Explicit modes avoid silently mutating an installation during an audit.
if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  void (async (): Promise<void> => {
    const [mode, root = process.cwd(), ...extra] = process.argv.slice(2);
    if ((mode !== "--apply" && mode !== "--verify") || extra.length !== 0)
      throw new Error("Usage: node tools/check/forge-patch.ts <--apply|--verify> [root]");
    const paths = await (mode === "--apply" ? applyForgePatch(root) : verifyForgePatch(root));
    process.stdout.write(
      `Verified ${FORGE_BACKPORT.advisoryUrl} backport in ${paths.join(", ")}.\n`,
    );
  })().catch((error: unknown) => {
    process.stderr.write(
      `Forge mitigation failed: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  });
}
