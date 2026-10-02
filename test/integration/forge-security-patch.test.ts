import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { cp, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { promisify } from "node:util";

import { afterEach, expect, it } from "vitest";

import { applyForgePatch, FORGE_BACKPORT, verifyForgePatch } from "../../tools/check/forge-patch";

const execute = promisify(execFile);
const directories: string[] = [];
const forgePath = "node_modules/node-forge";
const nestedPath = "node_modules/jks-js/node_modules/node-forge";
const script = resolve("tools/check/forge-patch.ts");
const lockedForge = {
  version: FORGE_BACKPORT.version,
  resolved: FORGE_BACKPORT.registryUrl,
  integrity: FORGE_BACKPORT.integrity,
};

interface TestLock {
  lockfileVersion: number;
  packages: Record<string, Record<string, unknown>>;
}

afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

async function stockSource(): Promise<string> {
  let source = await readFile(join(forgePath, "lib/rsa.js"), "utf8");
  if (sha256(source) === FORGE_BACKPORT.patchedSha256) {
    // Tests also run after the qualification entry point has applied the fix.
    // Reconstruct only the byte-exact vulnerable npm source in this temp fixture.
    source = source.replace(
      / {10}\/\/ validate DigestInfo structure and element counts \(outer DigestInfo\n[\s\S]+? {14}\(\('parameters' in capture\) \? 2 : 1\)\) \{/u,
      `          // validate DigestInfo structure and element count
          var capture = {};
          var errors = [];
          if(!asn1.validate(obj, digestInfoValidator, capture, errors) ||
            obj.value.length !== 2) {`,
    );
  }
  expect(sha256(source)).toBe(FORGE_BACKPORT.originalSha256);
  return source;
}

async function writeJson(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, JSON.stringify(value));
}

async function copyForge(root: string, path = forgePath): Promise<void> {
  await mkdir(join(root, path), { recursive: true });
  await cp(join(forgePath, "lib"), join(root, path, "lib"), { recursive: true });
  await cp(join(forgePath, "package.json"), join(root, path, "package.json"));
  await writeFile(join(root, path, "lib/rsa.js"), await stockSource());
}

async function fixture(nested = false): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-forge-patch-"));
  directories.push(root);
  const dependencies = { "node-forge": "1.4.0", "jks-js": "1.1.7" };
  await writeJson(join(root, "package.json"), { dependencies });
  await writeJson(join(root, "node_modules/jks-js/package.json"), {
    name: "jks-js",
    version: "1.1.7",
    dependencies: { "node-forge": "^1.4.0" },
  });
  const lock: TestLock = {
    lockfileVersion: 3,
    packages: {
      "": { dependencies },
      [forgePath]: { ...lockedForge },
      "node_modules/jks-js": { version: "1.1.7", dependencies: { "node-forge": "^1.4.0" } },
      ...(nested ? { [nestedPath]: { ...lockedForge } } : {}),
    },
  };
  await writeJson(join(root, "package-lock.json"), lock);
  await copyForge(root);
  if (nested) await copyForge(root, nestedPath);
  return root;
}

async function lock(root: string): Promise<TestLock> {
  return JSON.parse(await readFile(join(root, "package-lock.json"), "utf8")) as TestLock;
}

async function signatureBehavior(root: string): Promise<unknown> {
  // Node creates the signatures independently of Forge. The malformed signature
  // is over an explicitly constructed DigestInfo, not a snapshot of patch text.
  const { stdout } = await execute(
    process.execPath,
    [
      "--input-type=commonjs",
      "-e",
      `
    const { createHash, generateKeyPairSync, privateEncrypt, verify, constants } = require('node:crypto');
    const forge = require('node-forge');
    const { privateKey, publicKey } = generateKeyPairSync('rsa', { modulusLength: 1024 });
    const data = Buffer.from('StreamSkope independent RSA verifier regression');
    const hash = createHash('sha256').update(data).digest();
    const der = (tag, content) => Buffer.concat([Buffer.from([tag, content.length]), content]);
    const oid = Buffer.from('0609608648016503040201', 'hex');
    const key = forge.pki.publicKeyFromPem(publicKey.export({ type: 'spki', format: 'pem' }));
    const results = {};
    for (const [name, suffix] of [['canonical', '0500'], ['noNull', ''], ['garbageWithNull', '05000401ff'], ['garbageWithoutNull', '0401ff']]) {
      const algorithm = Buffer.concat([oid, Buffer.from(suffix, 'hex')]);
      const digestInfo = der(0x30, Buffer.concat([der(0x30, algorithm), der(0x04, hash)]));
      const signature = privateEncrypt({ key: privateKey, padding: constants.RSA_PKCS1_PADDING }, digestInfo);
      let accepted = false;
      try { accepted = key.verify(hash.toString('binary'), signature.toString('binary')); } catch {}
      results[name] = { forge: accepted, native: verify('sha256', data, publicKey, signature) };
    }
    process.stdout.write(JSON.stringify(results));
  `,
    ],
    { cwd: root, timeout: 15_000 },
  );
  return JSON.parse(stdout) as unknown;
}

it("rejects nested DigestAlgorithm garbage accepted by the stock verifier and preserves valid forms", async () => {
  const root = await fixture();
  const valid = {
    canonical: { forge: true, native: true },
    // Forge supports omitted SHA256 NULL parameters; native OpenSSL does not.
    noNull: { forge: true, native: false },
  };
  expect(await signatureBehavior(root)).toEqual({
    ...valid,
    garbageWithNull: { forge: true, native: false },
    garbageWithoutNull: { forge: true, native: false },
  });
  await applyForgePatch(root);
  expect(await signatureBehavior(root)).toEqual({
    ...valid,
    garbageWithNull: { forge: false, native: false },
    garbageWithoutNull: { forge: false, native: false },
  });
});

it("verification never repairs stock code; applying is idempotent and preserves package identity", async () => {
  const root = await fixture();
  const rsa = join(root, forgePath, "lib/rsa.js");
  const original = await readFile(rsa, "utf8");
  const metadata = await Promise.all(
    ["package.json", "package-lock.json"].map((path) => readFile(join(root, path), "utf8")),
  );
  await expect(verifyForgePatch(root)).rejects.toThrow("mitigation is missing");
  expect(await readFile(rsa, "utf8")).toBe(original);
  expect(await applyForgePatch(root)).toEqual([forgePath]);
  const patched = await readFile(rsa, "utf8");
  expect(sha256(patched)).toBe(FORGE_BACKPORT.patchedSha256);
  expect(await applyForgePatch(root)).toEqual([forgePath]);
  expect(await verifyForgePatch(root)).toEqual([forgePath]);
  expect(await readFile(rsa, "utf8")).toBe(patched);
  expect(
    await Promise.all(
      ["package.json", "package-lock.json"].map((path) => readFile(join(root, path), "utf8")),
    ),
  ).toEqual(metadata);
});

it("patches and verifies every locked nested Forge copy used by a consumer", async () => {
  const root = await fixture(true);
  expect(await applyForgePatch(root)).toEqual([nestedPath, forgePath]);
  for (const path of [forgePath, nestedPath]) {
    expect(sha256(await readFile(join(root, path, "lib/rsa.js"), "utf8"))).toBe(
      FORGE_BACKPORT.patchedSha256,
    );
  }
  expect(await verifyForgePatch(root)).toEqual([nestedPath, forgePath]);
});

it("validates every copy before changing any copy", async () => {
  const root = await fixture(true);
  await writeFile(join(root, nestedPath, "lib/rsa.js"), "unreviewed source");
  await expect(applyForgePatch(root)).rejects.toThrow("source differs");
  expect(sha256(await readFile(join(root, forgePath, "lib/rsa.js"), "utf8"))).toBe(
    FORGE_BACKPORT.originalSha256,
  );
});

it.each(["version", "resolved", "integrity"])("rejects conflicting locked %s", async (field) => {
  const root = await fixture();
  const value = await lock(root);
  value.packages[forgePath]![field] = "unreviewed";
  await writeJson(join(root, "package-lock.json"), value);
  await expect(applyForgePatch(root)).rejects.toThrow("Unreviewed locked");
});

it.each(["name", "version", "main"])("rejects conflicting installed %s", async (field) => {
  const root = await fixture();
  const path = join(root, forgePath, "package.json");
  const metadata = JSON.parse(await readFile(path, "utf8")) as Record<string, unknown>;
  metadata[field] = "unreviewed";
  await writeJson(path, metadata);
  await expect(applyForgePatch(root)).rejects.toThrow("Unreviewed installed");
});

it("rejects an undeclared nested copy that shadows the locked Forge for jks-js", async () => {
  const root = await fixture();
  await copyForge(root, nestedPath);
  await expect(applyForgePatch(root)).rejects.toThrow("Unlisted node-forge resolution");
});

it("detects a newly added shadow copy even after Node has cached dependency resolution", async () => {
  const root = await fixture();
  await applyForgePatch(root);
  await copyForge(root, nestedPath);
  await expect(verifyForgePatch(root)).rejects.toThrow("Unlisted node-forge resolution");
});

it("rejects a missing locked dependency", async () => {
  const root = await fixture();
  await rm(join(root, forgePath), { recursive: true });
  await expect(applyForgePatch(root)).rejects.toThrow();
});

it("rejects alterations made after patch verification", async () => {
  const root = await fixture();
  await applyForgePatch(root);
  const path = join(root, forgePath, "lib/rsa.js");
  await writeFile(path, `${await readFile(path, "utf8")}\n// altered\n`);
  await expect(verifyForgePatch(root)).rejects.toThrow("source differs");
  await expect(applyForgePatch(root)).rejects.toThrow("source differs");
});

it("refuses linked RSA source rather than modifying another installation", async () => {
  const root = await fixture();
  const source = await stockSource();
  const target = join(root, "rsa-external.js");
  const path = join(root, forgePath, "lib/rsa.js");
  await writeFile(target, source);
  await rm(path);
  await symlink(target, path);
  await expect(applyForgePatch(root)).rejects.toThrow("linked or non-regular");
  expect(await readFile(target, "utf8")).toBe(source);
});

it("runs with native Node TypeScript stripping and requires explicit apply or verify", async () => {
  const root = await fixture();
  await expect(execute(process.execPath, [script], { cwd: root })).rejects.toThrow();
  await expect(execute(process.execPath, [script, "--verify"], { cwd: root })).rejects.toThrow();
  const result = await execute(process.execPath, [script, "--apply", root]);
  expect(result.stdout).toContain(FORGE_BACKPORT.advisoryUrl);
  await execute(process.execPath, [script, "--verify", root]);
});
