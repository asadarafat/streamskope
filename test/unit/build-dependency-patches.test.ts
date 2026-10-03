import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { cp, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { afterEach, expect, it } from "vitest";

import { BUILD_DEPENDENCY_PATCHES } from "../../tools/check/build-dependency-patch-data";
import {
  applyBuildDependencyPatches,
  verifyBuildDependencyPatches,
} from "../../tools/check/build-dependency-patches";

const root = process.cwd();
const temporary: string[] = [];
afterEach(async () => {
  await Promise.all(temporary.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

async function fixture(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-build-patch-"));
  temporary.push(directory);
  const lock = JSON.parse(await readFile(join(root, "package-lock.json"), "utf8")) as {
    packages: Record<string, unknown>;
  };
  const packages: Record<string, unknown> = {
    "": { dependencies: { braces: "3.0.3", "http-cache-semantics": "4.2.0" } },
  };
  await writeFile(join(directory, "package.json"), '{"name":"fixture","version":"1.0.0"}');
  for (const patch of BUILD_DEPENDENCY_PATCHES) {
    const path = `node_modules/${patch.name}`;
    await cp(join(root, path), join(directory, path), { recursive: true });
    packages[path] = lock.packages[path];
    for (const file of patch.files) {
      const filename = join(directory, path, file.file);
      let source = await readFile(filename, "utf8");
      const hash = createHash("sha256").update(source).digest("hex");
      if (hash === file.patchedSha256) {
        for (const hunk of [...file.replacements].reverse())
          source = source.replace(hunk.after, hunk.before);
      }
      expect(createHash("sha256").update(source).digest("hex")).toBe(file.originalSha256);
      await writeFile(filename, source);
    }
  }
  await writeFile(
    join(directory, "package-lock.json"),
    JSON.stringify({ lockfileVersion: 3, packages }),
  );
  return directory;
}

function exercise(directory: string): {
  nested: string;
  shallow: string[];
  cookie: boolean;
  noCache: boolean;
  noStore: boolean;
  publicStale: boolean;
} {
  const program = `
    const path = require('node:path');
    const root = process.argv[1];
    const braces = require(path.join(root, 'node_modules/braces'));
    const CachePolicy = require(path.join(root, 'node_modules/http-cache-semantics'));
    let nested = 'accepted';
    try { braces.expand('{'.repeat(4998) + 'a' + '}'.repeat(4998)); }
    catch (e) { nested = e.name + ': ' + e.message; }
    const request = { url:'https://fixture.test/file', method:'GET', headers:{host:'fixture.test'} };
    const reuse = headers => {
      const policy = new CachePolicy(request,{status:200,headers:{date:new Date().toUTCString(),...headers}});
      const at = policy.now(); policy.now = () => at + 120000;
      return policy.satisfiesWithoutRevalidation({...request,headers:{...request.headers,'cache-control':'max-stale=999999'}});
    };
    console.log(JSON.stringify({nested, shallow:braces.expand('file-{one,two}.js'), cookie:reuse({'set-cookie':'fixture=only','cache-control':'max-age=60'}),noCache:reuse({'cache-control':'no-cache'}),noStore:reuse({'cache-control':'no-store'}),publicStale:reuse({'cache-control':'public, max-age=60'})}));
  `;
  const result = spawnSync(process.execPath, ["--stack-size=512", "-e", program, directory], {
    encoding: "utf8",
    timeout: 10_000,
    env: { ...process.env, NODE_PATH: resolve(root, "node_modules") },
  });
  expect(result.status, result.stderr).toBe(0);
  return JSON.parse(result.stdout) as ReturnType<typeof exercise>;
}

it("reproduces both stock failures and preserves ordinary behavior after mitigation", async () => {
  const directory = await fixture();
  const before = exercise(directory);
  expect(before.nested).toContain("RangeError: Maximum call stack size exceeded");
  expect(before.cookie).toBe(true);
  await expect(verifyBuildDependencyPatches(directory)).rejects.toThrow(/Missing or unreviewed/u);
  const verified = await applyBuildDependencyPatches(directory);
  expect(verified.map((p) => p.name)).toEqual(["braces", "http-cache-semantics"]);
  const after = exercise(directory);
  expect(after.nested).toContain("SyntaxError: Brace nesting exceeds");
  expect(after.shallow).toEqual(before.shallow);
  expect(after).toMatchObject({ cookie: false, noCache: false, noStore: false, publicStale: true });
  expect(await applyBuildDependencyPatches(directory)).toEqual(verified);
  expect(await verifyBuildDependencyPatches(directory)).toEqual(verified);
});

it("refuses modified source, runtime classification and an unlisted nested copy", async () => {
  const directory = await fixture();
  const file = join(directory, "node_modules/braces/lib/compile.js");
  const source = await readFile(file, "utf8");
  await writeFile(file, source + "\n// unreviewed\n");
  await expect(applyBuildDependencyPatches(directory)).rejects.toThrow(/Missing or unreviewed/u);
  await writeFile(file, source);
  const lockPath = join(directory, "package-lock.json");
  const lock = JSON.parse(await readFile(lockPath, "utf8")) as {
    packages: Record<string, Record<string, unknown>>;
  };
  lock.packages["node_modules/braces"]!.dev = false;
  await writeFile(lockPath, JSON.stringify(lock));
  await expect(applyBuildDependencyPatches(directory)).rejects.toThrow(/runtime reachability/u);
  lock.packages["node_modules/braces"]!.dev = true;
  lock.packages["node_modules/consumer"] = { dev: true, dependencies: { braces: "3.0.3" } };
  const consumer = join(directory, "node_modules/consumer");
  await mkdir(consumer);
  await writeFile(join(consumer, "package.json"), '{"name":"consumer"}');
  await cp(join(directory, "node_modules/braces"), join(consumer, "node_modules/braces"), {
    recursive: true,
  });
  await writeFile(lockPath, JSON.stringify(lock));
  await expect(applyBuildDependencyPatches(directory)).rejects.toThrow(
    /Unlisted dependency resolution/u,
  );
});
