import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { cp, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { build } from "vite";
import { afterEach, expect, it } from "vitest";

import { RUNTIME_DEPENDENCY_PATCHES } from "../../tools/check/runtime-dependency-patch-data";
import {
  applyRuntimeDependencyPatches,
  verifyRuntimeDependencyPatches,
} from "../../tools/check/build-dependency-patches";

const root = process.cwd();
const temporary: string[] = [];
const patch = RUNTIME_DEPENDENCY_PATCHES[0];
interface ProbeResult {
  readonly results: readonly {
    readonly mode: string;
    readonly boundedFailure: boolean;
    readonly fallbackConfirmed: boolean;
    readonly socketCleanupConfirmed: boolean;
    readonly activeBeforeFixtureTeardown: number;
    readonly teardownFailures: readonly string[];
  }[];
  readonly unhandledRejections: readonly string[];
}
interface TlsProbeResult {
  readonly results: readonly {
    readonly mode: "dns-match" | "ip-mismatch";
    readonly connected: boolean;
    readonly hostnameRejected: boolean;
    readonly identityConfirmed: boolean;
    readonly peerCleanupConfirmed: boolean;
    readonly pings: number;
    readonly teardownFailures: readonly string[];
  }[];
  readonly unhandledRejections: readonly string[];
}
afterEach(async () => {
  await Promise.all(
    temporary.splice(0).map((directory) => rm(directory, { recursive: true, force: true })),
  );
});
async function fixture(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-nats-runtime-"));
  temporary.push(directory);
  const path = `node_modules/${patch.name}`;
  await cp(join(root, path), join(directory, path), { recursive: true });
  const lock = JSON.parse(await readFile(join(root, "package-lock.json"), "utf8")) as {
    packages: Record<string, unknown>;
  };
  await writeFile(
    join(directory, "package.json"),
    JSON.stringify({
      name: "owned-runtime-fixture",
      dependencies: { [patch.name]: patch.version },
    }),
  );
  await writeFile(
    join(directory, "package-lock.json"),
    JSON.stringify({
      lockfileVersion: 3,
      packages: {
        "": { dependencies: { [patch.name]: patch.version } },
        [path]: lock.packages[path],
      },
    }),
  );
  for (const file of patch.files) {
    const filename = join(directory, path, file.file);
    let source = await readFile(filename, "utf8");
    if (createHash("sha256").update(source).digest("hex") === file.patchedSha256) {
      for (const hunk of [...file.replacements].reverse())
        source = source.replace(hunk.after, hunk.before);
    }
    expect(createHash("sha256").update(source).digest("hex")).toBe(file.originalSha256);
    await writeFile(filename, source);
  }
  return directory;
}
function executeProbe<Result>(filename: string, sdk: string, cases?: readonly string[]): Result {
  const execution = spawnSync(
    process.execPath,
    [
      resolve(root, "test/support", filename),
      sdk,
      ...(cases === undefined ? [] : [JSON.stringify(cases)]),
    ],
    {
      encoding: "utf8",
      timeout: 25_000,
      env: { ...process.env, NODE_PATH: resolve(root, "node_modules") },
    },
  );
  expect(execution.status, execution.stderr).toBe(0);
  return JSON.parse(execution.stdout) as Result;
}
function probe(sdk: string, cases?: readonly string[]): ProbeResult {
  return executeProbe("nats-sdk-socket-probe.mjs", sdk, cases);
}
function tlsProbe(sdk: string): TlsProbeResult {
  return executeProbe("nats-sdk-tls-probe.mjs", sdk);
}
function verifyTlsIdentity(proof: TlsProbeResult): void {
  expect(proof.results).toHaveLength(2);
  expect(proof.results.find((item) => item.mode === "dns-match")).toMatchObject({
    connected: true,
    identityConfirmed: true,
  });
  expect(proof.results.find((item) => item.mode === "ip-mismatch")).toMatchObject({
    connected: false,
    hostnameRejected: true,
    identityConfirmed: true,
    pings: 0,
  });
  expect(
    proof.results.every((item) => item.peerCleanupConfirmed && item.teardownFailures.length === 0),
  ).toBe(true);
  expect(proof.unhandledRejections).toEqual([]);
}
it("corrects public handshake sockets and TLS identity without breaking server fallback", async () => {
  const directory = await fixture();
  const sdk = join(directory, "node_modules", patch.name);
  expect(probe(sdk, ["fallback"]).results[0]?.fallbackConfirmed).toBe(true);
  const identityBefore = tlsProbe(sdk);
  expect(identityBefore.results.find((item) => item.mode === "dns-match")?.connected).toBe(true);
  expect(identityBefore.results.find((item) => item.mode === "ip-mismatch")?.connected).toBe(true);
  expect(identityBefore.unhandledRejections).toEqual([]);
  const before = probe(sdk, ["silent", "late-info", "tls-handshake"]);
  expect(
    before.results.every(
      (result) =>
        result.boundedFailure &&
        result.activeBeforeFixtureTeardown === 1 &&
        !result.socketCleanupConfirmed,
    ),
  ).toBe(true);
  expect(before.unhandledRejections).toEqual([]);
  await expect(verifyRuntimeDependencyPatches(directory)).rejects.toThrow(/Missing or unreviewed/u);
  const verified = await applyRuntimeDependencyPatches(directory);
  expect(verified).toEqual([
    { name: patch.name, rationaleUrl: patch.rationaleUrl, paths: [`node_modules/${patch.name}`] },
  ]);
  const after = probe(sdk);
  expect(after.results).toHaveLength(6);
  expect(
    after.results.every(
      (result) =>
        result.boundedFailure &&
        result.socketCleanupConfirmed &&
        result.teardownFailures.length === 0,
    ),
  ).toBe(true);
  expect(after.unhandledRejections).toEqual([]);
  const fallback = probe(sdk, ["fallback"]);
  expect(fallback.results[0]).toMatchObject({
    fallbackConfirmed: true,
    socketCleanupConfirmed: true,
    teardownFailures: [],
  });
  expect(fallback.unhandledRejections).toEqual([]);
  verifyTlsIdentity(tlsProbe(sdk));
  expect(await applyRuntimeDependencyPatches(directory)).toEqual(verified);
}, 60_000);

it("verifies a real Node bundle consumes the corrected public SDK", async () => {
  const directory = await mkdtemp(join(root, ".artifacts/nats-bundle-probe-"));
  temporary.push(directory);
  const entry = join(directory, "entry.ts");
  await writeFile(entry, 'export { connect } from "@nats-io/transport-node";\n');
  await verifyRuntimeDependencyPatches(root);
  await build({
    configFile: false,
    root,
    logLevel: "silent",
    build: {
      ssr: true,
      outDir: directory,
      emptyOutDir: false,
      minify: "esbuild",
      rollupOptions: {
        input: entry,
        output: { format: "cjs", entryFileNames: "probe.cjs", codeSplitting: false },
      },
    },
    ssr: { noExternal: true },
  });
  const result = probe(join(directory, "probe.cjs"), ["silent", "tls-upgrade", "fallback"]);
  expect(
    result.results.every(
      (item) =>
        (item.mode === "fallback" ? item.fallbackConfirmed : item.boundedFailure) &&
        item.socketCleanupConfirmed &&
        item.teardownFailures.length === 0,
    ),
  ).toBe(true);
  expect(result.unhandledRejections).toEqual([]);
  verifyTlsIdentity(tlsProbe(join(directory, "probe.cjs")));
}, 30_000);

it("refuses runtime reclassification, altered source and an unlisted consumer copy", async () => {
  const directory = await fixture();
  const filename = join(directory, "node_modules", patch.name, patch.files[0].file);
  const source = await readFile(filename, "utf8");
  await writeFile(filename, source + "\n// unexpected\n");
  await expect(applyRuntimeDependencyPatches(directory)).rejects.toThrow(/Missing or unreviewed/u);
  await writeFile(filename, source);
  const lockPath = join(directory, "package-lock.json");
  const lock = JSON.parse(await readFile(lockPath, "utf8")) as {
    packages: Record<string, Record<string, unknown>>;
  };
  lock.packages[`node_modules/${patch.name}`]!.dev = true;
  await writeFile(lockPath, JSON.stringify(lock));
  await expect(applyRuntimeDependencyPatches(directory)).rejects.toThrow(/runtime reachability/u);
  delete lock.packages[`node_modules/${patch.name}`]!.dev;
  lock.packages["node_modules/consumer"] = { dependencies: { [patch.name]: patch.version } };
  const consumer = join(directory, "node_modules/consumer");
  await cp(
    join(directory, "node_modules", patch.name),
    join(consumer, "node_modules", patch.name),
    { recursive: true },
  );
  await writeFile(join(consumer, "package.json"), JSON.stringify({ name: "consumer" }));
  await writeFile(lockPath, JSON.stringify(lock));
  await expect(applyRuntimeDependencyPatches(directory)).rejects.toThrow(
    /Unlisted dependency resolution/u,
  );
});
