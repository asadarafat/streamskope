import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { chmod, lstat, mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { BROWSER_DATA_COMPATIBILITY } from "../../src/platform/node/browser-data-compatibility";

import { createBrowserVaultFixture, verifyBrowserNativeWorkers } from "./browser-vault-fixture";
import {
  validateBrowserDataPreflight,
  type BrowserDataPreflightEvidence,
  type BrowserInstallerTarget,
} from "./browser-installer-evidence";

export interface BrowserContainerInstance {
  readonly container: string;
  readonly data: string;
  readonly port: number;
  readonly uid: number;
  readonly gid: number;
  /** Lets installer qualification resume the stopped instance through its own lifecycle. */
  readonly restart?: () => Promise<void>;
}
function docker(args: readonly string[], input?: string): string {
  const result = spawnSync("docker", [...args], {
    encoding: "utf8",
    maxBuffer: 4 * 1024 * 1024,
    ...(input === undefined ? {} : { input }),
  });
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) throw new Error("Container qualification Docker operation failed.");
  return result.stdout.trim();
}
function record(value: unknown): Record<string, unknown> {
  assert.ok(value !== null && typeof value === "object" && !Array.isArray(value));
  return value as Record<string, unknown>;
}

function inspected(value: string): Record<string, unknown> {
  const records: unknown = JSON.parse(value);
  assert.ok(Array.isArray(records) && records.length === 1);
  return record(records[0]);
}

function verifyImage(image: string, expected: BrowserInstallerTarget): void {
  const actual = inspected(docker(["image", "inspect", image]));
  const labels = record(record(actual.Config).Labels);
  assert.equal(actual.Id, expected.imageId);
  assert.equal(`${String(actual.Os)}/${String(actual.Architecture)}`, expected.platform);
  assert.equal(labels["org.opencontainers.image.version"], expected.version);
  assert.equal(labels["org.opencontainers.image.revision"], expected.sourceRevision);
  assert.match(expected.sourceRevision, /^[a-f0-9]{40}$/u);
  assert.match(expected.imageId, /^sha256:[a-f0-9]{64}$/u);
}

/** Hash the entire disposable fixture, including directories, permissions and ownership. */
async function snapshotData(root: string): Promise<string> {
  const hash = createHash("sha256");
  let entries = 0;
  let bytes = 0;
  const visit = async (relative: string): Promise<void> => {
    const path = join(root, relative);
    const metadata = await lstat(path);
    assert.ok(++entries <= 512, "Disposable preflight fixture has excessive entries.");
    assert.ok(metadata.isDirectory() || (metadata.isFile() && metadata.nlink === 1));
    let content: string | null = null;
    if (metadata.isFile()) {
      bytes += metadata.size;
      assert.ok(bytes <= 16 * 1024 * 1024, "Disposable preflight fixture exceeds its bound.");
      content = createHash("sha256")
        .update(await readFile(path))
        .digest("hex");
    }
    hash.update(
      JSON.stringify([relative, metadata.mode, metadata.uid, metadata.gid, metadata.size, content]),
    );
    if (metadata.isDirectory())
      for (const name of (await readdir(path)).sort()) await visit(join(relative, name));
  };
  await visit(".");
  return hash.digest("hex");
}

async function inspectStoppedData(
  data: string,
  uid: number,
  gid: number,
  expected: BrowserInstallerTarget,
): Promise<BrowserDataPreflightEvidence> {
  const before = await snapshotData(data);
  const name = `streamskope-preflight-${randomUUID()}`;
  let output: string;
  try {
    const result = spawnSync(
      "docker",
      [
        "run",
        "--pull",
        "never",
        "--rm",
        "--name",
        name,
        "--label",
        `io.streamskope.preflight=${name}`,
        "--network",
        "none",
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges:true",
        "--user",
        `${uid}:${gid}`,
        "--mount",
        `type=bind,src=${data},dst=/data,readonly`,
        "--entrypoint",
        "node",
        expected.imageId,
        BROWSER_DATA_COMPATIBILITY.inspector,
        "/data",
      ],
      { encoding: "utf8", timeout: 60_000, maxBuffer: 64 * 1024 },
    );
    // Do not copy arbitrary inspector stderr or malformed document bytes into the report.
    assert.ok(result.error === undefined && result.status === 0, "Native data preflight failed.");
    assert.equal(result.stderr, "", "Native data preflight must produce only its safe report.");
    output = result.stdout;
  } finally {
    const remaining = spawnSync("docker", ["inspect", name], {
      encoding: "utf8",
      timeout: 10_000,
      maxBuffer: 64 * 1024,
    });
    if (remaining.status === 0) {
      const owner = inspected(remaining.stdout);
      assert.equal(owner.Image, expected.imageId);
      assert.equal(record(record(owner.Config).Labels)["io.streamskope.preflight"], name);
      docker(["rm", "--force", String(owner.Id)]);
    } else
      assert.ok(
        /No such object/u.test(remaining.stderr),
        "Preflight process cleanup is unconfirmed.",
      );
  }
  assert.equal(await snapshotData(data), before, "Data preflight changed the disposable data.");
  let inspection: unknown;
  try {
    inspection = JSON.parse(output);
  } catch {
    throw new Error("Native data preflight returned malformed structured evidence.");
  }
  return validateBrowserDataPreflight(
    { imageId: expected.imageId, dataSnapshotSha256: before, inspection },
    expected,
  );
}
async function availablePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  assert.ok(address !== null && typeof address !== "string");
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
  return address.port;
}
/**
 * Exercises a fresh disposable vault, never operator data. Supplied instances remain
 * caller-owned: qualification restarts them but never removes their container or data.
 */
export async function verifyBrowserContainer(
  image: string,
  expected: BrowserInstallerTarget,
  instance?: BrowserContainerInstance,
): Promise<BrowserDataPreflightEvidence> {
  if (process.platform !== "linux" || process.getuid === undefined || process.getgid === undefined)
    throw new Error("Container qualification requires a Linux Docker host.");
  if (instance !== undefined) {
    assert.ok(instance.container.length > 0 && instance.data.startsWith("/"));
    assert.ok(Number.isInteger(instance.port) && instance.port > 0 && instance.port <= 65_535);
  }
  const uid = instance?.uid ?? process.getuid();
  const gid = instance?.gid ?? process.getgid();
  assert.ok(Number.isSafeInteger(uid) && uid > 0 && Number.isSafeInteger(gid) && gid >= 0);
  verifyImage(image, expected);
  const data =
    instance?.data ?? (await mkdtemp(join(tmpdir(), "streamskope-container-qualification-")));
  if (instance === undefined) await chmod(data, 0o700);
  const port = instance?.port ?? (await availablePort());
  const origin = `http://127.0.0.1:${port}`;
  let container: string | undefined = instance?.container;
  try {
    if (instance === undefined) {
      container = docker([
        "run",
        "--detach",
        "--name",
        `streamskope-qualification-${randomUUID()}`,
        "--user",
        `${uid}:${gid}`,
        "--security-opt",
        "no-new-privileges:true",
        "--cap-drop",
        "ALL",
        "--memory",
        "1g",
        "--cpus",
        "1",
        "--publish",
        `127.0.0.1:${port}:8080`,
        "--env",
        `STREAMSKOPE_PUBLIC_ORIGIN=${origin}`,
        "--mount",
        `type=bind,src=${data},dst=/data`,
        expected.imageId,
      ]);
      assert.match(container, /^[0-9a-f]{64}$/u);
    }
    assert.ok(container !== undefined);
    const running = inspected(docker(["inspect", container]));
    assert.equal(running.Image, expected.imageId);
    assert.equal(record(running.Config).User, `${uid}:${gid}`);
    assert.equal(typeof running.Id, "string");
    container = running.Id as string;
    const fixture = await createBrowserVaultFixture({ port, data });
    verifyBrowserNativeWorkers(container);
    docker(["stop", "--time", "120", container]);
    const stopped = inspected(docker(["inspect", container]));
    assert.equal(stopped.Image, expected.imageId);
    const state = record(stopped.State);
    assert.equal(state.Running, false);
    assert.equal(state.OOMKilled, false);
    assert.equal(state.ExitCode, 0, "Gateway must confirm graceful runtime cleanup.");
    const preflight = await inspectStoppedData(data, uid, gid, expected);
    if (instance?.restart === undefined) docker(["start", container]);
    else await instance.restart();
    await fixture.unlockAfterReplacement();
    process.stdout.write(
      "Container qualification passed: authenticated gateway, encrypted profile persistence, native workers, read-only data preflight and graceful restart.\n",
    );
    return preflight;
  } finally {
    if (instance === undefined && container !== undefined) {
      // Never remove persistent files before their owning process has stopped.
      docker(["stop", "--time", "120", container]);
      const stopped = inspected(docker(["inspect", container]));
      const state = record(stopped.State);
      assert.ok(
        stopped.Id === container &&
          stopped.Image === expected.imageId &&
          state.Running === false &&
          state.OOMKilled === false &&
          state.ExitCode === 0,
        "Disposable gateway cleanup is unconfirmed; its container and data were retained.",
      );
      docker(["rm", container]);
    }
    if (instance === undefined) await rm(data, { recursive: true, force: true });
  }
}
