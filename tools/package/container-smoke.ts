import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { chmod, lstat, mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import { request as httpRequest, type IncomingHttpHeaders } from "node:http";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { BROWSER_DATA_COMPATIBILITY } from "../../src/platform/node/browser-data-compatibility";

import {
  validateBrowserDataPreflight,
  type BrowserDataPreflightEvidence,
  type BrowserInstallerTarget,
} from "./browser-installer-evidence";

interface Reply {
  readonly status: number;
  readonly headers: IncomingHttpHeaders;
  readonly body: string;
}
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
function request(
  port: number,
  path: string,
  value?: unknown,
  cookie?: string,
  origin?: string,
): Promise<Reply> {
  const body = value === undefined ? undefined : JSON.stringify(value);
  return new Promise((resolve, reject) => {
    const request = httpRequest(
      {
        hostname: "127.0.0.1",
        port,
        path,
        method: body === undefined ? "GET" : "POST",
        headers: {
          origin: origin ?? `http://127.0.0.1:${port}`,
          ...(body === undefined
            ? {}
            : { "content-type": "application/json", "content-length": Buffer.byteLength(body) }),
          ...(cookie === undefined ? {} : { cookie }),
        },
      },
      (response) => {
        const parts: Buffer[] = [];
        let size = 0;
        response.on("data", (chunk: Buffer) => {
          size += chunk.length;
          if (size > 4 * 1024 * 1024)
            response.destroy(new Error("Container qualification response limit."));
          else parts.push(chunk);
        });
        response.once("error", reject);
        response.once("end", () =>
          resolve({
            status: response.statusCode ?? 0,
            headers: response.headers,
            body: Buffer.concat(parts).toString("utf8"),
          }),
        );
      },
    );
    request.once("error", reject);
    request.setTimeout(10_000, () =>
      request.destroy(new Error("Container qualification request timeout.")),
    );
    request.end(body);
  });
}
function cookieFrom(reply: Reply): string {
  const cookie = reply.headers["set-cookie"]?.[0];
  assert.ok(cookie !== undefined);
  assert.ok(cookie.includes("HttpOnly") && cookie.includes("SameSite=Strict"));
  return cookie.split(";")[0]!;
}

async function ready(port: number): Promise<void> {
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    try {
      if ((await request(port, "/health")).status === 200) return;
    } catch {
      /* Starting listener. */
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error("Container qualification listener did not become ready.");
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
  const passphrase = `isolated qualification ${randomUUID()}`;
  const secret = `qualification-${randomUUID()}`;
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
    await ready(port);
    assert.equal(record(JSON.parse((await request(port, "/health")).body)).status, "locked");
    assert.equal((await request(port, "/")).status, 303);
    assert.equal((await request(port, "/__streamskope_host/providers/nats/health")).status, 401);
    const setupCode = (await readFile(join(data, "setup-code"), "utf8")).trim();
    assert.equal(
      (
        await request(
          port,
          "/__streamskope_session/create",
          { passphrase, setupCode },
          undefined,
          "http://invalid.example.test",
        )
      ).status,
      403,
    );
    const created = await request(port, "/__streamskope_session/create", { passphrase, setupCode });
    assert.equal(created.status, 200);
    const cookie = cookieFrom(created);
    const health = await request(
      port,
      "/__streamskope_host/providers/nats/health",
      undefined,
      cookie,
    );
    assert.equal(health.status, 200);
    const protocolVersion = record(JSON.parse(health.body)).protocolVersion;
    assert.ok(
      typeof protocolVersion === "number" &&
        Number.isSafeInteger(protocolVersion) &&
        protocolVersion > 0,
    );
    assert.equal((await request(port, "/", undefined, cookie)).status, 200);
    assert.ok(
      (
        await request(port, "/__streamskope_session/browser-runtime.js", undefined, cookie)
      ).body.includes("lockVault"),
    );
    const command = async (
      name: string,
      payload: unknown,
      selectedCookie: string,
    ): Promise<Record<string, unknown>> => {
      const reply = await request(
        port,
        "/__streamskope_host/providers/nats/commands",
        {
          id: randomUUID(),
          version: protocolVersion,
          command: name,
          payload,
        },
        selectedCookie,
      );
      assert.equal(reply.status, 200);
      const body = record(JSON.parse(reply.body));
      assert.equal(body.ok, true);
      return body;
    };
    const createdProfile = await command(
      "profiles.create",
      {
        profile: {
          name: "Disposable container qualification",
          servers: ["nats://remote.example.test:4222"],
          authentication: { mode: "token", token: { mode: "replace", value: secret } },
          tls: { mode: "plaintext" },
        },
      },
      cookie,
    );
    assert.ok(!JSON.stringify(createdProfile).includes(secret));
    for (const filename of ["vault.json", "nats-profiles.json"]) {
      const raw = await readFile(join(data, filename), "utf8");
      assert.ok(!raw.includes(secret) && !raw.includes(passphrase));
    }
    assert.equal((await request(port, "/__streamskope_session/lock", {}, cookie)).status, 200);
    assert.equal(
      (await request(port, "/__streamskope_host/providers/nats/health", undefined, cookie)).status,
      401,
    );
    assert.equal(
      (
        await request(port, "/__streamskope_session/unlock", {
          passphrase: "wrong qualification passphrase",
        })
      ).status,
      401,
    );
    const unlocked = await request(port, "/__streamskope_session/unlock", { passphrase });
    assert.equal(unlocked.status, 200);
    const restored = await command("profiles.list", {}, cookieFrom(unlocked));
    assert.ok(JSON.stringify(restored).includes("Disposable container qualification"));
    assert.ok(!JSON.stringify(restored).includes(secret));
    const worker = docker(
      ["exec", "--interactive", container, "node"],
      `
      const {Worker}=require('node:worker_threads'); const assert=require('node:assert/strict'); const fs=require('node:fs');
      (async()=>{
        const run=(name,workerData)=>new Promise((resolve,reject)=>{const w=new Worker(process.cwd()+'/dist/web/'+name+'.cjs',{workerData});w.once('message',resolve);w.once('error',reject);});
        const trust=await run('trust-material-worker',{kind:'jks',material:fs.readFileSync('node_modules/jks-js/examples/assets/truststore.jks').toString('base64'),password:'password'});assert.equal(trust.ok,true);
        const decoded=await run('record-codec-worker',{input:{format:'json',bytes:Buffer.from('{"nativeImage":true}').toString('base64')},bundle:null});assert.equal(decoded.state,'decoded');assert.equal(JSON.parse(decoded.json).nativeImage,true);
        console.log('native-workers-qualified');
      })().catch(()=>{process.exitCode=1;});
    `,
    );
    assert.equal(worker, "native-workers-qualified");
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
    await ready(port);
    assert.equal(record(JSON.parse((await request(port, "/health")).body)).status, "locked");
    const restarted = await request(port, "/__streamskope_session/unlock", { passphrase });
    assert.equal(restarted.status, 200);
    assert.ok(
      JSON.stringify(await command("profiles.list", {}, cookieFrom(restarted))).includes(
        "Disposable container qualification",
      ),
    );
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
