import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { createReadStream } from "node:fs";
import { access, chmod, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { delimiter, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { prepareBrowserReleaseAssets } from "./browser-release";
import { BROWSER_INSTALLER_NAME } from "./browser-installer";
import { parseBrowserRegistryMetadata } from "./browser-registry-metadata";
import { verifyBrowserContainer } from "./container-smoke";
import {
  browserInstallerEvidence,
  type BrowserDataPreflightEvidence,
} from "./browser-installer-evidence";

interface CommandResult {
  readonly code: number;
  readonly stdout: string;
  readonly stderr: string;
}

/** Keep private browser secrets out of command arguments and qualification output. */
function run(
  command: string,
  args: readonly string[],
  environment = process.env,
  allowFailure = false,
  timeout = 12 * 60_000,
): Promise<CommandResult> {
  return new Promise((accept, reject) => {
    // The installer must not inherit a controlling terminal that could disclose
    // its fresh setup code into a recorded qualification session.
    const child = spawn(command, [...args], {
      detached: true,
      env: environment,
      stdio: ["ignore", "pipe", "pipe"],
    });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    let bytes = 0;
    let failed = false;
    const fail = (message: string): void => {
      if (failed) return;
      failed = true;
      clearTimeout(timer);
      if (child.pid !== undefined) {
        try {
          process.kill(-child.pid, "SIGKILL");
        } catch {
          // The command may have ended just before its execution bound expired.
        }
      }
      reject(new Error(message));
    };
    const capture = (parts: Buffer[], chunk: Buffer): void => {
      bytes += chunk.length;
      if (bytes > 2 * 1024 * 1024)
        fail("Installer qualification command exceeded its output bound.");
      else parts.push(chunk);
    };
    child.stdout.on("data", (chunk: Buffer) => capture(stdout, chunk));
    child.stderr.on("data", (chunk: Buffer) => capture(stderr, chunk));
    const timer = setTimeout(
      () => fail("Installer qualification command exceeded its execution bound."),
      timeout,
    );
    child.once("error", () => fail("Installer qualification command could not start."));
    child.once("close", (code) => {
      clearTimeout(timer);
      if (failed) return;
      const result = {
        code: code ?? 1,
        stdout: Buffer.concat(stdout).toString("utf8"),
        stderr: Buffer.concat(stderr).toString("utf8"),
      };
      if (result.code !== 0 && !allowFailure)
        reject(new Error(`Installer qualification command failed: ${result.stderr.trim()}`));
      else accept(result);
    });
  });
}

function replaceConstant(source: string, name: string, value: string): string {
  assert.ok(!value.includes("'") && !value.includes("\n"));
  const expression = new RegExp(`^${name}='[^']*'$`, "mu");
  assert.ok(expression.test(source), `Installer qualification requires one ${name} constant.`);
  return source.replace(expression, `${name}='${value}'`);
}

/** Qualify staged release delivery with the real public image and native host. */
export async function verifyBrowserInstaller(
  staging: string,
  version: string,
  sourceRevision: string,
  registryReceipt: string,
): Promise<void> {
  const startedAt = new Date().toISOString();
  if (process.platform !== "linux" || process.getuid?.() !== 0)
    throw new Error("Installer qualification requires sudo on a native Linux Docker host.");
  const uid = Number(process.env.SUDO_UID);
  const gid = Number(process.env.SUDO_GID);
  if (!Number.isSafeInteger(uid) || uid <= 0 || !Number.isSafeInteger(gid) || gid < 0)
    throw new Error("Run installer qualification through an existing non-root sudo account.");
  const architecture = process.arch === "x64" ? "amd64" : process.arch === "arm64" ? "arm64" : null;
  if (
    architecture === null ||
    (process.env.EXPECTED_ARCH !== undefined && process.env.EXPECTED_ARCH !== architecture)
  )
    throw new Error("Installer qualification requires the selected native AMD64 or ARM64 host.");
  const evidenceRoot = resolve(".artifacts/ci");
  const evidenceFile = join(evidenceRoot, `browser-installer-${architecture}.json`);
  await rm(evidenceFile, { force: true });
  const registry = parseBrowserRegistryMetadata(
    JSON.parse(await readFile(registryReceipt, "utf8")),
    version,
    sourceRevision,
  );
  const temporary = await mkdtemp(join(tmpdir(), "streamskope-installer-qualification-"));
  await chmod(temporary, 0o700);
  const lab = `streamskope-installer-${randomUUID().replaceAll("-", "").slice(0, 12)}`;
  const container = `clab-${lab}-app`;
  const network = `${lab}-mgmt`;
  const state = join(temporary, "deployment");
  const data = join(state, "streamskope-data");
  const topology = `streamskope-${version}.clab.yml`;
  const topologyPath = join(state, topology);
  const output = join(temporary, "release-assets");
  const installer = join(temporary, BROWSER_INSTALLER_NAME);
  const fixtureCalls = join(temporary, "release-downloads.jsonl");
  const originalDockerConfig = process.env.DOCKER_CONFIG;
  const originalDockerHost = process.env.DOCKER_HOST;
  const originalDockerContext = process.env.DOCKER_CONTEXT;
  let cleaned = false;
  let installationStarted = false;
  let preflight: BrowserDataPreflightEvidence | undefined;
  try {
    // Empty credentials ensure this exercises public GHCR delivery, rather than
    // silently borrowing the maintainer's or release publisher's registry login.
    process.env.DOCKER_CONFIG = join(temporary, "docker-config");
    process.env.DOCKER_HOST = "unix:///var/run/docker.sock";
    process.env.DOCKER_CONTEXT = "default";
    await mkdir(process.env.DOCKER_CONFIG, { mode: 0o700 });
    await prepareBrowserReleaseAssets(
      staging,
      output,
      version,
      sourceRevision,
      "streamskope.clab.yml",
      registryReceipt,
    );
    const names = (await readdir(output)).sort();
    assert.ok(names.includes(BROWSER_INSTALLER_NAME));
    const checksums: string[] = [];
    for (const name of names) {
      const hash = createHash("sha256");
      for await (const chunk of createReadStream(join(output, name))) hash.update(chunk as Buffer);
      checksums.push(`${hash.digest("hex")}  ${name}`);
    }
    await writeFile(join(output, "SHA256SUMS"), `${checksums.join("\n")}\n`, { mode: 0o600 });
    const realCurl = (await run("sh", ["-c", "command -v curl"])).stdout.trim();
    assert.ok(realCurl.startsWith("/"));
    const shim = join(temporary, "delivery");
    await mkdir(shim, { mode: 0o700 });
    const downloadRoot = `https://github.com/asadarafat/streamskope/releases/download/v${version}/`;
    const deliveries = Object.fromEntries(
      ["SHA256SUMS", topology, `streamskope-${version}-container.json`].map((name) => [
        downloadRoot + name,
        join(output, name),
      ]),
    );
    // Only unpublished release-file transport is substituted. GHCR pulls,
    // Containerlab deployment, gateway HTTP, and browser storage stay real.
    await writeFile(
      join(shim, "curl"),
      `#!/usr/bin/env python3
import json,os,shutil,sys
deliveries=json.loads(${JSON.stringify(JSON.stringify(deliveries))})
args=sys.argv[1:]
matches=[argument for argument in args if argument in deliveries]
if not matches:
    os.execv(${JSON.stringify(realCurl)},[${JSON.stringify(realCurl)}]+args)
if len(matches)!=1:
    sys.exit('Qualification delivery requires one exact release asset URL.')
selected=matches[0]
outputs=[args[index+1] for index,argument in enumerate(args[:-1]) if argument in ['--output','-o']]
if len(outputs)!=1:
    sys.exit('Qualification delivery requires an explicit output file.')
shutil.copyfile(deliveries[selected],outputs[0])
with open(${JSON.stringify(fixtureCalls)},'a',encoding='utf8') as receipt:
    receipt.write(json.dumps(selected)+'\\n')
`,
      { mode: 0o700 },
    );
    let script = await readFile(join(output, BROWSER_INSTALLER_NAME), "utf8");
    script = replaceConstant(script, "INSTALL_ROOT", state);
    script = replaceConstant(script, "LAB_NAME", lab);
    script = replaceConstant(script, "CONTAINER_NAME", container);
    script = replaceConstant(script, "NETWORK_NAME", network);
    await writeFile(installer, script, { mode: 0o700 });
    const environment = { ...process.env, PATH: [shim, process.env.PATH ?? ""].join(delimiter) };
    const install = async (): Promise<CommandResult> => run("bash", [installer], environment);
    const priorNetwork = await run(
      "docker",
      ["--host", "unix:///var/run/docker.sock", "network", "inspect", network],
      process.env,
      true,
      30_000,
    );
    assert.ok(
      priorNetwork.code !== 0 && /No such network|not found/u.test(priorNetwork.stderr),
      "Qualification network must not already exist.",
    );
    installationStarted = true;
    const first = await install();
    // Repeated local qualification may already have this exact image cached.
    // An explicit credential-free pull still establishes its public availability.
    await run("docker", ["--host", "unix:///var/run/docker.sock", "pull", registry.reference]);
    const setupCode = (await readFile(join(data, "setup-code"), "utf8")).trim();
    assert.ok(
      !first.stdout.includes(setupCode) && !first.stderr.includes(setupCode),
      "Installer qualification must not disclose the setup code.",
    );
    const installation = await readFile(join(state, "installation.json"));
    const saved = JSON.parse(installation.toString("utf8")) as {
      uid: number;
      gid: number;
      port: number;
      version: string;
      sourceRevision: string;
    };
    assert.equal(saved.uid, uid);
    assert.equal(saved.gid, gid);
    assert.equal(saved.version, version);
    assert.equal(saved.sourceRevision, sourceRevision);
    assert.ok(first.stdout.includes(`http://127.0.0.1:${saved.port}`));
    assert.deepEqual(await readFile(topologyPath), await readFile(join(output, topology)));
    const delivered = (await readFile(fixtureCalls, "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as string);
    assert.deepEqual([...new Set(delivered)].sort(), Object.keys(deliveries).sort());
    preflight = await verifyBrowserContainer(
      registry.reference,
      {
        version,
        sourceRevision,
        platform: `linux/${architecture}`,
        image: registry.reference,
        imageId: registry.platforms.find((item) => item.platform === `linux/${architecture}`)!
          .imageId,
      },
      {
        container,
        data,
        port: saved.port,
        uid,
        gid,
        restart: async () => {
          const vault = await readFile(join(data, "vault.json"));
          const profiles = await readFile(join(data, "nats-profiles.json"));
          const resumed = await install();
          assert.ok(resumed.stdout.includes("Unlock"));
          assert.deepEqual(await readFile(join(state, "installation.json")), installation);
          assert.deepEqual(await readFile(join(data, "vault.json")), vault);
          assert.deepEqual(await readFile(join(data, "nats-profiles.json")), profiles);
        },
      },
    );
    const vault = await readFile(join(data, "vault.json"));
    const profiles = await readFile(join(data, "nats-profiles.json"));
    const repeated = await install();
    assert.ok(repeated.stdout.includes("Unlock"));
    assert.deepEqual(await readFile(join(state, "installation.json")), installation);
    assert.deepEqual(await readFile(join(data, "vault.json")), vault);
    assert.deepEqual(await readFile(join(data, "nats-profiles.json")), profiles);
  } finally {
    try {
      if (!installationStarted) {
        cleaned = true;
      } else {
        const inspection = await run(
          "docker",
          ["--host", "unix:///var/run/docker.sock", "inspect", container],
          process.env,
          true,
          30_000,
        );
        if (inspection.code === 0) {
          const [value] = JSON.parse(inspection.stdout) as [
            { Config: { Labels: Record<string, string> } },
          ];
          assert.equal(value.Config.Labels.containerlab, lab);
          assert.equal(value.Config.Labels["clab-topo-file"], topologyPath);
          await run("docker", [
            "--host",
            "unix:///var/run/docker.sock",
            "stop",
            "--time",
            "120",
            container,
          ]);
          const stopped = await run("docker", [
            "--host",
            "unix:///var/run/docker.sock",
            "inspect",
            "--format",
            "{{.State.ExitCode}}",
            container,
          ]);
          assert.equal(
            stopped.stdout.trim(),
            "0",
            "Qualification cleanup must confirm graceful shutdown.",
          );
        } else {
          assert.ok(
            inspection.stderr.includes("No such object"),
            "Qualification cleanup could not establish that its container is absent.",
          );
        }
        if (
          await access(topologyPath).then(
            () => true,
            () => false,
          )
        ) {
          const clab = (
            await run("sh", ["-c", "command -v containerlab || command -v clab"])
          ).stdout.trim();
          await run(
            clab,
            [
              "--runtime",
              "docker",
              "destroy",
              "--topo",
              topologyPath,
              "--name",
              lab,
              "--cleanup",
              "--graceful",
              "--keep-mgmt-net",
              "--max-workers",
              "1",
            ],
            process.env,
            false,
            3 * 60_000,
          );
        }
        const inspectedNetwork = await run(
          "docker",
          ["--host", "unix:///var/run/docker.sock", "network", "inspect", network],
          process.env,
          true,
          30_000,
        );
        if (inspectedNetwork.code === 0) {
          const [value] = JSON.parse(inspectedNetwork.stdout) as [{ Name: string }];
          assert.equal(value.Name, network);
          await run("docker", ["--host", "unix:///var/run/docker.sock", "network", "rm", network]);
        } else
          assert.ok(
            /No such network|not found/u.test(inspectedNetwork.stderr),
            "Qualification cleanup could not establish that its network is absent.",
          );
        cleaned = true;
      }
    } finally {
      if (cleaned) await rm(temporary, { recursive: true, force: true });
      for (const [key, value] of [
        ["DOCKER_CONFIG", originalDockerConfig],
        ["DOCKER_HOST", originalDockerHost],
        ["DOCKER_CONTEXT", originalDockerContext],
      ] as const) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  }
  assert.ok(preflight !== undefined, "Native data preflight must finish before publication.");
  const evidence = browserInstallerEvidence({
    version,
    sourceRevision,
    platform: `linux/${architecture}`,
    image: registry.reference,
    startedAt,
    preflight,
  });
  await mkdir(evidenceRoot, { recursive: true });
  await writeFile(evidenceFile, `${JSON.stringify(evidence, null, 2)}\n`, { mode: 0o644 });
  process.stdout.write(`Native ${architecture} browser installer qualification passed.\n`);
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [staging, version, sourceRevision, registryReceipt, ...extra] = process.argv.slice(2);
  if (!staging || !version || !sourceRevision || !registryReceipt || extra.length)
    throw new Error(
      "Usage: browser-installer-smoke.ts <native-archive-staging> <version> <source-commit> <registry-receipt>",
    );
  void verifyBrowserInstaller(staging, version, sourceRevision, registryReceipt).catch(
    (error: unknown) => {
      process.stderr.write(
        `Installer qualification failed: ${error instanceof Error ? error.message : String(error)}\n`,
      );
      process.exitCode = 1;
    },
  );
}
