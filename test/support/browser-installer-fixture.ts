import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { chmod, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

import { renderBrowserWorkbenchInstaller } from "../../tools/package/browser-installer";
import { browserRegistryTopology } from "../../tools/package/browser-release";

const execute = promisify(execFile);
export const INSTALL_SOURCE = "a".repeat(40);
export const INSTALL_DIGEST = `sha256:${"b".repeat(64)}`;
export const INSTALL_SETUP_CODE = "owner-only-fixture-setup-code".padEnd(43, "x");
const owner = { uid: process.getuid?.() ?? 1000, gid: process.getgid?.() ?? 1000 };
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const quote = (value: string): string => `'${value.replaceAll("'", "'\\''")}'`;

export interface InstallerControl {
  architecture?: "x86_64" | "aarch64";
  corruptDownload?: string;
  failedDownload?: string;
  failedDeploy?: boolean;
  setupRequired?: boolean;
  container?: Record<string, unknown> | undefined;
  firstPort?: number;
  missingDocker?: boolean;
  installedPackages?: string[];
}

interface InstallerArtifact {
  file: string;
  version: string;
  image: string;
  topology: string;
  manifest: Record<string, unknown>;
}
interface InstallerResult {
  stdout: string;
  stderr: string;
  exitCode: number;
}
interface Command {
  command: string;
  args: string[];
}
export interface BrowserInstallerFixture {
  root: string;
  state: string;
  data: string;
  owner: { uid: number; gid: number };
  installer(
    version?: string,
    metadata?: (value: Record<string, unknown>) => void,
  ): Promise<InstallerArtifact>;
  run(file: string, args?: string[]): Promise<InstallerResult>;
  calls(): Promise<Command[]>;
  control(changes: InstallerControl): Promise<void>;
  cleanup(): Promise<void>;
}

const commandMock = String.raw`
import { appendFileSync, existsSync, readFileSync, writeFileSync, mkdirSync, chmodSync } from "node:fs";
import { basename, join } from "node:path";
const root = process.env.INSTALL_FIXTURE_ROOT;
if (!root || !root.startsWith("/tmp/")) throw new Error("Missing isolated installer fixture.");
const command = basename(process.argv[1]);
let args = process.argv.slice(2);
const controlPath = join(root, "control.json");
const control = JSON.parse(readFileSync(controlPath, "utf8"));
const stateRoot = join(root, "state");
appendFileSync(join(root, "calls.jsonl"), JSON.stringify({ command, args }) + "\n");
const save = () => writeFileSync(controlPath, JSON.stringify(control));
const out = value => process.stdout.write(typeof value === "string" ? value + "\n" : JSON.stringify(value) + "\n");
const fail = (message, code = 1) => { process.stderr.write(message + "\n"); process.exit(code); };
if (command === "id") {
  if (args.includes("-u")) out("0");
  else if (args.includes("-g")) out(process.env.SUDO_GID);
  else fail("Unexpected fixture id invocation.");
} else if (command === "uname") {
  out(args.includes("-m") ? control.architecture || "x86_64" : "Linux");
} else if (command === "getent") {
  if (args[0] === "passwd" && args[1] === process.env.SUDO_UID)
    out("operator:x:" + process.env.SUDO_UID + ":" + process.env.SUDO_GID + ":fixture:" + join(root, "operator") + ":/bin/bash");
  else fail("Fixture has no requested system account.", 2);
} else if (command === "dpkg") {
  if (args.length !== 1 || args[0] !== "--print-architecture") fail("Unexpected fixture dpkg invocation.");
  out(control.architecture === "aarch64" ? "arm64" : "amd64");
} else if (command === "dpkg-query") {
  const installed = args.filter(value => !value.startsWith("-") && control.installedPackages?.includes(value));
  if (!installed.length) process.exit(1);
  const format = args.find(value => value.startsWith("-f")) || "";
  if (format.includes("Status-Abbrev")) out(installed.map(() => "ii ").join("\n"));
  else if (format.includes("binary:Package")) out(installed.join("\n"));
  else if (format.includes("Status")) out(installed.map(() => "install ok installed").join("\n"));
  else fail("Unexpected fixture package-query format.");
} else if (command === "docker") {
  if (args[0] === "--host" || args[0] === "-H") args = args.slice(2);
  if (args[0] === "context") out("\"unix:///var/run/docker.sock\"");
  else if (args[0] === "info") out({ OSType: "linux", Architecture: control.architecture === "aarch64" ? "aarch64" : "x86_64", ServerVersion: "29.0.1" });
  else if (args[0] === "version" || args.includes("--version")) out("Docker version 29.0.1");
  else if (args[0] === "image" && args[1] === "inspect") {
    const version = args.at(-1).match(/streamskope:([^@]+)/)?.[1];
    const architecture = control.architecture === "aarch64" ? "arm64" : "amd64";
    out([{ Id: "sha256:" + (architecture === "arm64" ? "4" : "3").repeat(64), Os: "linux", Architecture: architecture, Config: { Labels: { "org.opencontainers.image.version": version, "org.opencontainers.image.revision": "${INSTALL_SOURCE}" } }, RepoDigests: ["ghcr.io/asadarafat/streamskope@${INSTALL_DIGEST}"] }]);
  }
  else if (args[0] === "container" && args[1] === "ls") { if (control.container) out(control.container.Id || "fixture-container"); }
  else if (args[0] === "inspect") {
    if (!control.container) fail("No such container: clab-streamskope-app");
    out([control.container]);
  } else if (args[0] === "start") {
    if (!control.container) fail("Cannot start an absent fixture container.");
    control.container.State.Running = true;
    control.container.State.Status = "running";
    save();
    out("clab-streamskope-app");
  } else if (args[0] === "ps") { if (control.container) out("clab-streamskope-app"); }
  else if (args[0] === "pull") out("Pulled fixture image.");
  else fail("Unexpected fixture docker invocation: " + args.join(" "));
} else if (command === "containerlab" || command === "clab") {
  if (args.includes("version") || args.includes("--version")) out("0.79.1");
  else if (args.includes("deploy")) {
    if (control.failedDeploy) fail("Fixture deployment interrupted.");
    const flag = args.findIndex(value => value === "--topo" || value === "-t" || value === "--topology");
    if (flag < 0) fail("Deployment must provide its pinned topology.");
    const topology = readFileSync(args[flag + 1], "utf8");
    const image = topology.match(/ghcr\.io\/asadarafat\/streamskope:[^\s}\"']+/)?.[0];
    if (!image || !image.includes("@sha256:")) fail("Fixture deployment received an unpinned image.");
    const data = join(stateRoot, "streamskope-data");
    mkdirSync(data, { recursive: true, mode: 0o700 });
    const port = String(process.env.STREAMSKOPE_HOST_PORT || "8080");
    const version = image.match(/streamskope:([^@]+)/)[1];
    control.container = {
      Name: "/clab-streamskope-app", Id: "fixture-container", Image: "sha256:" + (control.architecture === "aarch64" ? "4" : "3").repeat(64),
      Config: { Image: image, User: process.env.SUDO_UID + ":" + process.env.SUDO_GID,
        Env: ["STREAMSKOPE_PUBLIC_ORIGIN=http://127.0.0.1:" + port],
        Labels: { "containerlab": "streamskope", "clab-node-name": "app", "clab-topo-file": args[flag + 1], "io.streamskope.deployment": "browser", "org.opencontainers.image.version": version, "org.opencontainers.image.revision": "${INSTALL_SOURCE}" } },
      State: { Running: true, Status: "running" },
      Mounts: [{ Type: "bind", Source: data, Destination: "/data", RW: true }],
      HostConfig: { Privileged: false, PortBindings: { "8080/tcp": [{ HostIp: "127.0.0.1", HostPort: port }] } },
      NetworkSettings: { Ports: { "8080/tcp": [{ HostIp: "127.0.0.1", HostPort: port }] } }
    };
    if (!existsSync(join(data, "vault.json"))) {
      writeFileSync(join(data, "setup-code"), "${INSTALL_SETUP_CODE}\n", { mode: 0o600 });
      chmodSync(join(data, "setup-code"), 0o600);
    }
    save();
    out("Fixture deployment completed.");
  } else fail("Unexpected fixture Containerlab invocation: " + args.join(" "));
} else if (command === "curl") {
  const url = args.find(value => /^https?:\/\//.test(value));
  if (!url) fail("Fixture curl requires an explicit URL.");
  const parsed = new URL(url);
  let contents;
  if (parsed.hostname === "127.0.0.1") {
    contents = parsed.pathname === "/health" ? JSON.stringify({ status: "locked" }) : JSON.stringify({ state: "locked", setupRequired: control.setupRequired !== false && !existsSync(join(stateRoot, "streamskope-data/vault.json")) });
  } else if (parsed.hostname === "github.com") {
    const file = basename(parsed.pathname);
    if (control.failedDownload === file) fail("Fixture HTTP 404", 22);
    const release = parsed.pathname.match(/\/download\/v([^/]+)\//)?.[1];
    if (!release) fail("Fixture requires immutable release downloads.");
    const asset = join(root, "assets", release, file);
    if (!existsSync(asset)) fail("Unexpected release asset: " + file, 22);
    contents = readFileSync(asset);
    if (control.corruptDownload === file) contents = Buffer.concat([Buffer.from(contents), Buffer.from("corruption")]);
  } else fail("Unexpected network endpoint: " + url);
  const output = args.findIndex(value => value === "--output" || value === "-o");
  if (output >= 0) {
    const destination = args[output + 1];
    if (!destination.startsWith(root + "/")) fail("Fixture download escaped its temporary directory.");
    writeFileSync(destination, contents);
  } else process.stdout.write(contents);
} else {
  fail("Host mutation prohibited in installer fixture: " + command);
}
`;

/** Runs the real installer with isolated OS/state paths and bounded command substitutes. */
export async function browserInstallerFixture(
  control: InstallerControl = {},
): Promise<BrowserInstallerFixture> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-installer-"));
  const bin = join(root, "bin");
  await mkdir(bin);
  await mkdir(join(root, "operator"));
  await writeFile(join(root, "control.json"), JSON.stringify(control));
  await writeFile(join(root, "calls.jsonl"), "");
  await writeFile(
    join(root, "os-release"),
    'ID=ubuntu\nVERSION_ID="24.04"\nVERSION_CODENAME=noble\n',
  );
  const mock = join(root, "mock-command.mjs");
  await writeFile(mock, `#!${process.execPath}\n${commandMock}`);
  await chmod(mock, 0o755);
  for (const name of [
    "id",
    "uname",
    "getent",
    "docker",
    "containerlab",
    "clab",
    "curl",
    "apt-get",
    "dpkg-query",
    "dpkg",
    "systemctl",
    "useradd",
    "sudo",
  ]) {
    if (name === "docker" && control.missingDocker) continue;
    await symlink(mock, join(bin, name));
  }
  // Only these harmless utilities can execute; host package/service/network tools stay mocked.
  for (const name of [
    "bash",
    "python3",
    "mkdir",
    "rmdir",
    "mktemp",
    "rm",
    "mv",
    "cp",
    "cat",
    "chmod",
    "chown",
    "stat",
    "install",
    "flock",
    "sha256sum",
    "readlink",
    "dirname",
    "basename",
    "grep",
    "sed",
    "awk",
    "cut",
    "head",
    "tail",
    "tr",
    "sort",
    "sleep",
    "date",
    "touch",
    "tee",
    "printf",
    "env",
    "wc",
    "find",
  ]) {
    const path = (await execute("bash", ["-c", `command -v ${name}`])).stdout.trim();
    if (path.startsWith("/")) await symlink(path, join(bin, name));
  }
  await symlink(process.execPath, join(bin, "node"));

  async function installer(
    version = "0.10.1",
    metadata?: (value: Record<string, unknown>) => void,
  ): Promise<InstallerArtifact> {
    const registry = {
      schemaVersion: 1 as const,
      version,
      sourceRevision: INSTALL_SOURCE,
      image: `ghcr.io/asadarafat/streamskope:${version}`,
      reference: `ghcr.io/asadarafat/streamskope:${version}@${INSTALL_DIGEST}`,
      digest: INSTALL_DIGEST,
      platforms: (["amd64", "arm64"] as const).map((architecture, index) => ({
        platform: `linux/${architecture}` as const,
        manifestDigest: `sha256:${String(index + 1).repeat(64)}`,
        imageId: `sha256:${String(index + 3).repeat(64)}`,
      })),
    };
    const topology = browserRegistryTopology(
      await readFile("streamskope.clab.yml", "utf8"),
      registry,
    );
    const manifest: Record<string, unknown> = {
      schemaVersion: 2,
      version,
      sourceRevision: INSTALL_SOURCE,
      image: `streamskope:${version}`,
      format: "docker-save-gzip",
      topology: { file: `streamskope-${version}.clab.yml`, sha256: hash(topology) },
      registry,
      offlineTopology: { file: `streamskope-${version}-offline.clab.yml`, sha256: "d".repeat(64) },
      archives: registry.platforms.map((platform, index) => ({
        version,
        sourceRevision: INSTALL_SOURCE,
        image: `streamskope:${version}`,
        imageId: platform.imageId,
        platform: platform.platform,
        archive: `StreamSkope-${version}-container-linux-${index ? "arm64" : "amd64"}.tar.gz`,
        bytes: 100,
        sha256: "e".repeat(64),
      })),
    };
    metadata?.(manifest);
    const contents = `${JSON.stringify(manifest)}\n`;
    const assets = join(root, "assets", version);
    await mkdir(assets, { recursive: true });
    await writeFile(join(assets, `streamskope-${version}.clab.yml`), topology);
    await writeFile(join(assets, `streamskope-${version}-container.json`), contents);
    await writeFile(
      join(assets, "SHA256SUMS"),
      `${hash(topology)}  streamskope-${version}.clab.yml\n${hash(contents)}  streamskope-${version}-container.json\n`,
    );
    let source = renderBrowserWorkbenchInstaller(
      {
        version,
        sourceRevision: INSTALL_SOURCE,
        topologySha256: hash(topology),
        manifestSha256: hash(contents),
      },
      await readFile("tools/package/install-browser-workbench.sh", "utf8"),
    );
    for (const [constant, value] of Object.entries({
      INSTALL_ROOT: join(root, "state"),
      OS_RELEASE: join(root, "os-release"),
      TTY_DEVICE: join(root, "absent-tty"),
      APT_KEYRING: join(root, "etc/docker-key"),
      DOCKER_APT_SOURCE: join(root, "etc/docker-source"),
      CLAB_APT_SOURCE: join(root, "etc/clab-source"),
    })) {
      const pattern = new RegExp(`^${constant}=.*$`, "m");
      if (!pattern.test(source))
        throw new Error(`Installer fixture could not isolate ${constant}.`);
      source = source.replace(pattern, `${constant}=${quote(value)}`);
    }
    if (!/^STATE_OWNER_UID=0$/m.test(source))
      throw new Error("Installer fixture requires an explicit state-owner constant.");
    source = source.replace(/^STATE_OWNER_UID=0$/m, `STATE_OWNER_UID=${owner.uid}`);
    if (!/^STATE_OWNER_GID=0$/m.test(source))
      throw new Error("Installer fixture requires an explicit state-group constant.");
    source = source.replace(/^STATE_OWNER_GID=0$/m, `STATE_OWNER_GID=${owner.gid}`);
    if (control.firstPort !== undefined) {
      if (!/^FIRST_PORT=8080$/m.test(source))
        throw new Error("Installer fixture requires an explicit first-port constant.");
      source = source.replace(/^FIRST_PORT=8080$/m, `FIRST_PORT=${control.firstPort}`);
    }
    const file = join(root, `install-${version}.sh`);
    await writeFile(file, source);
    return { file, version, image: registry.reference, topology, manifest };
  }
  async function run(file: string, args: string[] = []): Promise<InstallerResult> {
    try {
      const result = await execute("bash", [file, ...args], {
        cwd: root,
        env: {
          ...process.env,
          PATH: bin,
          TMPDIR: root,
          INSTALL_FIXTURE_ROOT: root,
          SUDO_UID: String(owner.uid),
          SUDO_GID: String(owner.gid),
          SUDO_USER: "operator",
        },
        timeout: 15_000,
        maxBuffer: 1024 * 1024,
      });
      return { ...result, exitCode: 0 };
    } catch (error) {
      const result = error as { stdout?: string; stderr?: string; code?: number | string };
      return {
        stdout: result.stdout ?? "",
        stderr: result.stderr ?? "",
        exitCode: typeof result.code === "number" ? result.code : 1,
      };
    }
  }
  return {
    root,
    state: join(root, "state"),
    data: join(root, "state/streamskope-data"),
    owner,
    installer,
    run,
    calls: async (): Promise<Array<{ command: string; args: string[] }>> =>
      (await readFile(join(root, "calls.jsonl"), "utf8"))
        .trim()
        .split("\n")
        .filter(Boolean)
        .map((line) => JSON.parse(line) as { command: string; args: string[] }),
    control: async (changes: InstallerControl): Promise<void> => {
      const current = JSON.parse(
        await readFile(join(root, "control.json"), "utf8"),
      ) as InstallerControl;
      await writeFile(join(root, "control.json"), JSON.stringify({ ...current, ...changes }));
    },
    cleanup: (): Promise<void> => rm(root, { recursive: true, force: true }),
  };
}
