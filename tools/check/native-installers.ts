import { createHash } from "node:crypto";
import { execFile, spawn } from "node:child_process";
import { createReadStream, createWriteStream } from "node:fs";
import {
  access,
  chmod,
  copyFile,
  cp,
  mkdir,
  readFile,
  readdir,
  rm,
  writeFile,
} from "node:fs/promises";
import { basename, join, resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream } from "node:stream/web";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

export interface NativeInstaller {
  readonly version: string;
  readonly name: string;
  readonly sha256: string;
  readonly url?: string;
  readonly sourceRevision?: string;
  readonly path: string;
}

export interface NativeRecoveryPlan {
  readonly schemaVersion: 1;
  readonly root: string;
  readonly platform: NodeJS.Platform;
  readonly architecture: string;
  readonly from: NativeInstaller;
  readonly to: NativeInstaller;
  readonly candidateSourceRevision?: string;
}

const repository = "asadarafat/streamskope";
const marker = "streamskope-native-recovery-owned-root";

export function installerName(
  version: string,
  platform = process.platform,
  arch = process.arch,
): string {
  if (!/^\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?$/u.test(version)) {
    throw new Error("Native recovery requires explicit release versions without a leading v.");
  }
  const suffixes: Readonly<Record<string, string>> = {
    "darwin-arm64": ".dmg",
    "linux-x64": ".AppImage",
    "win32-x64": "-Setup.exe",
  };
  const suffix = suffixes[`${platform}-${arch}`];
  if (suffix === undefined)
    throw new Error("Native recovery requires a supported native release platform.");
  return `StreamSkope-${version}-${platform}-${arch}${suffix}`;
}

export function installerChecksum(checksums: string, name: string): string {
  const entries = checksums.split(/\r?\n/u).flatMap((line) => {
    const match = /^([a-f0-9]{64})\s+[ *]?(.+)$/u.exec(line);
    return match?.[2] === name ? [match[1]!] : [];
  });
  if (entries.length !== 1) throw new Error("Installer must have exactly one SHA256SUMS entry.");
  return entries[0]!;
}

export async function fileSha256(path: string): Promise<string> {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk as Buffer);
  return hash.digest("hex");
}

export async function nativeCommand(
  command: string,
  args: readonly string[],
  options: { readonly cwd?: string; readonly quiet?: boolean; readonly timeoutMs?: number } = {},
): Promise<void> {
  const verbatim =
    process.platform === "win32" &&
    args.some((arg) => arg.startsWith("/D=") || arg.startsWith("_?="));
  const child = spawn(command, [...args], {
    ...(verbatim ? { argv0: `"${command}"` } : {}),
    cwd: options.cwd,
    stdio: ["ignore", options.quiet ? "ignore" : "inherit", "inherit"],
    windowsHide: true,
    // NSIS consumes the final path verbatim, including spaces, without quotes.
    windowsVerbatimArguments: verbatim,
    // Own a process group so a timed-out launcher cannot leave Electron/Playwright alive.
    detached: process.platform !== "win32",
  });
  const exited = new Promise<
    { code: number | null; signal: NodeJS.Signals | null } | { error: Error }
  >((resolveExit) => {
    child.once("error", (error) => resolveExit({ error }));
    child.once("exit", (code, signal) => resolveExit({ code, signal }));
  });
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<"timeout">((resolveTimeout) => {
    timer = setTimeout(() => resolveTimeout("timeout"), options.timeoutMs ?? 120_000);
  });
  const outcome = await Promise.race([exited, deadline]);
  clearTimeout(timer);
  if (outcome === "timeout") {
    if (child.pid !== undefined) {
      if (process.platform === "win32") {
        // /T targets descendants of this owned launcher; no process-name matching.
        await promisify(execFile)("taskkill", ["/PID", String(child.pid), "/T", "/F"], {
          windowsHide: true,
          timeout: 5_000,
        }).catch(() => {
          if (child.exitCode === null && child.signalCode === null)
            throw new Error("Native process-tree cleanup failed.");
        });
      } else {
        // Playwright starts Electron in another session. Snapshot actual ancestry before
        // terminating its launcher, otherwise those detached children become untraceable.
        const { stdout } = await promisify(execFile)("ps", ["-axo", "pid=,ppid=,pgid="], {
          timeout: 5_000,
          maxBuffer: 4 * 1_048_576,
        });
        const rows = stdout.split("\n").flatMap((line) => {
          const match = /^\s*(\d+)\s+(\d+)\s+(\d+)\s*$/u.exec(line);
          return match
            ? [{ pid: Number(match[1]), parent: Number(match[2]), group: Number(match[3]) }]
            : [];
        });
        const owner = rows.find((row) => row.pid === child.pid);
        // A PID observed after its original child exited is not authority to kill it.
        if (
          child.exitCode !== null ||
          child.signalCode !== null ||
          owner === undefined ||
          owner.parent !== process.pid ||
          owner.group !== child.pid
        )
          throw new Error("Native process ownership changed before timeout cleanup.");
        const owned = new Set([child.pid]);
        for (let previous = -1; previous !== owned.size;) {
          previous = owned.size;
          for (const row of rows) if (owned.has(row.parent)) owned.add(row.pid);
        }
        // Only groups whose leader is in the captured owned tree may be signalled.
        const groups = new Set(
          rows.filter((row) => owned.has(row.pid) && owned.has(row.group)).map((row) => row.group),
        );
        const targets = [
          ...[...groups].map((group) => -group),
          ...rows
            .filter((row) => owned.has(row.pid) && !groups.has(row.group))
            .map((row) => row.pid),
        ];
        const signalTarget = (target: number, signal: NodeJS.Signals | 0): boolean => {
          try {
            process.kill(target, signal);
            return true;
          } catch (error) {
            if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
            throw error;
          }
        };
        for (const target of targets) signalTarget(target, "SIGTERM");
        for (let attempt = 0; attempt < 10; attempt += 1) {
          if (!targets.some((target) => signalTarget(target, 0))) break;
          await delay(200);
        }
        for (const target of targets) signalTarget(target, "SIGKILL");
      }
    }
    await Promise.race([exited, delay(2_000)]);
    throw Object.assign(new Error(`${basename(command)} exceeded its execution deadline.`), {
      code: "ETIMEDOUT",
    });
  }
  if ("error" in outcome) throw outcome.error;
  if (outcome.code !== 0)
    throw new Error(`${basename(command)} failed (${outcome.code ?? outcome.signal}).`);
}

export async function downloadNativeInstaller(
  version: string,
  root: string,
): Promise<NativeInstaller> {
  const name = installerName(version);
  const base = `https://github.com/${repository}/releases/download/v${version}`;
  const checksumResponse = await fetch(`${base}/SHA256SUMS`, {
    signal: AbortSignal.timeout(30_000),
  });
  if (!checksumResponse.ok)
    throw new Error(`Release checksums unavailable (${checksumResponse.status}).`);
  const sha256 = installerChecksum(await checksumResponse.text(), name);
  const url = `${base}/${name}`;
  const response = await fetch(url, { signal: AbortSignal.timeout(180_000) });
  if (!response.ok || response.body === null)
    throw new Error(`Release installer unavailable (${response.status}).`);
  const path = join(root, name);
  await pipeline(
    Readable.fromWeb(response.body as ReadableStream<Uint8Array>),
    createWriteStream(path, { mode: 0o600, flags: "wx" }),
  );
  if ((await fileSha256(path)) !== sha256)
    throw new Error("Published installer checksum mismatch.");
  return { version, name, sha256, url, path };
}

/** Qualification of a locally built installer never creates a release identity. */
export async function loadCandidateInstaller(
  candidatePath: string,
  version: string,
  root: string,
): Promise<NativeInstaller & { sourceRevision: string }> {
  if (version !== "0.0.0-dev")
    throw new Error("Source qualification retains the development version 0.0.0-dev.");
  const name = installerName(version);
  if (resolve(candidatePath) !== resolve("dist", "installers", name))
    throw new Error("Use the matching candidate from dist/installers after npm run package.");
  const run = promisify(execFile);
  await run("git", ["diff", "--quiet", "HEAD", "--"]);
  const { stdout } = await run("git", ["rev-parse", "HEAD"]);
  const sourceRevision = stdout.trim();
  if (!/^[a-f0-9]{40}$/u.test(sourceRevision))
    throw new Error("Candidate source revision is unavailable.");
  const { version: currentVersion } = JSON.parse(await readFile("package.json", "utf8")) as {
    version: string;
  };
  if (currentVersion !== version)
    throw new Error("Candidate version differs from the source checkout.");
  // Build here so a stale, same-named artifact cannot be attributed to this checkout.
  // Enter through npm so verification subprocesses receive npm_execpath.
  // Windows npm.cmd needs cmd.exe; the shell command is fixed, without input interpolation.
  await nativeCommand(
    process.platform === "win32" ? "cmd.exe" : "npm",
    process.platform === "win32" ? ["/d", "/s", "/c", "npm run package"] : ["run", "package"],
    { timeoutMs: 1_200_000 },
  );
  await run("git", ["diff", "--quiet", "HEAD", "--"]);
  const after = await run("git", ["rev-parse", "HEAD"]);
  if (after.stdout.trim() !== sourceRevision)
    throw new Error("Candidate source changed during packaging.");
  const sha256 = await fileSha256(candidatePath);
  const path = join(root, name);
  await copyFile(candidatePath, path);
  if ((await fileSha256(path)) !== sha256)
    throw new Error("Candidate changed while preparing qualification.");
  return { version, name, sha256, path, sourceRevision };
}

export async function writeRecoveryPlan(plan: NativeRecoveryPlan): Promise<string> {
  const path = join(plan.root, "plan.json");
  await writeFile(
    join(plan.root, marker),
    "Owned by the native recovery qualification harness.\n",
    { mode: 0o600 },
  );
  await writeFile(path, `${JSON.stringify(plan, null, 2)}\n`, { mode: 0o600 });
  return path;
}

export async function readRecoveryPlan(path: string): Promise<NativeRecoveryPlan> {
  const plan = JSON.parse(await readFile(path, "utf8")) as NativeRecoveryPlan;
  if (
    plan.schemaVersion !== 1 ||
    plan.platform !== process.platform ||
    plan.architecture !== process.arch ||
    resolve(path) !== join(plan.root, "plan.json")
  ) {
    throw new Error("Native recovery plan does not match this native runner.");
  }
  if (
    plan.candidateSourceRevision !== undefined &&
    (!/^[a-f0-9]{40}$/u.test(plan.candidateSourceRevision) ||
      plan.to.version !== "0.0.0-dev" ||
      plan.to.sourceRevision !== plan.candidateSourceRevision ||
      plan.to.url !== undefined)
  )
    throw new Error("Candidate recovery must identify an unreleased source build.");
  await access(join(plan.root, marker));
  for (const installer of [plan.from, plan.to]) {
    if (
      installer.name !== installerName(installer.version) ||
      installer.path !== join(plan.root, installer.name) ||
      !/^[a-f0-9]{64}$/u.test(installer.sha256)
    ) {
      throw new Error("Native recovery installer is outside its owned directory.");
    }
  }
  return plan;
}

export async function installNativeRelease(
  plan: NativeRecoveryPlan,
  installer: NativeInstaller,
): Promise<{ executablePath: string; archiveSha256: string; method: string }> {
  await access(join(plan.root, marker));
  if (
    installer.path !== join(plan.root, installerName(installer.version)) ||
    (await fileSha256(installer.path)) !== installer.sha256
  ) {
    throw new Error("Refusing to install an unverified or unowned installer.");
  }
  const destination = join(plan.root, "installed", "StreamSkope");
  await mkdir(destination, { recursive: true });
  let executablePath: string;
  let archivePath: string;
  let method: string;
  if (process.platform === "darwin") {
    const mount = join(plan.root, "mounted-dmg");
    await mkdir(mount, { recursive: true });
    await nativeCommand("hdiutil", [
      "attach",
      "-readonly",
      "-nobrowse",
      "-mountpoint",
      mount,
      installer.path,
    ]);
    try {
      const app = join(destination, "StreamSkope.app");
      await rm(app, { recursive: true, force: true });
      await nativeCommand("ditto", [join(mount, "StreamSkope.app"), app]);
      executablePath = join(app, "Contents", "MacOS", "StreamSkope");
      archivePath = join(app, "Contents", "Resources", "app.asar");
      method = "Replace isolated .app with the application from the verified DMG";
    } finally {
      await nativeCommand("hdiutil", ["detach", mount]);
    }
  } else if (process.platform === "win32") {
    // NSIS also registers uninstall metadata and shortcuts for the current user.
    // Run only in an ephemeral Actions account, never the user's normal desktop.
    if (process.env.GITHUB_ACTIONS !== "true")
      throw new Error("Windows installer recovery requires a disposable GitHub Actions account.");
    await nativeCommand(installer.path, ["/S", "/currentuser", `/D=${destination}`]);
    executablePath = join(destination, "StreamSkope.exe");
    archivePath = join(destination, "resources", "app.asar");
    method = "NSIS installer installs and upgrades the same isolated directory";
  } else if (process.platform === "linux") {
    const appImage = join(destination, "StreamSkope.AppImage");
    await copyFile(installer.path, appImage);
    await chmod(appImage, 0o700);
    await rm(join(destination, "squashfs-root"), { recursive: true, force: true });
    await nativeCommand(appImage, ["--appimage-extract"], { cwd: destination, quiet: true });
    executablePath = join(destination, "squashfs-root", "StreamSkope");
    archivePath = join(destination, "squashfs-root", "resources", "app.asar");
    method =
      "Replace the same AppImage and launch its extracted native application (no FUSE dependency)";
  } else {
    throw new Error("Unsupported native installer platform.");
  }
  await access(executablePath);
  return { executablePath, archiveSha256: await fileSha256(archivePath), method };
}

export async function uninstallNativeRecovery(plan: NativeRecoveryPlan): Promise<void> {
  if (process.platform !== "win32") return;
  const destination = join(plan.root, "installed", "StreamSkope");
  const entries = await readdir(destination).catch((): string[] => []);
  const uninstallers = entries.filter((name) => /^Uninstall.*\.exe$/u.test(name));
  if (entries.includes("StreamSkope.exe") && uninstallers.length === 0)
    throw new Error("Installed application has no owned uninstaller.");
  if (uninstallers.length > 1) throw new Error("Unexpected extra native uninstallers.");
  if (uninstallers[0]) {
    // Keep the uninstaller outside its own installation directory during cleanup.
    const uninstaller = join(plan.root, "cleanup.exe");
    await cp(join(destination, uninstallers[0]), uninstaller);
    await nativeCommand(uninstaller, ["/S", "/currentuser", "/KEEP_APP_DATA", `_?=${destination}`]);
  }
}
