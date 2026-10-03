import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
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

export interface NativeInstaller {
  readonly version: string;
  readonly name: string;
  readonly sha256: string;
  readonly url: string;
  readonly path: string;
}

export interface NativeRecoveryPlan {
  readonly schemaVersion: 1;
  readonly root: string;
  readonly platform: NodeJS.Platform;
  readonly architecture: string;
  readonly from: NativeInstaller;
  readonly to: NativeInstaller;
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
  await new Promise<void>((resolvePromise, reject) => {
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
      timeout: options.timeoutMs ?? 120_000,
    });
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (code === 0) resolvePromise();
      else reject(new Error(`${basename(command)} failed (${code ?? signal}).`));
    });
  });
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
      method = "Replace isolated .app with the application from the published DMG";
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
  const entries = await readdir(destination).catch(() => []);
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
