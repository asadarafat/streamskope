import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { access, copyFile, mkdir, readFile, readdir, rm } from "node:fs/promises";
import { join, resolve } from "node:path";

function run(command: string, args: readonly string[]): void {
  const result = spawnSync(command, [...args], { stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0)
    throw new Error(`${command} failed (${result.status ?? result.signal}).`);
}

function node(script: string, ...args: string[]): void {
  run(process.execPath, ["--import", "tsx", script, ...args]);
}

async function desktop(): Promise<void> {
  assert.equal(
    process.platform,
    process.env.EXPECTED_PLATFORM ?? process.platform,
    "Native package platform mismatch.",
  );
  assert.equal(
    process.arch,
    process.env.EXPECTED_ARCH ?? process.arch,
    "Native package architecture mismatch.",
  );
  const target = `${process.platform}-${process.arch}`;
  if (!["linux-x64", "win32-x64", "darwin-arm64"].includes(target)) {
    throw new Error(`Unsupported native package target: ${target}`);
  }
  node("tools/package/e2e.mjs", "boundary");
  node("tools/build.mjs");
  node("tools/package/verify.ts");
  node("tools/package/e2e.mjs", "package");

  const { version } = JSON.parse(await readFile("package.json", "utf8")) as { version: string };
  const extension =
    process.platform === "win32"
      ? "-Setup.exe"
      : process.platform === "darwin"
        ? ".dmg"
        : ".AppImage";
  const name = `StreamSkope-${version}-${target}${extension}`;
  let source: string;
  if (process.platform === "darwin") {
    const { createMacosPreviewDmg } = await import("./package/macos-dmg.js");
    source = await createMacosPreviewDmg(process.cwd());
  } else {
    const { Arch, build, Platform } = await import("electron-builder");
    const windows = process.platform === "win32";
    const prepackaged = resolve("dist/package", `StreamSkope-${target}`);
    const output = resolve("dist/release", windows ? "windows-installer" : "linux-appimage");
    await access(join(prepackaged, windows ? "StreamSkope.exe" : "StreamSkope"));
    await rm(output, { recursive: true, force: true });
    await mkdir(output, { recursive: true });
    await build({
      prepackaged,
      publish: "never",
      targets: (windows ? Platform.WINDOWS : Platform.LINUX).createTarget(
        windows ? "nsis" : "AppImage",
        Arch.x64,
      ),
      config: {
        appId: "com.streamskope.desktop",
        productName: "StreamSkope",
        artifactName: name,
        directories: { output },
        ...(windows
          ? {
              win: { icon: "assets/icons/streamskope.ico" },
              nsis: { allowToChangeInstallationDirectory: true, oneClick: false },
            }
          : {
              linux: {
                category: "Development",
                executableName: "StreamSkope",
                icon: "assets/icons/streamskope.png",
              },
            }),
      },
    });
    const artifacts = (await readdir(output)).filter((file) =>
      file.endsWith(windows ? ".exe" : ".AppImage"),
    );
    if (artifacts.length !== 1 || artifacts[0] !== name) {
      throw new Error(`Expected exactly the version-matched installer: ${name}`);
    }
    source = join(output, name);
  }
  await rm("dist/installers", { force: true, recursive: true });
  await mkdir("dist/installers", { recursive: true });
  await copyFile(source, join("dist/installers", name));
  process.stdout.write(`Built unsigned installer: ${join("dist/installers", name)}\n`);
}

async function main(): Promise<void> {
  const [target = "desktop", ...args] = process.argv.slice(2);
  if (target === "eda" && args.length === 0) run("bash", ["tools/package/eda.sh"]);
  else if (target === "plugin" && args.length <= 1) node("tools/package/plugin.ts", ...args);
  else if (target === "release") node("tools/package/release.ts", ...args);
  else if (target === "desktop" && args.length === 0) await desktop();
  else
    throw new Error(
      "Usage: npm run package [-- desktop|eda|plugin [eda|nsp]|release <release arguments>]",
    );
}

void main().catch((error: unknown) => {
  process.stderr.write(
    `Packaging failed: ${error instanceof Error ? error.message : String(error)}\n`,
  );
  process.exitCode = 1;
});
