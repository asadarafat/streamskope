import { access, mkdir, readdir, rename, rm } from "node:fs/promises";
import { join, resolve } from "node:path";
import process from "node:process";
import { Arch, build, Platform } from "electron-builder";

if (process.platform !== "win32" || process.arch !== "x64") {
  throw new Error("Windows installer packaging must run on a native win32-x64 runner.");
}

const root = process.cwd();
const prepackaged = resolve(root, "dist/package/StreamSkope-win32-x64");
const output = resolve(root, "dist/release/windows-installer");
await access(join(prepackaged, "StreamSkope.exe"));
await rm(output, { recursive: true, force: true });
await mkdir(output, { recursive: true });
await build({
  prepackaged,
  publish: "never",
  targets: Platform.WINDOWS.createTarget("nsis", Arch.x64),
  config: {
    appId: "com.streamskope.desktop",
    productName: "StreamSkope",
    artifactName: "StreamSkope-${version}-win32-x64-Setup.exe",
    directories: { output },
    win: { icon: "assets/icons/streamskope.ico" },
    nsis: { allowToChangeInstallationDirectory: true, oneClick: false },
  },
});
const artifacts = (await readdir(output)).filter((name) => name.endsWith(".exe"));
if (artifacts.length !== 1) {
  throw new Error(`Expected exactly one Windows installer, found ${String(artifacts.length)}.`);
}
await rename(join(output, artifacts[0]), resolve(root, "dist/release", artifacts[0]));
