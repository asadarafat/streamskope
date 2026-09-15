import { access, mkdir, readdir, rename, rm } from "node:fs/promises";
import { join, resolve } from "node:path";
import process from "node:process";
import { Arch, build, Platform } from "electron-builder";

if (process.platform !== "linux" || process.arch !== "x64") {
  throw new Error("Linux AppImage packaging must run on a native linux-x64 runner.");
}

const root = process.cwd();
const prepackaged = resolve(root, "dist/package/StreamSkope-linux-x64");
const output = resolve(root, "dist/release/linux-appimage");
await access(join(prepackaged, "StreamSkope"));
await rm(output, { recursive: true, force: true });
await mkdir(output, { recursive: true });
await build({
  prepackaged,
  publish: "never",
  targets: Platform.LINUX.createTarget("AppImage", Arch.x64),
  config: {
    appId: "com.streamskope.desktop",
    productName: "StreamSkope",
    artifactName: "StreamSkope-${version}-linux-x64.AppImage",
    directories: { output },
    linux: {
      category: "Development",
      executableName: "StreamSkope",
      icon: "assets/icons/streamskope.png",
    },
  },
});
const artifacts = (await readdir(output)).filter((name) => name.endsWith(".AppImage"));
if (artifacts.length !== 1) {
  throw new Error(`Expected exactly one Linux AppImage, found ${String(artifacts.length)}.`);
}
await rename(join(output, artifacts[0]), resolve(root, "dist/release", artifacts[0]));
