import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import {
  copyFile,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  realpath,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { promisify } from "node:util";
import { setTimeout as delay } from "node:timers/promises";

interface NativeDmgRuntime {
  readonly platform: string;
  readonly arch: string;
  readonly run: (command: string, args: readonly string[]) => Promise<void>;
  readonly detachOwnedImage: (image: string) => Promise<void>;
  readonly settle: () => Promise<void>;
}

const execFileAsync = promisify(execFile);
const nativeRuntime: NativeDmgRuntime = {
  platform: process.platform,
  arch: process.arch,
  settle: () => delay(1_000),
  async detachOwnedImage(image): Promise<void> {
    const { stdout } = await execFileAsync("hdiutil", ["info", "-plist"], {
      timeout: 30_000,
      maxBuffer: 1_048_576,
    });
    // Keep the system image inventory private and within this attempt's directory.
    const inventory = join(dirname(image), "disk-images.plist");
    await writeFile(inventory, stdout, { mode: 0o600 });
    const converted = await execFileAsync("plutil", ["-convert", "json", "-o", "-", inventory], {
      timeout: 30_000,
      maxBuffer: 1_048_576,
    });
    const data = JSON.parse(converted.stdout) as {
      images?: { "image-path"?: string; "system-entities"?: { "dev-entry"?: string }[] }[];
    };
    if (!Array.isArray(data.images)) throw new Error("Unable to inspect owned disk image mounts.");
    const owned = await realpath(image).catch(() => resolve(image));
    for (const mounted of data.images) {
      if (typeof mounted["image-path"] !== "string") continue;
      const path = await realpath(mounted["image-path"]).catch(() =>
        resolve(mounted["image-path"]!),
      );
      if (path !== owned) continue;
      const devices = (mounted["system-entities"] ?? [])
        .map((entity) => entity["dev-entry"])
        .filter(
          (device): device is string =>
            typeof device === "string" && /^\/dev\/disk\d+$/u.test(device),
        )
        .sort((left, right) => Number(left.slice(9)) - Number(right.slice(9)));
      if (devices[0] === undefined) throw new Error("Owned disk image has no detachable device.");
      await execFileAsync("hdiutil", ["detach", devices[0]], { timeout: 30_000 });
    }
  },
  async run(command, args): Promise<void> {
    await execFileAsync(command, [...args], { timeout: 180_000, maxBuffer: 1_048_576 });
  },
};

export async function createMacosPreviewDmg(
  repositoryRoot: string,
  runtime: NativeDmgRuntime = nativeRuntime,
): Promise<string> {
  if (runtime.platform !== "darwin" || runtime.arch !== "arm64") {
    throw new Error("DMG creation requires native macOS ARM64 with ARM64 Node.js.");
  }
  const root = resolve(repositoryRoot);
  const manifest = JSON.parse(await readFile(join(root, "package.json"), "utf8")) as {
    version?: unknown;
  };
  if (
    typeof manifest.version !== "string" ||
    !/^\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?$/u.test(manifest.version)
  ) {
    throw new Error("Invalid package version for DMG filename.");
  }
  const app = join(root, "dist/package/StreamSkope-darwin-arm64/StreamSkope.app");
  if (
    !(await lstat(app)).isDirectory() ||
    !(await lstat(join(app, "Contents/MacOS/StreamSkope"))).isFile()
  ) {
    throw new Error("A packaged StreamSkope.app is required; run npm run package.");
  }
  const name = `StreamSkope-${manifest.version}-darwin-arm64-unsigned-preview.dmg`;
  const temporary = await mkdtemp(join(tmpdir(), "streamskope-dmg-"));
  let outputDirectory: string | undefined;
  let attachedImageMayRemain = false;
  try {
    let image: string | undefined;
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      const attemptRoot = join(temporary, `attempt-${String(attempt)}`);
      const stage = join(attemptRoot, "contents");
      await mkdir(stage, { recursive: true });
      await runtime.run("ditto", [app, join(stage, "StreamSkope.app")]);
      await symlink("/Applications", join(stage, "Applications"));
      await writeFile(
        join(stage, "UNSIGNED-PREVIEW.txt"),
        "StreamSkope — unsigned macOS build.\n" +
          "This workflow does not apply Developer ID signing. This image is not notarized.\n" +
          "Install only from a trusted source after verifying the supplied SHA-256.\n" +
          "Drag StreamSkope.app to Applications. macOS may require a per-app security exception.\n",
      );
      const candidate = join(attemptRoot, name);
      try {
        attachedImageMayRemain = true;
        try {
          await runtime.run("hdiutil", [
            "create",
            "-volname",
            "StreamSkope",
            "-srcfolder",
            stage,
            "-format",
            "UDZO",
            candidate,
          ]);
          // DiskImages may return from create while its backing device is still
          // attached. Close only our exact image before attempting verification.
          await runtime.detachOwnedImage(candidate);
          await runtime.run("hdiutil", ["verify", candidate]);
        } finally {
          await runtime.detachOwnedImage(candidate);
          attachedImageMayRemain = false;
        }
        image = candidate;
        break;
      } catch (error) {
        const busy =
          error instanceof Error &&
          /Resource (?:busy|temporarily unavailable)/iu.test(error.message);
        if (attachedImageMayRemain || !busy || attempt === 3) throw error;
        await rm(attemptRoot, { recursive: true, force: true });
        await runtime.settle();
      }
    }
    if (image === undefined) throw new Error("No verified disk image was produced.");
    const hash = createHash("sha256");
    for await (const chunk of createReadStream(image)) hash.update(chunk as Buffer);
    const digest = hash.digest("hex");
    const release = join(root, "dist/release");
    await mkdir(release, { recursive: true });
    outputDirectory = await mkdtemp(join(release, "unsigned-macos-"));
    const output = join(outputDirectory, name);
    await copyFile(image, output);
    await writeFile(`${output}.sha256`, `${digest}  ${basename(output)}\n`, { flag: "wx" });
    return output;
  } catch (error) {
    if (outputDirectory !== undefined) await rm(outputDirectory, { recursive: true, force: true });
    throw error;
  } finally {
    // Never unlink a backing image when an owned device could not be detached.
    if (!attachedImageMayRemain) await rm(temporary, { recursive: true, force: true });
  }
}
