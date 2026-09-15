import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import {
  copyFile,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join, resolve } from "node:path";
import { promisify } from "node:util";

interface NativeDmgRuntime {
  readonly platform: string;
  readonly arch: string;
  readonly run: (command: string, args: readonly string[]) => Promise<void>;
}

const execFileAsync = promisify(execFile);
const nativeRuntime: NativeDmgRuntime = {
  platform: process.platform,
  arch: process.arch,
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
    throw new Error("A packaged StreamSkope.app is required; run npm run package:dmg:macos.");
  }
  const name = `StreamSkope-${manifest.version}-darwin-arm64-unsigned-preview.dmg`;
  const temporary = await mkdtemp(join(tmpdir(), "streamskope-dmg-"));
  let outputDirectory: string | undefined;
  try {
    const stage = join(temporary, "contents");
    await mkdir(stage);
    await runtime.run("ditto", [app, join(stage, "StreamSkope.app")]);
    await symlink("/Applications", join(stage, "Applications"));
    await writeFile(
      join(stage, "UNSIGNED-PREVIEW.txt"),
      "StreamSkope — unsigned macOS build.\n" +
        "This workflow does not apply Developer ID signing. This image is not notarized.\n" +
        "Install only from a trusted source after verifying the supplied SHA-256.\n" +
        "Drag StreamSkope.app to Applications. macOS may require a per-app security exception.\n",
    );
    const image = join(temporary, name);
    await runtime.run("hdiutil", [
      "create",
      "-volname",
      "StreamSkope",
      "-srcfolder",
      stage,
      "-format",
      "UDZO",
      image,
    ]);
    await runtime.run("hdiutil", ["verify", image]);
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
    await rm(temporary, { recursive: true, force: true });
  }
}
