import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, readdir, readlink, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";

import { afterEach, expect, it } from "vitest";

import { createMacosPreviewDmg } from "../../tools/macos-preview-dmg";

const roots: string[] = [];
afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function fixture(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-dmg-test-"));
  roots.push(root);
  const app = join(root, "dist/package/StreamSkope-darwin-arm64/StreamSkope.app");
  await mkdir(join(app, "Contents/MacOS"), { recursive: true });
  await writeFile(join(app, "Contents/MacOS/StreamSkope"), "test executable");
  await writeFile(join(root, "package.json"), JSON.stringify({ version: "0.1.0" }));
  return root;
}

it("stages only the app, shortcut and warning, verifies the image, then publishes its checksum", async () => {
  const root = await fixture();
  const calls: string[] = [];
  let stage = "";
  const output = await createMacosPreviewDmg(root, {
    platform: "darwin",
    arch: "arm64",
    run: async (command, args) => {
      calls.push(`${command} ${args[0] ?? ""}`);
      if (command === "ditto") {
        expect(args[0]).toBe(join(root, "dist/package/StreamSkope-darwin-arm64/StreamSkope.app"));
        await mkdir(args[1] ?? "");
      } else if (args[0] === "create") {
        stage = args[args.indexOf("-srcfolder") + 1] ?? "";
        expect((await readdir(stage)).sort()).toEqual([
          "Applications",
          "StreamSkope.app",
          "UNSIGNED-PREVIEW.txt",
        ]);
        expect(await readlink(join(stage, "Applications"))).toBe("/Applications");
        expect(await readFile(join(stage, "UNSIGNED-PREVIEW.txt"), "utf8")).toContain(
          "not notarized",
        );
        expect(args).toContain("UDZO");
        await writeFile(args.at(-1) ?? "", "synthetic disk image");
      } else {
        expect(args[0]).toBe("verify");
        expect(await readFile(args[1] ?? "", "utf8")).toBe("synthetic disk image");
      }
    },
  });
  expect(calls.map((call) => call.split(" ")[0])).toEqual(["ditto", "hdiutil", "hdiutil"]);
  expect(basename(output)).toBe("StreamSkope-0.1.0-darwin-arm64-unsigned-preview.dmg");
  const digest = createHash("sha256").update("synthetic disk image").digest("hex");
  expect(await readFile(`${output}.sha256`, "utf8")).toBe(`${digest}  ${basename(output)}\n`);
  await expect(readdir(stage)).rejects.toMatchObject({ code: "ENOENT" });
});

it.each(["create", "verify"])(
  "does not publish a preview when hdiutil %s fails",
  async (failure) => {
    const root = await fixture();
    const release = join(root, "dist/release");
    await mkdir(release);
    await writeFile(join(release, "existing.dmg"), "keep existing output");
    let stage = "";
    await expect(
      createMacosPreviewDmg(root, {
        platform: "darwin",
        arch: "arm64",
        run: async (command, args) => {
          if (command === "ditto") return;
          if (args[0] === "create") stage = args[args.indexOf("-srcfolder") + 1] ?? "";
          if (args[0] === failure) throw new Error("native command failed");
          if (args[0] === "create") await writeFile(args.at(-1) ?? "", "partial image");
        },
      }),
    ).rejects.toThrow("native command failed");
    expect(await readdir(release)).toEqual(["existing.dmg"]);
    expect(await readFile(join(release, "existing.dmg"), "utf8")).toBe("keep existing output");
    await expect(readdir(stage)).rejects.toMatchObject({ code: "ENOENT" });
  },
);

it.each([
  ["linux", "arm64"],
  ["darwin", "x64"],
])("rejects %s/%s before native commands", async (platform, arch) => {
  const root = await fixture();
  await expect(
    createMacosPreviewDmg(root, {
      platform: platform ?? "",
      arch: arch ?? "",
      run: () => Promise.reject(new Error("must not run")),
    }),
  ).rejects.toThrow("macOS ARM64");
});

it("rejects unsafe artifact versions before running native commands", async () => {
  const root = await fixture();
  await writeFile(join(root, "package.json"), JSON.stringify({ version: "../../escape" }));
  await expect(
    createMacosPreviewDmg(root, {
      platform: "darwin",
      arch: "arm64",
      run: () => Promise.reject(new Error("must not run")),
    }),
  ).rejects.toThrow("version");
});
