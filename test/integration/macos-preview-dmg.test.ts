import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, readdir, readlink, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";

import { afterEach, expect, it } from "vitest";

import { createMacosPreviewDmg } from "../../tools/package/macos-dmg";

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
    detachOwnedImage: async () => undefined,
    settle: async () => undefined,
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
        detachOwnedImage: async () => undefined,
        settle: async () => undefined,
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
      detachOwnedImage: async () => undefined,
      settle: async () => undefined,
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
      detachOwnedImage: async () => undefined,
      settle: async () => undefined,
      run: () => Promise.reject(new Error("must not run")),
    }),
  ).rejects.toThrow("version");
});

it("retries transient busy images with fresh staging and mandatory verification after owned detach", async () => {
  const root = await fixture();
  const images: string[] = [];
  const detached: string[] = [];
  const verified: string[] = [];
  let settles = 0;
  const output = await createMacosPreviewDmg(root, {
    platform: "darwin",
    arch: "arm64",
    settle: async () => {
      settles += 1;
    },
    detachOwnedImage: async (image) => {
      detached.push(image);
    },
    run: async (command, args) => {
      if (command === "ditto") return;
      if (args[0] === "create") {
        const image = args.at(-1)!;
        images.push(image);
        await writeFile(image, "complete image");
        if (images.length === 1) throw new Error("hdiutil: create failed - Resource busy");
      } else if (args[0] === "verify") {
        expect(detached).toContain(args[1]);
        verified.push(args[1]!);
      }
    },
  });
  expect(images).toHaveLength(2);
  expect(images[0]).not.toBe(images[1]);
  expect(detached).toContain(images[0]);
  expect(verified).toEqual([images[1]]);
  expect(settles).toBe(1);
  expect(await readFile(output, "utf8")).toBe("complete image");
});

it("stops after three busy attempts without publishing an unverified image", async () => {
  const root = await fixture();
  let attempts = 0;
  const detached: string[] = [];
  await expect(
    createMacosPreviewDmg(root, {
      platform: "darwin",
      arch: "arm64",
      settle: async () => undefined,
      detachOwnedImage: async (image) => {
        detached.push(image);
      },
      run: async (command, args) => {
        if (command === "ditto") return;
        if (args[0] === "create") {
          attempts += 1;
          await writeFile(args.at(-1)!, "image");
        } else throw new Error("hdiutil: verify failed - Resource temporarily unavailable");
      },
    }),
  ).rejects.toThrow("Resource temporarily unavailable");
  expect(attempts).toBe(3);
  expect(new Set(detached).size).toBe(3);
  await expect(readdir(join(root, "dist/release"))).rejects.toMatchObject({ code: "ENOENT" });
});

it("retains an owned image when its backing device cannot be detached", async () => {
  const root = await fixture();
  let image = "";
  await expect(
    createMacosPreviewDmg(root, {
      platform: "darwin",
      arch: "arm64",
      settle: async () => {
        throw new Error("must not retry failed cleanup");
      },
      detachOwnedImage: async () => {
        throw new Error("owned device cleanup failed");
      },
      run: async (command, args) => {
        if (command === "ditto") return;
        image = args.at(-1)!;
        await writeFile(image, "retain mounted backing image");
      },
    }),
  ).rejects.toThrow("owned device cleanup failed");
  expect(await readFile(image, "utf8")).toBe("retain mounted backing image");
  await expect(readdir(join(root, "dist/release"))).rejects.toMatchObject({ code: "ENOENT" });
  await rm(join(image, "../.."), { recursive: true, force: true });
});
