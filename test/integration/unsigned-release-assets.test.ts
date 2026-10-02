import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

import { afterEach, expect, it } from "vitest";

import { prepareUnsignedRelease } from "../../tools/package/release-policy";

const directories: string[] = [];
const names = [
  "StreamSkope-0.1.0-darwin-arm64.dmg",
  "StreamSkope-0.1.0-win32-x64-Setup.exe",
  "StreamSkope-0.1.0-linux-x64.AppImage",
] as const;
const commit = "a".repeat(40);
const execute = promisify(execFile);

afterEach(async () => {
  for (const directory of directories.splice(0)) {
    await rm(directory, { recursive: true, force: true });
  }
});

async function fixture(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-unsigned-release-"));
  directories.push(directory);
  for (const name of names) await writeFile(join(directory, name), "abc");
  return directory;
}

it("hashes the exact release assets and discloses signing status and source identity", async () => {
  const directory = await fixture();
  const notes = await prepareUnsignedRelease(directory, "0.1.0", commit);
  const checksums = await readFile(join(directory, "SHA256SUMS"), "utf8");
  for (const name of names) {
    expect(checksums).toContain(
      `ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad  ${name}\n`,
    );
  }
  expect(checksums.trim().split("\n")).toHaveLength(3);
  expect(notes).toContain(commit);
  expect(notes).toContain("not notarized");
  expect(notes).toContain("not Authenticode-signed");
  expect(notes).toContain("not publisher-signed");
  expect(notes).toContain("SHA256SUMS");
  expect(notes).not.toContain("production-approved");
});

it.each(["missing", "extra", "empty", "symlink"])(
  "rejects %s artifacts before checksums",
  async (kind) => {
    const directory = await fixture();
    const first = join(directory, names[0]);
    if (kind === "missing" || kind === "symlink") await rm(first);
    if (kind === "extra") await writeFile(join(directory, "unexpected.txt"), "not a release asset");
    if (kind === "empty") await writeFile(first, "");
    if (kind === "symlink") await symlink(join(directory, names[1]), first);
    await expect(prepareUnsignedRelease(directory, "0.1.0", commit)).rejects.toThrow();
    await expect(readFile(join(directory, "SHA256SUMS"))).rejects.toThrow();
  },
);

it("refuses invalid identity and never overwrites an assembled set", async () => {
  const directory = await fixture();
  await expect(prepareUnsignedRelease(directory, "../other", commit)).rejects.toThrow();
  await expect(prepareUnsignedRelease(directory, "0.1.0", "not-a-commit")).rejects.toThrow();
  await prepareUnsignedRelease(directory, "0.1.0", commit);
  const original = await readFile(join(directory, "SHA256SUMS"), "utf8");
  await expect(prepareUnsignedRelease(directory, "0.1.0", commit)).rejects.toThrow();
  expect(await readFile(join(directory, "SHA256SUMS"), "utf8")).toBe(original);
});

it.each(["0.1.0", "0.2.0-rc.1"])(
  "assembles reviewed notes and checksums through the consolidated package command for %s",
  async (version) => {
    const tag = `v${version}`;
    const versionedNames = names.map((name) => name.replace("0.1.0", version));
    const root = await mkdtemp(join(tmpdir(), "streamskope-release-command-"));
    directories.push(root);
    const assets = join(root, "installers");
    await mkdir(assets);
    for (const name of versionedNames) await writeFile(join(assets, name), "abc");
    const source = join(root, "source.md");
    const output = join(root, "notes.md");
    await writeFile(
      source,
      `---\nrelease_version: ${version}\nrelease_tag: ${tag}\n---\n# Reviewed release\n\nReviewed change.\n`,
    );
    const args = [
      "--import",
      "tsx",
      "tools/package.ts",
      "release",
      assets,
      version,
      commit,
      source,
      output,
      tag,
    ];
    await execute(process.execPath, args, { timeout: 15000 });
    const notes = await readFile(output, "utf8");
    expect(notes).toContain("# Reviewed release\n\nReviewed change.");
    expect(notes).toContain("These downloads are unsigned.");
    expect(notes).toContain(commit);
    expect(notes).toContain("## Distribution\n");
    expect(notes.match(/^# /gmu)).toHaveLength(1);
    expect(notes).toContain(`Source: ${commit} (tag ${tag}).`);
    expect(notes).toContain(`StreamSkope-${version}-linux-x64.AppImage`);
    expect((await readFile(join(assets, "SHA256SUMS"), "utf8")).trim().split("\n")).toHaveLength(3);
    await expect(execute(process.execPath, args, { timeout: 15000 })).rejects.toMatchObject({
      code: 1,
    });
    expect(await readFile(output, "utf8")).toBe(notes);
  },
);

it.each(["v0.1.1", "v0.1.0+build.2", "v0.1.0-rc.1"])(
  "rejects unsupported or mismatched tag %s before writing release checksums",
  async (tag) => {
    const assets = await fixture();
    await expect(prepareUnsignedRelease(assets, "0.1.0", commit, tag)).rejects.toThrow();
    await expect(readFile(join(assets, "SHA256SUMS"))).rejects.toMatchObject({ code: "ENOENT" });
  },
);

it("rejects mismatched reviewed notes before writing release checksums", async () => {
  const assets = await fixture();
  const root = await mkdtemp(join(tmpdir(), "streamskope-release-source-"));
  directories.push(root);
  const source = join(root, "source.md");
  const output = join(root, "notes.md");
  await writeFile(source, "---\nrelease_version: 0.2.0\n---\n# Other version\n");
  await expect(
    execute(
      process.execPath,
      ["--import", "tsx", "tools/package.ts", "release", assets, "0.1.0", commit, source, output],
      { timeout: 15000 },
    ),
  ).rejects.toMatchObject({ code: 1 });
  await expect(readFile(join(assets, "SHA256SUMS"))).rejects.toMatchObject({ code: "ENOENT" });
  await expect(readFile(output)).rejects.toMatchObject({ code: "ENOENT" });
});

it.each(["0.0.0", "0.0.0-dev", "0.0.0-dev.123"])(
  "refuses development identity %s even when release assembly is invoked directly",
  async (version) => {
    const assets = await fixture();
    await expect(prepareUnsignedRelease(assets, version, commit)).rejects.toThrow(
      /Invalid unsigned release/u,
    );
    await expect(readFile(join(assets, "SHA256SUMS"))).rejects.toMatchObject({ code: "ENOENT" });
  },
);

it("refuses the wrong native runner before starting packaging checks or a build", async () => {
  const result = execute(process.execPath, ["--import", "tsx", "tools/package.ts"], {
    env: { ...process.env, EXPECTED_PLATFORM: "different-platform" },
    timeout: 15000,
  });
  await expect(result).rejects.toMatchObject({ code: 1, stdout: "" });
  await expect(result).rejects.toThrow("Native package platform mismatch");
});
