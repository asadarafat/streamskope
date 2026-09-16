import { mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, expect, it } from "vitest";

import { prepareUnsignedRelease } from "../../tools/unsigned-release-assets";

const directories: string[] = [];
const names = [
  "StreamSkope-0.1.0-darwin-arm64.dmg",
  "StreamSkope-0.1.0-win32-x64-Setup.exe",
  "StreamSkope-0.1.0-linux-x64.AppImage",
] as const;
const commit = "a".repeat(40);

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
