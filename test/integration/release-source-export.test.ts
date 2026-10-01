import { execFileSync, spawnSync } from "node:child_process";
import {
  existsSync,
  mkdtempSync,
  mkdirSync,
  readFileSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

const exporter = fileURLToPath(
  new URL("../../tools/maintenance/export-source.mjs", import.meta.url),
);

function fixture(): { root: string; source: string; destination: string } {
  const root = mkdtempSync(join(tmpdir(), "streamskope-export-test-"));
  const source = join(root, "development");
  mkdirSync(source);
  const git = (args: string[]): string =>
    execFileSync("git", args, {
      cwd: source,
      encoding: "utf8",
      env: {
        ...process.env,
        GIT_AUTHOR_NAME: "Export test",
        GIT_AUTHOR_EMAIL: "export@example.invalid",
        GIT_COMMITTER_NAME: "Export test",
        GIT_COMMITTER_EMAIL: "export@example.invalid",
      },
    });
  git(["init", "--initial-branch=development"]);
  writeFileSync(join(source, "package.json"), '{"version":"0.1.0"}\n');
  writeFileSync(join(source, ".gitignore"), ".env\ndist/\n");
  writeFileSync(join(source, "source.txt"), "original source\n");
  git(["add", "."]);
  git(["commit", "-m", "Private development history"]);
  git(["tag", "private-development-tag"]);
  return { root, source, destination: join(root, "public") };
}

function run(source: string, destination: string): ReturnType<typeof spawnSync> {
  return spawnSync(process.execPath, [exporter, destination], { cwd: source, encoding: "utf8" });
}

describe("first public source export", () => {
  it("exports current source and new public files without inherited Git history, secrets, build output or old signed catalog artifacts", () => {
    const { source, destination } = fixture();
    writeFileSync(join(source, "source.txt"), "current reviewed source\n");
    writeFileSync(join(source, "new.txt"), "new public source\n");
    writeFileSync(join(source, ".env"), "PRIVATE_TEST_VALUE=excluded\n");
    mkdirSync(join(source, "dist"));
    writeFileSync(join(source, "dist/artifact.txt"), "generated\n");
    mkdirSync(join(source, "apps"));
    writeFileSync(join(source, "apps/old-signed-catalog.json"), "historical publication\n");
    expect(run(source, destination).status).toBe(0);
    expect(readFileSync(join(destination, "source.txt"), "utf8")).toBe("current reviewed source\n");
    expect(existsSync(join(destination, "new.txt"))).toBe(true);
    for (const file of [".env", "dist", "apps"])
      expect(existsSync(join(destination, file))).toBe(false);
    const git = (args: string[]): ReturnType<typeof spawnSync> =>
      spawnSync("git", args, { cwd: destination, encoding: "utf8" });
    expect(git(["rev-parse", "--verify", "HEAD"]).status).not.toBe(0);
    expect(git(["tag", "--list"]).stdout).toBe("");
    expect(git(["remote", "-v"]).stdout).toBe("");
    expect(git(["symbolic-ref", "--short", "HEAD"]).stdout).toBe("main\n");
    expect(readFileSync(join(source, "source.txt"), "utf8")).toBe("current reviewed source\n");
  });

  it("rejects an existing destination and destinations inside the source, including a parent symlink", () => {
    const { root, source, destination } = fixture();
    mkdirSync(destination);
    writeFileSync(join(destination, "keep.txt"), "keep\n");
    expect(run(source, destination).status).not.toBe(0);
    expect(readFileSync(join(destination, "keep.txt"), "utf8")).toBe("keep\n");
    expect(run(source, join(source, "..public")).status).not.toBe(0);
    symlinkSync(source, join(root, "alias"), "dir");
    expect(run(source, join(root, "alias/public")).status).not.toBe(0);
    expect(existsSync(join(source, "public"))).toBe(false);
  });

  it("refuses a different first-release version", () => {
    const { source, destination } = fixture();
    writeFileSync(join(source, "package.json"), '{"version":"0.1.1"}\n');
    expect(run(source, destination).status).not.toBe(0);
    expect(existsSync(destination)).toBe(false);
  });
});
