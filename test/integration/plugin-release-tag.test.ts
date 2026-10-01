import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import { afterEach, expect, it } from "vitest";

const execute = promisify(execFile);
const directories: string[] = [];
const releaseScript = resolve("tools/package/release-tag.ts");
const loader = import.meta.resolve("tsx");

afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

async function validate(
  tag: string,
  project?: Record<string, unknown>,
): Promise<{ output: string; result: ReturnType<typeof execute> }> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-release-tag-"));
  directories.push(directory);
  const output = join(directory, "output");
  if (project !== undefined)
    await writeFile(join(directory, "package.json"), JSON.stringify(project));
  return {
    output,
    result: execute(process.execPath, ["--import", loader, releaseScript], {
      ...(project === undefined ? {} : { cwd: directory }),
      env: {
        ...process.env,
        GITHUB_REF_NAME: tag,
        GITHUB_SHA: "a".repeat(40),
        GITHUB_OUTPUT: output,
      },
      timeout: 15_000,
    }),
  };
}

it.each([
  ["v0.1.0+build.1", "0.1.0"],
  ["plugins/eda/v0.1.0+build.1--eda-26.8.2-26.8.2--r1", "v0.1.0+build.1--eda-26.8.2-26.8.2--r1"],
  ["plugins/nsp/v0.1.0+build.1--nsp-26.4.0-26.4.0--r1", "v0.1.0+build.1--nsp-26.4.0-26.4.0--r1"],
])("validates %s against its own product version", async (tag, version) => {
  const { output, result } = await validate(tag);
  await result;
  expect(await readFile(output, "utf8")).toBe(`version=${version}\n`);
});

it.each([
  "v0.1.0",
  "v0.1.0+build.2",
  "v0.1.0+build.4",
  "v0.1.0+build.5",
  "v0.1.0+build.6",
  "plugins/eda/v0.1.0",
  "plugins/eda/v0.1.0+build.5--eda-26.8.2-26.8.2--r1",
  "plugins/nsp/v0.1.0+build.5--nsp-26.4.0-26.4.0--r1",
  "plugins/eda/v0.1.0+build.1--eda-26.8.2-27.4.0--r1",
  "plugins/eda/v0.1.0+build.1--eda-26.8.2-26.8.2--r2",
  "plugins/nsp/v0.1.0+build.4--nsp-26.4.0-26.4.0--r1",
  "plugins/nsp/v0.1.0+build.1--eda-26.8.2-26.8.2--r1",
  "v26.8.2",
  "plugins/eda/v26.8.3",
  "plugins/other/v26.8.2",
  "plugins/nsp/v26.8.2",
  "plugins/nsp/v0.1.1",
  "plugins/nsp/v0.1.0+build.2",
  "plugins/eda/v26.8.2+build.2",
  "v0.1.1+build.2",
  "v0.1.0+build.0",
  "v0.1.0+build.02",
  "v0.1.0+build.-1",
  "v0.1.0+other.2",
  "v0.1.0-rc.1",
  "v0.1.0+build.1\n",
])("rejects mismatched or unsupported release tag %s before emitting a version", async (tag) => {
  const { output, result } = await validate(tag);
  await expect(result).rejects.toMatchObject({ code: 1 });
  await expect(readFile(output)).rejects.toMatchObject({ code: "ENOENT" });
});

it.each(["v0.1.0", "v0.1.0+build.1", "v0.1.0+build.6"])(
  "allows desktop tag %s only when it matches the embedded release identity",
  async (tag) => {
    const { output, result } = await validate(tag, { version: "0.1.0", streamskopeRelease: tag });
    await result;
    expect(await readFile(output, "utf8")).toBe("version=0.1.0\n");
  },
);

it.each([
  { version: "0.1.0" },
  { version: "0.1.0", streamskopeRelease: "v0.1.0+build.4" },
  { version: "0.1.0", streamskopeRelease: "v0.1.0+build.6" },
  { version: "0.1.1", streamskopeRelease: "v0.1.0+build.1" },
  { version: "0.1.0", streamskopeRelease: 5 },
])("rejects absent or inconsistent embedded desktop metadata %j", async (project) => {
  const { output, result } = await validate("v0.1.0+build.1", project);
  await expect(result).rejects.toMatchObject({ code: 1 });
  await expect(readFile(output)).rejects.toMatchObject({ code: "ENOENT" });
});
