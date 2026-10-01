import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
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
  plugin?: { readonly directory: "eda" | "nsp"; readonly manifest: Record<string, unknown> },
): Promise<{ output: string; result: ReturnType<typeof execute> }> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-release-tag-"));
  directories.push(directory);
  const output = join(directory, "output");
  if (project !== undefined)
    await writeFile(join(directory, "package.json"), JSON.stringify(project));
  if (plugin !== undefined) {
    const pluginDirectory = join(directory, "plugins", plugin.directory);
    await mkdir(pluginDirectory, { recursive: true });
    await writeFile(join(pluginDirectory, "manifest.json"), JSON.stringify(plugin.manifest));
  }
  return {
    output,
    result: execute(process.execPath, ["--import", loader, releaseScript], {
      ...(project === undefined && plugin === undefined ? {} : { cwd: directory }),
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
  ["v0.2.0", "0.2.0"],
  ["plugins/eda/v0.1.0", "0.1.0"],
  ["plugins/nsp/v0.1.0", "0.1.0"],
])("validates %s against its own product version", async (tag, version) => {
  const { output, result } = await validate(tag);
  await result;
  expect(await readFile(output, "utf8")).toBe(`version=${version}\nprerelease=false\n`);
});

it.each([
  "v0.1.0",
  "v0.2.1",
  "v0.2.0+build.1",
  "v0.2.0-rc.1",
  "v0.1.0+build.2",
  "plugins/eda/0.1.0",
  "plugins/eda/v0.1.0+build.1",
  "plugins/eda/v0.1.0-rc.1",
  "plugins/eda/v0.1.0+build.1--eda-26.8.2-26.8.2--r1",
  "plugins/nsp/v0.1.0+build.1--nsp-26.4.0-26.4.0--r1",
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

it.each(["0.2.0", "0.2.1", "0.3.0-rc.1", "1.0.0-beta.2"])(
  "derives desktop tag and preview status from the single package version %s",
  async (version) => {
    const { output, result } = await validate(`v${version}`, { version });
    await result;
    expect(await readFile(output, "utf8")).toBe(
      `version=${version}\nprerelease=${version.includes("-")}\n`,
    );
  },
);

it.each(["0.1.1", "0.2.0-rc.1"])(
  "validates a plugin-only %s release independently of the desktop version",
  async (version) => {
    const manifest = JSON.parse(await readFile("plugins/nsp/manifest.json", "utf8")) as Record<
      string,
      unknown
    >;
    const { output, result } = await validate(
      `plugins/nsp/v${version}`,
      { version: "0.2.7" },
      { directory: "nsp", manifest: { ...manifest, version } },
    );
    await result;
    expect(await readFile(output, "utf8")).toBe(
      `version=${version}\nprerelease=${version.includes("-")}\n`,
    );
  },
);

it("rejects a plugin tag whose manifest belongs to another plugin", async () => {
  const manifest = JSON.parse(await readFile("plugins/eda/manifest.json", "utf8")) as Record<
    string,
    unknown
  >;
  const { output, result } = await validate("plugins/nsp/v0.1.0", undefined, {
    directory: "nsp",
    manifest,
  });
  await expect(result).rejects.toMatchObject({ code: 1 });
  await expect(readFile(output)).rejects.toMatchObject({ code: "ENOENT" });
});

it.each([
  {},
  { version: "0.2.1" },
  { version: 5 },
  { version: "0.2.0+build.1" },
  { version: "0.2.0-rc.01" },
  { version: "00.2.0" },
  { version: "0.2.0\n" },
])("rejects absent, inconsistent or noncanonical desktop metadata %j", async (project) => {
  const { output, result } = await validate("v0.2.0", project);
  await expect(result).rejects.toMatchObject({ code: 1 });
  await expect(readFile(output)).rejects.toMatchObject({ code: "ENOENT" });
});

it.each(["0.2.0+build.1", "0.2.0+sha.abc123", "0.2.0-rc.01", "0.2.0\n"])(
  "rejects unsupported release %s even when the package and tag agree",
  async (version) => {
    const { output, result } = await validate(`v${version}`, { version });
    await expect(result).rejects.toMatchObject({ code: 1 });
    await expect(readFile(output)).rejects.toMatchObject({ code: "ENOENT" });
  },
);
