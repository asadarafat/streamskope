import { execFile } from "node:child_process";
import { cp, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import { afterEach, expect, it } from "vitest";

import { prepareReleaseVersion, releaseIdentity } from "../../tools/package/release-version";
import { releaseNotesBody } from "../../tools/package/release-policy";

const execute = promisify(execFile);
const directories: string[] = [];
const loader = import.meta.resolve("tsx");
const script = resolve("tools/package/release-version.ts");
const sourceFiles = [
  "package.json",
  "package-lock.json",
  "plugins/eda/manifest.json",
  "plugins/nsp/manifest.json",
  "website/docs/releases/unreleased.md",
];
const notes =
  "---\ntitle: Unreleased changes\nunreleased: true\n---\n\n# Unreleased changes\n\nReviewed feature and known qualification gaps.\n";

interface ProjectManifest {
  version: string;
  dependencies: Record<string, string>;
  scripts: Record<string, string>;
}

interface LockManifest {
  version: string;
  packages: Record<string, { version: string }>;
}

afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

async function fixture(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-release-version-"));
  directories.push(root);
  for (const name of sourceFiles.slice(0, -1)) {
    await mkdir(join(root, name, ".."), { recursive: true });
    await cp(name, join(root, name));
  }
  await mkdir(join(root, "website/docs/releases"), { recursive: true });
  await writeFile(join(root, "website/docs/releases/unreleased.md"), notes);
  return root;
}

async function contents(root: string): Promise<readonly string[]> {
  return Promise.all(sourceFiles.map((name) => readFile(join(root, name), "utf8")));
}

it.each([
  ["desktop", "0.2.1", "v0.2.1"],
  ["desktop", "1.0.0-rc.2", "v1.0.0-rc.2"],
  ["eda", "0.1.2", "plugins/eda/v0.1.2"],
  ["nsp", "1.0.0-beta.1", "plugins/nsp/v1.0.0-beta.1"],
])(
  "assigns %s version %s from release input without changing source during validation",
  async (component, version, tag) => {
    const root = await fixture();
    const before = await contents(root);
    expect(await prepareReleaseVersion(root, component, version)).toEqual({
      component,
      version,
      tag,
      prerelease: version.includes("-"),
    });
    expect(await contents(root)).toEqual(before);
  },
);

it.each([
  "",
  "v0.2.0",
  "0.2",
  "00.2.0",
  "0.2.0-rc.01",
  "0.2.0+build.1",
  "0.2.0\n",
  "../0.2.0",
  "0.0.0",
  "0.0.0-dev",
  "0.0.0-dev.12",
  "0.0.0-rc.1",
])("rejects release identity %j without writing version output or source", async (version) => {
  const root = await fixture();
  const before = await contents(root);
  const output = join(root, "output");
  await expect(
    execute(process.execPath, ["--import", loader, script, "desktop", version, "--stamp"], {
      cwd: root,
      env: { ...process.env, GITHUB_OUTPUT: output },
      timeout: 15000,
    }),
  ).rejects.toMatchObject({ code: 1 });
  expect(await contents(root)).toEqual(before);
  await expect(readFile(output)).rejects.toMatchObject({ code: "ENOENT" });
});

it.each(["other", "plugins/eda", "../nsp", "Desktop"])(
  "rejects unknown release component %s",
  (component) => {
    expect(() => releaseIdentity(component, "0.2.0")).toThrow(/component/u);
  },
);

it.each(["0.2.4", "0.3.0-rc.2"])(
  "stamps desktop %s consistently and reproducibly without touching plugins",
  async (version) => {
    const root = await fixture();
    const original = await contents(root);
    await prepareReleaseVersion(root, "desktop", version, true);
    const project = JSON.parse(
      await readFile(join(root, "package.json"), "utf8"),
    ) as ProjectManifest;
    const lock = JSON.parse(
      await readFile(join(root, "package-lock.json"), "utf8"),
    ) as LockManifest;
    expect(project.version).toBe(version);
    expect(lock.version).toBe(version);
    expect(lock.packages[""]?.version).toBe(version);
    expect(project.dependencies).toEqual(
      (JSON.parse(original[0]!) as ProjectManifest).dependencies,
    );
    expect(Object.keys(project.scripts)).toHaveLength(5);
    expect(lock.packages["node_modules/react"]).toEqual(
      (JSON.parse(original[1]!) as LockManifest).packages["node_modules/react"],
    );
    expect((await contents(root)).slice(2)).toEqual(original.slice(2));
    const generated = await readFile(join(root, `website/docs/releases/v${version}.md`), "utf8");
    expect(releaseNotesBody(generated, version)).toContain(`# StreamSkope v${version}\n`);
    expect(generated).toContain("Reviewed feature and known qualification gaps.");
    expect(generated).not.toContain("unreleased: true");
    const stamped = await contents(root);
    await prepareReleaseVersion(root, "desktop", version, true);
    expect(await contents(root)).toEqual(stamped);
    expect(await readFile(join(root, `website/docs/releases/v${version}.md`), "utf8")).toBe(
      generated,
    );

    // The host code reads the stamped checkout, rather than retaining a compiled-in future version.
    await mkdir(join(root, "src/plugins"), { recursive: true });
    for (const file of ["host-release.ts", "compatibility.ts", "contracts.ts"])
      await cp(join("src/plugins", file), join(root, "src/plugins", file));
    const result = await execute(
      process.execPath,
      [
        "--import",
        loader,
        "--eval",
        "process.stdout.write(require('./src/plugins/host-release.ts').STREAMSKOPE_RELEASE)",
      ],
      { cwd: root, timeout: 15000 },
    );
    expect(result.stdout).toBe(`v${version}`);
  },
);

it.each(["eda", "nsp"])(
  "stamps only the selected %s plugin and preserves compatibility/resource declarations",
  async (component) => {
    const root = await fixture();
    const original = await contents(root);
    const result = await execute(
      process.execPath,
      ["--import", loader, script, component, "0.7.2", "--stamp"],
      {
        cwd: root,
        env: { ...process.env, GITHUB_OUTPUT: join(root, "output") },
        timeout: 15000,
      },
    );
    expect(result.stdout).toContain(`plugins/${component}/v0.7.2`);
    const output = await readFile(join(root, "output"), "utf8");
    expect(output).toBe(
      `component=${component}\nversion=0.7.2\ntag=plugins/${component}/v0.7.2\nprerelease=false\n`,
    );
    const modified = sourceFiles.indexOf(`plugins/${component}/manifest.json`);
    const after = await contents(root);
    for (const [index, content] of after.entries()) {
      if (index === modified)
        expect(JSON.parse(content)).toEqual({
          ...(JSON.parse(original[index]!) as Record<string, unknown>),
          version: "0.7.2",
        });
      else expect(content).toBe(original[index]);
    }
    await prepareReleaseVersion(root, component, "0.7.2", true);
    expect(await contents(root)).toEqual(after);
  },
);

it.each([
  "conflicting version",
  "missing lock root",
  "wrong plugin identity",
  "wrong plugin API",
  "wrong target",
  "versioned source notes",
  "empty notes",
  "existing notes",
])("fails closed for %s before changing any files", async (kind) => {
  const root = await fixture();
  let component = "desktop";
  if (kind === "conflicting version") {
    const file = join(root, "package-lock.json");
    const lock = JSON.parse(await readFile(file, "utf8")) as LockManifest;
    lock.version = "9.0.0";
    await writeFile(file, JSON.stringify(lock));
  } else if (kind === "missing lock root") {
    await writeFile(
      join(root, "package-lock.json"),
      JSON.stringify({ version: "0.0.0-dev", packages: {} }),
    );
  } else if (kind.startsWith("wrong")) {
    component = "nsp";
    const file = join(root, "plugins/nsp/manifest.json");
    const manifest = JSON.parse(await readFile(file, "utf8")) as Record<string, unknown> & {
      compatibility: { target: { system: string } };
    };
    if (kind === "wrong plugin identity") manifest.id = "streamskope.eda";
    if (kind === "wrong plugin API") manifest.apiVersion = 2;
    if (kind === "wrong target") manifest.compatibility.target.system = "eda";
    await writeFile(file, JSON.stringify(manifest));
  } else if (kind === "existing notes") {
    await writeFile(
      join(root, "website/docs/releases/v0.2.0.md"),
      "Historical notes, never overwrite.\n",
    );
  } else {
    await writeFile(
      join(root, "website/docs/releases/unreleased.md"),
      kind === "empty notes"
        ? ""
        : notes.replace("unreleased: true", "unreleased: true\nrelease_version: 0.2.0"),
    );
  }
  const before = await contents(root);
  await expect(prepareReleaseVersion(root, component, "0.2.0", true)).rejects.toThrow();
  expect(await contents(root)).toEqual(before);
});
