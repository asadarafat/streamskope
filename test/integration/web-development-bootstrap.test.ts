import { execFile, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { once } from "node:events";
import { cp, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

import { afterEach, describe, expect, it } from "vitest";

import { BUILD_DEPENDENCY_PATCHES } from "../../tools/check/build-dependency-patch-data";
import { RUNTIME_DEPENDENCY_PATCHES } from "../../tools/check/runtime-dependency-patch-data";

const execute = promisify(execFile);
const roots: string[] = [];
const sourceRoot = new URL("../../", import.meta.url);
const dependencyPatches = [...BUILD_DEPENDENCY_PATCHES, ...RUNTIME_DEPENDENCY_PATCHES];

afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function fixture(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-web-bootstrap-"));
  roots.push(root);
  await mkdir(join(root, "tools"));
  await mkdir(join(root, "tools", "dev"));
  await mkdir(join(root, "tools", "check"));
  await cp(new URL("tools/dev.mjs", sourceRoot), join(root, "tools", "dev.mjs"));
  for (const name of [
    "forge-patch.ts",
    "build-dependency-patches.ts",
    "build-dependency-patch-data.ts",
    "runtime-dependency-patch-data.ts",
  ])
    await cp(new URL(`tools/check/${name}`, sourceRoot), join(root, "tools", "check", name));
  const manifest = {
    name: "bootstrap-test",
    private: true,
    type: "module",
    dependencies: {
      "node-forge": "1.4.0",
      "jks-js": "1.1.7",
      ...Object.fromEntries(RUNTIME_DEPENDENCY_PATCHES.map(({ name, version }) => [name, version])),
    },
    devDependencies: Object.fromEntries(
      BUILD_DEPENDENCY_PATCHES.map(({ name, version }) => [name, version]),
    ),
  };
  await writeFile(join(root, "package.json"), JSON.stringify(manifest));
  const dependencies = ["node-forge", "jks-js", ...dependencyPatches.map(({ name }) => name)];
  const actualLock = JSON.parse(
    await readFile(new URL("package-lock.json", sourceRoot), "utf8"),
  ) as { packages: Record<string, unknown> };
  await writeFile(
    join(root, "package-lock.json"),
    JSON.stringify({
      lockfileVersion: 3,
      packages: {
        "": manifest,
        ...Object.fromEntries(
          dependencies.map((name) => [
            `node_modules/${name}`,
            actualLock.packages[`node_modules/${name}`],
          ]),
        ),
      },
    }),
  );
  for (const name of dependencies)
    await cp(new URL(`node_modules/${name}/`, sourceRoot), join(root, "node_modules", name), {
      recursive: true,
    });
  // Start from the reviewed original sources even when npm ci already patched the workspace.
  for (const patch of dependencyPatches) {
    for (const file of patch.files) {
      const path = join(root, "node_modules", patch.name, file.file);
      let source = await readFile(path, "utf8");
      if (createHash("sha256").update(source).digest("hex") === file.patchedSha256) {
        for (const hunk of [...file.replacements].reverse())
          source = source.replace(hunk.after, hunk.before);
      }
      expect(createHash("sha256").update(source).digest("hex")).toBe(file.originalSha256);
      await writeFile(path, source);
    }
  }
  // Keep the real TypeScript loader separate from the simulated native-health failure.
  await cp(new URL("node_modules/tsx/", sourceRoot), join(root, "node_modules", "tsx"), {
    recursive: true,
  });
  await cp(
    new URL("node_modules/esbuild/", sourceRoot),
    join(root, "node_modules", "tsx", "node_modules", "esbuild"),
    { recursive: true },
  );
  for (const name of await readdir(new URL("node_modules/@esbuild/", sourceRoot)))
    await cp(
      new URL(`node_modules/@esbuild/${name}/`, sourceRoot),
      join(root, "node_modules", "tsx", "node_modules", "@esbuild", name),
      { recursive: true },
    );
  await writeFile(join(root, ".npmrc"), "fund=false\n");
  const modules: Record<string, string> = {
    esbuild: `exports.transformSync = () => { if (process.env.TEST_HEALTHY !== "1") require("streamskope-native-fixture"); return {code:""}; };`,
    vite: "exports.version = 'fixture';",
    "@node-rs/crc32": "exports.crc32 = () => 0;",
  };
  for (const [name, content] of Object.entries(modules)) {
    const path = join(root, "node_modules", name);
    await mkdir(path, { recursive: true });
    await writeFile(join(path, "package.json"), JSON.stringify({ name, main: "index.cjs" }));
    await writeFile(join(path, "index.cjs"), content);
  }
  await writeFile(join(root, "node_modules", "mac-installation-marker"), "preserve");
  await writeFile(
    join(root, "tools", "dev", "start.ts"),
    `
    process.stdout.write("fixture launcher ready\\n");
    process.exitCode = Number(process.env.TEST_LAUNCH_EXIT ?? 0);
  `,
  );
  // npm is an external installation boundary; this fixture never accesses a registry.
  await writeFile(
    join(root, "tools", "fake-npm.cjs"),
    `
    const fs = require("node:fs");
    const path = require("node:path");
    const root = path.dirname(__dirname);
    fs.appendFileSync(path.join(root, "install-calls"), "install\\n");
    const args = process.argv.slice(2);
    if (!args.includes("ci") || !args.includes("--ignore-scripts")) process.exit(9);
    if (process.env.TEST_INSTALL_FAIL === "1") process.exit(7);
    const target = args[args.indexOf("--prefix") + 1];
    for (const name of ["node-forge", "jks-js"])
      fs.cpSync(path.join(root, "node_modules", name), path.join(target, "node_modules", name), { recursive: true });
    const native = path.join(target, "node_modules", "streamskope-native-fixture");
    fs.mkdirSync(native, { recursive: true });
    fs.writeFileSync(path.join(native, "index.js"), "module.exports = true;");
  `,
  );
  return root;
}

async function launch(root: string, extra: Record<string, string> = {}): Promise<string> {
  const result = await execute(process.execPath, ["tools/dev.mjs"], {
    cwd: root,
    env: {
      ...process.env,
      NODE_PATH: "",
      npm_execpath: join(root, "tools", "fake-npm.cjs"),
      ...extra,
    },
    timeout: 15_000,
  });
  return result.stdout;
}

describe("web development native bootstrap", () => {
  it("runs before tsx so a wrong esbuild cannot prevent recovery", async () => {
    const manifest = JSON.parse(await readFile(new URL("package.json", sourceRoot), "utf8")) as {
      scripts: Record<string, string>;
    };
    expect(manifest.scripts.dev).toBe("node tools/dev.mjs");
  });

  it("starts a healthy installation without npm and propagates launcher failures", async () => {
    const root = await fixture();
    const output = await launch(root, { TEST_HEALTHY: "1" });
    expect(output).toContain("fixture launcher ready");
    expect(output).toContain(
      `Verified exact-source dependency mitigations: ${dependencyPatches.map(({ name }) => name).join(", ")}.`,
    );
    for (const patch of dependencyPatches) {
      for (const file of patch.files) {
        const source = await readFile(join(root, "node_modules", patch.name, file.file), "utf8");
        expect(createHash("sha256").update(source).digest("hex")).toBe(file.patchedSha256);
      }
    }
    await expect(readFile(join(root, "install-calls"))).rejects.toMatchObject({ code: "ENOENT" });
    await expect(launch(root, { TEST_HEALTHY: "1", TEST_LAUNCH_EXIT: "7" })).rejects.toMatchObject({
      code: 7,
    });
  });

  it("refuses a modified NATS runtime dependency before starting the launcher", async () => {
    const root = await fixture();
    const patch = RUNTIME_DEPENDENCY_PATCHES[0];
    const path = join(root, "node_modules", patch.name, patch.files[0].file);
    await writeFile(path, `${await readFile(path, "utf8")}\n// changed dependency\n`);
    await expect(launch(root, { TEST_HEALTHY: "1" })).rejects.toMatchObject({
      code: 1,
      stdout: expect.not.stringContaining("fixture launcher ready") as unknown,
      stderr: expect.stringContaining("Missing or unreviewed dependency mitigation") as unknown,
    });
    await expect(readFile(join(root, "install-calls"))).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("repairs through a cache, reuses it and preserves the shared installation", async () => {
    const root = await fixture();
    const lock = await readFile(join(root, "package-lock.json"));
    expect(await launch(root)).toContain("fixture launcher ready");
    expect(await launch(root, { TEST_INSTALL_FAIL: "1" })).toContain("fixture launcher ready");
    expect(await readFile(join(root, "install-calls"), "utf8")).toBe("install\n");
    expect(await readFile(join(root, "package-lock.json"))).toEqual(lock);
    expect(await readFile(join(root, "node_modules", "mac-installation-marker"), "utf8")).toBe(
      "preserve",
    );
    await expect(
      readFile(join(root, "node_modules", "streamskope-native-fixture", "index.js")),
    ).rejects.toMatchObject({ code: "ENOENT" });
    await writeFile(
      join(root, "package-lock.json"),
      JSON.stringify({ ...(JSON.parse(lock.toString()) as object), revision: 2 }),
    );
    expect(await launch(root)).toContain("fixture launcher ready");
    expect(await readFile(join(root, "install-calls"), "utf8")).toBe("install\ninstall\n");
  });

  it("refuses a modified security dependency in a reused native cache", async () => {
    const root = await fixture();
    expect(await launch(root)).toContain("fixture launcher ready");
    const [cache] = await readdir(join(root, ".cache", "web-native"));
    const path = join(root, ".cache", "web-native", cache!, "node_modules/node-forge/lib/rsa.js");
    await writeFile(path, `${await readFile(path, "utf8")}\n// changed dependency\n`);
    await expect(launch(root)).rejects.toMatchObject({ code: 1 });
    expect(await readFile(join(root, "install-calls"), "utf8")).toBe("install\n");
  });

  it("cleans failed installation staging without publishing a partial cache", async () => {
    const root = await fixture();
    await expect(launch(root, { TEST_INSTALL_FAIL: "1" })).rejects.toMatchObject({ code: 1 });
    expect(await readdir(join(root, ".cache", "web-native"))).toEqual([]);
    expect(await launch(root)).toContain("fixture launcher ready");
  });

  it("allows concurrent preparation without publishing partial caches", async () => {
    const root = await fixture();
    expect(await launch(root, { TEST_HEALTHY: "1" })).toContain("fixture launcher ready");
    const results = await Promise.all([launch(root), launch(root)]);
    for (const result of results) expect(result).toContain("fixture launcher ready");
    const caches = await readdir(join(root, ".cache", "web-native"));
    expect(caches).toHaveLength(1);
    expect(caches[0]).not.toContain(".preparing-");
  });

  it.skipIf(process.platform === "win32")("forwards shutdown to the running launcher", async () => {
    const root = await fixture();
    await writeFile(
      join(root, "tools", "dev", "start.ts"),
      `
      const timer = setInterval(() => {}, 1000);
      process.on("SIGTERM", () => { clearInterval(timer); process.exitCode = 143; });
      process.stdout.write("fixture launcher ready\\n");
    `,
    );
    const child = spawn(process.execPath, ["tools/dev.mjs"], {
      cwd: root,
      env: { ...process.env, TEST_HEALTHY: "1" },
      stdio: ["ignore", "pipe", "pipe"],
    });
    try {
      await Promise.race([
        (async (): Promise<void> => {
          let output = "";
          while (!output.includes("fixture launcher ready\n"))
            output += String((await once(child.stdout, "data"))[0]);
        })(),
        once(child, "exit").then(([code]) => {
          throw new Error(`Fixture launcher exited before readiness (exit ${code}).`);
        }),
      ]);
      const exited = once(child, "exit");
      child.kill("SIGTERM");
      expect((await exited)[0]).toBe(143);
    } finally {
      if (child.exitCode === null) child.kill("SIGKILL");
    }
  });

  it("reports missing base packages without reinstalling the shared directory", async () => {
    const root = await fixture();
    await rm(join(root, "node_modules", "tsx"), { recursive: true });
    await expect(launch(root)).rejects.toMatchObject({
      code: 1,
      stderr: expect.stringContaining("npm ci") as unknown,
    });
    await expect(readFile(join(root, "install-calls"))).rejects.toMatchObject({ code: "ENOENT" });
  });
});
