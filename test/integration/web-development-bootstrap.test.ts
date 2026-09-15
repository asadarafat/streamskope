import { execFile, spawn } from "node:child_process";
import { once } from "node:events";
import { cp, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

import { afterEach, describe, expect, it } from "vitest";

const execute = promisify(execFile);
const roots: string[] = [];
const sourceRoot = new URL("../../", import.meta.url);

afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function fixture(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-web-bootstrap-"));
  roots.push(root);
  await mkdir(join(root, "tools"));
  for (const file of ["bootstrap-web-development.mjs", "check-web-development-runtime.mjs"]) {
    await cp(new URL(`tools/${file}`, sourceRoot), join(root, "tools", file));
  }
  await writeFile(
    join(root, "package.json"),
    JSON.stringify({ name: "bootstrap-test", private: true }),
  );
  await writeFile(join(root, "package-lock.json"), JSON.stringify({ lockfileVersion: 3 }));
  await writeFile(join(root, ".npmrc"), "fund=false\n");
  const modules: Record<string, string> = {
    esbuild: `exports.transformSync = () => { if (process.env.TEST_HEALTHY !== "1") require("streamskope-native-fixture"); return {code:""}; };`,
    vite: "exports.version = 'fixture';",
    "@node-rs/crc32": "exports.crc32 = () => 0;",
    tsx: "",
  };
  for (const [name, content] of Object.entries(modules)) {
    const path = join(root, "node_modules", name);
    await mkdir(path, { recursive: true });
    await writeFile(join(path, "package.json"), JSON.stringify({ name, main: "index.cjs" }));
    await writeFile(join(path, "index.cjs"), content);
  }
  await writeFile(join(root, "node_modules", "mac-installation-marker"), "preserve");
  await writeFile(
    join(root, "tools", "start-web-development.ts"),
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
    const native = path.join(target, "node_modules", "streamskope-native-fixture");
    fs.mkdirSync(native, { recursive: true });
    fs.writeFileSync(path.join(native, "index.js"), "module.exports = true;");
  `,
  );
  return root;
}

async function launch(root: string, extra: Record<string, string> = {}): Promise<string> {
  const result = await execute(process.execPath, ["tools/bootstrap-web-development.mjs"], {
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
    expect(manifest.scripts["dev:web"]).toBe("node tools/bootstrap-web-development.mjs");
  });

  it("starts a healthy installation without npm and propagates launcher failures", async () => {
    const root = await fixture();
    expect(await launch(root, { TEST_HEALTHY: "1" })).toContain("fixture launcher ready");
    await expect(readFile(join(root, "install-calls"))).rejects.toMatchObject({ code: "ENOENT" });
    await expect(launch(root, { TEST_HEALTHY: "1", TEST_LAUNCH_EXIT: "7" })).rejects.toMatchObject({
      code: 7,
    });
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
      JSON.stringify({ lockfileVersion: 3, revision: 2 }),
    );
    expect(await launch(root)).toContain("fixture launcher ready");
    expect(await readFile(join(root, "install-calls"), "utf8")).toBe("install\ninstall\n");
  });

  it("cleans failed installation staging without publishing a partial cache", async () => {
    const root = await fixture();
    await expect(launch(root, { TEST_INSTALL_FAIL: "1" })).rejects.toMatchObject({ code: 1 });
    expect(await readdir(join(root, ".cache", "web-native"))).toEqual([]);
    expect(await launch(root)).toContain("fixture launcher ready");
  });

  it("allows concurrent preparation without publishing partial caches", async () => {
    const root = await fixture();
    const results = await Promise.all([launch(root), launch(root)]);
    for (const result of results) expect(result).toContain("fixture launcher ready");
    const caches = await readdir(join(root, ".cache", "web-native"));
    expect(caches).toHaveLength(1);
    expect(caches[0]).not.toContain(".preparing-");
  });

  it.skipIf(process.platform === "win32")("forwards shutdown to the running launcher", async () => {
    const root = await fixture();
    await writeFile(
      join(root, "tools", "start-web-development.ts"),
      `
      const timer = setInterval(() => {}, 1000);
      process.on("SIGTERM", () => { clearInterval(timer); process.exitCode = 143; });
      process.stdout.write("ready");
    `,
    );
    const child = spawn(process.execPath, ["tools/bootstrap-web-development.mjs"], {
      cwd: root,
      env: { ...process.env, TEST_HEALTHY: "1" },
      stdio: ["ignore", "pipe", "pipe"],
    });
    try {
      await once(child.stdout, "data");
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
