import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { access, mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import { delimiter, join } from "node:path";
import process from "node:process";

const root = process.cwd();
let interrupted = false;
let child;
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    interrupted = true;
    process.exitCode = signal === "SIGINT" ? 130 : 143;
    child?.kill(signal);
  });
}

function run(args, env, timeout = 0) {
  return new Promise((resolve, reject) => {
    if (interrupted) return reject(new Error("Startup cancelled."));
    child = spawn(process.execPath, args, { cwd: root, env, stdio: "inherit", timeout });
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      child = undefined;
      resolve(code ?? (signal === "SIGINT" ? 130 : 143));
    });
  });
}

function environment(cache) {
  return {
    ...process.env,
    NODE_PATH: [join(cache, "node_modules"), process.env.NODE_PATH].filter(Boolean).join(delimiter),
  };
}

function healthy(env) {
  return (
    spawnSync(process.execPath, ["tools/check-web-development-runtime.mjs"], {
      cwd: root,
      env,
      timeout: 15_000,
      stdio: "pipe",
    }).status === 0
  );
}

async function prepare() {
  for (const name of ["tsx", "esbuild", "vite", "@node-rs/crc32"]) {
    try {
      await access(join(root, "node_modules", name, "package.json"));
    } catch {
      throw new Error(`Missing ${name}. Run npm ci before npm run dev:web.`);
    }
  }
  if (healthy(process.env)) return process.env;
  if (!process.env.npm_execpath)
    throw new Error("Start through npm run dev:web to enable native dependency recovery.");

  const files = new Map();
  for (const name of ["package.json", "package-lock.json", ".npmrc"]) {
    try {
      files.set(name, await readFile(join(root, name)));
    } catch (error) {
      if (name !== ".npmrc" || error.code !== "ENOENT") throw error;
    }
  }
  const hash = createHash("sha256");
  for (const [name, bytes] of files) hash.update(name).update(bytes);
  const libc =
    process.platform === "linux"
      ? (process.report.getReport().header.glibcVersionRuntime ?? "musl")
      : "native";
  const identity = `${process.platform}-${process.arch}-${libc}-${process.versions.modules}-${hash.digest("hex").slice(0, 24)}`;
  const directory = join(root, ".cache", "web-native");
  const cache = join(directory, identity);
  const env = environment(cache);
  await mkdir(directory, { recursive: true });
  try {
    await access(cache);
    if (!healthy(env))
      throw new Error(
        `Native cache is unhealthy: ${cache}. Move it aside and retry npm run dev:web.`,
      );
    return env;
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }

  process.stdout.write(
    `Preparing isolated ${process.platform}-${process.arch} web dependencies; shared node_modules will not change.\n`,
  );
  const staging = await mkdtemp(join(directory, ".preparing-"));
  try {
    for (const [name, bytes] of files) await writeFile(join(staging, name), bytes, { mode: 0o600 });
    const code = await run(
      [
        process.env.npm_execpath,
        "ci",
        "--prefix",
        staging,
        "--ignore-scripts",
        "--no-audit",
        "--no-fund",
      ],
      process.env,
      300_000,
    );
    if (interrupted || code !== 0)
      throw new Error(
        `Native dependency preparation stopped (exit ${code}). Retry npm run dev:web; the shared installation is unchanged.`,
      );
    if (!healthy(environment(staging)))
      throw new Error(
        "Prepared dependencies failed the native runtime check. Shared installation is unchanged.",
      );
    try {
      await rename(staging, cache);
    } catch (error) {
      if (!["EEXIST", "ENOTEMPTY"].includes(error.code) || !healthy(env)) throw error;
    }
    return env;
  } finally {
    // Only this invocation's uniquely created staging directory is disposable.
    await rm(staging, { recursive: true, force: true });
  }
}

try {
  const env = await prepare();
  process.exitCode = await run(["--import", "tsx", "tools/start-web-development.ts"], env);
} catch (error) {
  process.stderr.write(`StreamSkope web development startup failed: ${error.message}\n`);
  if (!interrupted) process.exitCode = 1;
}
