import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { afterEach, expect, it } from "vitest";

const execute = promisify(execFile);
const script = fileURLToPath(new URL("../../tools/check.sh", import.meta.url));
const directories: string[] = [];

afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

async function run(
  args: string[] = [],
  failure?: string,
): Promise<{
  status: number;
  calls: Array<{ command: string; args: string[]; suite?: string }>;
}> {
  const cwd = await mkdtemp(join(tmpdir(), "streamskope-check-command-"));
  directories.push(cwd);
  const bin = join(cwd, "bin");
  await mkdir(bin);
  await mkdir(join(cwd, "tools/check"), { recursive: true });
  await mkdir(join(cwd, "vendors/streamskope/apps/capture/agent"), { recursive: true });
  await writeFile(join(cwd, "tools/check/workflows.sh"), "#!/bin/sh\nnode workflow-validation\n");
  const log = join(cwd, "calls.jsonl");
  for (const command of ["node", "npx", "npm", "go", "cp"]) {
    await writeFile(
      join(bin, command),
      `#!${process.execPath}
const { appendFileSync } = require("node:fs");
const { basename } = require("node:path");
const command = basename(process.argv[1]);
const args = process.argv.slice(2);
appendFileSync(process.env.CHECK_COMMAND_LOG, JSON.stringify({command, args,
  ...(process.env.STREAMSKOPE_TEST_SUITE ? {suite: process.env.STREAMSKOPE_TEST_SUITE} : {})
}) + "\\n");
if (args.includes(process.env.CHECK_COMMAND_FAILURE)) process.exit(7);
`,
      { mode: 0o755 },
    );
  }
  let status = 0;
  try {
    await execute("bash", [script, ...args], {
      cwd,
      env: {
        ...process.env,
        PATH: `${bin}:${process.env.PATH ?? ""}`,
        CHECK_COMMAND_LOG: log,
        ...(failure ? { CHECK_COMMAND_FAILURE: failure } : {}),
      },
      timeout: 10_000,
    });
  } catch (error) {
    status = (error as { code: number }).code;
  }
  const lines = await readFile(log, "utf8").catch(() => "");
  return {
    status,
    calls: lines.trim()
      ? lines
          .trim()
          .split("\n")
          .map((line) => JSON.parse(line) as { command: string; args: string[] })
      : [],
  };
}

it("GitHub CI runs shared checks, docs and runtime checks without local soak, live clusters or packaging", async () => {
  const result = await run(["--ci"]);
  expect(result.status).toBe(0);
  const stages = result.calls.flatMap(({ args }) => args.map((arg) => basename(arg)));
  expect(result.calls.find(({ args }) => args[0] !== "tools/check/ci-evidence.ts")).toEqual({
    command: "node",
    args: ["tools/check/forge-patch.ts", "--apply"],
  });
  for (const stage of [
    "workflow-validation",
    "prettier",
    "eslint",
    "tsc",
    "vitest",
    "eda-source.mjs",
    "dependencies.ts",
    "audit.ts",
    "qualify",
  ])
    expect(stages).toContain(stage);
  expect(result.calls).toContainEqual({ command: "go", args: ["test", "-race", "./..."] });
  expect(stages).not.toContain("stream-pipeline-replay.ts");
  expect(stages).not.toContain("eda-live.ts");
  expect(stages).not.toContain("nsp-live.ts");
  expect(stages).not.toContain("package");
});

it("the default local command adds the 60-second soak and configured EDA/NSP qualification", async () => {
  const result = await run();
  expect(result.status).toBe(0);
  const soak = result.calls.find(({ args }) =>
    args.some((arg) => arg.endsWith("stream-pipeline-replay.ts")),
  );
  expect(soak?.args).toContain("--seconds=60");
  expect(result.calls).toContainEqual({
    command: "node",
    args: ["--import", "tsx", "tools/check/eda-live.ts"],
  });
  expect(result.calls).toContainEqual({
    command: "node",
    args: ["--import", "tsx", "tools/check/nsp-live.ts"],
  });
});

it("a failing shared check stops qualification before tests and docs", async () => {
  const result = await run(["--ci"], "eslint");
  expect(result.status).toBe(7);
  expect(result.calls.at(-2)?.args).toContain("eslint");
  expect(result.calls.at(-1)?.args.slice(-1)).toEqual(["7"]);
  expect(result.calls.some(({ command }) => command === "npm" || command === "go")).toBe(false);
});

it.each(["tools/check/forge-patch.ts", "tools/check/audit.ts"])(
  "stops qualification when the security gate %s fails",
  async (stage) => {
    const result = await run(["--ci"], stage);
    expect(result.status).toBe(7);
    expect(result.calls.at(-2)?.args).toContain(stage);
    expect(result.calls.some(({ args }) => args.includes("qualify"))).toBe(false);
  },
);

it.each([
  ["--relaxed"],
  ["--ci", "--extra"],
  ["--lane", "shared"],
  ["--ci", "--lane", "../shared"],
  ["--ci", "--lane", "unknown"],
  ["--ci", "--lane", "shared", "--extra"],
])("rejects unsupported options before running checks: %j", async (...args) => {
  const result = await run(args);
  expect(result.status).toBe(2);
  expect(result.calls).toEqual([]);
});

it("the three fixed lanes execute exactly the complete CI command sequence", async () => {
  const complete = await run(["--ci"]);
  const parts = await Promise.all(
    ["shared", "docs", "runtime"].map((lane) => run(["--ci", "--lane", lane])),
  );
  const commands = (result: Awaited<ReturnType<typeof run>>): typeof result.calls =>
    result.calls.filter(({ args }) => args[0] !== "tools/check/ci-evidence.ts");
  expect(parts.every(({ status }) => status === 0)).toBe(true);
  expect(parts.flatMap(commands)).toEqual(commands(complete));
  const browser = parts[2]?.calls.filter(({ args }) => args[0] === "tools/package/e2e.mjs") ?? [];
  expect(browser.map(({ suite }) => suite)).toEqual([
    "production-startup",
    "workbench",
    "nats-workspace",
    "structured-records",
    "plugin-lifecycle",
  ]);
  expect(browser.flatMap(({ args }) => args)).toEqual(
    expect.arrayContaining([
      "test/e2e/web-production-startup.spec.ts",
      "test/e2e/web-stream-monitor.spec.ts",
      "test/e2e/web-observations-recovery.spec.ts",
      "test/e2e/web-responsive-workbench.spec.ts",
      "test/e2e/web-nats-workspace.spec.ts",
      "test/e2e/web-structured-events.spec.ts",
      "test/e2e/web-plugin-installation.spec.ts",
    ]),
  );
});
