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
const qualificationId = "a342bc68-187d-4a50-b3b8-2ac631e16660";
const edaSettings = {
  STREAMSKOPE_EDA_API_URL: "https://eda.example.test",
  STREAMSKOPE_EDA_API_USERNAME: "fixture-user",
  STREAMSKOPE_EDA_API_PASSWORD: "fixture-private-password",
};
const nspSettings = { STREAMSKOPE_NSP_CONFIG: "nsp.json" };

afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

async function run(
  args: string[] = [],
  failure?: string,
  settings: NodeJS.ProcessEnv = {},
): Promise<{
  status: number;
  calls: Array<{ command: string; args: string[]; suite?: string }>;
  output: string;
}> {
  const cwd = await mkdtemp(join(tmpdir(), "streamskope-check-command-"));
  directories.push(cwd);
  const bin = join(cwd, "bin");
  await mkdir(bin);
  await mkdir(join(cwd, "tools/check"), { recursive: true });
  await mkdir(join(cwd, "vendors/streamskope/apps/capture/agent"), { recursive: true });
  await writeFile(join(cwd, "nsp.json"), JSON.stringify({ fixture: true }), { mode: 0o600 });
  await writeFile(join(cwd, "empty.json"), "");
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
if (args.includes("tools/check/qualification.ts") && args.includes("begin")) process.stdout.write("${qualificationId}\\n");
`,
      { mode: 0o755 },
    );
  }
  let status = 0;
  let output: string;
  const inherited = Object.fromEntries(
    Object.entries(process.env).filter(
      ([name]) =>
        !name.startsWith("STREAMSKOPE_EDA_") &&
        !name.startsWith("STREAMSKOPE_NSP_") &&
        !name.startsWith("CHECK_COMMAND_") &&
        name !== "GITHUB_ACTIONS",
    ),
  );
  try {
    const result = await execute("bash", [script, ...args], {
      cwd,
      env: {
        ...inherited,
        ...settings,
        PATH: `${bin}:${process.env.PATH ?? ""}`,
        CHECK_COMMAND_LOG: log,
        ...(failure ? { CHECK_COMMAND_FAILURE: failure } : {}),
      },
      timeout: 10_000,
    });
    output = result.stdout + result.stderr;
  } catch (error) {
    const failure = error as { code: number; stdout: string; stderr: string };
    status = failure.code;
    output = failure.stdout + failure.stderr;
  }
  const lines = await readFile(log, "utf8").catch(() => "");
  return {
    status,
    output,
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

function receiptCalls(result: Awaited<ReturnType<typeof run>>): string[][] {
  return result.calls
    .filter(({ args }) => args.includes("tools/check/qualification.ts"))
    .map(({ args }) => args.slice(3));
}

function expectReceipt(
  result: Awaited<ReturnType<typeof run>>,
  scope: string,
  stages: string[],
  status = 0,
): void {
  expect(receiptCalls(result)).toEqual([
    ["begin", scope],
    ...stages.flatMap((stage) => [
      ["stage", qualificationId, stage],
      ["complete", qualificationId, stage],
    ]),
    ["finish", qualificationId, String(status)],
  ]);
}

it("the default local command qualifies shared checks, the 60-second soak and docs without live targets", async () => {
  const result = await run([], undefined, { ...edaSettings, ...nspSettings });
  expect(result.status).toBe(0);
  const soak = result.calls.find(({ args }) =>
    args.some((arg) => arg.endsWith("stream-pipeline-replay.ts")),
  );
  expect(soak?.args).toContain("--seconds=60");
  expectReceipt(result, "core", ["shared", "soak", "docs"]);
  expect(
    result.calls.some(
      ({ args }) =>
        args.includes("tools/check/eda-live.ts") || args.includes("tools/check/nsp-live.ts"),
    ),
  ).toBe(false);
});

it("full local qualification preserves configured-or-skipped vendor checks after the core stages", async () => {
  const result = await run(["--full"]);
  expect(result.status).toBe(0);
  expectReceipt(result, "full", ["shared", "soak", "docs", "eda-live", "nsp-live"]);
  expect(result.calls).toContainEqual({
    command: "node",
    args: ["--import", "tsx", "tools/check/eda-live.ts"],
  });
  expect(result.calls).toContainEqual({
    command: "node",
    args: ["--import", "tsx", "tools/check/nsp-live.ts"],
  });
});

it.each([
  { target: "eda", scope: "eda", stages: ["eda-live"], settings: edaSettings },
  { target: "nsp", scope: "nsp", stages: ["nsp-live"], settings: nspSettings },
  {
    target: "all",
    scope: "live",
    stages: ["eda-live", "nsp-live"],
    settings: { ...edaSettings, ...nspSettings },
  },
])(
  "explicit live $target runs only its selected stages once",
  async ({ target, scope, stages, settings }) => {
    const result = await run(["--live", target], undefined, settings);
    expect(result.status).toBe(0);
    expectReceipt(result, scope, stages);
    expect(
      result.calls.filter(({ args }) => !args.includes("tools/check/qualification.ts")),
    ).toEqual([
      { command: "node", args: ["tools/check/forge-patch.ts", "--apply"] },
      {
        command: "node",
        args: ["--import", "tsx", "tools/check/build-dependency-patches.ts", "--apply"],
      },
      ...stages.map((stage) => ({
        command: "node",
        args: ["--import", "tsx", `tools/check/${stage}.ts`],
      })),
    ]);
  },
);

it.each([
  { target: "eda", scope: "eda", settings: {}, missing: "STREAMSKOPE_EDA_API_URL" },
  {
    target: "eda",
    scope: "eda",
    settings: { STREAMSKOPE_EDA_API_PASSWORD: edaSettings.STREAMSKOPE_EDA_API_PASSWORD },
    missing: "STREAMSKOPE_EDA_API_URL",
  },
  {
    target: "eda",
    scope: "eda",
    settings: { ...edaSettings, STREAMSKOPE_EDA_API_CA: "missing.pem" },
    missing: "STREAMSKOPE_EDA_API_CA",
  },
  { target: "nsp", scope: "nsp", settings: {}, missing: "STREAMSKOPE_NSP_CONFIG" },
  {
    target: "nsp",
    scope: "nsp",
    settings: { STREAMSKOPE_NSP_CONFIG: "missing.json" },
    missing: "STREAMSKOPE_NSP_CONFIG",
  },
  {
    target: "nsp",
    scope: "nsp",
    settings: { STREAMSKOPE_NSP_CONFIG: "empty.json" },
    missing: "STREAMSKOPE_NSP_CONFIG",
  },
  {
    target: "nsp",
    scope: "nsp",
    settings: { STREAMSKOPE_NSP_CONFIG: "tools" },
    missing: "STREAMSKOPE_NSP_CONFIG",
  },
  { target: "all", scope: "live", settings: edaSettings, missing: "STREAMSKOPE_NSP_CONFIG" },
  { target: "all", scope: "live", settings: nspSettings, missing: "STREAMSKOPE_EDA_API_URL" },
])(
  "records failed preflight before any live mutation: $target $missing",
  async ({ target, scope, settings, missing }) => {
    const result = await run(["--live", target], undefined, settings);
    expect(result.status).toBe(1);
    expectReceipt(result, scope, [], 1);
    expect(result.calls).toHaveLength(2);
    expect(result.output).toContain(missing);
    expect(result.output).not.toContain(edaSettings.STREAMSKOPE_EDA_API_PASSWORD);
  },
);

it("retains local fail-fast stage ownership and finalizes its receipt", async () => {
  const result = await run([], "eslint");
  expect(result.status).toBe(7);
  expect(receiptCalls(result)).toEqual([
    ["begin", "core"],
    ["stage", qualificationId, "shared"],
    ["finish", qualificationId, "7"],
  ]);
  expect(
    result.calls.some(
      ({ args }) =>
        args.includes("vitest") ||
        args.includes("qualify") ||
        args.some((arg) => arg.endsWith("stream-pipeline-replay.ts")),
    ),
  ).toBe(false);
});

it("does not start NSP after a failing explicit EDA stage", async () => {
  const result = await run(["--live", "all"], "tools/check/eda-live.ts", {
    ...edaSettings,
    ...nspSettings,
  });
  expect(result.status).toBe(7);
  expect(receiptCalls(result)).toEqual([
    ["begin", "live"],
    ["stage", qualificationId, "eda-live"],
    ["finish", qualificationId, "7"],
  ]);
  expect(result.calls.some(({ args }) => args.includes("tools/check/nsp-live.ts"))).toBe(false);
});

it.each(["tools/check/forge-patch.ts", "tools/check/build-dependency-patches.ts"])(
  "refuses standalone live work when required dependency mitigation fails: %s",
  async (patcher) => {
    const result = await run(["--live", "all"], patcher, { ...edaSettings, ...nspSettings });
    expect(result.status).toBe(7);
    expectReceipt(result, "live", [], 7);
    expect(result.calls.at(-2)?.args).toContain(patcher);
    expect(
      result.calls.some(
        ({ args }) =>
          args.includes("tools/check/eda-live.ts") || args.includes("tools/check/nsp-live.ts"),
      ),
    ).toBe(false);
  },
);

it("does not report success when receipt finalization fails", async () => {
  const result = await run(["--live", "eda"], "finish", edaSettings);
  expect(result.status).toBe(7);
  expectReceipt(result, "eda", ["eda-live"]);
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
  ["--full", "--ci"],
  ["--full", "--live", "eda"],
  ["--live"],
  ["--live", "unknown"],
  ["--live", "../eda"],
  ["--live", "eda", "--full"],
  ["--live", "eda", "--live", "nsp"],
  ["--ci", "--live", "nsp"],
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
