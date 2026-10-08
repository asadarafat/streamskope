import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import { afterEach, expect, it } from "vitest";

import {
  parseOperationalDiagnostic,
  type OperationalDiagnostic,
} from "../../src/platform/diagnostics";

const execute = promisify(execFile);
const directories: string[] = [];
afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});
async function directory(): Promise<string> {
  const path = await mkdtemp(join(tmpdir(), "streamskope-entry-diagnostic-"));
  directories.push(path);
  return path;
}
async function failed(env: NodeJS.ProcessEnv): Promise<OperationalDiagnostic> {
  try {
    await execute(
      process.execPath,
      ["--import", "tsx", resolve("src/platform/node/browser-entry.ts")],
      { env: { ...process.env, ...env }, timeout: 10000, maxBuffer: 16384 },
    );
    throw new Error("Expected the browser startup to fail.");
  } catch (error) {
    expect(error).toMatchObject({ code: 1, stdout: "" });
    const stderr = (error as { stderr: string }).stderr;
    expect(stderr.trim().split("\n")).toHaveLength(1);
    const diagnostic = parseOperationalDiagnostic(JSON.parse(stderr));
    expect(diagnostic).not.toBeNull();
    expect(stderr).not.toMatch(
      /private-password|private-host.example|credential-path|Error:| at /u,
    );
    return diagnostic!;
  }
}
it("reports invalid environment configuration without echoing the supplied value", async () => {
  const diagnostic = await failed({
    STREAMSKOPE_DATA_DIR: await directory(),
    STREAMSKOPE_PORT: "private-password",
  });
  expect(diagnostic).toMatchObject({
    code: "BROWSER_CONFIGURATION_INVALID",
    stage: "configuration",
  });
});
it("reports an occupied real TCP port while preserving its unrelated listener", async () => {
  const owner = createServer();
  await new Promise<void>((resolve) => owner.listen(0, "127.0.0.1", resolve));
  const port = (owner.address() as { port: number }).port;
  try {
    const diagnostic = await failed({
      STREAMSKOPE_DATA_DIR: await directory(),
      STREAMSKOPE_PORT: String(port),
      STREAMSKOPE_LISTEN_HOST: "127.0.0.1",
      STREAMSKOPE_PUBLIC_ORIGIN: `http://127.0.0.1:${port}`,
    });
    expect(diagnostic).toMatchObject({
      code: "BROWSER_PORT_IN_USE",
      owner: "browser-host",
      stage: "listen",
    });
    expect(owner.listening).toBe(true);
  } finally {
    await new Promise<void>((resolve, reject) =>
      owner.close((error) => (error ? reject(error) : resolve())),
    );
  }
});
it("reports unavailable data storage without exposing the rejected path", async () => {
  const root = await directory();
  const invalid = join(root, "credential-path");
  await writeFile(invalid, "private-password");
  const diagnostic = await failed({ STREAMSKOPE_DATA_DIR: invalid, STREAMSKOPE_PORT: "8080" });
  expect(diagnostic.code).toBe("BROWSER_DATA_UNAVAILABLE");
  expect(diagnostic.stage).toBe("storage");
});
