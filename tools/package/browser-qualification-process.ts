import assert from "node:assert/strict";
import { spawn } from "node:child_process";

export interface CommandResult {
  readonly code: number;
  readonly stdout: string;
  readonly stderr: string;
}

/** Keep private browser secrets out of command arguments and qualification output. */
export function run(
  command: string,
  args: readonly string[],
  environment = process.env,
  allowFailure = false,
  timeout = 12 * 60_000,
): Promise<CommandResult> {
  return new Promise((accept, reject) => {
    // The installer must not inherit a controlling terminal that could disclose
    // its fresh setup code into a recorded qualification session.
    const child = spawn(command, [...args], {
      detached: true,
      env: environment,
      stdio: ["ignore", "pipe", "pipe"],
    });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    let bytes = 0;
    let failed = false;
    const fail = (message: string): void => {
      if (failed) return;
      failed = true;
      clearTimeout(timer);
      if (child.pid !== undefined) {
        try {
          process.kill(-child.pid, "SIGKILL");
        } catch {
          // The command may have ended just before its execution bound expired.
        }
      }
      reject(new Error(message));
    };
    const capture = (parts: Buffer[], chunk: Buffer): void => {
      bytes += chunk.length;
      if (bytes > 2 * 1024 * 1024)
        fail("Installer qualification command exceeded its output bound.");
      else parts.push(chunk);
    };
    child.stdout.on("data", (chunk: Buffer) => capture(stdout, chunk));
    child.stderr.on("data", (chunk: Buffer) => capture(stderr, chunk));
    const timer = setTimeout(
      () => fail("Installer qualification command exceeded its execution bound."),
      timeout,
    );
    child.once("error", () => fail("Installer qualification command could not start."));
    child.once("close", (code) => {
      clearTimeout(timer);
      if (failed) return;
      const result = {
        code: code ?? 1,
        stdout: Buffer.concat(stdout).toString("utf8"),
        stderr: Buffer.concat(stderr).toString("utf8"),
      };
      if (result.code !== 0 && !allowFailure)
        reject(
          new Error("Installer qualification command failed; private process output was withheld."),
        );
      else accept(result);
    });
  });
}

export function replaceConstant(source: string, name: string, value: string): string {
  assert.ok(!value.includes("'") && !value.includes("\n"));
  const expression = new RegExp(`^${name}='[^']*'$`, "mu");
  assert.ok(expression.test(source), `Installer qualification requires one ${name} constant.`);
  return source.replace(expression, `${name}='${value}'`);
}
