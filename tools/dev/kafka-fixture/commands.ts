import { spawn } from "node:child_process";

const COMMAND_TIMEOUT_MS = 600_000;
export const DIAGNOSTIC_CHARACTER_LIMIT = 16_384;

interface CommandRequest {
  readonly args: readonly string[];
  readonly command: string;
  readonly cwd: string;
  readonly environment?: Readonly<Record<string, string>>;
}

interface CommandOutput {
  readonly standardError: string;
  readonly standardOutput: string;
}

class FixtureCommandError extends Error {
  constructor(message: string, options: { readonly cause?: unknown } = {}) {
    super(message, { cause: options.cause });
    this.name = "FixtureCommandError";
  }
}

function appendBounded(current: string, chunk: string): string {
  const combined = current + chunk;
  return combined.length <= DIAGNOSTIC_CHARACTER_LIMIT
    ? combined
    : combined.slice(-DIAGNOSTIC_CHARACTER_LIMIT);
}

export async function runCommand(request: CommandRequest): Promise<CommandOutput> {
  return new Promise<CommandOutput>((resolvePromise, rejectPromise) => {
    const child = spawn(request.command, [...request.args], {
      cwd: request.cwd,
      env: { ...process.env, ...request.environment },
      stdio: ["ignore", "pipe", "pipe"],
    });
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");

    let standardOutput = "";
    let standardError = "";
    let settled = false;
    let timedOut = false;

    child.stdout.on("data", (chunk: string) => {
      standardOutput = appendBounded(standardOutput, chunk);
    });
    child.stderr.on("data", (chunk: string) => {
      standardError = appendBounded(standardError, chunk);
    });

    const timeout = setTimeout(() => {
      timedOut = true;
      child.kill("SIGTERM");
    }, COMMAND_TIMEOUT_MS);

    child.once("error", (error: Error) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      rejectPromise(
        new FixtureCommandError(`Unable to execute ${request.command}.`, { cause: error }),
      );
    });

    child.once("close", (exitCode, signal) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);

      if (exitCode === 0 && !timedOut) {
        resolvePromise({ standardError, standardOutput });
        return;
      }

      const diagnostic = (standardError || standardOutput).trim();
      const outcome = timedOut
        ? `timed out after ${COMMAND_TIMEOUT_MS} ms`
        : `exited with code ${String(exitCode)}${signal === null ? "" : ` (${signal})`}`;
      rejectPromise(
        new FixtureCommandError(
          `${request.command} ${outcome}${diagnostic.length === 0 ? "" : `: ${diagnostic}`}`,
        ),
      );
    });
  });
}
