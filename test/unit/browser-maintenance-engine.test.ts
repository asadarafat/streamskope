import { execFile } from "node:child_process";
import { promisify } from "node:util";

import { describe, expect, it } from "vitest";

const execute = promisify(execFile);

describe.skipIf(process.platform !== "linux")("browser maintenance transaction engine", () => {
  it("qualifies durable recovery, real filesystem/lease ownership and immutable data preservation", async () => {
    const result = await execute("python3", ["test/support/browser-maintenance-engine.py"], {
      timeout: 60_000,
      maxBuffer: 128 * 1024,
      env: { ...process.env, PYTHONDONTWRITEBYTECODE: "1" },
    });
    expect(result.stderr).toContain("OK");
    expect(result.stdout).toBe("");
  }, 65_000);
});
