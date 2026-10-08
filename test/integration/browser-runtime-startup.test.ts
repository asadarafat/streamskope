import { execFile } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import { expect, it } from "vitest";

const execute = promisify(execFile);

it.skipIf(process.platform !== "linux")(
  "releases the real vault lease and preserves its data when a production worker is missing",
  async () => {
    const dataRoot = await mkdtemp(join(tmpdir(), "streamskope-startup-recovery-"));
    try {
      const result = await execute(
        process.execPath,
        ["--import", "tsx", resolve("test/support/browser-runtime-startup-process.ts"), dataRoot],
        { cwd: process.cwd(), timeout: 10000, maxBuffer: 4096 },
      );
      expect(result.stderr).toBe("");
      expect(JSON.parse(result.stdout) as unknown).toMatchObject({
        startupRejected: true,
        leaseReleased: true,
        vaultPreserved: true,
        formatPreserved: true,
        diagnostic: {
          code: "KAFKA_RUNTIME_START_FAILED",
          owner: "kafka",
          stage: "startup",
          correlationId: expect.any(String) as unknown,
        },
      });
      expect(result.stdout).not.toContain(dataRoot);
      expect(result.stdout).not.toContain("independent startup fixture passphrase");
    } finally {
      await rm(dataRoot, { recursive: true, force: true });
    }
  },
);
