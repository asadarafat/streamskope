import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { expect, it } from "vitest";

it.each([false, true])(
  "records explicit NSP availability without logging private configuration (configured=%s)",
  async (configured) => {
    const cwd = await mkdtemp(join(tmpdir(), "streamskope-nsp-local-test-"));
    try {
      const secret = "private-test-credential-do-not-log";
      const path = join(cwd, "settings.json");
      if (configured) await writeFile(path, `{ "password": "${secret}", malformed`);
      const env = Object.fromEntries(
        Object.entries(process.env).filter(
          ([name]) => !name.startsWith("STREAMSKOPE_NSP_") && name !== "GITHUB_ACTIONS",
        ),
      );
      const result = spawnSync(
        process.execPath,
        [
          "--import",
          fileURLToPath(new URL("../../node_modules/tsx/dist/loader.mjs", import.meta.url)),
          fileURLToPath(new URL("../../tools/check/nsp-live.ts", import.meta.url)),
        ],
        {
          cwd,
          env: { ...env, ...(configured ? { STREAMSKOPE_NSP_CONFIG: path } : {}) },
          encoding: "utf8",
          timeout: 15_000,
        },
      );
      const report = JSON.parse(await readFile(join(cwd, "dist/ci/nsp-live.json"), "utf8")) as {
        outcome: string;
        reasonCode: string;
        checks: string[];
      };
      expect(result.status).toBe(configured ? 1 : 0);
      expect(report.outcome).toBe(configured ? "failed" : "skipped");
      expect(report.reasonCode).toBe(configured ? "configuration-invalid" : "not-configured");
      expect(report.checks).toEqual([]);
      expect(result.stdout + result.stderr + JSON.stringify(report)).not.toContain(secret);
    } finally {
      await rm(cwd, { recursive: true, force: true });
    }
  },
);
