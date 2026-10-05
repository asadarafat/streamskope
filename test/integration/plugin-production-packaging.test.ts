import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import { afterEach, expect, it } from "vitest";

import { pluginPublisherFixture } from "../support/plugin-publisher-fixture";

const execute = promisify(execFile);
const roots: string[] = [];

afterEach(async () => {
  for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true });
});

it.each(["missing", "mismatched"] as const)(
  "fails closed before building production plugins with a %s signing key",
  async (scenario) => {
    const root = await mkdtemp(join(tmpdir(), "streamskope-production-plugin-"));
    roots.push(root);
    await mkdir(join(root, "plugins", "eda"), { recursive: true });
    const source = JSON.parse(await readFile("plugins/eda/manifest.json", "utf8")) as Record<
      string,
      unknown
    >;
    await writeFile(
      join(root, "plugins", "eda", "manifest.json"),
      JSON.stringify({ ...source, version: "0.2.0" }),
    );
    await expect(
      execute(
        process.execPath,
        ["--import", import.meta.resolve("tsx"), resolve("tools/package/plugin.ts"), "eda"],
        {
          cwd: root,
          env: {
            ...process.env,
            STREAMSKOPE_PLUGIN_SIGNING_KEY_B64:
              scenario === "missing" ? "" : pluginPublisherFixture().encodedKey,
          },
          timeout: 15_000,
          maxBuffer: 128 * 1024,
        },
      ),
    ).rejects.toThrow(
      scenario === "missing"
        ? "requires STREAMSKOPE_PLUGIN_SIGNING_KEY_B64"
        : "does not match a trusted publisher",
    );
    await expect(
      readFile(join(root, "dist", "plugin-package", "streamskope-eda-v0.2.0.skope-plugin")),
    ).rejects.toMatchObject({ code: "ENOENT" });
  },
);
