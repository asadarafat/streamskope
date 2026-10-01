import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import { sourceFromResource } from "../../plugins/eda/backend/eda-capture-source";
import { selectLiveEdaTopic } from "../../tools/check/eda-live";

const script = fileURLToPath(new URL("../../tools/check/eda-live.ts", import.meta.url));
const loader = fileURLToPath(new URL("../../node_modules/tsx/dist/loader.mjs", import.meta.url));

function run(settings: Record<string, string>): {
  status: number | null;
  output: string;
  outcome: string;
} {
  const cwd = mkdtempSync(join(tmpdir(), "streamskope-eda-local-test-"));
  const env = Object.fromEntries(
    Object.entries(process.env).filter(
      ([name]) => !name.startsWith("STREAMSKOPE_EDA_") && name !== "GITHUB_ACTIONS",
    ),
  );
  const result = spawnSync(process.execPath, ["--import", loader, script], {
    cwd,
    env: { ...env, ...settings },
    encoding: "utf8",
    timeout: 10_000,
  });
  const evidence = JSON.parse(readFileSync(join(cwd, "dist/ci/eda-live.json"), "utf8")) as {
    outcome: string;
  };
  return {
    status: result.status,
    output: result.stdout + result.stderr,
    outcome: evidence.outcome,
  };
}

describe("local EDA qualification availability", () => {
  it("records a skip when no local cluster connection is configured", () => {
    const result = run({});
    expect(result.status).toBe(0);
    expect(result.outcome).toBe("skipped");
    expect(result.output).toContain("Live EDA: skipped");
  });

  it("fails partial configuration and keeps credentials out of output", () => {
    const credential = "private-test-credential-do-not-log";
    const result = run({ STREAMSKOPE_EDA_API_PASSWORD: credential });
    expect(result.status).toBe(1);
    expect(result.outcome).toBe("failed");
    expect(result.output).not.toContain(credential);
  });

  it("refuses to run live cluster tests on GitHub Actions", () => {
    const result = run({ GITHUB_ACTIONS: "true" });
    expect(result.status).toBe(1);
    expect(result.outcome).toBe("failed");
  });
});

describe("local EDA qualification topic", () => {
  it("qualifies periodic node records while preserving the displayed alphabetical topic order", () => {
    const spec = {
      exports: [
        { topic: "eda-nodes", mode: "periodic", period: "10s" },
        { topic: "eda-current-alarms", mode: "onChange" },
      ],
    };
    const source = sourceFromResource(
      { metadata: { name: "kafka-export", namespace: "eda" }, spec },
      "kafka.eda.nokia.com/v1",
      "Producer",
      "eda",
    );
    expect(source?.topics).toEqual(["eda-current-alarms", "eda-nodes"]);
    expect(selectLiveEdaTopic(spec, source?.topics ?? [])).toBe("eda-nodes");
    expect(source?.topics).toEqual(["eda-current-alarms", "eda-nodes"]);
  });

  it("keeps the first-topic fallback when no discovered topic exports every period", () => {
    const topics = ["eda-current-alarms", "eda-nodes"];
    expect(
      selectLiveEdaTopic(
        {
          exports: [
            { topic: "eda-nodes", mode: "periodicOnChange" },
            { topic: "unadvertised-topic", mode: "periodic" },
          ],
        },
        topics,
      ),
    ).toBe("eda-current-alarms");
    expect(selectLiveEdaTopic(undefined, topics)).toBe("eda-current-alarms");
    expect(selectLiveEdaTopic({ exports: [] }, [])).toBeUndefined();
  });
});
