import { readFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

const workflowUrl = new URL("../../.github/workflows/ci.yml", import.meta.url);
const packageUrl = new URL("../../package.json", import.meta.url);

async function workflowSource(): Promise<string> {
  try {
    return await readFile(workflowUrl, "utf8");
  } catch {
    return "";
  }
}

describe("GitHub Actions pull-request policy", () => {
  it("provides local native verification without a GitHub launch", async () => {
    const { scripts } = JSON.parse(await readFile(packageUrl, "utf8")) as {
      scripts: Record<string, string>;
    };
    expect(scripts["package:verify:macos"]).toBe(
      "node tools/verify-native-package-target.mjs darwin arm64 && npm run package:verify",
    );
    expect(scripts["package:verify:windows"]).toBe(
      "node tools/verify-native-package-target.mjs win32 x64 && npm run package:verify",
    );
    const script = fileURLToPath(
      new URL("../../tools/verify-native-package-target.mjs", import.meta.url),
    );
    const run = (args: string[]): ReturnType<typeof spawnSync> =>
      spawnSync(process.execPath, [script, ...args], {
        env: { ...process.env, EXPECTED_PLATFORM: "wrong", EXPECTED_ARCH: "wrong" },
        encoding: "utf8",
      });
    expect(run([process.platform, process.arch]).status).toBe(0);
    expect(run(["wrong", process.arch]).status).toBe(1);
    expect(run([process.platform, "wrong"]).status).toBe(1);
    expect(run([process.platform]).status).toBe(1);
    expect(run([process.platform, process.arch, "extra"]).status).toBe(1);
  });
  it("uses the active npm CLI through Node instead of Windows command shims", async () => {
    const source = await readFile(
      new URL("../../tools/verify-package.ts", import.meta.url),
      "utf8",
    );
    expect(source).not.toContain('"npm.cmd"');
    expect(source).not.toContain('"npx.cmd"');
    expect(source).toContain("process.env.npm_execpath");
    expect(source).toContain("process.execPath,");
  });
  it("rejects mismatched native runners before packaging", () => {
    const script = fileURLToPath(
      new URL("../../tools/verify-native-package-target.mjs", import.meta.url),
    );
    const run = (platform: string, arch: string): ReturnType<typeof spawnSync> =>
      spawnSync(process.execPath, [script], {
        env: { ...process.env, EXPECTED_PLATFORM: platform, EXPECTED_ARCH: arch },
        encoding: "utf8",
      });
    expect(run(process.platform, process.arch).status).toBe(0);
    expect(run("wrong-platform", process.arch).status).toBe(1);
    expect(run(process.platform, "wrong-architecture").status).toBe(1);
  });
  it("keeps unsigned native targets and verification in sync with release", async () => {
    const ci = await workflowSource();
    const release = await readFile(
      new URL("../../.github/workflows/release.yml", import.meta.url),
      "utf8",
    );
    const packaging =
      ci.split("  package-verification:")[1]?.split("  browser-accessibility:")[0] ?? "";
    const targets = [...packaging.matchAll(/- runner: (\S+)\s+platform: (\S+)\s+arch: (\S+)/gu)];
    expect(targets.map(([, runner]) => runner).sort()).toEqual(
      [
        ...release.matchAll(
          /runs-on: (\S+)\s+timeout-minutes: \d+\s+environment: production-release/gu,
        ),
      ]
        .map(([, runner]) => runner)
        .sort(),
    );
    expect(targets).toHaveLength(3);
    expect(targets.map(([, , platform, arch]) => `${platform}-${arch}`).sort()).toEqual([
      "darwin-arm64",
      "linux-x64",
      "win32-x64",
    ]);
    for (const [, , platform, arch] of targets) {
      expect(release).toContain(`EXPECTED_PLATFORM: ${platform}`);
      expect(release).toContain(`EXPECTED_ARCH: ${arch}`);
    }
    expect(packaging).toContain("fail-fast: false");
    expect(packaging).toContain("install-deps chromium");
    expect(packaging).toContain("${{ matrix.platform }}-${{ matrix.arch }}");
    expect(packaging).not.toContain("secrets.");
    expect(packaging).toContain("npm run verify:native");
    expect(release.match(/node tools\/verify-native-package-target.mjs/gu)).toHaveLength(3);
    const { scripts } = JSON.parse(await readFile(packageUrl, "utf8")) as {
      scripts: Record<string, string>;
    };
    expect(scripts["package:release:verify"]?.replace(" --release", "")).toBe(
      scripts["package:verify"],
    );
    expect(scripts["package:verify"]).toContain("npm run test:e2e:package");
  });

  it("runs every bounded evidence job for draft and ready pull requests", async () => {
    const workflow = await workflowSource();

    expect(workflow).toContain("pull_request:");
    expect(workflow).toContain("converted_to_draft");
    expect(workflow).toContain("ready_for_review");
    expect(workflow).toContain("contents: read");
    expect(workflow).toContain(
      "cancel-in-progress: ${{ github.event_name != 'workflow_dispatch' }}",
    );
    expect(workflow).toMatch(/static-and-unit:\s[\s\S]*?timeout-minutes:\s*20/u);
    expect(workflow).toMatch(/package-verification:\s[\s\S]*?timeout-minutes:\s*20/u);
    expect(workflow).not.toMatch(/wails|setup-go|go-version-file|go test/iu);
    expect(workflow).toMatch(/kafka-real-system:\s[\s\S]*?timeout-minutes:\s*30/u);
    expect(workflow).not.toMatch(/^ {4}if:/mu);
    expect(workflow).not.toContain("continue-on-error");
  });

  it("owns one focused UI command without representing it as a release gate", async () => {
    const packageSource = await readFile(packageUrl, "utf8");
    const packageJson = JSON.parse(packageSource) as {
      scripts?: Record<string, string>;
    };
    const scripts = packageJson.scripts ?? {};

    expect(scripts["test:ui:unit"]).toContain("streamskope-workbench-layout.test.tsx");
    expect(scripts["test:ui:unit"]).toContain("streamskope-activity-workbench.test.tsx");
    expect(scripts["test:ui:unit"]).toContain("message-presentation.test.tsx");
    expect(scripts["test:ui:unit"]).toContain("kafka-message-operations-workflow.test.tsx");
    expect(scripts["test:ui:unit"]).toContain("metric-plot.test.tsx");
    expect(scripts["test:ui:playwright"]).toContain("web-responsive-workbench.spec.ts");
    expect(scripts["test:ui:playwright"]).toContain("web-stream-monitor.spec.ts");
    expect(scripts["verify:ui:review"]).toBe(
      "npm run format:check && npm run lint && npm run typecheck && npm run test:architecture && npm run test:ui:unit && npm run test:ui:playwright",
    );
    expect(scripts.verify).not.toContain("verify:ui:review");
  });

  it("pins actions immutably and restores npm downloads from the authoritative lockfile", async () => {
    const workflow = await workflowSource();
    const actionReferences = [...workflow.matchAll(/uses:\s*[\w./-]+@([^\s#]+)/gu)].map(
      (match) => match[1],
    );

    expect(actionReferences.length).toBeGreaterThanOrEqual(9);
    for (const reference of actionReferences) {
      expect(reference).toMatch(/^[a-f0-9]{40}$/u);
    }
    expect(workflow).toContain("node-version: 24.12.0");
    expect(workflow).toContain("cache: npm");
    expect(workflow).toContain("cache-dependency-path: package-lock.json");
    expect(workflow.match(/run: npm ci/gu)?.length).toBe(3);
  });

  it("runs required gates and protects real fixture ownership and failure artifacts", async () => {
    const workflow = await workflowSource();

    for (const command of [
      "npm run verify:source",
      "npm run verify:system",
      "npm run verify:native",
    ]) {
      expect(workflow).toContain(command);
    }
    expect(workflow).toContain("containerlab_0.77.0_linux_amd64.deb");
    expect(workflow).toContain("675eea8bd4d05ea3abc4a98cfa859975c9886705d6a510fead4dfd8dbed8b793");
    expect(workflow).toContain(
      "FIXTURE_NAME: streamskope-ci-${{ github.run_id }}-${{ github.run_attempt }}",
    );
    expect(workflow).toContain("npm run fixture:start");
    expect(workflow).toMatch(
      /name:\s*Stop owned Kafka fixture[\s\S]*?if:\s*always\(\)[\s\S]*?npm run fixture:stop/u,
    );
    expect(workflow).toMatch(
      /id:\s*artifact-safety[\s\S]*?if:\s*always\(\)[\s\S]*?scan-sensitive-artifacts/u,
    );
    expect(workflow).toMatch(/if:\s*failure\(\) && steps\.artifact-safety\.outcome == 'success'/u);
    expect(workflow).toContain("retention-days: 7");
  });
});
