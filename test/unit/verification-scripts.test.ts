import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

const manifest = JSON.parse(
  readFileSync(new URL("../../package.json", import.meta.url), "utf8"),
) as { scripts: Record<string, string> };
const check = readFileSync(new URL("../../tools/check.sh", import.meta.url), "utf8");

const docs = readFileSync(new URL("../../tools/docs.py", import.meta.url), "utf8");

describe("development commands", () => {
  it("exposes exactly five supported npm commands", () => {
    expect(Object.keys(manifest.scripts).sort()).toEqual([
      "build",
      "check",
      "dev",
      "docs",
      "package",
    ]);
    expect(manifest.scripts.dev).toBe("node tools/dev.mjs");
    expect(manifest.scripts.build).toBe("node --import tsx tools/build.mjs");
    expect(manifest.scripts.package).toBe("node --import tsx tools/package.ts");
    expect(manifest.scripts.check).toBe("bash tools/check.sh");
    expect(manifest.scripts.docs).toBe("python3 tools/docs.py");
  });

  it("keeps all local qualification gates and fails on the first error", () => {
    expect(check).toContain("set -euo pipefail");
    for (const required of [
      "tools/check/workflows.sh",
      "prettier --check",
      "eslint . --max-warnings=0",
      "tsc -b",
      "test/architecture test/unit test/integration",
      "tools/check/eda-source.mjs",
      "cd vendors/streamskope/apps/capture/agent && go test -race ./...",
      "tools/check/dependencies.ts",
      "tools/check/forge-patch.ts --apply",
      "tools/check/audit.ts",
      "stream-pipeline-replay.ts --seconds=60",
      "--mixed --clone",
      "npm run docs -- qualify",
      "tools/check/eda-live.ts",
    ])
      expect(check, `missing local gate: ${required}`).toContain(required);
    expect(check).not.toContain("passWithNoTests");
    expect(docs).toMatch(
      /"unittest",\s*"discover",\s*"-s",\s*"test\/docs",\s*"-p",\s*"test_\*\.py"/,
    );
    for (const required of ["tools/docs.py", "tools/docs/smoke.mjs"])
      expect(docs).toContain(required);
    const ci = readFileSync(new URL("../../.github/workflows/ci.yml", import.meta.url), "utf8");
    expect(ci).toContain("npm run check -- --ci");
  });

  it("qualifies PRs and release calls without repeating CI on main pushes", () => {
    const ci = readFileSync(new URL("../../.github/workflows/ci.yml", import.meta.url), "utf8");
    const release = readFileSync(
      new URL("../../.github/workflows/release.yml", import.meta.url),
      "utf8",
    );
    expect(ci).toContain("pull_request:");
    expect(ci).not.toMatch(/^ {2}(?:push|workflow_dispatch|schedule):/mu);
    expect(ci).toContain("workflow_call:");
    expect(ci).not.toMatch(/tags:|release-version|npm run package|contents: write/);
    expect(release).toContain("workflow_dispatch:");
    expect(release).not.toMatch(/^ {2}(?:push|pull_request):/mu);
    expect(release).toContain("uses: ./.github/workflows/ci.yml");
    expect(release).toContain('"$GITHUB_REF" != refs/heads/main');
    expect(release).toContain('release-version.ts "$RELEASE_COMPONENT" "$RELEASE_VERSION" --stamp');
    expect(release).toContain('--target "$GITHUB_SHA" --draft');
    expect(release).not.toContain("git push");
  });
});
