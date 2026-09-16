import { readFile } from "node:fs/promises";

import { expect, it } from "vitest";

it("publishes unsigned native downloads only after complete qualification and checksum verification", async () => {
  const workflow = await readFile(
    new URL("../../.github/workflows/unsigned-release.yml", import.meta.url),
    "utf8",
  ).catch(() => "");
  expect(workflow).toContain("workflow_dispatch:");
  expect(workflow).not.toMatch(/push:|pull_request:|secrets\.|continue-on-error/u);
  expect(workflow).toContain("uses: ./.github/workflows/ci.yml");
  expect(workflow).toContain("needs: [validate, qualification]");
  expect(workflow).toContain("needs: [validate, package]");
  expect(workflow).toContain('test "$GITHUB_REF_TYPE" = "tag"');
  expect(workflow).toContain("npm run release:validate-tag");
  expect(workflow).toContain('git merge-base --is-ancestor "$GITHUB_SHA" origin/main');
  for (const target of ["darwin", "win32", "linux"]) {
    expect(workflow).toContain(`platform: ${target}`);
  }
  const ci = await readFile(new URL("../../.github/workflows/ci.yml", import.meta.url), "utf8");
  const targets = (source: string): string[] =>
    [...source.matchAll(/- runner: (\S+)\s+platform: (\S+)\s+arch: (\S+)/gu)]
      .map(([, runner, platform, arch]) => `${runner}/${platform}/${arch}`)
      .sort();
  expect(targets(workflow)).toEqual(targets(ci));
  expect(targets(workflow)).toHaveLength(3);
  for (const script of [
    "package:verify",
    "package:installer:windows:from-package",
    "package:appimage:linux:from-package",
  ]) {
    expect(workflow).toContain(`npm run ${script}`);
  }
  expect(workflow).toContain("tools/package-macos-dmg.ts");
  expect(workflow).toContain("tools/prepare-unsigned-release.ts");
  expect(workflow).toContain("tools/scan-sensitive-artifacts.ts");
  expect(workflow).toContain("--draft");
  expect(workflow).toContain("--verify-tag");
  expect(workflow).not.toContain("--clobber");
  expect(workflow.indexOf("sha256sum -c SHA256SUMS")).toBeLessThan(
    workflow.indexOf("--draft=false"),
  );
  const packageJob = workflow.split("  package:")[1]?.split("  publish:")[0] ?? "";
  expect(packageJob).not.toContain("contents: write");
  for (const [, reference] of workflow.matchAll(/uses:\s*[\w./-]+@([^\s#]+)/gu)) {
    expect(reference).toMatch(/^[a-f0-9]{40}$/u);
  }
});
