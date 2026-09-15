import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

const read = (path: string): string =>
  readFileSync(new URL(`../../${path}`, import.meta.url), "utf8");
const { scripts } = JSON.parse(read("package.json")) as {
  scripts: Record<string, string>;
};
const ci = read(".github/workflows/ci.yml");
const release = read(".github/workflows/release.yml");

describe("shared CI qualification", () => {
  it("keeps distribution launch names aligned with the inspected prepackaged executable", () => {
    for (const file of [
      "tools/package-windows-installer.mjs",
      "tools/package-linux-appimage.mjs",
    ]) {
      const script = read(file);
      expect(script).toContain('productName: "StreamSkope"');
      expect(script).toContain('publish: "never"');
    }
    expect(read("tools/package-linux-appimage.mjs")).toContain('executableName: "StreamSkope"');
  });
  it("composes the same complete gates locally and on GitHub", () => {
    expect(scripts.verify).toBe(
      "npm run verify:source && npm run verify:system && npm run verify:native",
    );
    for (const gate of ["source", "system", "native"]) {
      expect(ci.match(new RegExp(`run: npm run verify:${gate}\\b`, "gu"))).toHaveLength(1);
    }
    expect(scripts["verify:system"]).toBe(
      "npm run test:kafka && npm run test:e2e:web && npm run test:e2e:electron",
    );
    expect(ci).not.toContain("npm run verify:ui:review");
    expect(ci).not.toContain("--grep");
    expect(ci.match(/run: npm ci\b/gu)).toHaveLength(3);
  });

  it("makes every release channel depend on complete secret-free qualification", () => {
    expect(ci).toContain("workflow_call:");
    expect(release).toContain("uses: ./.github/workflows/ci.yml");
    const qualification = release.split("  qualification:")[1]?.split(/^ {2}[\w-]+:/mu)[0];
    expect(qualification).toContain("needs: validate");
    expect(qualification).not.toContain("secrets:");
    for (const job of ["macos-preview", "macos", "windows", "linux"]) {
      const block = release.split(`  ${job}:`)[1]?.split(/^ {2}[\w-]+:/mu)[0];
      expect(block).toContain("needs: [validate, qualification]");
    }
    expect(ci).toContain("cancel-in-progress: ${{ github.event_name != 'workflow_dispatch' }}");
  });

  it("keeps action references immutable in every workflow", () => {
    for (const workflow of [ci, release]) {
      for (const [, reference] of workflow.matchAll(/uses:\s*[\w./-]+@([^\s#]+)/gu)) {
        expect(reference).toMatch(/^[a-f0-9]{40}$/u);
      }
    }
  });

  it("never derives final installer acceptance from unpacked package success", () => {
    expect(release).not.toMatch(/--installation passed|--startup passed|--sensitive-scan passed/u);
    for (const target of ["MACOS", "WINDOWS", "LINUX"]) {
      expect(release).toContain(`STREAMSKOPE_${target}_ARTIFACT_EVIDENCE_B64`);
    }
    expect(release.match(/--external-evidence/gu)).toHaveLength(3);
  });

  it("retains separate evidence when complete Playwright projects run sequentially", () => {
    expect(read("tools/run-playwright-e2e.mjs")).toContain("STREAMSKOPE_TEST_PROJECT: project");
    expect(read("config/playwright.config.ts")).toContain('resolve("test-results", project)');
    expect(read("config/playwright.config.ts")).toContain(
      "${evidenceDirectory}/playwright-results.json",
    );
  });
});
