import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

describe("production release workflow policy", () => {
  it("offers an isolated unsigned DMG preview without enabling production publication", async () => {
    const workflow = await readFile(
      new URL("../../.github/workflows/release.yml", import.meta.url),
      "utf8",
    );
    expect(workflow).toContain("default: unsigned-macos-preview");
    expect(workflow).toContain("if: inputs.channel == 'production'");
    const preview = workflow.split("  macos-preview:")[1]?.split("  macos:")[0] ?? "";
    expect(preview).toContain("if: inputs.channel == 'unsigned-macos-preview'");
    expect(preview).toContain("npm run package:dmg:macos");
    expect(preview).toContain("tools/scan-sensitive-artifacts.ts");
    expect(preview).toContain("*.dmg");
    expect(preview).toContain("*.sha256");
    const manifest = JSON.parse(
      await readFile(new URL("../../package.json", import.meta.url), "utf8"),
    ) as { scripts: Record<string, string> };
    expect(manifest.scripts["package:dmg:macos"]).toBe(
      "npm run package:verify:macos && tsx tools/package-macos-dmg.ts",
    );
    expect(preview).not.toMatch(
      /secrets\.|production-release|contents: write|gh release|release:evidence|release:manifest/u,
    );
    expect(workflow).toContain("needs: [validate, macos, windows, linux]");
  });
  it("is incubation-blocked, native, credential-isolated, and retains future evidence gates", async () => {
    const workflow = await readFile(
      new URL("../../.github/workflows/release.yml", import.meta.url),
      "utf8",
    );

    expect(workflow).toContain("workflow_dispatch:");
    expect(workflow).not.toContain("push:");
    expect(workflow).not.toMatch(/["']v\*\.\*\.\*["']/u);
    expect(workflow).not.toContain("pull_request:");
    expect(workflow).toContain("STREAMSKOPE_RELEASE_PHASE: incubation");
    expect(workflow).toContain('test "$STREAMSKOPE_RELEASE_PHASE" = "production"');
    expect(workflow.indexOf("Enforce incubation release boundary")).toBeLessThan(
      workflow.indexOf("Check out the protected tag"),
    );
    expect(workflow).toContain("cancel-in-progress: false");
    expect(workflow).toMatch(/permissions:\n {2}contents: read/u);
    expect(workflow).toContain("artifact-metadata: write");
    expect(workflow).toContain("runs-on: ubuntu-24.04");
    expect(workflow).toContain("runs-on: macos-15");
    expect(workflow).toContain("runs-on: windows-2025");
    expect(workflow.match(/environment: production-release/gu)).toHaveLength(3);
    expect(workflow).toContain("STREAMSKOPE_LINUX_GPG_KEY_B64");
    expect(workflow).toContain("needs: [validate, macos, windows, linux]");
    expect(workflow).toContain("STREAMSKOPE_MAC_SIGN_IDENTITY");
    expect(workflow).toContain("WINDOWS_CERTIFICATE_PASSWORD");
    expect(workflow).toContain("npm run package:release:verify");
    expect(workflow).toContain("npm run package:installer:windows:from-package");
    expect(workflow).toContain("$signtool.FullName sign /fd SHA256");
    expect(workflow).toContain("win32-x64-Setup.exe");
    expect(workflow).toContain("npm run package:installer:windows:from-package");
    expect(workflow).not.toContain("StreamSkope-$env:VERSION-win32-x64.zip");
    expect(workflow).toContain("STREAMSKOPE_N_MINUS_ONE_EVIDENCE_B64");
    expect(workflow).toMatch(/npm\s+(?:--silent\s+)?run release:sbom/u);
    expect(workflow).toContain("npm run release:manifest");
    expect(workflow).toContain("tools/scan-sensitive-artifacts.ts");
    expect(workflow).toContain("attestations: write");
    expect(workflow).toContain("id-token: write");
    expect(workflow).toContain("actions/attest@59d89421af93a897026c735860bf21b6eb4f7b26");
    expect(workflow).toContain(
      "actions/download-artifact@37930b1c2abaa49bbe596cd826c3c89aef350131",
    );
    expect(workflow).toContain("gh release create");
    expect(workflow).toContain("linux-x64");
    expect(workflow).toContain("--draft");
    expect(workflow).toContain("--verify-tag");
    expect(workflow).not.toMatch(/uses:\s+[^#\n]+@(v|main|master)\b/u);
  });
});
