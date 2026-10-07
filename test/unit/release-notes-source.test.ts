import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import { releaseNotesBody, reviewedReleaseCommentary } from "../../tools/package/release-policy";

describe("versioned release notes source", () => {
  it("preserves real upgrade guidance and rejects malformed development-only blocks", () => {
    const reviewed = "## Release highlights\n\nBack up your profiles before upgrading.\n";
    expect(reviewedReleaseCommentary(reviewed)).toBe(reviewed.trim());
    expect(() =>
      reviewedReleaseCommentary("<!-- development-release-status -->\nsecret preview\n"),
    ).toThrow(/bounded block/);
    expect(() =>
      reviewedReleaseCommentary(
        "<!-- /development-release-status --><!-- development-release-status -->",
      ),
    ).toThrow(/bounded block/);
  });
  it("extracts the reviewed body for the matching release version without rewriting it", () => {
    const body = "# StreamSkope 1.2.3\n\n- Clear connection diagnostics.\n";
    expect(
      releaseNotesBody(`---\nrelease_version: 1.2.3\nrelease_tag: v1.2.3\n---\n${body}`, "1.2.3"),
    ).toBe(body);
  });

  it("rejects legacy rebuild identities for new release preparation", () => {
    const source = "---\nrelease_version: 0.1.0\nrelease_tag: v0.1.0+build.2\n---\n# Build 2\n";
    expect(() => releaseNotesBody(source, "0.1.0", "v0.1.0+build.2")).toThrow(/exact release tag/);
    expect(() => releaseNotesBody(source, "0.1.0", "v0.1.0")).toThrow(/exact release tag/);
    expect(() => releaseNotesBody(source, "0.1.0", "v0.1.0+build.3")).toThrow(/exact release tag/);
  });

  it("uses an exact prerelease version and tag for a release candidate", () => {
    const source = "---\nrelease_version: 0.2.0-rc.1\nrelease_tag: v0.2.0-rc.1\n---\n# Candidate\n";
    expect(releaseNotesBody(source, "0.2.0-rc.1")).toBe("# Candidate\n");
    expect(() => releaseNotesBody(source, "0.2.0-rc.1", "v0.2.0-rc.2")).toThrow(
      /exact release tag/,
    );
  });

  it("rejects a version mismatch or empty release body", () => {
    expect(() =>
      releaseNotesBody("---\nrelease_version: 1.2.3\nrelease_tag: v1.2.3\n---\nNotes\n", "1.2.4"),
    ).toThrow();
    expect(() =>
      releaseNotesBody("---\nrelease_version: 1.2.3\nrelease_tag: v1.2.3\n---\n", "1.2.3"),
    ).toThrow();
  });
});

it("uses the same versioned Markdown release source for the GitHub Release body", async () => {
  const workflow = await readFile(
    new URL("../../.github/workflows/release.yml", import.meta.url),
    "utf8",
  );
  expect(workflow).toContain("npm run package -- release");
  expect(workflow).toContain("website/docs/releases/$RELEASE_TAG.md");
  expect(workflow).toContain('--notes-file "$RUNNER_TEMP/notes.md"');
});
