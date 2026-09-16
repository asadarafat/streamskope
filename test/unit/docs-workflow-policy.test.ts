import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

describe("documentation workflow policy", () => {
  it("runs automatically only for published releases", async () => {
    const workflow = await readFile(
      new URL("../../.github/workflows/docs.yml", import.meta.url),
      "utf8",
    );

    expect(workflow).toContain("workflow_dispatch:");
    expect(workflow).toContain("release:");
    expect(workflow).toContain("published");
    expect(workflow).not.toContain("push:");
    expect(workflow).not.toContain("pull_request:");
    expect(workflow).toContain("if: github.event_name == 'release'");
    expect(workflow).toContain("actions/upload-pages-artifact");
    expect(workflow).toContain("actions/deploy-pages");
  });
});
