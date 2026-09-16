import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

describe("macOS installation guidance", () => {
  it("bounds the unsigned-build exception to a verified StreamSkope app", async () => {
    const readme = await readFile(new URL("../../README.md", import.meta.url), "utf8");
    expect(readme).toContain("(website/docs/start/installation.md)");
    const guide = await readFile(
      new URL("../../website/docs/start/installation.md", import.meta.url),
      "utf8",
    );
    const checksum = guide.indexOf("shasum -a 256");
    const openAnyway = guide.indexOf("Open Anyway");
    const quarantineRemoval = guide.indexOf(
      'xattr -dr com.apple.quarantine "/Applications/StreamSkope.app"',
    );

    expect(guide).toMatch(/not (?:Developer ID )?signed or notarized/iu);
    expect(guide).toMatch(/trusted source/iu);
    expect(guide).toMatch(/checksum mismatch/iu);
    expect(checksum).toBeGreaterThanOrEqual(0);
    expect(openAnyway).toBeGreaterThan(checksum);
    expect(quarantineRemoval).toBeGreaterThan(openAnyway);
    expect(guide.match(/xattr -dr com\.apple\.quarantine/gu)).toHaveLength(1);
    expect(guide).not.toMatch(/spctl\s+--master-disable/iu);
    expect(guide).not.toMatch(/csrutil\s+disable/iu);
    expect(guide).not.toMatch(/sudo\s+xattr/iu);
  });
});
