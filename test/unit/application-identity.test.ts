import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

const root = new URL("../../", import.meta.url);
const pngSignature = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);

describe("application identity assets", () => {
  it("keeps the native assets tied to the source vector", async () => {
    const evidence = JSON.parse(
      await readFile(new URL("assets/icons/generation.json", root), "utf8"),
    ) as {
      source: string;
      sha256: Record<string, string>;
    };
    expect(evidence.source).toBe("src/platform/ui/assets/streamskope.svg");
    expect(Object.keys(evidence.sha256).sort()).toEqual([
      "assets/icons/streamskope.icns",
      "assets/icons/streamskope.ico",
      "assets/icons/streamskope.png",
      "src/platform/ui/assets/streamskope.svg",
    ]);
    for (const [file, hash] of Object.entries(evidence.sha256)) {
      expect(
        createHash("sha256")
          .update(await readFile(new URL(file, root)))
          .digest("hex"),
      ).toBe(hash);
    }
  });

  it("contains PNG-backed native icon resolutions", async () => {
    const png = await readFile(new URL("assets/icons/streamskope.png", root));
    expect(png.subarray(0, 8)).toEqual(pngSignature);
    expect([png.readUInt32BE(16), png.readUInt32BE(20)]).toEqual([1024, 1024]);

    const ico = await readFile(new URL("assets/icons/streamskope.ico", root));
    expect([ico.readUInt16LE(0), ico.readUInt16LE(2), ico.readUInt16LE(4)]).toEqual([0, 1, 6]);
    const sizes: number[] = [];
    for (let index = 0; index < 6; index += 1) {
      const entry = 6 + index * 16;
      const start = ico.readUInt32LE(entry + 12);
      const length = ico.readUInt32LE(entry + 8);
      expect(start + length).toBeLessThanOrEqual(ico.length);
      const width = ico[entry] || 256;
      expect(ico[entry + 1] || 256).toBe(width);
      sizes.push(width);
      expect(ico.subarray(start, start + 8)).toEqual(pngSignature);
      expect(ico.readUInt32BE(start + 16)).toBe(width);
    }
    expect(sizes).toEqual([16, 32, 48, 64, 128, 256]);

    const icns = await readFile(new URL("assets/icons/streamskope.icns", root));
    expect(icns.toString("ascii", 0, 4)).toBe("icns");
    expect(icns.readUInt32BE(4)).toBe(icns.length);
    const frames: string[] = [];
    for (let offset = 8; offset < icns.length;) {
      frames.push(icns.toString("ascii", offset, offset + 4));
      const length = icns.readUInt32BE(offset + 4);
      expect(length).toBeGreaterThan(8);
      expect(offset + length).toBeLessThanOrEqual(icns.length);
      expect(icns.subarray(offset + 8, offset + 16)).toEqual(pngSignature);
      offset += length;
    }
    expect(frames).toEqual(["ic07", "ic08", "ic09", "ic10", "ic11", "ic12"]);
  });

  it("uses one identity source and the standard Settings icon across shells", async () => {
    const index = await readFile(new URL("index.html", root), "utf8");
    expect(index).toContain('href="/src/platform/ui/assets/streamskope.svg"');
    const presenter = await readFile(
      new URL("src/platform/ui/StreamSkopeAppIcon.tsx", root),
      "utf8",
    );
    expect(presenter).toContain('"./assets/streamskope.svg"');
    const source = await readFile(
      new URL("src/features/kafka/ui/WorkbenchApplicationBar.tsx", root),
      "utf8",
    );
    expect(source).toContain("<StreamSkopeAppIcon");
    expect(source).toContain('name="settings"');
    expect(source).not.toContain('name="preferences"');
    expect(source).toContain('aria-label="Preferences"');
    const packager = await readFile(new URL("tools/verify-package.ts", root), "utf8");
    expect(packager).toContain('"assets/icons"');
    expect(packager).toMatch(/icon:\s*resolve/u);
  });
});
