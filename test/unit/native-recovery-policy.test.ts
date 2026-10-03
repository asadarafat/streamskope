import { describe, expect, it } from "vitest";

import { installerChecksum, installerName } from "../../tools/check/native-installers";

describe("native recovery release identity", () => {
  it("selects the actual published installer on each supported native runner", () => {
    expect(installerName("0.6.0", "darwin", "arm64")).toBe("StreamSkope-0.6.0-darwin-arm64.dmg");
    expect(installerName("0.7.0", "win32", "x64")).toBe("StreamSkope-0.7.0-win32-x64-Setup.exe");
    expect(installerName("0.7.0", "linux", "x64")).toBe("StreamSkope-0.7.0-linux-x64.AppImage");
  });

  it("rejects floating, path-like and cross-architecture release identities", () => {
    for (const version of ["main", "v0.7.0", "../0.7.0", "0.7.0/other"]) {
      expect(() => installerName(version, "linux", "x64")).toThrow("explicit release versions");
    }
    expect(() => installerName("0.7.0", "linux", "arm64")).toThrow("supported native");
  });

  it("requires an exact filename match instead of trusting a similarly named checksum", () => {
    const digest = "a".repeat(64);
    const name = "StreamSkope-0.7.0-linux-x64.AppImage";
    expect(installerChecksum(`${digest}  ${name}\n`, name)).toBe(digest);
    expect(installerChecksum(`${digest} *${name}\r\n`, name)).toBe(digest);
    expect(() => installerChecksum(`${digest}  ${name}.old\n`, name)).toThrow("exactly one");
  });

  it("rejects ambiguous or malformed checksum records", () => {
    const name = "StreamSkope-0.7.0-linux-x64.AppImage";
    const entry = `${"a".repeat(64)}  ${name}\n`;
    expect(() => installerChecksum(entry + entry, name)).toThrow("exactly one");
    expect(() => installerChecksum(`not-a-digest  ${name}`, name)).toThrow("exactly one");
  });
});
