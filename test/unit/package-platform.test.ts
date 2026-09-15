import { describe, expect, it } from "vitest";

import { nativePackageLayout, productionSigningOptions } from "../../tools/package-platform";

describe("native Electron package policy", () => {
  it.each([
    [
      "linux",
      "/release/StreamSkope-linux-x64/StreamSkope",
      "/release/StreamSkope-linux-x64/resources/app.asar",
    ],
    [
      "darwin",
      "/release/StreamSkope-darwin-arm64/StreamSkope.app/Contents/MacOS/StreamSkope",
      "/release/StreamSkope-darwin-arm64/StreamSkope.app/Contents/Resources/app.asar",
    ],
    [
      "win32",
      "/release/StreamSkope-win32-x64/StreamSkope.exe",
      "/release/StreamSkope-win32-x64/resources/app.asar",
    ],
  ] as const)("owns the %s executable and archive paths", (platform, executable, archive) => {
    expect(
      nativePackageLayout(
        `/release/StreamSkope-${platform}-${platform === "darwin" ? "arm64" : "x64"}`,
        platform,
      ),
    ).toEqual({ archivePath: archive, executablePath: executable });
  });

  it("returns no signing configuration for an explicitly unsigned verification package", () => {
    expect(productionSigningOptions("darwin", false, {})).toEqual({});
    expect(productionSigningOptions("win32", false, {})).toEqual({});
  });

  it("requires complete macOS signing and notarization authority", () => {
    expect(() => productionSigningOptions("darwin", true, {})).toThrow(
      "STREAMSKOPE_MAC_SIGN_IDENTITY",
    );
    expect(
      productionSigningOptions("darwin", true, {
        STREAMSKOPE_APPLE_APP_PASSWORD: "app-password",
        STREAMSKOPE_APPLE_ID: "release@example.test",
        STREAMSKOPE_APPLE_TEAM_ID: "TEAM123",
        STREAMSKOPE_MAC_SIGN_IDENTITY: "Developer ID Application: StreamSkope",
      }),
    ).toEqual({
      osxNotarize: {
        appleId: "release@example.test",
        appleIdPassword: "app-password",
        teamId: "TEAM123",
      },
      osxSign: {
        identity: "Developer ID Application: StreamSkope",
      },
    });
  });

  it("requires complete Windows Authenticode authority", () => {
    expect(() => productionSigningOptions("win32", true, {})).toThrow("WINDOWS_CERTIFICATE_FILE");
    expect(
      productionSigningOptions("win32", true, {
        WINDOWS_CERTIFICATE_FILE: "C:\\release\\streamskope.pfx",
        WINDOWS_CERTIFICATE_PASSWORD: "certificate-password",
      }),
    ).toEqual({
      windowsSign: {
        certificateFile: "C:\\release\\streamskope.pfx",
        certificatePassword: "certificate-password",
      },
    });
  });

  it("uses detached artifact signing for Linux and rejects unsupported targets", () => {
    expect(productionSigningOptions("linux", true, {})).toEqual({});
    expect(() => nativePackageLayout("/release/app", "freebsd")).toThrow(
      "not a supported Electron release platform",
    );
  });
});
