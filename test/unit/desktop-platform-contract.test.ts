import { describe, expect, it } from "vitest";

import {
  DESKTOP_PLATFORM_VERSION,
  DesktopPlatformContractError,
  parseDesktopAction,
  parseArtifactReference,
  parseDesktopSaveResult,
  parseDesktopTextDocument,
} from "../../src/platform/desktop";

describe("desktop platform contract", () => {
  it("accepts only an opaque artifact and declared part, never renderer paths or data", () => {
    const reference = { artifactId: "823b690d-2cd2-42b8-a3d0-d2e07b1330ec", part: "data" };
    expect(parseArtifactReference(reference)).toEqual(reference);
    for (const value of [
      { ...reference, filePath: "/tmp/private" },
      { ...reference, artifactId: "../../private" },
      { ...reference, part: "key" },
    ])
      expect(() => parseArtifactReference(value)).toThrow(DesktopPlatformContractError);
  });
  const document = {
    byteSize: 17,
    content: '{\n  "ok": true\n}\n',
    fileName: "streamskope-export.json",
    mediaType: "application/json",
  } as const;

  it("accepts one bounded generic JSON document and declared native actions", () => {
    expect(parseDesktopTextDocument(document, "request")).toEqual(document);
    expect(
      parseDesktopAction({
        action: "preferences.open",
        version: DESKTOP_PLATFORM_VERSION,
      }),
    ).toEqual({
      action: "preferences.open",
      version: DESKTOP_PLATFORM_VERSION,
    });
  });

  it("rejects path-bearing names, mismatched bytes and undeclared actions", () => {
    for (const value of [
      { ...document, fileName: "../private.json" },
      { ...document, byteSize: 1 },
      { ...document, content: "not-json", byteSize: 8 },
    ]) {
      expect(() => parseDesktopTextDocument(value, "request")).toThrow(
        DesktopPlatformContractError,
      );
    }
    expect(() =>
      parseDesktopAction({
        action: "shell.execute",
        version: DESKTOP_PLATFORM_VERSION,
      }),
    ).toThrow(DesktopPlatformContractError);
  });

  it("distinguishes saved and cancelled results", () => {
    expect(
      parseDesktopSaveResult({
        state: "cancelled",
        version: DESKTOP_PLATFORM_VERSION,
      }),
    ).toEqual({
      state: "cancelled",
      version: DESKTOP_PLATFORM_VERSION,
    });
    expect(() =>
      parseDesktopSaveResult({
        filePath: "/tmp/private.json",
        state: "saved",
        version: DESKTOP_PLATFORM_VERSION,
      }),
    ).toThrow(DesktopPlatformContractError);
  });
});
