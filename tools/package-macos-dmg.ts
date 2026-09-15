import { createMacosPreviewDmg } from "./macos-preview-dmg";

void createMacosPreviewDmg(process.cwd()).then(
  (path) => {
    process.stdout.write(
      `Unsigned macOS preview: ${path}\nSHA-256: ${path}.sha256\nNo signing or notarization performed by the DMG command.\n`,
    );
  },
  (error: unknown) => {
    process.stderr.write(
      `DMG packaging failed: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  },
);
