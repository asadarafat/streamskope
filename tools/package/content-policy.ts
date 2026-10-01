const REQUIRED_APPLICATION_FILES = [
  "LICENSE",
  "package.json",
  "dist/electron/main.cjs",
  "dist/electron/preload.cjs",
  "dist/electron/trust-material-worker.cjs",
  "dist/renderer/index.html",
] as const;

const SENSITIVE_FILE_PATTERN = /\.(?:cer|crt|der|jks|key|keystore|p12|pem|pfx|truststore)$/iu;

function normalized(path: string): string {
  return path.replaceAll("\\", "/").replace(/^\.\/+/u, "");
}

function forbiddenApplicationPath(path: string): boolean {
  if (
    path.endsWith(".map") ||
    path.endsWith("/fixture.config.json") ||
    SENSITIVE_FILE_PATTERN.test(path)
  ) {
    return true;
  }
  if (path === "LICENSE" || path === "package.json" || path.startsWith("node_modules/")) {
    return false;
  }
  if (path.startsWith("dist/renderer/")) {
    return false;
  }
  return (
    path !== "dist/electron/main.cjs" &&
    path !== "dist/electron/preload.cjs" &&
    path !== "dist/electron/trust-material-worker.cjs"
  );
}

export function assertVerificationApplicationContents(paths: readonly string[]): void {
  const normalizedPaths = paths.map(normalized);
  for (const required of REQUIRED_APPLICATION_FILES) {
    if (!normalizedPaths.includes(required)) {
      throw new Error(`Verification package is missing required application file ${required}.`);
    }
  }
  const forbidden = normalizedPaths.find(forbiddenApplicationPath);
  if (forbidden !== undefined) {
    throw new Error(`Verification package contains forbidden application content ${forbidden}.`);
  }
}

export function assertVerificationBundleContents(paths: readonly string[]): void {
  const normalizedPaths = paths.map(normalized);
  const archives = normalizedPaths.filter(
    (path) => path === "resources/app.asar" || path.endsWith("/Contents/Resources/app.asar"),
  );
  if (archives.length !== 1) {
    throw new Error("Verification package must contain exactly one application ASAR.");
  }
  const archive = archives[0] as string;
  const resourcePrefix = archive.slice(0, -"app.asar".length);
  const unpacked = normalizedPaths.find(
    (path) =>
      path.startsWith(`${resourcePrefix}app/`) ||
      (path.startsWith(`${resourcePrefix}app.asar.unpacked/`) &&
        (!path.startsWith(`${resourcePrefix}app.asar.unpacked/node_modules/`) ||
          !path.endsWith(".node"))),
  );
  if (unpacked !== undefined) {
    throw new Error(`Verification package contains unpacked application content ${unpacked}.`);
  }
}
