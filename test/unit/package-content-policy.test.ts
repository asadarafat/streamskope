import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import {
  assertVerificationApplicationContents,
  assertVerificationBundleContents,
} from "../../tools/package-content-policy";

interface PackageManifest {
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly devDependencies?: Readonly<Record<string, string>>;
}

describe("Electron verification package content policy", () => {
  it("accepts one ASAR bundle containing only the built application and production graph", () => {
    const applicationPaths = [
      "LICENSE",
      "package.json",
      "dist/electron/main.cjs",
      "dist/electron/preload.cjs",
      "dist/electron/trust-material-worker.cjs",
      "dist/renderer/index.html",
      "dist/renderer/assets/index.js",
      "node_modules/@platformatic/kafka/package.json",
      "node_modules/jks-js/package.json",
      "node_modules/ssh2/package.json",
    ];
    const bundlePaths = [
      "StreamSkope",
      "resources/app.asar",
      "resources/app.asar.unpacked/node_modules/@node-rs/crc32/crc32.node",
      "resources/electron.asar",
    ];

    expect(() => assertVerificationApplicationContents(applicationPaths)).not.toThrow();
    expect(() => assertVerificationBundleContents(bundlePaths)).not.toThrow();
  });

  it("rejects an application archive that omits the project license", () => {
    expect(() =>
      assertVerificationApplicationContents([
        "package.json",
        "dist/electron/main.cjs",
        "dist/electron/preload.cjs",
        "dist/electron/trust-material-worker.cjs",
        "dist/renderer/index.html",
      ]),
    ).toThrow("LICENSE");
  });

  it.each([
    "resources/app/aio-kafka/fixture.config.json",
    "resources/app/docs/roadmap.md",
    "resources/app/openspec/config.yaml",
    "resources/app/src/main/electron-entry.ts",
    "resources/app/test/e2e/web-workbench.spec.ts",
    "resources/app/tools/start-web-development.ts",
    "resources/app/.github/workflows/ci.yml",
    "resources/app/test-results/trace.zip",
    "resources/app/dist/electron/main.cjs.map",
    "resources/app/dist/dev-host/server.cjs",
    "resources/app/ca.pem",
    "resources/app/client.crt",
    "resources/app/kafka.truststore",
    "resources/app/node_modules/jks-js/examples/assets/keystore.jks",
    "resources/app/node_modules/ssh2/test/fixtures/https_cert.pem",
  ])("rejects forbidden application content %s", (forbiddenPath) => {
    const baseline = [
      "LICENSE",
      "package.json",
      "dist/electron/main.cjs",
      "dist/electron/preload.cjs",
      "dist/electron/trust-material-worker.cjs",
      "dist/renderer/index.html",
      forbiddenPath.replace("resources/app/", ""),
    ];

    expect(() => assertVerificationApplicationContents(baseline)).toThrow(
      forbiddenPath.replace("resources/app/", ""),
    );
  });

  it.each([
    ["missing ASAR", ["StreamSkope", "resources/electron.asar"]],
    [
      "unpacked application root",
      [
        "StreamSkope",
        "resources/app.asar",
        "resources/app/package.json",
        "resources/electron.asar",
      ],
    ],
    [
      "unpacked ASAR content",
      [
        "StreamSkope",
        "resources/app.asar",
        "resources/app.asar.unpacked/node_modules/example/config.json",
        "resources/electron.asar",
      ],
    ],
  ])("rejects %s", (_label, paths) => {
    expect(() => assertVerificationBundleContents(paths)).toThrow();
  });

  it("accepts the macOS application archive beneath the native bundle", () => {
    expect(() =>
      assertVerificationBundleContents([
        "StreamSkope.app/Contents/MacOS/StreamSkope",
        "StreamSkope.app/Contents/Resources/app.asar",
        "StreamSkope.app/Contents/Resources/electron.asar",
      ]),
    ).not.toThrow();
  });

  it("keeps only the reviewed Kafka host dependencies in the external production graph", async () => {
    const manifest = JSON.parse(
      await readFile(new URL("../../package.json", import.meta.url), "utf8"),
    ) as PackageManifest;

    expect(Object.keys(manifest.dependencies ?? {}).sort()).toEqual([
      "@platformatic/kafka",
      "jks-js",
      "node-forge",
      "ssh2",
    ]);
    for (const dependency of [
      "@emotion/react",
      "@emotion/styled",
      "@mui/material",
      "@mui/x-data-grid",
      "react",
      "react-dom",
    ]) {
      expect(manifest.devDependencies?.[dependency]).toBeTypeOf("string");
    }
  });
});
