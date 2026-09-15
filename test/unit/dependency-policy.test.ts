import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import { assertDependencyPolicy } from "../../tools/dependency-policy";

const validManifest = {
  dependencies: {
    library: "1.2.3",
  },
  devDependencies: {
    tooling: "4.5.6",
  },
};

const validIntegrity = `sha512-${"A".repeat(86)}==`;

const validLock = {
  lockfileVersion: 3,
  packages: {
    "": {
      dependencies: validManifest.dependencies,
      devDependencies: validManifest.devDependencies,
    },
    "node_modules/library": {
      integrity: validIntegrity,
      license: "MIT",
      resolved: "https://registry.npmjs.org/library/-/library-1.2.3.tgz",
      version: "1.2.3",
    },
    "node_modules/tooling": {
      dev: true,
      integrity: validIntegrity,
      license: "Apache-2.0",
      resolved: "https://registry.npmjs.org/tooling/-/tooling-4.5.6.tgz",
      version: "4.5.6",
    },
  },
};

describe("dependency and license policy", () => {
  it("accepts the exact, integrity-protected repository lock graph", async () => {
    const manifest = JSON.parse(
      await readFile(new URL("../../package.json", import.meta.url), "utf8"),
    ) as unknown;
    const lock = JSON.parse(
      await readFile(new URL("../../package-lock.json", import.meta.url), "utf8"),
    ) as unknown;

    const result = assertDependencyPolicy(manifest, lock);

    expect(result.packageCount).toBeGreaterThan(400);
    expect(result.licenses).toContain("MIT");
  });

  it("rejects a floating direct dependency", () => {
    const manifest = {
      ...validManifest,
      dependencies: { library: "^1.2.3" },
    };

    expect(() => assertDependencyPolicy(manifest, validLock)).toThrow(
      "library must use an exact version",
    );
  });

  it("rejects a dependency without lockfile integrity", () => {
    const lock = structuredClone(validLock);
    Reflect.deleteProperty(lock.packages["node_modules/library"], "integrity");

    expect(() => assertDependencyPolicy(validManifest, lock)).toThrow(
      "node_modules/library has no SHA-512 integrity",
    );
  });

  it("rejects malformed lockfile integrity", () => {
    const lock = structuredClone(validLock);
    lock.packages["node_modules/library"].integrity = "sha512-not-a-digest";

    expect(() => assertDependencyPolicy(validManifest, lock)).toThrow(
      "node_modules/library has no SHA-512 integrity",
    );
  });

  it("rejects a non-registry dependency source", () => {
    const lock = structuredClone(validLock);
    lock.packages["node_modules/library"].resolved = "git+https://example.invalid/library.git";

    expect(() => assertDependencyPolicy(validManifest, lock)).toThrow(
      "node_modules/library does not resolve from the reviewed npm registry",
    );
  });

  it("rejects an unreviewed license", () => {
    const lock = structuredClone(validLock);
    lock.packages["node_modules/library"].license = "AGPL-3.0-only";

    expect(() => assertDependencyPolicy(validManifest, lock)).toThrow(
      "node_modules/library uses unreviewed license AGPL-3.0-only",
    );
  });

  it.each([false, true])(
    "does not extend packaging-only license review to another version (dev=%s)",
    (dev) => {
      const lock = structuredClone(validLock);
      Object.assign(lock.packages, {
        "node_modules/argparse": {
          dev,
          version: "2.0.2",
          license: "Python-2.0",
          integrity: validIntegrity,
          resolved: "https://registry.npmjs.org/argparse/-/argparse-2.0.2.tgz",
        },
      });
      expect(() => assertDependencyPolicy(validManifest, lock)).toThrow("unreviewed license");
    },
  );

  it("rejects a reviewed packaging-only license moved into the runtime graph", () => {
    const lock = structuredClone(validLock);
    Object.assign(lock.packages, {
      "node_modules/argparse": {
        version: "2.0.1",
        license: "Python-2.0",
        integrity: validIntegrity,
        resolved: "https://registry.npmjs.org/argparse/-/argparse-2.0.1.tgz",
      },
    });
    expect(() => assertDependencyPolicy(validManifest, lock)).toThrow("unreviewed license");
  });

  it("does not apply a reviewed license override to a different package version", () => {
    const manifest = {
      dependencies: { ssh2: "1.18.0" },
      devDependencies: {},
    };
    const lock = {
      lockfileVersion: 3,
      packages: {
        "": {
          dependencies: manifest.dependencies,
          devDependencies: manifest.devDependencies,
        },
        "node_modules/ssh2": {
          integrity: validIntegrity,
          resolved: "https://registry.npmjs.org/ssh2/-/ssh2-1.18.0.tgz",
          version: "1.18.0",
        },
      },
    };

    expect(() => assertDependencyPolicy(manifest, lock)).toThrow(
      "node_modules/ssh2 uses unreviewed license undefined",
    );
  });
});
