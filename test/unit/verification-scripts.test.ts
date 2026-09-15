import { existsSync, readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

interface PackageManifest {
  readonly scripts?: Readonly<Record<string, string>>;
}

const manifest = JSON.parse(
  readFileSync(new URL("../../package.json", import.meta.url), "utf8"),
) as PackageManifest;

const requiredChecks = [
  "format:check",
  "dependencies:verify",
  "lint",
  "performance:background-work",
  "performance:electron-consumer-soak",
  "performance:electron-message-retention",
  "performance:kafka-live-rules",
  "performance:kafka-message-operations",
  "performance:kafka-stream-tuning",
  "typecheck",
  "test:architecture",
  "test:unit",
  "test:integration",
  "test:kafka",
  "test:e2e:web",
  "test:e2e:electron",
  "build",
  "package:verify",
] as const;

function scriptCommands(name: string, ancestors: readonly string[] = []): string[] {
  if (ancestors.includes(name)) throw new Error(`Cyclic npm script: ${name}`);
  const script = manifest.scripts?.[name];
  if (script === undefined) throw new Error(`Missing npm script: ${name}`);
  return script.split(" && ").flatMap((command) => {
    const nested = /^npm run ([\w:-]+)$/u.exec(command)?.[1];
    return nested === undefined
      ? [command]
      : [command, ...scriptCommands(nested, [...ancestors, name])];
  });
}

describe("verification command policy", () => {
  it("keeps real OS adapter evidence in the mandatory integration gate", () => {
    const adapters = [
      "bounded-json-http",
      "web-development-bootstrap",
      "stop-web-development-owner",
      "macos-preview-dmg",
      "kafka-profile-file-store",
      "kafka-rule-file-store",
      "kafka-template-file-store",
      "kafka-trust-recipe-file-store",
      "kafka-operational-preference-file-store",
      "kafka-topic-configuration-history-file-store",
    ];
    for (const name of adapters) {
      expect(existsSync(new URL(`../integration/${name}.test.ts`, import.meta.url)), name).toBe(
        true,
      );
      expect(existsSync(new URL(`./${name}.test.ts`, import.meta.url)), name).toBe(false);
    }
    expect(manifest.scripts?.["test:integration"]).toBe(
      "vitest run --config config/vitest.config.ts test/integration",
    );
    expect(scriptCommands("verify")).toContain("npm run test:integration");
    expect(scriptCommands("verify:source")).toContain("npm run test:integration");
    const ci = readFileSync(new URL("../../.github/workflows/ci.yml", import.meta.url), "utf8");
    const release = readFileSync(
      new URL("../../.github/workflows/release.yml", import.meta.url),
      "utf8",
    );
    expect(ci).toContain("npm run verify:source");
    expect(release).toContain("uses: ./.github/workflows/ci.yml");
  });
  it("requires no Go or Wails tooling for development, verification or packaging", () => {
    const scripts = manifest.scripts ?? {};
    expect(Object.entries(scripts).flat().join("\n")).not.toMatch(
      /wails|\bgo (?:run|test|build)\b/iu,
    );
    expect(scripts["dev:web"]).toContain("bootstrap-web-development.mjs");
    expect(scripts["build:electron"]).toContain("build-electron.mjs");
    expect(scripts["package:verify:macos"]).toBeTypeOf("string");
    expect(scripts["package:verify:windows"]).toBeTypeOf("string");
  });

  it("type-checks builder options against the installed packaging API", () => {
    expect(scriptCommands("typecheck")).toContain("npm run typecheck:packaging");
    expect(manifest.scripts?.["typecheck:packaging"]).toContain("--allowJs --checkJs --noEmit");
    for (const file of ["package-windows-installer.mjs", "package-linux-appimage.mjs"]) {
      expect(manifest.scripts?.["typecheck:packaging"]).toContain(`tools/${file}`);
    }
  });

  it("declares every required check and runs each one from the aggregate gate", () => {
    const scripts = manifest.scripts ?? {};
    const aggregate = scriptCommands("verify");

    for (const check of requiredChecks) {
      expect(scripts[check], `missing package script ${check}`).toBeTypeOf("string");
      expect(aggregate, `aggregate gate omits ${check}`).toContain(`npm run ${check}`);
    }

    expect(Object.values(scripts).join("\n")).not.toContain("passWithNoTests");
  });

  it("builds once and preserves independent source and standalone package type checks", () => {
    for (const script of ["verify", "build", "package:verify", "package:release:verify"]) {
      const commands = scriptCommands(script);
      expect(
        commands.filter((command) => command === "vite build --config config/vite.config.ts"),
      ).toHaveLength(1);
      expect(
        commands.filter((command) => command === "node tools/build-electron.mjs"),
      ).toHaveLength(1);
      for (const project of ["host", "renderer", "test"]) {
        expect(
          commands.filter((command) =>
            command.startsWith(`tsc -p config/typescript/${project}.json `),
          ),
        ).toHaveLength(script === "verify" ? 2 : 1);
      }
    }
  });

  it("keeps each TypeScript project's incremental state in its own ignored cache", () => {
    for (const project of ["host", "renderer", "test"]) {
      const config = JSON.parse(
        readFileSync(new URL(`../../config/typescript/${project}.json`, import.meta.url), "utf8"),
      ) as {
        compilerOptions: { tsBuildInfoFile?: string };
      };
      expect(config.compilerOptions.tsBuildInfoFile).toBe(
        `../../.cache/typescript/${project}.tsbuildinfo`,
      );
    }
    expect(readFileSync(new URL("../../.gitignore", import.meta.url), "utf8")).toMatch(
      /^\.cache\/$/mu,
    );
  });

  it("provides one production-composed web-development launcher", () => {
    expect(manifest.scripts?.["dev:web"]).toBe("node tools/bootstrap-web-development.mjs");
  });
});
