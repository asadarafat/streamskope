import { execFileSync } from "node:child_process";

import { describe, expect, it } from "vitest";

import { resolveTrustRecipeExecution } from "../../src/features/kafka/application/trust-recipe-execution";
import { trustRecipeInput } from "../support/trust-recipe";

const recipe = { ...trustRecipeInput(), id: "recipe-1", revision: 1 };

describe("validated recipe execution planning", () => {
  it.skipIf(process.platform === "win32")(
    "passes hostile parameter text as literal arguments through a real shell",
    () => {
      const value = "a'b\";printf INJECTED;$(printf EXPANDED)`printf BACKTICK`\\$HOME";
      const plan = resolveTrustRecipeExecution(
        {
          ...recipe,
          ssh: {
            source: "stdout",
            value: `printf '%s\\n' {{certificate_path}} '{{certificate_path}}' "{{certificate_path}}"`,
            password: { source: "none" },
          },
        },
        { certificate_path: value },
        {},
        "host.test",
      );
      expect(
        execFileSync("/bin/sh", ["-c", plan.material.value], { encoding: "utf8", timeout: 2_000 }),
      ).toBe(`${value}\n${value}\n${value}\n`);
    },
  );
  it("resolves a file path literally without shell quoting", () => {
    expect(
      resolveTrustRecipeExecution(
        recipe,
        { certificate_path: "/remote/cert one.pem" },
        {},
        "host.test",
      ),
    ).toMatchObject({
      material: { source: "file", value: "/remote/cert one.pem" },
      password: { source: "none" },
    });
  });
  it("quotes substitutions in unquoted, single-quoted and double-quoted command contexts", () => {
    const command = {
      ...recipe,
      ssh: {
        source: "stdout" as const,
        value: `read {{certificate_path}} '{{certificate_path}}' "{{certificate_path}}"`,
        password: { source: "none" as const },
      },
    };
    const result = resolveTrustRecipeExecution(
      command,
      { certificate_path: `a'b$HOME` },
      {},
      "host.test",
    );
    expect(result.material.value).toBe(`read 'a'"'"'b$HOME' 'a'"'"'b$HOME' "a'b\\$HOME"`);
  });
  it("rejects missing, undeclared and misplaced secret values before execution", () => {
    expect(() => resolveTrustRecipeExecution(recipe, {}, {}, "host.test")).toThrow(/required/i);
    expect(() =>
      resolveTrustRecipeExecution(recipe, { certificate_path: "/ca", extra: "x" }, {}, "host.test"),
    ).toThrow();
    expect(() =>
      resolveTrustRecipeExecution(recipe, {}, { certificate_path: "/ca" }, "host.test"),
    ).toThrow();
  });
  it("keeps literal braces intact and does not recursively expand supplied values", () => {
    const command = {
      ...recipe,
      ssh: {
        source: "stdout" as const,
        value: "awk '{print $1}' {{certificate_path}}",
        password: { source: "none" as const },
      },
    };
    expect(
      resolveTrustRecipeExecution(command, { certificate_path: "{{host}}" }, {}, "host.test")
        .material.value,
    ).toBe("awk '{print $1}' '{{host}}'");
  });
  it("validates resolved OAuth suggestions rather than silently accepting injected credentials", () => {
    const withOAuth = {
      ...recipe,
      oauth: { endpoint: "https://{{certificate_path}}/token", clientId: "client", scope: "" },
    };
    expect(() =>
      resolveTrustRecipeExecution(
        withOAuth,
        { certificate_path: "user:secret@host.test" },
        {},
        "host.test",
      ),
    ).toThrow();
  });
});
