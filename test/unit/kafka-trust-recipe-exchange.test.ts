import { describe, expect, it } from "vitest";

import {
  HostContractValidationError,
  exportTrustAcquisitionRecipe,
  parseTrustAcquisitionRecipeImport,
} from "../../src/kafka/contracts";
import { trustRecipeInput } from "../support/trust-recipe";

describe("Trust acquisition recipe exchange", () => {
  it("exports only definitions, never identity, access, receipts or parameter defaults", () => {
    const recipe = {
      ...trustRecipeInput(),
      id: "not-exported",
      revision: 3,
      parameters: [
        {
          ...trustRecipeInput().parameters[0],
          key: "certificate_path",
          label: "Path",
          type: "path" as const,
          required: true,
          defaultValue: "private-path-sentinel",
        },
      ],
    };
    const exported = exportTrustAcquisitionRecipe(recipe);
    expect(exported).not.toContain("private-path-sentinel");
    expect(exported).not.toContain("not-exported");
    expect(JSON.parse(exported)).toEqual({
      format: "streamskope-trust-recipe",
      version: 1,
      recipe: {
        ...trustRecipeInput(),
        parameters: [{ key: "certificate_path", label: "Path", type: "path", required: true }],
      },
    });
    expect(parseTrustAcquisitionRecipeImport(exported).name).toBe("Certificate file");
  });

  it.each([
    "not-json",
    "{",
    "[]",
    '{"format":"prototype","version":1,"recipe":{}}',
    JSON.stringify({ format: "streamskope-trust-recipe", version: 2, recipe: trustRecipeInput() }),
    JSON.stringify({
      format: "streamskope-trust-recipe",
      version: 1,
      recipe: trustRecipeInput(),
      password: "sentinel",
    }),
    " ".repeat(262145),
    "[".repeat(17) + "0" + "]".repeat(17),
  ])("rejects malformed, oversized, deep or unsupported input %# without echo", (contents) => {
    expect(() => parseTrustAcquisitionRecipeImport(contents)).toThrow(HostContractValidationError);
    try {
      parseTrustAcquisitionRecipeImport(contents);
    } catch (error) {
      expect(String(error)).not.toContain("sentinel");
    }
  });

  it("rejects unknown nested fields and secret defaults while preserving literal reviewed text", () => {
    const recipe = {
      ...trustRecipeInput(),
      ssh: { source: "stdout", value: "awk '{print $2}'", password: { source: "none" } },
    };
    const envelope = { format: "streamskope-trust-recipe", version: 1, recipe };
    expect(parseTrustAcquisitionRecipeImport(JSON.stringify(envelope)).ssh?.value).toBe(
      "awk '{print $2}'",
    );
    expect(() =>
      parseTrustAcquisitionRecipeImport(
        JSON.stringify({
          ...envelope,
          recipe: {
            ...recipe,
            parameters: [
              {
                key: "secret",
                label: "Secret",
                type: "secret",
                required: true,
                defaultValue: "sentinel",
              },
            ],
          },
        }),
      ),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseTrustAcquisitionRecipeImport(
        JSON.stringify(envelope).replace('"name":', '"__proto__":{},"name":'),
      ),
    ).toThrow(HostContractValidationError);
  });
});
