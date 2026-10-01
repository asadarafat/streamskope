import { describe, expect, it } from "vitest";

import {
  parseProfileBindingInput,
  parseProfileAcquisitionBinding,
} from "../../src/features/kafka/contracts";
import {
  parseProfileCreateInput,
  parseProfileUpdateInput,
} from "../../src/features/kafka/contracts/profile-validation";
import { trustRecipeInput } from "../support/trust-recipe";

describe("independent API access binding", () => {
  const apiAccess = { host: "api.example.test", username: "operator", tls: "custom" };
  it("retains safe access without accepting API credentials or CA in the binding", () => {
    const input = {
      mode: "replace",
      recipeId: "recipe",
      recipeRevision: 1,
      overrides: {},
      apiAccess,
    };
    expect(parseProfileBindingInput(input)).toEqual(input);
    expect(parseProfileBindingInput({ ...input, apiAccess: null })).toMatchObject({
      apiAccess: null,
    });
    const binding = {
      recipe: { ...trustRecipeInput(), id: "recipe", revision: 1 },
      overrides: {},
      apiAccess,
    };
    expect(parseProfileAcquisitionBinding(binding)).toEqual(binding);
    for (const key of ["password", "token", "caPem"]) {
      expect(() =>
        parseProfileBindingInput({
          ...input,
          apiAccess: { ...apiAccess, [key]: "must-not-persist" },
        }),
      ).toThrow();
    }
  });
  it("rejects URL authorities and unsafe TLS modes in safe access fields", () => {
    for (const access of [
      { ...apiAccess, host: "user:password@api.example.test" },
      { ...apiAccess, host: "https://api.example.test" },
      { ...apiAccess, tls: "insecure" },
    ]) {
      expect(() =>
        parseProfileBindingInput({
          mode: "replace",
          recipeId: "recipe",
          recipeRevision: 1,
          overrides: {},
          apiAccess: access,
        }),
      ).toThrow();
    }
  });
  it("uses protected-value create/update semantics for API CA input", () => {
    const profile = {
      name: "fixture",
      brokers: ["localhost:9093"],
      trust: {
        kind: "pem",
        label: "CA",
        material: { mode: "replace", value: "fixture-ca" },
        password: { mode: "clear" },
      },
    };
    expect(
      parseProfileCreateInput(
        { ...profile, apiCa: { mode: "replace", value: "api-ca" } },
        "profile",
      ),
    ).toMatchObject({ apiCa: { mode: "replace", value: "api-ca" } });
    expect(() =>
      parseProfileCreateInput({ ...profile, apiCa: { mode: "retain" } }, "profile"),
    ).toThrow();
    expect(
      parseProfileUpdateInput({ ...profile, apiCa: { mode: "retain" } }, "profile"),
    ).toMatchObject({ apiCa: { mode: "retain" } });
    expect(() =>
      parseProfileCreateInput({ ...profile, apiToken: "not-storable" }, "profile"),
    ).toThrow();
  });
});
