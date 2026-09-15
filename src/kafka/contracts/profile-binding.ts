import type { TrustAcquisitionRecipe } from "./trust-recipe-types";
import { parseHttpsProfileAccess, type HttpsProfileAccess } from "./https-profile-access";
import {
  parseRemoteSshAccess,
  parseAcceptedSshIdentity,
  type RemoteSshAccess,
} from "./remote-ssh-access";
import type { AcceptedSshIdentity } from "./remote-trust-types";
import { TRUST_RECIPE_LIMITS } from "./trust-recipe-types";
import {
  parseTrustAcquisitionRecipe,
  validateTrustRecipeParameterValue,
} from "./trust-recipe-validation";
import { HostContractValidationError } from "./validation-error";
import {
  boundedText,
  declaredValue,
  exactKeys,
  positiveBoundedInteger,
  record,
  text,
  truth,
} from "./validation-primitives";

export type ProfileBindingInput =
  | { readonly mode: "clear" }
  | {
      readonly mode: "replace";
      readonly recipeId: string;
      readonly recipeRevision: number;
      readonly overrides: Readonly<Record<string, string>>;
      readonly access?: RemoteSshAccess | null;
      readonly apiAccess?: HttpsProfileAccess | null;
      readonly identity?:
        | { readonly mode: "reset" }
        | { readonly mode: "acquired"; readonly acquisitionId: string; readonly editorId: string };
    };

export interface ProfileAcquisitionBinding {
  readonly apiAccess?: HttpsProfileAccess;
  readonly recipe: TrustAcquisitionRecipe;
  readonly overrides: Readonly<Record<string, string>>;
  readonly access?: RemoteSshAccess;
  readonly identity?: AcceptedSshIdentity;
}

export interface ProfileBindingDetail {
  readonly apiCaPresent?: boolean;
  readonly profileId: string;
  readonly revision: number;
  readonly binding: ProfileAcquisitionBinding | null;
}

export interface ProfileBindingDetailResult {
  readonly correlationId: string;
  readonly bindingDetail: ProfileBindingDetail;
}

export function parseProfileBindingDetailResult(value: unknown): ProfileBindingDetailResult {
  const input = record(value, "response.result");
  exactKeys(input, ["correlationId", "bindingDetail"], "response.result");
  const detail = record(input.bindingDetail, "response.result.bindingDetail");
  exactKeys(
    detail,
    ["profileId", "revision", "binding", "apiCaPresent"],
    "response.result.bindingDetail",
  );
  return {
    correlationId: text(input.correlationId, "response.result.correlationId", 128),
    bindingDetail: {
      ...(detail.apiCaPresent === undefined
        ? {}
        : {
            apiCaPresent: truth(detail.apiCaPresent, "response.result.bindingDetail.apiCaPresent"),
          }),
      profileId: text(detail.profileId, "response.result.bindingDetail.profileId", 128),
      revision: positiveBoundedInteger(
        detail.revision,
        "response.result.bindingDetail.revision",
        Number.MAX_SAFE_INTEGER,
      ),
      binding: detail.binding === null ? null : parseProfileAcquisitionBinding(detail.binding),
    },
  };
}

function parseOverrides(value: unknown, path: string): Readonly<Record<string, string>> {
  const input = record(value, path);
  if (Object.keys(input).length > TRUST_RECIPE_LIMITS.parameters) {
    throw new HostContractValidationError(path, "has too many overrides");
  }
  return Object.fromEntries(
    Object.entries(input).map(([key, entry]) => {
      if (
        !/^[A-Za-z][A-Za-z0-9_]*$/u.test(key) ||
        key.length > TRUST_RECIPE_LIMITS.parameterKeyCharacters ||
        ["host", "constructor", "prototype"].includes(key)
      ) {
        throw new HostContractValidationError(path, "contains an invalid parameter identifier");
      }
      return [
        key,
        boundedText(entry, `${path}.${key}`, TRUST_RECIPE_LIMITS.parameterValueCharacters),
      ];
    }),
  );
}

export function parseProfileBindingInput(value: unknown, path = "binding"): ProfileBindingInput {
  const input = record(value, path);
  const mode = declaredValue(input.mode, ["clear", "replace"], `${path}.mode`);
  if (mode === "clear") {
    exactKeys(input, ["mode"], path);
    return { mode };
  }
  exactKeys(
    input,
    ["mode", "recipeId", "recipeRevision", "overrides", "access", "apiAccess", "identity"],
    path,
  );
  let identity: Extract<ProfileBindingInput, { mode: "replace" }>["identity"];
  if (input.identity !== undefined) {
    const value = record(input.identity, `${path}.identity`);
    const mode = declaredValue(value.mode, ["reset", "acquired"], `${path}.identity.mode`);
    exactKeys(
      value,
      mode === "reset" ? ["mode"] : ["mode", "acquisitionId", "editorId"],
      `${path}.identity`,
    );
    identity =
      mode === "reset"
        ? { mode }
        : {
            mode,
            acquisitionId: text(
              value.acquisitionId,
              `${path}.identity.acquisitionId`,
              TRUST_RECIPE_LIMITS.idCharacters,
            ),
            editorId: text(
              value.editorId,
              `${path}.identity.editorId`,
              TRUST_RECIPE_LIMITS.idCharacters,
            ),
          };
  }
  return {
    ...(identity === undefined ? {} : { identity }),
    mode,
    recipeId: text(input.recipeId, `${path}.recipeId`, TRUST_RECIPE_LIMITS.idCharacters),
    ...(input.apiAccess === undefined
      ? {}
      : {
          apiAccess:
            input.apiAccess === null
              ? null
              : parseHttpsProfileAccess(input.apiAccess, `${path}.apiAccess`),
        }),
    recipeRevision: positiveBoundedInteger(
      input.recipeRevision,
      `${path}.recipeRevision`,
      Number.MAX_SAFE_INTEGER,
    ),
    overrides: parseOverrides(input.overrides, `${path}.overrides`),
    ...(input.access === undefined
      ? {}
      : {
          access:
            input.access === null ? null : parseRemoteSshAccess(input.access, `${path}.access`),
        }),
  };
}

export function parseProfileAcquisitionBinding(
  value: unknown,
  path = "binding",
): ProfileAcquisitionBinding {
  const input = record(value, path);
  exactKeys(input, ["recipe", "overrides", "access", "apiAccess", "identity"], path);
  const recipe = parseTrustAcquisitionRecipe(input.recipe, `${path}.recipe`);
  const overrides = parseOverrides(input.overrides, `${path}.overrides`);
  for (const [key, entry] of Object.entries(overrides)) {
    const parameter = recipe.parameters.find((candidate) => candidate.key === key);
    if (parameter === undefined || parameter.type === "secret") {
      throw new HostContractValidationError(
        `${path}.overrides`,
        "must contain only declared non-secret parameters",
      );
    }
    validateTrustRecipeParameterValue(parameter, entry, `${path}.overrides.${key}`);
  }
  return {
    recipe,
    ...(input.apiAccess === undefined
      ? {}
      : { apiAccess: parseHttpsProfileAccess(input.apiAccess, `${path}.apiAccess`) }),
    ...(input.identity === undefined
      ? {}
      : { identity: parseAcceptedSshIdentity(input.identity, `${path}.identity`) }),
    overrides,
    ...(input.access === undefined
      ? {}
      : { access: parseRemoteSshAccess(input.access, `${path}.access`) }),
  };
}
