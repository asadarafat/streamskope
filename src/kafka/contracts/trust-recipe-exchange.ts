import { utf8ByteLength } from "./message-limits";
import {
  TRUST_RECIPE_LIMITS,
  type TrustAcquisitionRecipe,
  type TrustAcquisitionRecipeInput,
} from "./trust-recipe-types";
import {
  parseTrustAcquisitionRecipeInput,
  parseTrustAcquisitionRecipe,
  trustAcquisitionRecipeDefinition,
} from "./trust-recipe-validation";
import { HostContractValidationError } from "./validation-error";
import { exactKeys, record } from "./validation-primitives";

const exchangeFormat = "streamskope-trust-recipe";

export function parseTrustRecipeJson(contents: string, maximumBytes: number): unknown {
  if (utf8ByteLength(contents) > maximumBytes)
    throw new HostContractValidationError("document", "exceeds the byte limit");
  let depth = 0;
  let quoted = false;
  let escaped = false;
  for (const character of contents) {
    if (quoted) {
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === '"') quoted = false;
    } else if (character === '"') quoted = true;
    else if (character === "{" || character === "[") {
      depth += 1;
      if (depth > TRUST_RECIPE_LIMITS.jsonDepth)
        throw new HostContractValidationError("document", "exceeds the nesting limit");
    } else if (character === "}" || character === "]") depth -= 1;
  }
  try {
    return JSON.parse(contents) as unknown;
  } catch {
    throw new HostContractValidationError("document", "must contain valid JSON");
  }
}

export function parseTrustAcquisitionRecipeImport(contents: string): TrustAcquisitionRecipeInput {
  const envelope = record(
    parseTrustRecipeJson(contents, TRUST_RECIPE_LIMITS.exchangeBytes),
    "import",
  );
  exactKeys(envelope, ["format", "version", "recipe"], "import");
  if (envelope.format !== exchangeFormat || envelope.version !== 1)
    throw new HostContractValidationError("import", "uses an unsupported format or version");
  return parseTrustAcquisitionRecipeInput(envelope.recipe, "import.recipe");
}

export function exportTrustAcquisitionRecipe(value: TrustAcquisitionRecipe): string {
  const recipe = trustAcquisitionRecipeDefinition(parseTrustAcquisitionRecipe(value));
  const parameters = recipe.parameters.map(
    ({ defaultValue: _defaultValue, ...parameter }) => parameter,
  );
  const output = JSON.stringify(
    { format: exchangeFormat, version: 1, recipe: { ...recipe, parameters } },
    null,
    2,
  );
  if (utf8ByteLength(output) > TRUST_RECIPE_LIMITS.exchangeBytes)
    throw new HostContractValidationError("export", "exceeds the exchange byte limit");
  return output;
}
