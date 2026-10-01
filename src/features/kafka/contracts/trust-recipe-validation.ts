import {
  canonicalConnectionTemplateName,
  validateConnectionTemplateInput,
} from "./connection-template";
import { utf8ByteLength } from "./message-limits";
import { parseTrustRecipeHttps } from "./https-trust-validation";
import { PROFILE_TRUST_KINDS } from "./profile-types";
import { parseRemoteSshEndpoint } from "./remote-trust-validation";
import {
  TRUST_RECIPE_LIMITS as limits,
  TRUST_RECIPE_PARAMETER_TYPES,
  type TrustAcquisitionRecipe,
  type TrustAcquisitionRecipeDocument,
  type TrustAcquisitionRecipeInput,
  type TrustRecipeOAuth,
  type TrustRecipeParameter,
  type TrustRecipePassword,
  type TrustRecipeSsh,
} from "./trust-recipe-types";
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

const inputKeys = [
  "name",
  "kind",
  "syntax",
  "method",
  "ssh",
  "https",
  "parameters",
  "timeoutSeconds",
  "oauth",
] as const;

function invalid(path: string, message: string): never {
  throw new HostContractValidationError(path, message);
}

function cleanText(value: unknown, path: string, maximum: number): string {
  const parsed = text(value, path, maximum);
  if (parsed.includes("\u0000")) invalid(path, "must not contain NUL");
  return parsed;
}

export function trustRecipeComparableName(value: string): string {
  return canonicalConnectionTemplateName(value).toLocaleLowerCase("en-US");
}

export function validateTrustRecipeParameterValue(
  parameter: TrustRecipeParameter,
  value: string,
  path: string,
): void {
  boundedText(value, path, limits.parameterValueCharacters);
  if (value.includes("\u0000")) invalid(path, "must not contain NUL");
  if (parameter.type === "number" && (value.trim() === "" || !Number.isFinite(Number(value)))) {
    invalid(path, "must be a finite number");
  }
  if (parameter.type === "choice" && !parameter.choices?.includes(value))
    invalid(path, "must match a declared choice");
  if (parameter.type === "host") {
    parseRemoteSshEndpoint({ host: value, port: 22 }, path);
    if (/[/@?#\\\s]/u.test(value)) invalid(path, "must be a hostname or IP address");
    try {
      new URL(`https://${value.includes(":") && !value.startsWith("[") ? `[${value}]` : value}/`);
    } catch {
      invalid(path, "must be a hostname or IP address");
    }
  }
}

function parameter(value: unknown, path: string): TrustRecipeParameter {
  const input = record(value, path);
  exactKeys(input, ["key", "label", "type", "required", "help", "defaultValue", "choices"], path);
  const key = text(input.key, `${path}.key`, limits.parameterKeyCharacters);
  if (
    !/^[A-Za-z][A-Za-z0-9_]*$/u.test(key) ||
    key === "host" ||
    key === "constructor" ||
    key === "prototype"
  ) {
    invalid(`${path}.key`, "must be a non-reserved parameter identifier");
  }
  const type = declaredValue(input.type, TRUST_RECIPE_PARAMETER_TYPES, `${path}.type`);
  let choices: readonly string[] | undefined;
  if (type === "choice") {
    if (
      !Array.isArray(input.choices) ||
      input.choices.length < 1 ||
      input.choices.length > limits.choices
    ) {
      invalid(`${path}.choices`, "must contain a bounded non-empty choice list");
    }
    choices = input.choices.map((choice, i) =>
      cleanText(choice, `${path}.choices[${i}]`, limits.choiceCharacters),
    );
    if (new Set(choices).size !== choices.length)
      invalid(`${path}.choices`, "must contain unique choices");
  } else if (Object.hasOwn(input, "choices"))
    invalid(`${path}.choices`, "is only allowed for a Choice parameter");
  const parsed: TrustRecipeParameter = {
    key,
    type,
    label: cleanText(input.label, `${path}.label`, limits.parameterLabelCharacters),
    required: truth(input.required, `${path}.required`),
    ...(Object.hasOwn(input, "help")
      ? { help: boundedText(input.help, `${path}.help`, limits.parameterHelpCharacters) }
      : {}),
    ...(choices === undefined ? {} : { choices }),
  };
  if (!Object.hasOwn(input, "defaultValue")) return parsed;
  if (type === "secret") invalid(`${path}.defaultValue`, "secret defaults cannot be stored");
  const defaultValue = boundedText(
    input.defaultValue,
    `${path}.defaultValue`,
    limits.parameterValueCharacters,
  );
  validateTrustRecipeParameterValue(parsed, defaultValue, `${path}.defaultValue`);
  return { ...parsed, defaultValue };
}

function password(value: unknown, path: string): TrustRecipePassword {
  const input = record(value, path);
  const source = declaredValue(input.source, ["none", "ask", "command"], `${path}.source`);
  exactKeys(input, source === "command" ? ["source", "command"] : ["source"], path);
  return source === "command"
    ? { source, command: cleanText(input.command, `${path}.command`, limits.commandCharacters) }
    : { source };
}

function ssh(value: unknown, path: string): TrustRecipeSsh {
  const input = record(value, path);
  exactKeys(input, ["source", "value", "password"], path);
  return {
    source: declaredValue(input.source, ["file", "stdout", "legacy-tempfile"], `${path}.source`),
    value: cleanText(input.value, `${path}.value`, limits.commandCharacters),
    password: password(input.password, `${path}.password`),
  };
}

function validateReferences(
  value: string,
  parameters: readonly TrustRecipeParameter[],
  path: string,
  secretAllowed: boolean,
): string {
  const replaced = value.replace(
    /\{\{\s*([A-Za-z][A-Za-z0-9_]*)\s*\}\}/gu,
    (_token: string, key: string) => {
      if (key === "host") return "example.test";
      const definition = parameters.find((entry) => entry.key === key);
      if (definition === undefined) invalid(path, "references an undeclared parameter");
      if (!secretAllowed && definition.type === "secret")
        invalid(path, "must not reference a Secret parameter");
      return "example";
    },
  );
  if (replaced.includes("{{") || replaced.includes("}}"))
    invalid(path, "contains an invalid parameter token");
  return replaced;
}

function oauth(
  value: unknown,
  recipe: Omit<TrustAcquisitionRecipeInput, "oauth">,
  path: string,
): TrustRecipeOAuth {
  const input = record(value, path);
  exactKeys(input, ["endpoint", "clientId", "scope"], path);
  const parsed = {
    endpoint: cleanText(input.endpoint, `${path}.endpoint`, limits.urlCharacters),
    clientId: boundedText(input.clientId, `${path}.clientId`, limits.parameterValueCharacters),
    scope: boundedText(input.scope, `${path}.scope`, limits.parameterValueCharacters),
  };
  if (recipe.syntax === "legacy-v1") {
    const issue = validateConnectionTemplateInput({
      catalog: "oauth-endpoint",
      name: recipe.name,
      template: parsed.endpoint,
    })[0];
    if (issue) invalid(`${path}.endpoint`, issue.message);
  } else {
    const endpoint = validateReferences(
      parsed.endpoint,
      recipe.parameters,
      `${path}.endpoint`,
      false,
    );
    try {
      const url = new URL(endpoint);
      if (
        !["http:", "https:"].includes(url.protocol) ||
        url.username ||
        url.password ||
        url.hash ||
        !url.hostname
      ) {
        invalid(
          `${path}.endpoint`,
          "must be an HTTP/HTTPS endpoint without credentials or fragment",
        );
      }
    } catch {
      invalid(`${path}.endpoint`, "must be an HTTP/HTTPS endpoint without credentials or fragment");
    }
    validateReferences(parsed.clientId, recipe.parameters, `${path}.clientId`, false);
    validateReferences(parsed.scope, recipe.parameters, `${path}.scope`, false);
  }
  return parsed;
}

export function parseTrustAcquisitionRecipeInput(
  value: unknown,
  path = "recipe",
): TrustAcquisitionRecipeInput {
  const input = record(value, path);
  exactKeys(input, inputKeys, path);
  const name = canonicalConnectionTemplateName(
    cleanText(input.name, `${path}.name`, limits.nameCharacters),
  );
  cleanText(name, `${path}.name`, limits.nameCharacters);
  if (!Array.isArray(input.parameters) || input.parameters.length > limits.parameters)
    invalid(`${path}.parameters`, "must be a bounded parameter list");
  const parameters = input.parameters.map((entry, i) =>
    parameter(entry, `${path}.parameters[${i}]`),
  );
  if (new Set(parameters.map((entry) => entry.key)).size !== parameters.length)
    invalid(`${path}.parameters`, "must have unique keys");
  const common = {
    name,
    kind: declaredValue(input.kind, PROFILE_TRUST_KINDS, `${path}.kind`),
    syntax: declaredValue(input.syntax, ["named-v1", "legacy-v1"], `${path}.syntax`),
    parameters,
    timeoutSeconds: positiveBoundedInteger(
      input.timeoutSeconds,
      `${path}.timeoutSeconds`,
      limits.maximumTimeoutSeconds,
    ),
  };
  const method = declaredValue(input.method, ["ssh", "https"], `${path}.method`);
  const sshSettings = input.ssh === undefined ? undefined : ssh(input.ssh, `${path}.ssh`);
  const httpsSettings =
    input.https === undefined
      ? undefined
      : parseTrustRecipeHttps(input.https, parameters, `${path}.https`);
  let parsed: TrustAcquisitionRecipeInput;
  if (method === "ssh") {
    if (sshSettings === undefined) invalid(`${path}.ssh`, "is required for SSH");
    parsed = {
      ...common,
      method,
      ssh: sshSettings,
      ...(httpsSettings === undefined ? {} : { https: httpsSettings }),
    };
  } else {
    if (httpsSettings === undefined) invalid(`${path}.https`, "is required for HTTPS");
    if (common.syntax !== "named-v1")
      invalid(`${path}.syntax`, "HTTPS requires named parameter syntax");
    if ((common.kind === "pem") !== (httpsSettings.password.source === "none"))
      invalid(`${path}.https.password`, "must match the active trust format");
    if (common.kind !== "pem" && httpsSettings.material.extraction.mode === "json-pem")
      invalid(
        `${path}.https.material.extraction`,
        "binary material requires raw bytes or JSON base64",
      );
    parsed = {
      ...common,
      method,
      https: httpsSettings,
      ...(sshSettings === undefined ? {} : { ssh: sshSettings }),
    };
  }
  if (parsed.ssh !== undefined) {
    if (
      parsed.method === "ssh" &&
      (parsed.kind === "pem") !== (parsed.ssh.password.source === "none")
    )
      invalid(`${path}.ssh.password`, "must match the declared trust format");
    if (
      parsed.method === "ssh" &&
      (parsed.syntax === "legacy-v1") !== (parsed.ssh.source === "legacy-tempfile")
    )
      invalid(`${path}.ssh.source`, "must match the definition syntax");
    if (parsed.ssh.source === "legacy-tempfile") {
      const issue = validateConnectionTemplateInput({
        catalog: "truststore-fetch",
        name,
        template: parsed.ssh.value,
      })[0];
      if (issue) invalid(`${path}.ssh.value`, issue.message);
      if (parsed.ssh.password.source === "command") {
        const passwordIssue = validateConnectionTemplateInput({
          catalog: "truststore-password",
          name,
          template: parsed.ssh.password.command,
        })[0];
        if (passwordIssue) invalid(`${path}.ssh.password.command`, passwordIssue.message);
      }
    } else {
      validateReferences(
        parsed.ssh.value,
        parameters,
        `${path}.ssh.value`,
        parsed.ssh.source !== "file",
      );
      if (parsed.ssh.password.source === "command")
        validateReferences(
          parsed.ssh.password.command,
          parameters,
          `${path}.ssh.password.command`,
          true,
        );
    }
  }
  return Object.hasOwn(input, "oauth")
    ? { ...parsed, oauth: oauth(input.oauth, parsed, `${path}.oauth`) }
    : parsed;
}

export function parseTrustAcquisitionRecipe(
  value: unknown,
  path = "recipe",
): TrustAcquisitionRecipe {
  const input = record(value, path);
  exactKeys(input, [...inputKeys, "id", "revision", "legacySourceId"], path);
  const { id, revision, legacySourceId, ...definition } = input;
  if (
    legacySourceId !== undefined &&
    (typeof legacySourceId !== "string" || !/^[a-f0-9]{64}$/u.test(legacySourceId))
  )
    invalid(`${path}.legacySourceId`, "must identify a legacy conversion receipt");
  return {
    ...parseTrustAcquisitionRecipeInput(definition, path),
    id: text(id, `${path}.id`, limits.idCharacters),
    revision: positiveBoundedInteger(revision, `${path}.revision`, Number.MAX_SAFE_INTEGER),
    ...(typeof legacySourceId === "string" ? { legacySourceId } : {}),
  };
}

export function trustAcquisitionRecipeDefinition(
  recipe: TrustAcquisitionRecipe,
): TrustAcquisitionRecipeInput {
  return parseTrustAcquisitionRecipeInput({
    name: recipe.name,
    kind: recipe.kind,
    syntax: recipe.syntax,
    method: recipe.method,
    ...(recipe.ssh === undefined ? {} : { ssh: recipe.ssh }),
    ...(recipe.https === undefined ? {} : { https: recipe.https }),
    parameters: recipe.parameters,
    timeoutSeconds: recipe.timeoutSeconds,
    ...(recipe.oauth === undefined ? {} : { oauth: recipe.oauth }),
  });
}

export function parseTrustAcquisitionRecipeDocument(
  value: unknown,
  path = "library",
): TrustAcquisitionRecipeDocument {
  const input = record(value, path);
  exactKeys(input, ["version", "recipes"], path);
  if (input.version !== 1) invalid(`${path}.version`, "is unsupported");
  if (!Array.isArray(input.recipes) || input.recipes.length > limits.entries)
    invalid(`${path}.recipes`, "must be a bounded recipe list");
  const recipes = input.recipes.map((entry, i) =>
    parseTrustAcquisitionRecipe(entry, `${path}.recipes[${i}]`),
  );
  if (new Set(recipes.map((entry) => entry.id)).size !== recipes.length)
    invalid(`${path}.recipes`, "must have unique IDs");
  if (
    new Set(recipes.map((entry) => trustRecipeComparableName(entry.name))).size !== recipes.length
  )
    invalid(`${path}.recipes`, "must have unique normalized names");
  const document = { version: 1, recipes } as const;
  if (utf8ByteLength(JSON.stringify(document)) > limits.documentBytes)
    invalid(path, "exceeds the library byte limit");
  return document;
}
