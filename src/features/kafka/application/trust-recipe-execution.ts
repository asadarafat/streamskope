import {
  HostContractValidationError,
  expandOAuthEndpointTemplate,
  parseProfileAcquisitionBinding,
  parseTrustAcquisitionRecipe,
  validateTrustRecipeParameterValue,
  type TrustAcquisitionRecipe,
  type TrustRecipeOAuth,
} from "../contracts";

export interface TrustRecipeExecution {
  readonly material: {
    readonly source: "file" | "stdout" | "legacy-tempfile";
    readonly value: string;
  };
  readonly password:
    { readonly source: "none" | "ask" } | { readonly source: "command"; readonly command: string };
  readonly oauth?: TrustRecipeOAuth;
}

const token = /\{\{\s*([A-Za-z][A-Za-z0-9_]*)\s*\}\}/gu;

function invalid(path: string, reason: string): never {
  throw new HostContractValidationError(path, reason);
}

function shellExpand(template: string, values: ReadonlyMap<string, string>): string {
  if (template.includes("{{") && /`|\$\(|<</u.test(template)) {
    invalid(
      "recipe.ssh",
      "named parameters cannot be embedded in command substitution or here-documents; use direct command arguments",
    );
  }
  let quote: "'" | '"' | undefined;
  let escaped = false;
  let offset = 0;
  let output = "";
  for (const match of template.matchAll(token)) {
    const prefix = template.slice(offset, match.index);
    for (const character of prefix) {
      if (escaped) {
        escaped = false;
        continue;
      }
      if (character === "\\" && quote !== "'") {
        escaped = true;
        continue;
      }
      if (character === quote) quote = undefined;
      else if (quote === undefined && (character === "'" || character === '"')) quote = character;
    }
    if (escaped) invalid("recipe.ssh", "a named parameter cannot follow a shell escape");
    const value = values.get(match[1] ?? "");
    if (value === undefined) invalid("recipe.parameters", "a referenced parameter is required");
    const single = value.replaceAll("'", "'\"'\"'");
    const quoted =
      quote === "'" ? single : quote === '"' ? value.replace(/[\\$`"]/gu, "\\$&") : `'${single}'`;
    output += prefix + quoted;
    offset = match.index + match[0].length;
  }
  return output + template.slice(offset);
}

export function resolveTrustRecipeParameters(
  recipe: TrustAcquisitionRecipe,
  overrides: Readonly<Record<string, string>>,
  secrets: Readonly<Record<string, string>>,
  host: string,
): ReadonlyMap<string, string> {
  parseProfileAcquisitionBinding({ recipe, overrides });
  for (const key of Object.keys(secrets)) {
    if (!recipe.parameters.some((entry) => entry.key === key && entry.type === "secret"))
      invalid("recipe.parameters", "ephemeral inputs must name declared secret parameters");
  }
  const values = new Map<string, string>([["host", host]]);
  for (const parameter of recipe.parameters) {
    const value =
      parameter.type === "secret"
        ? secrets[parameter.key]
        : (overrides[parameter.key] ?? parameter.defaultValue);
    if (value === undefined || (parameter.required && value.length === 0)) {
      if (parameter.required) invalid(`recipe.parameters.${parameter.key}`, "is required");
      continue;
    }
    validateTrustRecipeParameterValue(parameter, value, `recipe.parameters.${parameter.key}`);
    values.set(parameter.key, value);
  }
  return values;
}

function literalExpand(value: string, values: ReadonlyMap<string, string>): string {
  return value.replace(token, (_match: string, key: string) => {
    const resolved = values.get(key);
    if (resolved === undefined)
      invalid(`recipe.parameters.${key}`, "is required by the selected recipe");
    return resolved;
  });
}

export function resolveTrustRecipeOAuth(
  recipe: TrustAcquisitionRecipe,
  values: ReadonlyMap<string, string>,
): TrustRecipeOAuth | undefined {
  const host = values.get("host") ?? "";
  const literal = (value: string): string => literalExpand(value, values);
  let oauth: TrustRecipeOAuth | undefined;
  if (recipe.oauth !== undefined) {
    const endpoint =
      recipe.syntax === "legacy-v1"
        ? expandOAuthEndpointTemplate(recipe.oauth.endpoint, [
            `${host.includes(":") && !host.startsWith("[") ? `[${host}]` : host}:22`,
          ])
        : literal(recipe.oauth.endpoint);
    try {
      const parsed = new URL(endpoint);
      if (
        !["http:", "https:"].includes(parsed.protocol) ||
        parsed.username ||
        parsed.password ||
        parsed.hash ||
        !parsed.hostname
      )
        invalid("recipe.oauth.endpoint", "must resolve to a credential-free HTTP/HTTPS endpoint");
    } catch {
      invalid("recipe.oauth.endpoint", "must resolve to a credential-free HTTP/HTTPS endpoint");
    }
    oauth = {
      endpoint,
      clientId: literal(recipe.oauth.clientId),
      scope: literal(recipe.oauth.scope),
    };
  }
  return oauth;
}

export function resolveTrustRecipeExecution(
  input: TrustAcquisitionRecipe,
  overrides: Readonly<Record<string, string>>,
  secrets: Readonly<Record<string, string>>,
  host: string,
): TrustRecipeExecution {
  const recipe = parseTrustAcquisitionRecipe(input);
  if (recipe.method !== "ssh") invalid("recipe.method", "SSH execution requires an SSH recipe");
  const values = resolveTrustRecipeParameters(recipe, overrides, secrets, host);
  const literal = (value: string): string => literalExpand(value, values);
  const expand =
    recipe.syntax === "legacy-v1"
      ? (value: string): string => value
      : (value: string): string => shellExpand(value, values);
  const oauth = resolveTrustRecipeOAuth(recipe, values);
  return {
    material: {
      source: recipe.ssh.source,
      value: recipe.ssh.source === "file" ? literal(recipe.ssh.value) : expand(recipe.ssh.value),
    },
    password:
      recipe.ssh.password.source === "command"
        ? { source: "command", command: expand(recipe.ssh.password.command) }
        : recipe.ssh.password,
    ...(oauth === undefined ? {} : { oauth }),
  };
}
