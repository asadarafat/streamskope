import {
  HTTPS_TRUST_LIMITS as limits,
  type HttpsTrustGetDefinition,
  type HttpsTrustMaterialExtraction,
  type HttpsTrustPasswordExtraction,
  type TrustRecipeHttps,
} from "./https-trust-types";
import { TRUST_RECIPE_LIMITS, type TrustRecipeParameter } from "./trust-recipe-types";
import { HostContractValidationError } from "./validation-error";
import { boundedText, declaredValue, exactKeys, record, text } from "./validation-primitives";

const tokens = /\{\{\s*([A-Za-z][A-Za-z0-9_]*)\s*\}\}/gu;
const forbiddenHeaders = new Set([
  "authorization",
  "host",
  "cookie",
  "cookie2",
  "content-length",
  "connection",
  "transfer-encoding",
  "te",
  "trailer",
  "upgrade",
  "keep-alive",
  "expect",
  "accept-encoding",
  "proxy-authorization",
  "proxy-connection",
]);
function invalid(path: string, message: string): never {
  throw new HostContractValidationError(path, message);
}

export function validateHttpsHeader(name: string, value: string, path: string): void {
  if (
    name.length > limits.nameCharacters ||
    !/^[!#$%&'*+.^_`|~0-9a-z-]+$/iu.test(name) ||
    forbiddenHeaders.has(name.toLowerCase()) ||
    /^(sec-|proxy-)/iu.test(name)
  )
    invalid(path, "must be a non-reserved header name");
  if (value.length > limits.valueCharacters || /[^\u0020-\u007e\u0080-\u00ff]/u.test(value))
    invalid(path, "must be a bounded header value without control characters");
}

export function parseHttpsUrl(value: string, path: string): URL {
  if (value.length > TRUST_RECIPE_LIMITS.urlCharacters || /[\p{Cc}\s\\]/u.test(value))
    invalid(path, "must be a bounded HTTPS URL without whitespace or control characters");
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    invalid(path, "must be an HTTPS URL");
  }
  if (
    url.protocol !== "https:" ||
    url.username ||
    url.password ||
    value.includes("#") ||
    !url.hostname
  )
    invalid(path, "must be HTTPS without credentials or fragment");
  return url;
}

export function parseHttpsPointer(value: unknown, path: string): string {
  const pointer = boundedText(value, path, limits.pointerCharacters);
  if (
    (pointer !== "" && !pointer.startsWith("/")) ||
    /~(?![01])/u.test(pointer) ||
    (pointer !== "" && pointer.slice(1).split("/").length > limits.pointerSegments)
  )
    invalid(path, "must be a bounded JSON Pointer");
  return pointer;
}

function expand(
  value: string,
  parameters: readonly TrustRecipeParameter[],
  values: ReadonlyMap<string, string> | undefined,
  secretAllowed: boolean,
  encode: boolean,
  path: string,
): string {
  const result = value.replace(tokens, (_token: string, key: string) => {
    const parameter = parameters.find((entry) => entry.key === key);
    if (key !== "host" && parameter === undefined)
      invalid(path, "references an undeclared parameter");
    if (parameter?.type === "secret" && !secretAllowed)
      invalid(path, "must not reference a Secret parameter");
    const resolved =
      values === undefined ? (key === "host" ? "example.test" : "example") : values.get(key);
    if (resolved === undefined) invalid(path, "requires a parameter value");
    if (/[\uD800-\uDFFF]/u.test(resolved)) invalid(path, "parameter must be valid Unicode");
    return encode ? encodeURIComponent(resolved) : resolved;
  });
  if (result.includes("{{") || result.includes("}}"))
    invalid(path, "contains a malformed parameter token");
  return result;
}

export function resolveHttpsGet(
  definition: HttpsTrustGetDefinition,
  parameters: readonly TrustRecipeParameter[],
  values?: ReadonlyMap<string, string>,
  path = "recipe.https",
): { url: string; headers: readonly { name: string; value: string }[] } {
  const normalized = definition.url.replace(tokens, (_token: string, key: string) => `{{${key}}}`);
  const parts = /^https:\/\/([^/?#]+)(.*)$/u.exec(normalized);
  if (!parts) invalid(path, "must have an explicit HTTPS authority");
  const hostPart = parts[1] ?? "";
  if (
    (hostPart.includes("{{") || hostPart.includes("}}")) &&
    !/^\{\{host\}\}(?::[0-9]+)?$/u.test(hostPart)
  )
    invalid(path, "only the reserved host may replace the whole hostname; ports must be literal");
  if (hostPart.includes("{{host}}") && values !== undefined && !values.get("host"))
    invalid(path, "requires a host value");
  let host = values?.get("host") ?? "example.test";
  if (/[/@?#\\\s\p{Cc}]/u.test(host)) invalid(path, "host must be a hostname or IP address");
  if (host.includes(":") && !host.startsWith("[")) host = `[${host}]`;
  const resolvedAuthority = hostPart.replace("{{host}}", host);
  const tail = expand(parts[2] ?? "", parameters, values, false, true, path);
  const url = parseHttpsUrl(`https://${resolvedAuthority}${tail}`, path);
  const seen = new Set<string>();
  for (const key of url.searchParams.keys()) {
    if (seen.has(key)) invalid(path, "query names must be unique");
    seen.add(key);
  }
  for (const row of definition.query) {
    if (seen.has(row.name)) invalid(path, "query names must be unique");
    seen.add(row.name);
    const value = expand(row.value, parameters, values, false, false, path);
    if (value.length > limits.valueCharacters || /\p{Cc}/u.test(value))
      invalid(path, "query value exceeds bounds or contains controls");
    url.searchParams.append(row.name, value);
  }
  parseHttpsUrl(url.href, path);
  const headers = definition.headers.map((row) => {
    const value = expand(row.value, parameters, values, true, false, path);
    validateHttpsHeader(row.name, value, path);
    return { name: row.name, value };
  });
  return { url: url.href, headers };
}

function rows(
  value: unknown,
  path: string,
  header: boolean,
): readonly { name: string; value: string }[] {
  if (!Array.isArray(value) || value.length > limits.entries)
    invalid(path, "must be a bounded list");
  const parsed = value.map((entry, index) => {
    const item = record(entry, `${path}[${index}]`);
    exactKeys(item, ["name", "value"], path);
    const name = text(item.name, path, limits.nameCharacters);
    const value = boundedText(item.value, path, limits.valueCharacters);
    if (header) validateHttpsHeader(name, value, path);
    else if (/\p{Cc}/u.test(name + value) || name.includes("{{"))
      invalid(path, "query names must be literal and values must not contain controls");
    return { name, value };
  });
  if (
    new Set(parsed.map(({ name }) => (header ? name.toLowerCase() : name))).size !== parsed.length
  )
    invalid(path, "names must be unique");
  return parsed;
}

function get(
  value: unknown,
  path: string,
): { input: Record<string, unknown>; base: HttpsTrustGetDefinition } {
  const input = record(value, path);
  exactKeys(input, ["url", "headers", "query", "extraction"], path);
  return {
    input,
    base: {
      url: text(input.url, `${path}.url`, TRUST_RECIPE_LIMITS.urlCharacters),
      headers: rows(input.headers, `${path}.headers`, true),
      query: rows(input.query, `${path}.query`, false),
    },
  };
}

export function parseTrustRecipeHttps(
  value: unknown,
  parameters: readonly TrustRecipeParameter[],
  path: string,
): TrustRecipeHttps {
  const input = record(value, path);
  exactKeys(input, ["authentication", "material", "password"], path);
  const material = get(input.material, `${path}.material`);
  const output = record(material.input.extraction, `${path}.material.extraction`);
  const mode = declaredValue(
    output.mode,
    ["raw", "json-pem", "json-base64"],
    `${path}.material.extraction.mode`,
  );
  exactKeys(output, mode === "raw" ? ["mode"] : ["mode", "pointer"], path);
  const extraction: HttpsTrustMaterialExtraction =
    mode === "raw"
      ? { mode }
      : { mode, pointer: parseHttpsPointer(output.pointer, `${path}.material.extraction.pointer`) };
  const password = record(input.password, `${path}.password`);
  const source = declaredValue(
    password.source,
    ["none", "ask", "https"],
    `${path}.password.source`,
  );
  exactKeys(password, source === "https" ? ["source", "request"] : ["source"], path);
  let parsedPassword: TrustRecipeHttps["password"];
  if (source === "https") {
    const request = get(password.request, `${path}.password.request`);
    const result = record(request.input.extraction, `${path}.password.request.extraction`);
    const mode = declaredValue(
      result.mode,
      ["text", "json"],
      `${path}.password.request.extraction.mode`,
    );
    exactKeys(result, mode === "text" ? ["mode"] : ["mode", "pointer"], path);
    const extraction: HttpsTrustPasswordExtraction =
      mode === "text"
        ? { mode }
        : {
            mode,
            pointer: parseHttpsPointer(
              result.pointer,
              `${path}.password.request.extraction.pointer`,
            ),
          };
    parsedPassword = { source, request: { ...request.base, extraction } };
  } else parsedPassword = { source };
  const origin = new URL(resolveHttpsGet(material.base, parameters, undefined, path).url).origin;
  if (
    parsedPassword.source === "https" &&
    new URL(resolveHttpsGet(parsedPassword.request, parameters, undefined, path).url).origin !==
      origin
  )
    invalid(`${path}.password.request.url`, "must have the same origin as material retrieval");
  return {
    authentication: declaredValue(
      input.authentication,
      ["none", "bearer", "basic"],
      `${path}.authentication`,
    ),
    material: { ...material.base, extraction },
    password: parsedPassword,
  };
}
