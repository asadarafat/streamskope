import {
  CONNECTION_TEMPLATE_LIMITS,
  type CommandTemplateCatalog,
  type ConnectionTemplateInput,
  type ConnectionTemplateIssue,
} from "./connection-template-types";

const COMMAND_PLACEHOLDER_PATTERN = /\{([A-Za-z][A-Za-z0-9_-]*)\}/g;
const ENDPOINT_HOST_PLACEHOLDERS = [
  "kafka-cluseter-server",
  "kafka-cluster-server",
  "kafka_host",
  "kafka-host",
  "host",
] as const;
const ENDPOINT_HOST_PLACEHOLDER_PATTERN =
  /\{(?:kafka-cluseter-server|kafka-cluster-server|kafka_host|kafka-host|host)\}/gi;

export class TemplateExpansionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TemplateExpansionError";
  }
}

export function canonicalConnectionTemplateName(name: string): string {
  return name.normalize("NFKC").trim();
}

function placeholderNames(template: string): readonly string[] {
  return [...template.matchAll(COMMAND_PLACEHOLDER_PATTERN)].map((match) => match[1] ?? "");
}

function validEndpoint(value: string): boolean {
  try {
    const endpoint = new URL(value);
    return (
      (endpoint.protocol === "http:" || endpoint.protocol === "https:") &&
      endpoint.hostname.length > 0 &&
      endpoint.username.length === 0 &&
      endpoint.password.length === 0 &&
      endpoint.hash.length === 0
    );
  } catch {
    return false;
  }
}

function replaceEndpointHost(template: string, host: string): string {
  return template.replace(ENDPOINT_HOST_PLACEHOLDER_PATTERN, host);
}

function endpointTemplateIssue(template: string): string | undefined {
  const placeholders = placeholderNames(template);
  const allowed = new Set<string>(ENDPOINT_HOST_PLACEHOLDERS);
  const unsupported = placeholders.find((placeholder) => !allowed.has(placeholder.toLowerCase()));
  if (unsupported !== undefined) {
    return `Placeholder {${unsupported}} is not supported in oauth-endpoint templates.`;
  }
  if (!ENDPOINT_HOST_PLACEHOLDER_PATTERN.test(template)) {
    return "OAuth endpoint templates must include a supported broker host placeholder.";
  }
  ENDPOINT_HOST_PLACEHOLDER_PATTERN.lastIndex = 0;
  if (!validEndpoint(replaceEndpointHost(template, "example.test"))) {
    return "OAuth endpoint templates must expand to an HTTP or HTTPS URL without credentials or a fragment.";
  }
  return undefined;
}

function commandTemplateIssue(input: ConnectionTemplateInput): string | undefined {
  const placeholders = placeholderNames(input.template);
  if (input.catalog === "truststore-password") {
    const unsupported = placeholders[0];
    return unsupported === undefined
      ? undefined
      : `Placeholder {${unsupported}} is not supported in truststore-password templates.`;
  }

  const allowed = new Set(["truststorePath", "destDir", "storepass"]);
  const unsupported = placeholders.find((placeholder) => !allowed.has(placeholder));
  if (unsupported !== undefined) {
    return `Placeholder {${unsupported}} is not supported in truststore-fetch templates.`;
  }
  return placeholders.includes("truststorePath")
    ? undefined
    : "Truststore fetch templates must include {truststorePath}.";
}

export function validateConnectionTemplateInput(
  input: ConnectionTemplateInput,
): readonly ConnectionTemplateIssue[] {
  const name = canonicalConnectionTemplateName(input.name);
  if (name.length === 0 || input.name.length > CONNECTION_TEMPLATE_LIMITS.nameCharacters) {
    return [
      {
        field: "name",
        message: `Enter a template name no longer than ${CONNECTION_TEMPLATE_LIMITS.nameCharacters} characters.`,
      },
    ];
  }
  const maximumTemplateCharacters =
    input.catalog === "oauth-endpoint"
      ? CONNECTION_TEMPLATE_LIMITS.endpointCharacters
      : CONNECTION_TEMPLATE_LIMITS.commandCharacters;
  if (input.template.trim().length === 0 || input.template.length > maximumTemplateCharacters) {
    return [
      {
        field: "template",
        message: `Enter template content no longer than ${maximumTemplateCharacters} characters.`,
      },
    ];
  }
  const message =
    input.catalog === "oauth-endpoint"
      ? endpointTemplateIssue(input.template)
      : commandTemplateIssue(input);
  return message === undefined ? [] : [{ field: "template", message }];
}

export function previewCommandTemplate(catalog: CommandTemplateCatalog, template: string): string {
  if (catalog === "truststore-password") {
    return template;
  }
  return template
    .replaceAll("{truststorePath}", "<truststore-path>")
    .replaceAll("{destDir}", "<destination-directory>")
    .replaceAll("{storepass}", "<masked-password>");
}

function firstBrokerHost(brokers: readonly string[]): string {
  const broker = brokers[0]?.trim();
  if (broker === undefined || broker.length === 0) {
    throw new TemplateExpansionError("Enter a valid first bootstrap broker before applying.");
  }
  try {
    const parsed = new URL(`tcp://${broker}`);
    if (
      parsed.hostname.length === 0 ||
      parsed.port.length === 0 ||
      Number(parsed.port) <= 0 ||
      Number(parsed.port) > 65_535 ||
      parsed.username.length > 0 ||
      parsed.password.length > 0 ||
      parsed.pathname.length > 0 ||
      parsed.search.length > 0 ||
      parsed.hash.length > 0
    ) {
      throw new Error("invalid broker");
    }
    return parsed.hostname;
  } catch {
    throw new TemplateExpansionError(
      "Enter the first bootstrap broker as host:port before applying.",
    );
  }
}

export function expandOAuthEndpointTemplate(template: string, brokers: readonly string[]): string {
  const host = firstBrokerHost(brokers);
  const expanded = replaceEndpointHost(template, host);
  if (!validEndpoint(expanded)) {
    throw new TemplateExpansionError(
      "The selected endpoint template did not produce a safe HTTP or HTTPS URL.",
    );
  }
  return expanded;
}
