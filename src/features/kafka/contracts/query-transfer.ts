import { parseKafkaInvestigationQuery, type KafkaInvestigationQuery } from "./investigation-query";
import { HostContractValidationError } from "./validation-error";

export const KAFKA_QUERY_TRANSFER_LIMITS = {
  documentBytes: 32_768,
  linkCharacters: 49_152,
} as const;
const fragmentPrefix = "#query=";

/** Portable configuration only. Profile references, endpoints and message data are not fields. */
export function serializeKafkaQuery(query: KafkaInvestigationQuery): string {
  const text = JSON.stringify(parseKafkaInvestigationQuery(query), null, 2) + "\n";
  checkDocumentSize(text);
  return text;
}

function checkDocumentSize(text: string): void {
  if (
    text.length > KAFKA_QUERY_TRANSFER_LIMITS.documentBytes ||
    new TextEncoder().encode(text).length > KAFKA_QUERY_TRANSFER_LIMITS.documentBytes
  )
    throw new HostContractValidationError("query", "query documents are limited to 32 KiB");
}

export function createKafkaQueryLink(
  query: KafkaInvestigationQuery,
  applicationUrl = "streamskope://app/",
): string {
  const url = new URL(applicationUrl);
  if (!allowedApplicationUrl(url))
    throw new HostContractValidationError("query", "unsupported application URL");
  url.search = "";
  url.hash = "";
  const bytes = new TextEncoder().encode(serializeKafkaQuery(query));
  const encoded = btoa(String.fromCharCode(...bytes))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replace(/=+$/u, "");
  url.hash = fragmentPrefix + encoded;
  const link = url.toString();
  if (link.length > KAFKA_QUERY_TRANSFER_LIMITS.linkCharacters)
    throw new HostContractValidationError("query", "query link exceeds its size limit");
  return link;
}

function allowedApplicationUrl(url: URL): boolean {
  return (
    url.username === "" &&
    url.password === "" &&
    (["https:", "http:"].includes(url.protocol) ||
      (url.protocol === "streamskope:" && url.host === "app" && url.pathname === "/"))
  );
}

/** Never navigates to or fetches an imported URL. Decode its fragment as untrusted input. */
export function parseKafkaQueryTransfer(input: string): KafkaInvestigationQuery {
  try {
    if (input.length > KAFKA_QUERY_TRANSFER_LIMITS.linkCharacters) throw new Error("size");
    let document = input.trim();
    if (!document.startsWith("{")) {
      const url = new URL(
        document.startsWith(fragmentPrefix) ? `streamskope://app/${document}` : document,
      );
      if (!allowedApplicationUrl(url) || url.search !== "" || !url.hash.startsWith(fragmentPrefix))
        throw new Error("URL");
      const encoded = url.hash.slice(fragmentPrefix.length);
      if (!/^[A-Za-z0-9_-]+$/u.test(encoded)) throw new Error("encoding");
      const bytes = Uint8Array.from(
        atob(encoded.replaceAll("-", "+").replaceAll("_", "/")),
        (character) => character.charCodeAt(0),
      );
      if (bytes.length > KAFKA_QUERY_TRANSFER_LIMITS.documentBytes) throw new Error("size");
      document = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    }
    checkDocumentSize(document);
    return parseKafkaInvestigationQuery(JSON.parse(document) as unknown);
  } catch {
    // Do not echo an imported document or URL: it might contain credentials or payloads.
    throw new HostContractValidationError(
      "query",
      "invalid or unsupported query. Use a version 1 query JSON file (maximum 32 KiB) or a StreamSkope query link without credentials or query parameters",
    );
  }
}
