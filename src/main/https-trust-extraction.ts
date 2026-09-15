import { HostContractValidationError, parseTrustRecipeJson } from "../kafka/contracts";
import {
  HTTPS_TRUST_LIMITS as limits,
  type HttpsTrustMaterialExtraction,
  type HttpsTrustPasswordExtraction,
} from "../kafka/contracts/https-trust-types";

function invalid(stage: "material" | "password"): never {
  throw new HostContractValidationError(
    `${stage}.extraction`,
    "response does not match the configured bounded extraction; verify its format and JSON Pointer",
  );
}

function utf8(bytes: Uint8Array, stage: "material" | "password"): string {
  try {
    return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);
  } catch {
    invalid(stage);
  }
}

function jsonString(
  bytes: Uint8Array,
  pointer: string,
  maximumBytes: number,
  stage: "material" | "password",
): string {
  if (
    pointer.length > limits.pointerCharacters ||
    (pointer !== "" && !pointer.startsWith("/")) ||
    /~(?![01])/u.test(pointer)
  )
    invalid(stage);
  const segments = pointer === "" ? [] : pointer.slice(1).split("/");
  if (segments.length > limits.pointerSegments) invalid(stage);
  let value: unknown;
  try {
    value = parseTrustRecipeJson(utf8(bytes, stage), maximumBytes);
  } catch {
    invalid(stage);
  }
  for (const segment of segments) {
    const key = segment.replaceAll("~1", "/").replaceAll("~0", "~");
    if (typeof value !== "object" || value === null || !Object.hasOwn(value, key)) invalid(stage);
    if (Array.isArray(value)) {
      if (!/^(0|[1-9][0-9]*)$/u.test(key)) invalid(stage);
      value = value[Number(key)] as unknown;
    } else value = (value as Record<string, unknown>)[key];
  }
  if (typeof value !== "string" || /[\uD800-\uDFFF]/u.test(value)) invalid(stage);
  return value;
}

export function extractHttpsTrustMaterial(
  bytes: Uint8Array,
  extraction: HttpsTrustMaterialExtraction,
): Uint8Array {
  const maximum = extraction.mode === "raw" ? limits.materialBytes : limits.jsonWireBytes;
  if (bytes.byteLength === 0 || bytes.byteLength > maximum) invalid("material");
  if (extraction.mode === "raw") return bytes;
  const value = jsonString(bytes, extraction.pointer, maximum, "material");
  if (extraction.mode === "json-pem") {
    if (Buffer.byteLength(value, "utf8") > limits.materialBytes || value.length === 0)
      invalid("material");
    return Buffer.from(value, "utf8");
  }
  if (value.length === 0 || value.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/u.test(value))
    invalid("material");
  const padding = value.endsWith("==") ? 2 : value.endsWith("=") ? 1 : 0;
  if ((value.length / 4) * 3 - padding > limits.materialBytes) invalid("material");
  const result = Buffer.from(value, "base64");
  if (result.toString("base64") !== value) invalid("material");
  return result;
}

export function extractHttpsTrustPassword(
  bytes: Uint8Array,
  extraction: HttpsTrustPasswordExtraction,
): string {
  if (bytes.byteLength > limits.passwordWireBytes) invalid("password");
  const value = (
    extraction.mode === "text"
      ? utf8(bytes, "password")
      : jsonString(bytes, extraction.pointer, limits.passwordWireBytes, "password")
  ).replace(/^[\r\n]+|[\r\n]+$/gu, "");
  if (value.length === 0 || value.length > limits.passwordCharacters || value.includes("\u0000"))
    invalid("password");
  return value;
}
