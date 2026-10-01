import type { TrustRecipeHttps, HttpsTrustAuthentication } from "../contracts/https-trust-types";
export type { HttpsTrustAuthentication } from "../contracts/https-trust-types";
import type { TrustRecipeParameter } from "../contracts/trust-recipe-types";
import type { HostErrorCode } from "../contracts";

import type { KafkaProfileStructuredError } from "./profile-types";

export interface HttpsTrustAcquisitionRequest {
  readonly definition: TrustRecipeHttps;
  readonly parameters: readonly TrustRecipeParameter[];
  readonly values: ReadonlyMap<string, string>;
  readonly authentication: HttpsTrustAuthentication;
  readonly tls: HttpsTrustRequest["tls"];
  readonly password?: string;
  readonly signal: AbortSignal;
}

export interface HttpsTrustAcquisitionPort {
  fetch(
    input: HttpsTrustAcquisitionRequest,
  ): Promise<{ readonly bytes: Uint8Array; readonly password?: string }>;
}

export interface HttpsTrustRequest {
  readonly url: string;
  readonly authentication: HttpsTrustAuthentication;
  readonly tls: { readonly mode: "system" } | { readonly mode: "custom"; readonly caPem: string };
  readonly headers: readonly { readonly name: string; readonly value: string }[];
  readonly maximumBytes: number;
  readonly signal: AbortSignal;
}

export interface HttpsTrustPort {
  get(input: HttpsTrustRequest): Promise<Uint8Array>;
}

export type HttpsTrustFailureCategory =
  | "configuration"
  | "tls"
  | "authentication"
  | "authorization"
  | "redirect"
  | "status"
  | "encoding"
  | "extraction"
  | "bounds"
  | "network"
  | "cancelled"
  | "timeout";

export class HttpsTrustTransportError extends Error implements KafkaProfileStructuredError {
  readonly stage = "acquisition" as const;
  readonly code: HostErrorCode;
  readonly recovery: string;
  readonly retryable: boolean;
  readonly target: string;
  constructor(
    readonly category: HttpsTrustFailureCategory,
    readonly origin: string,
    readonly step: "material" | "password" = "material",
  ) {
    super(`HTTPS ${step} retrieval failed (${category}). Existing trust is unchanged.`);
    this.name = "HttpsTrustTransportError";
    this.target = origin;
    this.retryable = ["network", "timeout", "cancelled"].includes(category);
    switch (category) {
      case "authentication":
        this.code = "HTTPS_AUTHENTICATION";
        this.recovery =
          "Re-enter the API credential and verify the template authentication mode. Kafka OAuth credentials are not reused.";
        break;
      case "authorization":
        this.code = "HTTPS_AUTHORIZATION";
        this.recovery =
          "Ask the API owner to grant read access to the configured certificate endpoint.";
        break;
      case "tls":
        this.code = "TLS_TRUST";
        this.recovery =
          "Verify the API hostname, certificate dates and independently supplied API CA. Kafka trust is not reused; insecure retries are not permitted.";
        break;
      case "redirect":
        this.code = "HTTPS_REDIRECT";
        this.recovery =
          "Configure the verified final HTTPS endpoint explicitly; credentials are never forwarded to redirects.";
        break;
      case "cancelled":
        this.code = "CANCELLED";
        this.recovery = "Acquire again when ready. Existing trust is unchanged.";
        break;
      case "timeout":
        this.code = "TIMEOUT";
        this.recovery = "Check API availability and the template execution budget, then retry.";
        break;
      case "configuration":
        this.code = "VALIDATION";
        this.recovery =
          "Check the API access fields, declared parameters and same-origin password request before retrying.";
        break;
      case "extraction":
        this.code = "HTTPS_RESPONSE";
        this.recovery = `Check the ${step === "password" ? "Password response and Password JSON Pointer" : "Material response and Material JSON Pointer"} fields and declared encoding. Password strings must be non-empty and bounded. No response content is included in diagnostics.`;
        break;
      default:
        this.code = "HTTPS_RESPONSE";
        this.recovery =
          "Check API availability, a 200 identity-encoded response, extraction format and transfer limits. Response bodies are omitted from diagnostics.";
    }
  }
}
