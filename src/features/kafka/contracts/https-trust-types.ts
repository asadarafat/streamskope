import { REMOTE_TRUST_ACQUISITION_LIMITS } from "./remote-trust-types";

export type HttpsTrustAuthentication =
  | { readonly mode: "none" }
  | { readonly mode: "bearer"; readonly token: string }
  | { readonly mode: "basic"; readonly username: string; readonly password: string };

export const HTTPS_TRUST_LIMITS = {
  materialBytes: REMOTE_TRUST_ACQUISITION_LIMITS.materialBytes,
  jsonWireBytes: 12 * 1024 * 1024,
  passwordWireBytes: 64 * 1024,
  passwordCharacters: REMOTE_TRUST_ACQUISITION_LIMITS.passwordCharacters,
  pointerCharacters: 1024,
  pointerSegments: 32,
  entries: 32,
  nameCharacters: 128,
  valueCharacters: 4096,
} as const;

export type HttpsTrustMaterialExtraction =
  | { readonly mode: "raw" }
  | { readonly mode: "json-pem" | "json-base64"; readonly pointer: string };

export type HttpsTrustPasswordExtraction =
  { readonly mode: "text" } | { readonly mode: "json"; readonly pointer: string };

export interface HttpsTrustGetDefinition {
  readonly url: string;
  readonly headers: readonly { readonly name: string; readonly value: string }[];
  readonly query: readonly { readonly name: string; readonly value: string }[];
}

export interface TrustRecipeHttps {
  readonly authentication: "none" | "bearer" | "basic";
  readonly material: HttpsTrustGetDefinition & {
    readonly extraction: HttpsTrustMaterialExtraction;
  };
  readonly password:
    | { readonly source: "none" | "ask" }
    | {
        readonly source: "https";
        readonly request: HttpsTrustGetDefinition & {
          readonly extraction: HttpsTrustPasswordExtraction;
        };
      };
}
