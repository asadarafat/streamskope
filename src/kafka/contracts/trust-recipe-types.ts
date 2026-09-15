import {
  CONNECTION_TEMPLATE_LIMITS,
  type ConnectionTemplateStoreCapability,
  type ConnectionTemplateCatalogSnapshot,
} from "./connection-template-types";
import type { ProfileTrustKind } from "./profile-types";
import type { HostTextDocument } from "./text-document-types";
import type { TrustRecipeHttps } from "./https-trust-types";

export const TRUST_RECIPE_LIMITS = {
  entries: CONNECTION_TEMPLATE_LIMITS.entriesPerCatalog,
  nameCharacters: CONNECTION_TEMPLATE_LIMITS.nameCharacters,
  commandCharacters: CONNECTION_TEMPLATE_LIMITS.commandCharacters,
  urlCharacters: CONNECTION_TEMPLATE_LIMITS.endpointCharacters,
  documentBytes: 4 * 1_048_576,
  exchangeBytes: 256 * 1_024,
  jsonDepth: 16,
  parameters: 32,
  parameterKeyCharacters: 64,
  parameterLabelCharacters: 128,
  parameterHelpCharacters: 512,
  parameterValueCharacters: 4_096,
  choices: 32,
  choiceCharacters: 128,
  maximumTimeoutSeconds: 120,
  defaultTimeoutSeconds: 30,
  idCharacters: 128,
} as const;

export const TRUST_RECIPE_PARAMETER_TYPES = [
  "text",
  "host",
  "path",
  "number",
  "choice",
  "secret",
] as const;
export type TrustRecipeParameterType = (typeof TRUST_RECIPE_PARAMETER_TYPES)[number];

export interface TrustRecipeParameter {
  readonly key: string;
  readonly label: string;
  readonly type: TrustRecipeParameterType;
  readonly required: boolean;
  readonly help?: string;
  readonly defaultValue?: string;
  readonly choices?: readonly string[];
}

export type TrustRecipePassword =
  { readonly source: "none" | "ask" } | { readonly source: "command"; readonly command: string };

export interface TrustRecipeSsh {
  readonly source: "file" | "stdout" | "legacy-tempfile";
  readonly value: string;
  readonly password: TrustRecipePassword;
}

export interface TrustRecipeOAuth {
  readonly endpoint: string;
  readonly clientId: string;
  readonly scope: string;
}

interface TrustAcquisitionRecipeCommon {
  readonly name: string;
  readonly kind: ProfileTrustKind;
  readonly syntax: "named-v1" | "legacy-v1";
  readonly parameters: readonly TrustRecipeParameter[];
  readonly timeoutSeconds: number;
  readonly oauth?: TrustRecipeOAuth;
}

export type TrustAcquisitionRecipeInput = TrustAcquisitionRecipeCommon &
  (
    | { readonly method: "ssh"; readonly ssh: TrustRecipeSsh; readonly https?: TrustRecipeHttps }
    | { readonly method: "https"; readonly https: TrustRecipeHttps; readonly ssh?: TrustRecipeSsh }
  );

export type TrustAcquisitionRecipe = TrustAcquisitionRecipeInput & {
  readonly id: string;
  readonly revision: number;
  readonly legacySourceId?: string;
};

export interface TrustAcquisitionRecipeDocument {
  readonly version: 1;
  readonly recipes: readonly TrustAcquisitionRecipe[];
}

export interface TrustAcquisitionRecipeSnapshot {
  readonly recipes: readonly TrustAcquisitionRecipe[];
  readonly store: ConnectionTemplateStoreCapability;
}

export type TrustRecipeReviewResult =
  | {
      readonly correlationId: string;
      readonly usage: readonly {
        readonly id: string;
        readonly name: string;
        readonly revision: number;
      }[];
    }
  | { readonly correlationId: string; readonly legacy: LegacyTrustRecipeSource | null }
  | { readonly correlationId: string; readonly draft: TrustAcquisitionRecipeInput }
  | {
      readonly correlationId: string;
      readonly document: HostTextDocument;
      readonly warning: string;
    };

export interface LegacyTrustRecipeSource {
  readonly catalogs: readonly ConnectionTemplateCatalogSnapshot[];
  readonly sourceRevision: string;
}

export interface LegacyTrustRecipeSelection {
  readonly name: string;
  readonly kind: ProfileTrustKind;
  readonly materialName: string;
  readonly passwordName: string | null;
  readonly oauthName: string | null;
}
