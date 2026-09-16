import {
  PROFILE_LIMITS,
  SECURE_CONNECTION_LIMITS,
  type ClusterServiceAuthenticationMode,
  type ProfileSummary,
  type RemoteTrustAcquisitionSummary,
  type ProfileTrustKind,
  type ProtectedValueCreateInput,
  type ProtectedValueUpdateInput,
} from "../contracts";

export type TrustValueMode = "acquired" | "clear" | "replace" | "retain";

export interface ProfileForm {
  readonly apiCa?: ProtectedValueCreateInput;
  readonly brokers: string;
  readonly clientId: string;
  readonly clientSecret: string;
  readonly name: string;
  readonly oauthEnabled: boolean;
  readonly scope: string;
  readonly redpandaAdminAuthentication: ClusterServiceAuthenticationMode;
  readonly redpandaAdminUrl: string;
  readonly schemaRegistryAuthentication: ClusterServiceAuthenticationMode;
  readonly schemaRegistryUrl: string;
  readonly tokenEndpoint: string;
  readonly trustKind: ProfileTrustKind;
  readonly trustLabel: string;
  readonly trustMaterial: string;
  readonly trustMaterialMode: TrustValueMode;
  readonly trustPassword: string;
  readonly trustPasswordMode: TrustValueMode;
  readonly trustSource: "local" | "remote";
}

export interface ProfileFormIssues {
  readonly brokers?: string;
  readonly clientId?: string;
  readonly clientSecret?: string;
  readonly name?: string;
  readonly redpandaAdminUrl?: string;
  readonly schemaRegistryUrl?: string;
  readonly scope?: string;
  readonly tokenEndpoint?: string;
  readonly trustMaterial?: string;
  readonly trustPassword?: string;
}

export function validateProfileForm(
  form: ProfileForm,
  acquisition: RemoteTrustAcquisitionSummary | null,
  profile?: ProfileSummary,
): ProfileFormIssues {
  const issues: {
    brokers?: string;
    clientId?: string;
    clientSecret?: string;
    name?: string;
    redpandaAdminUrl?: string;
    schemaRegistryUrl?: string;
    scope?: string;
    tokenEndpoint?: string;
    trustMaterial?: string;
    trustPassword?: string;
  } = {};
  if (form.name.trim().length === 0) {
    issues.name = "Profile name is required.";
  }
  if (splitBrokers(form.brokers).length === 0) {
    issues.brokers = "Enter at least one bootstrap broker.";
  }
  if (form.trustMaterialMode === "clear") {
    issues.trustMaterial = "Select certificate or truststore material.";
  }
  if (
    form.trustMaterialMode === "acquired" &&
    (acquisition?.material === null ||
      acquisition?.material === undefined ||
      acquisition.material.kind !== form.trustKind)
  ) {
    issues.trustMaterial = `Acquire ${form.trustKind.toUpperCase()} trust material before saving.`;
  }
  if (
    form.trustKind !== "pem" &&
    (form.trustPasswordMode === "clear" ||
      (form.trustPasswordMode === "retain" && profile?.trust.passwordPresent !== true) ||
      (form.trustPasswordMode === "acquired" && acquisition?.password.present !== true))
  ) {
    issues.trustPassword = "Truststore password is required.";
  }
  if (form.oauthEnabled) {
    if (form.tokenEndpoint.trim().length === 0) {
      issues.tokenEndpoint = "OAuth token endpoint is required.";
    }
    if (form.clientId.trim().length === 0) {
      issues.clientId = "OAuth client ID is required.";
    }
    if (form.scope.trim().length === 0) {
      issues.scope = "OAuth scope is required.";
    }
    if (form.clientSecret.length === 0 && profile?.oauth?.clientSecretPresent !== true) {
      issues.clientSecret = "OAuth client secret is required.";
    }
  }
  for (const [field, label, value, authentication] of [
    [
      "schemaRegistryUrl",
      "Schema Registry",
      form.schemaRegistryUrl,
      form.schemaRegistryAuthentication,
    ],
    ["redpandaAdminUrl", "Redpanda Admin", form.redpandaAdminUrl, form.redpandaAdminAuthentication],
  ] as const) {
    if (value.trim().length === 0) continue;
    try {
      const endpoint = new URL(value.trim());
      if (endpoint.protocol !== "http:" && endpoint.protocol !== "https:") throw new Error();
      if (
        endpoint.username.length > 0 ||
        endpoint.password.length > 0 ||
        endpoint.search.length > 0 ||
        endpoint.hash.length > 0
      )
        throw new Error();
    } catch {
      issues[field] = `${label} must be an HTTP(S) URL without credentials, query, or fragment.`;
    }
    if (authentication === "oauth" && !form.oauthEnabled) {
      issues[field] = `${label} OAuth requires the profile OAuth configuration.`;
    }
  }
  return issues;
}

export function derivedTrustLabel(kind: ProfileTrustKind): string {
  if (kind === "pem") return "ca.pem";
  return kind === "jks" ? "truststore.jks" : "truststore.p12";
}

export function initialProfileForm(profile?: ProfileSummary): ProfileForm {
  if (profile === undefined) {
    return {
      brokers: "",
      clientId: "",
      clientSecret: "",
      name: "",
      oauthEnabled: false,
      scope: "",
      redpandaAdminAuthentication: "none",
      redpandaAdminUrl: "",
      schemaRegistryAuthentication: "none",
      schemaRegistryUrl: "",
      tokenEndpoint: "",
      trustKind: "pem",
      trustLabel: derivedTrustLabel("pem"),
      trustMaterial: "",
      trustMaterialMode: "clear",
      trustPassword: "",
      trustPasswordMode: "clear",
      trustSource: "local",
    };
  }
  return {
    brokers: profile.brokers.join(", "),
    clientId: profile.oauth?.clientId ?? "",
    clientSecret: "",
    name: profile.name,
    oauthEnabled: profile.oauth !== undefined,
    scope: profile.oauth?.scope ?? "",
    redpandaAdminAuthentication: profile.services?.redpandaAdmin?.authentication ?? "none",
    redpandaAdminUrl: profile.services?.redpandaAdmin?.baseUrl ?? "",
    schemaRegistryAuthentication: profile.services?.schemaRegistry?.authentication ?? "none",
    schemaRegistryUrl: profile.services?.schemaRegistry?.baseUrl ?? "",
    tokenEndpoint: profile.oauth?.tokenEndpoint ?? "",
    trustKind: profile.trust.kind,
    trustLabel: profile.trust.label,
    trustMaterial: "",
    trustMaterialMode: "retain",
    trustPassword: "",
    trustPasswordMode: profile.trust.passwordPresent ? "retain" : "clear",
    trustSource: "local",
  };
}

export function splitBrokers(value: string): readonly string[] {
  return value
    .split(/[,\n]/)
    .map((broker) => broker.trim())
    .filter((broker) => broker.length > 0);
}

export function readTrustFile(
  file: File,
  kind: ProfileTrustKind,
  signal?: AbortSignal,
): Promise<string> {
  if (signal?.aborted)
    return Promise.reject(new DOMException("The file read was cancelled.", "AbortError"));
  const maximumBytes =
    kind === "pem" ? SECURE_CONNECTION_LIMITS.caPemCharacters : PROFILE_LIMITS.trustBinaryBytes;
  if (file.size > maximumBytes) {
    return Promise.reject(
      new Error(`The selected trust material exceeds ${maximumBytes.toLocaleString()} bytes.`),
    );
  }
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    const abort = (): void => reader.abort();
    signal?.addEventListener("abort", abort, { once: true });
    reader.addEventListener("loadend", () => signal?.removeEventListener("abort", abort), {
      once: true,
    });
    reader.addEventListener(
      "abort",
      () => reject(new DOMException("The file read was cancelled.", "AbortError")),
      { once: true },
    );
    reader.addEventListener("error", () => {
      reject(new Error("The selected trust material could not be read."));
    });
    reader.addEventListener("load", () => {
      if (kind === "pem") {
        if (typeof reader.result !== "string") {
          reject(new Error("The selected PEM trust material is not text."));
          return;
        }
        resolve(reader.result);
        return;
      }
      if (!(reader.result instanceof ArrayBuffer)) {
        reject(new Error("The selected truststore is not binary data."));
        return;
      }
      const bytes = new Uint8Array(reader.result);
      const chunks: string[] = [];
      const chunkSize = 32_768;
      for (let index = 0; index < bytes.length; index += chunkSize) {
        chunks.push(String.fromCharCode(...bytes.subarray(index, index + chunkSize)));
      }
      resolve(globalThis.btoa(chunks.join("")));
    });
    if (kind === "pem") reader.readAsText(file);
    else reader.readAsArrayBuffer(file);
  });
}

export function createProtectedValue(value: string): ProtectedValueCreateInput {
  return value.length === 0 ? { mode: "clear" } : { mode: "replace", value };
}

export function updateProtectedValue(value: string, retained: boolean): ProtectedValueUpdateInput {
  if (value.length > 0) return { mode: "replace", value };
  return retained ? { mode: "retain" } : { mode: "clear" };
}
