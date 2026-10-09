import type {
  ClusterServiceEndpointInput,
  ClusterServiceEndpointsSummary,
  ProfileClientIdentityInput,
  ProfileSummaryClientIdentity,
  ProfileTrustKind,
  ProtectedValueCreateInput,
  ProtectedValueUpdateInput,
} from "../contracts";

export interface ProtectedFieldForm {
  readonly value: string;
  readonly retain: boolean;
}

export interface ClientIdentityForm {
  readonly enabled: boolean;
  readonly certificatePem: ProtectedFieldForm;
  readonly privateKeyPem: ProtectedFieldForm;
  readonly passphrase: ProtectedFieldForm;
}

export interface ServiceSecurityForm {
  readonly username: string;
  readonly password: ProtectedFieldForm;
  readonly bearer: ProtectedFieldForm;
  readonly clientId: string;
  readonly clientSecret: ProtectedFieldForm;
  readonly tokenEndpoint: string;
  readonly scope: string;
  readonly trustMode: "broker" | "system" | "custom";
  readonly trustKind: ProfileTrustKind;
  readonly trustLabel: string;
  readonly trustMaterial: ProtectedFieldForm;
  readonly trustPassword: ProtectedFieldForm;
  readonly clientIdentity: ClientIdentityForm;
}

type ServiceSummary = NonNullable<ClusterServiceEndpointsSummary["connect"]>;

export function initialProtectedField(retain = false): ProtectedFieldForm {
  return { value: "", retain };
}

export function initialClientIdentity(summary?: ProfileSummaryClientIdentity): ClientIdentityForm {
  return {
    enabled: summary !== undefined,
    certificatePem: initialProtectedField(summary?.certificatePresent),
    privateKeyPem: initialProtectedField(summary?.privateKeyPresent),
    passphrase: initialProtectedField(summary?.passphrasePresent),
  };
}

export function initialServiceSecurity(summary?: ServiceSummary): ServiceSecurityForm {
  const customTrust = summary?.trust?.mode === "custom" ? summary.trust : undefined;
  return {
    username: summary?.basic?.username ?? "",
    password: initialProtectedField(summary?.basic?.passwordPresent),
    bearer: initialProtectedField(summary?.bearerPresent),
    clientId: summary?.oauth?.clientId ?? "",
    clientSecret: initialProtectedField(summary?.oauth?.clientSecretPresent),
    tokenEndpoint: summary?.oauth?.tokenEndpoint ?? "",
    scope: summary?.oauth?.scope ?? "",
    // An omitted legacy trust configuration reused the broker CA.
    trustMode: summary === undefined ? "system" : (summary.trust?.mode ?? "broker"),
    trustKind: customTrust?.kind ?? "pem",
    trustLabel: customTrust?.label ?? "ca.pem",
    trustMaterial: initialProtectedField(customTrust?.materialPresent),
    trustPassword: initialProtectedField(customTrust?.passwordPresent),
    clientIdentity: initialClientIdentity(summary?.clientIdentity),
  };
}

export function createProtectedField(field: ProtectedFieldForm): ProtectedValueCreateInput {
  return field.value.length > 0 ? { mode: "replace", value: field.value } : { mode: "clear" };
}

export function updateProtectedField(field: ProtectedFieldForm): ProtectedValueUpdateInput {
  return field.value.length > 0
    ? { mode: "replace", value: field.value }
    : field.retain
      ? { mode: "retain" }
      : { mode: "clear" };
}

export function buildClientIdentity<T extends ProtectedValueUpdateInput>(
  form: ClientIdentityForm,
  protectedValue: (field: ProtectedFieldForm) => T,
): ProfileClientIdentityInput<T> | undefined {
  return form.enabled
    ? {
        certificatePem: protectedValue(form.certificatePem),
        privateKeyPem: protectedValue(form.privateKeyPem),
        passphrase: protectedValue(form.passphrase),
      }
    : undefined;
}

export function buildServiceEndpoint<T extends ProtectedValueUpdateInput>(
  baseUrl: string,
  authentication: ClusterServiceEndpointInput["authentication"],
  form: ServiceSecurityForm,
  protectedValue: (field: ProtectedFieldForm) => T,
): ClusterServiceEndpointInput<T> {
  const https = baseUrl.startsWith("https:");
  const identity = https ? buildClientIdentity(form.clientIdentity, protectedValue) : undefined;
  return {
    baseUrl,
    authentication,
    ...(authentication === "basic"
      ? { basic: { username: form.username, password: protectedValue(form.password) } }
      : {}),
    ...(authentication === "bearer" ? { bearer: protectedValue(form.bearer) } : {}),
    ...(authentication === "oauth-client"
      ? {
          oauth: {
            clientId: form.clientId.trim(),
            clientSecret: protectedValue(form.clientSecret),
            tokenEndpoint: form.tokenEndpoint.trim(),
            scope: form.scope.trim(),
          },
        }
      : {}),
    ...(https
      ? {
          trust:
            form.trustMode === "custom"
              ? {
                  mode: "custom" as const,
                  kind: form.trustKind,
                  label: form.trustLabel,
                  material: protectedValue(form.trustMaterial),
                  password: protectedValue(
                    form.trustKind === "pem" ? initialProtectedField() : form.trustPassword,
                  ),
                }
              : { mode: form.trustMode },
        }
      : {}),
    ...(identity === undefined ? {} : { clientIdentity: identity }),
  };
}

function hasProtectedValue(field: ProtectedFieldForm): boolean {
  return field.value.length > 0 || field.retain;
}

export function validateClientIdentity(form: ClientIdentityForm): string | undefined {
  if (!form.enabled) return undefined;
  if (!hasProtectedValue(form.certificatePem)) return "Client certificate is required.";
  if (!hasProtectedValue(form.privateKeyPem)) return "Client private key is required.";
  return undefined;
}

export function validateServiceSecurity(
  baseUrl: string,
  authentication: ClusterServiceEndpointInput["authentication"],
  form: ServiceSecurityForm,
): string | undefined {
  if (authentication === "basic") {
    if (form.username.length === 0) return "HTTP Basic username is required.";
    if (!hasProtectedValue(form.password)) return "HTTP Basic password is required.";
  }
  if (authentication === "bearer" && !hasProtectedValue(form.bearer)) {
    return "Bearer token is required.";
  }
  if (authentication === "oauth-client") {
    if (form.tokenEndpoint.trim().length === 0) return "Service OAuth token endpoint is required.";
    if (form.clientId.trim().length === 0) return "Service OAuth client ID is required.";
    if (!hasProtectedValue(form.clientSecret)) return "Service OAuth client secret is required.";
  }
  if (!baseUrl.trim().startsWith("https:")) return undefined;
  if (form.trustMode === "custom") {
    if (!hasProtectedValue(form.trustMaterial)) return "Service CA or truststore is required.";
    if (form.trustKind !== "pem" && !hasProtectedValue(form.trustPassword)) {
      return "Service truststore password is required.";
    }
  }
  return validateClientIdentity(form.clientIdentity);
}
