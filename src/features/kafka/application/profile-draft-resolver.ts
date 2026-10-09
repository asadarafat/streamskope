import {
  PROFILE_LIMITS,
  parseProfileAcquisitionBinding,
  parseProfileBindingInput,
  type ProfileBindingInput,
  type ProfileAcquisitionBinding,
  type ProfileCreateInput,
  type ProfileUpdateInput,
  type ProtectedValueUpdateInput,
} from "../contracts";

import {
  resolveProfileSecurity,
  resolveProtectedCredential,
  resolveServiceConnections,
  securityValidationInput,
} from "./profile-security";
import { KafkaProfileValidationError } from "./profile-errors";
import { resolveCreateProfileTrust, resolveUpdateProfileTrust } from "./profile-trust-resolution";
import { assertProfileRevision, createIssues, trustLabel } from "./profile-validation";
import type {
  KafkaProfileRecord,
  KafkaResolvedProfileDraft,
  KafkaProfileServiceOptions,
  KafkaProfileTrustDecoder,
} from "./profile-types";

function acquiredDraftSignal(
  signal: AbortSignal | undefined,
  lifetime: AbortSignal | undefined,
): AbortSignal | undefined {
  return lifetime === undefined
    ? signal
    : signal === undefined
      ? lifetime
      : AbortSignal.any([signal, lifetime]);
}

// Resolves draft secrets and acquisition bindings without owning profile persistence.
export class KafkaProfileDraftResolver {
  private readonly trustAcquisitions;
  private readonly resolveRecipe;
  constructor(
    private readonly trustDecoder: KafkaProfileTrustDecoder,
    options: KafkaProfileServiceOptions,
  ) {
    this.trustAcquisitions = options.trustAcquisitions;
    this.resolveRecipe = options.resolveRecipe;
  }

  async resolveBinding(
    input: ProfileBindingInput | undefined,
    existing: ProfileAcquisitionBinding | undefined,
    signal?: AbortSignal,
  ): Promise<ProfileAcquisitionBinding | undefined> {
    if (input === undefined)
      return existing === undefined ? undefined : parseProfileAcquisitionBinding(existing);
    const parsed = parseProfileBindingInput(input);
    if (parsed.mode === "clear") return undefined;
    const access = parsed.access === undefined ? existing?.access : parsed.access;
    const apiAccess = parsed.apiAccess === undefined ? existing?.apiAccess : parsed.apiAccess;
    const withIdentity = (
      recipe: ProfileAcquisitionBinding["recipe"],
    ): ProfileAcquisitionBinding => {
      const candidate =
        parsed.identity?.mode === "acquired"
          ? this.trustAcquisitions?.resolve(
              parsed.identity.acquisitionId,
              recipe.kind,
              parsed.identity.editorId,
            )
          : undefined;
      const identity =
        parsed.identity === undefined
          ? existing?.identity
          : parsed.identity.mode === "reset"
            ? undefined
            : candidate?.identity;
      const retrievalAccess = parsed.access === undefined ? (candidate?.access ?? access) : access;
      if (parsed.identity?.mode === "acquired" && identity === undefined)
        throw new KafkaProfileValidationError([
          {
            field: "binding",
            message:
              "Acquire and apply a complete trust candidate before retaining its SSH identity.",
          },
        ]);
      if (
        parsed.identity?.mode === "acquired" &&
        existing?.identity !== undefined &&
        identity !== undefined &&
        existing.identity.host === identity.host &&
        existing.identity.port === identity.port &&
        existing.identity.fingerprint !== identity.fingerprint
      )
        throw new KafkaProfileValidationError([
          {
            field: "binding",
            message:
              "The saved SSH identity changed. Verify it independently and explicitly reset the saved identity before acquisition.",
          },
        ]);
      return parseProfileAcquisitionBinding({
        recipe,
        ...(apiAccess == null ? {} : { apiAccess }),
        overrides: parsed.overrides,
        ...(retrievalAccess == null ? {} : { access: retrievalAccess }),
        ...(identity === undefined ? {} : { identity }),
      });
    };
    if (
      existing?.recipe.id === parsed.recipeId &&
      existing.recipe.revision === parsed.recipeRevision
    ) {
      return withIdentity(existing.recipe);
    }
    if (this.resolveRecipe === undefined)
      throw new KafkaProfileValidationError([
        {
          field: "binding",
          message: "Template resolution is unavailable. Refresh the application host.",
        },
      ]);
    const recipe = await this.resolveRecipe(parsed.recipeId, parsed.recipeRevision, signal);
    signal?.throwIfAborted();
    return withIdentity(recipe);
  }

  private async resolveApiCa(
    input: ProtectedValueUpdateInput | undefined,
    existing: string | undefined,
    signal?: AbortSignal,
  ): Promise<string | undefined> {
    if (input === undefined || input.mode === "retain") return existing;
    if (input.mode === "clear") return undefined;
    if (
      input.value.length === 0 ||
      new TextEncoder().encode(input.value).byteLength > PROFILE_LIMITS.trustBinaryBytes
    )
      throw new KafkaProfileValidationError([
        { field: "apiCa", message: "API CA must be a non-empty bounded PEM certificate bundle." },
      ]);
    const decoded = await this.trustDecoder.decode({ kind: "pem", material: input.value }, signal);
    signal?.throwIfAborted();
    return decoded.caPem;
  }

  async resolveCreateDraft(
    input: ProfileCreateInput,
    signal?: AbortSignal,
  ): Promise<KafkaResolvedProfileDraft> {
    const issues = createIssues(input);
    if (issues.length > 0) {
      throw new KafkaProfileValidationError(issues);
    }
    const security = resolveProfileSecurity(input, undefined);
    const oauth =
      input.oauth === undefined || input.oauth.clientSecret.mode !== "replace"
        ? undefined
        : {
            clientId: input.oauth.clientId.trim(),
            clientSecret: resolveProtectedCredential(
              input.oauth.clientSecret,
              undefined,
              "oauth.clientSecret",
            ),
            scope: input.oauth.scope.trim(),
            tokenEndpoint: input.oauth.tokenEndpoint.trim(),
          };
    const base = {
      ...(security.sasl === undefined ? {} : { sasl: security.sasl }),
      brokers: input.brokers.map((broker) => broker.trim()),
      name: input.name.normalize("NFKC").trim(),
      ...(oauth === undefined ? {} : { oauth }),
      ...(security.services === undefined ? {} : { services: security.services }),
      ...(input.source === undefined ? {} : { source: input.source }),
    };
    if (input.transport === "plaintext") {
      const resolvedServices = await resolveServiceConnections(
        security.services,
        this.trustDecoder,
        signal,
      );
      return {
        ...base,
        ...(resolvedServices === undefined ? {} : { resolvedServices }),
        transport: "plaintext",
      };
    }
    const binding = await this.resolveBinding(input.binding, undefined, signal);
    const apiCaPem = await this.resolveApiCa(input.apiCa, undefined, signal);
    signal?.throwIfAborted();
    const trust = resolveCreateProfileTrust(input.trust, this.trustAcquisitions);
    const effectiveSignal = acquiredDraftSignal(signal, trust.lifetimeSignal);
    const material = trust.material ?? "";
    const decoded = await this.trustDecoder.decode(
      {
        kind: input.trust.kind,
        material,
        ...(trust.password === undefined ? {} : { password: trust.password }),
      },
      effectiveSignal,
    );
    effectiveSignal?.throwIfAborted();
    const resolvedServices = await resolveServiceConnections(
      security.services,
      this.trustDecoder,
      effectiveSignal,
      decoded.caPem,
    );
    return {
      ...(security.clientIdentity === undefined ? {} : { clientIdentity: security.clientIdentity }),
      ...(resolvedServices === undefined ? {} : { resolvedServices }),
      ...base,
      ...(trust.acquisitionId === undefined ? {} : { acquisitionId: trust.acquisitionId }),
      ...(apiCaPem === undefined ? {} : { apiCaPem }),
      ...(effectiveSignal === undefined ? {} : { lifetimeSignal: effectiveSignal }),
      ...(binding === undefined ? {} : { binding }),
      transport: "tls",
      trust: {
        caPem: decoded.caPem,
        kind: decoded.kind,
        label: trustLabel(input.trust.label),
        material,
        ...(trust.password === undefined ? {} : { password: trust.password }),
      },
    };
  }

  async resolveUpdateDraft(
    input: ProfileUpdateInput,
    existing: KafkaProfileRecord,
    signal?: AbortSignal,
  ): Promise<KafkaResolvedProfileDraft> {
    assertProfileRevision(existing, input.expectedRevision);
    const preliminaryIssues = createIssues(input, true);
    if (preliminaryIssues.length > 0) {
      throw new KafkaProfileValidationError(preliminaryIssues);
    }
    if (
      existing.transport === "plaintext" &&
      input.transport !== "plaintext" &&
      (input.trust.material.mode === "retain" || input.trust.password.mode === "retain")
    )
      throw new KafkaProfileValidationError([
        {
          field: input.trust.material.mode === "retain" ? "trust.material" : "trust.password",
          message: "Switching a plaintext profile to TLS requires fresh trust values.",
        },
      ]);
    const security = resolveProfileSecurity(input, existing);
    const securityInput = securityValidationInput(security);
    const clientSecret =
      input.oauth === undefined
        ? undefined
        : resolveProtectedCredential(
            input.oauth.clientSecret,
            existing.oauth?.clientSecret,
            "oauth.clientSecret",
          );
    const oauth =
      input.oauth === undefined || clientSecret === undefined
        ? undefined
        : {
            clientId: input.oauth.clientId.trim(),
            clientSecret,
            scope: input.oauth.scope.trim(),
            tokenEndpoint: input.oauth.tokenEndpoint.trim(),
          };
    const source = input.source ?? existing.source;
    const base = {
      ...(security.sasl === undefined ? {} : { sasl: security.sasl }),
      brokers: input.brokers,
      name: input.name,
      ...(security.services === undefined ? {} : { services: security.services }),
      ...(oauth === undefined ? {} : { oauth }),
      ...(source === undefined ? {} : { source }),
    };
    if (input.transport === "plaintext") {
      const validationInput: ProfileCreateInput = {
        ...(securityInput.sasl === undefined ? {} : { sasl: securityInput.sasl }),
        brokers: input.brokers,
        name: input.name,
        ...(securityInput.services === undefined ? {} : { services: securityInput.services }),
        ...(source === undefined ? {} : { source }),
        ...(input.oauth === undefined
          ? {}
          : {
              oauth: {
                clientId: input.oauth.clientId,
                clientSecret:
                  clientSecret === undefined
                    ? { mode: "clear" }
                    : { mode: "replace", value: clientSecret },
                scope: input.oauth.scope,
                tokenEndpoint: input.oauth.tokenEndpoint,
              },
            }),
        transport: "plaintext",
      };
      const issues = createIssues(validationInput);
      if (issues.length > 0) {
        throw new KafkaProfileValidationError(issues);
      }
      const resolvedServices = await resolveServiceConnections(
        security.services,
        this.trustDecoder,
        signal,
      );
      return {
        ...base,
        ...(resolvedServices === undefined ? {} : { resolvedServices }),
        brokers: input.brokers.map((broker) => broker.trim()),
        name: input.name.normalize("NFKC").trim(),
        ...(security.services === undefined ? {} : { services: security.services }),
        transport: "plaintext",
      };
    }
    const existingTrust = existing.transport === "plaintext" ? undefined : existing.trust;
    const binding = await this.resolveBinding(input.binding, existing.binding, signal);
    const apiCaPem = await this.resolveApiCa(input.apiCa, existing.apiCaPem, signal);
    const trust = resolveUpdateProfileTrust(input.trust, existingTrust, this.trustAcquisitions);
    const effectiveSignal = acquiredDraftSignal(signal, trust.lifetimeSignal);
    const material = trust.material;
    const password = trust.password;
    const validationInput: ProfileCreateInput = {
      ...(securityInput.sasl === undefined ? {} : { sasl: securityInput.sasl }),
      ...(securityInput.clientIdentity === undefined
        ? {}
        : { clientIdentity: securityInput.clientIdentity }),
      brokers: input.brokers,
      name: input.name,
      ...(securityInput.services === undefined ? {} : { services: securityInput.services }),
      ...(source === undefined ? {} : { source }),
      ...(input.oauth === undefined
        ? {}
        : {
            oauth: {
              clientId: input.oauth.clientId,
              clientSecret:
                clientSecret === undefined
                  ? { mode: "clear" }
                  : { mode: "replace", value: clientSecret },
              scope: input.oauth.scope,
              tokenEndpoint: input.oauth.tokenEndpoint,
            },
          }),
      transport: "tls",
      trust: {
        kind: input.trust.kind,
        label: input.trust.label,
        material: material === undefined ? { mode: "clear" } : { mode: "replace", value: material },
        password: password === undefined ? { mode: "clear" } : { mode: "replace", value: password },
      },
    };
    const issues = createIssues(validationInput);
    if (issues.length > 0) {
      throw new KafkaProfileValidationError(issues);
    }
    signal?.throwIfAborted();
    const decoded = await this.trustDecoder.decode(
      {
        kind: input.trust.kind,
        material: material ?? "",
        ...(password === undefined ? {} : { password }),
      },
      effectiveSignal,
    );
    effectiveSignal?.throwIfAborted();
    const resolvedServices = await resolveServiceConnections(
      security.services,
      this.trustDecoder,
      effectiveSignal,
      decoded.caPem,
    );
    return {
      ...(security.clientIdentity === undefined ? {} : { clientIdentity: security.clientIdentity }),
      ...(resolvedServices === undefined ? {} : { resolvedServices }),
      ...base,
      ...(trust.acquisitionId === undefined ? {} : { acquisitionId: trust.acquisitionId }),
      ...(apiCaPem === undefined ? {} : { apiCaPem }),
      ...(effectiveSignal === undefined ? {} : { lifetimeSignal: effectiveSignal }),
      ...(binding === undefined ? {} : { binding }),
      brokers: input.brokers.map((broker) => broker.trim()),
      name: input.name.normalize("NFKC").trim(),
      ...(security.services === undefined ? {} : { services: security.services }),
      transport: "tls",
      trust: {
        caPem: decoded.caPem,
        kind: decoded.kind,
        label: trustLabel(input.trust.label),
        material: material ?? "",
        ...(password === undefined ? {} : { password }),
      },
    };
  }
}
