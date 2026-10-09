import type {
  ClusterServiceEndpointsInput,
  ProfileCreateInput,
  ProfileSummary,
  ProfileTrustCreateValueInput,
  ProfileTrustUpdateValueInput,
  ProfileUpdateInput,
  ProtectedValueUpdateInput,
  RemoteTrustAcquisitionSummary,
} from "../contracts";
import type { ProfileBindingInput } from "../contracts/profile-binding";

import {
  createProtectedValue,
  updateProtectedValue,
  splitBrokers,
  type ProfileForm,
  type TrustValueMode,
} from "./profile-dialog-model";
import {
  buildClientIdentity,
  buildServiceEndpoint,
  createProtectedField,
  updateProtectedField,
  type ProtectedFieldForm,
} from "./profile-security-form";

function buildServices<T extends ProtectedValueUpdateInput>(
  form: ProfileForm,
  protectedValue: (field: ProtectedFieldForm) => T,
): ClusterServiceEndpointsInput<T> | undefined {
  const connect = form.connectUrl.trim();
  const schemaRegistry = form.schemaRegistryUrl.trim();
  const redpandaAdmin = form.redpandaAdminUrl.trim();
  if (connect.length === 0 && schemaRegistry.length === 0 && redpandaAdmin.length === 0)
    return undefined;
  return {
    ...(connect.length === 0
      ? {}
      : {
          connect: buildServiceEndpoint(
            connect,
            form.connectAuthentication,
            form.connectSecurity,
            protectedValue,
          ),
        }),
    ...(schemaRegistry.length === 0
      ? {}
      : {
          schemaRegistry: buildServiceEndpoint(
            schemaRegistry,
            form.schemaRegistryAuthentication,
            form.schemaRegistrySecurity,
            protectedValue,
          ),
        }),
    ...(redpandaAdmin.length === 0
      ? {}
      : {
          redpandaAdmin: buildServiceEndpoint(
            redpandaAdmin,
            form.redpandaAdminAuthentication,
            form.redpandaAdminSecurity,
            protectedValue,
          ),
        }),
  };
}

function createTrustValue(
  mode: TrustValueMode,
  value: string,
  acquisition: RemoteTrustAcquisitionSummary | null,
): ProfileTrustCreateValueInput {
  if (mode === "acquired") {
    return {
      acquisitionId: acquisition?.id ?? "",
      mode,
      ...(acquisition?.editor === undefined ? {} : { editorId: acquisition.editor.id }),
    };
  }
  return mode === "replace" ? { mode, value } : { mode: "clear" };
}

function updateTrustValue(
  mode: TrustValueMode,
  value: string,
  acquisition: RemoteTrustAcquisitionSummary | null,
): ProfileTrustUpdateValueInput {
  if (mode === "acquired") {
    return {
      acquisitionId: acquisition?.id ?? "",
      mode,
      ...(acquisition?.editor === undefined ? {} : { editorId: acquisition.editor.id }),
    };
  }
  if (mode === "replace") {
    return { mode, value };
  }
  return { mode };
}

export function buildCreateInput(
  form: ProfileForm,
  acquisition: RemoteTrustAcquisitionSummary | null,
  binding?: ProfileBindingInput,
): ProfileCreateInput {
  const base = {
    brokers: splitBrokers(form.brokers),
    name: form.name.trim(),
    ...(buildServices(form, createProtectedField) === undefined
      ? {}
      : { services: buildServices(form, createProtectedField)! }),
  };
  const withOAuth =
    form.authentication === "oauth"
      ? {
          ...base,
          oauth: {
            clientId: form.clientId.trim(),
            clientSecret: createProtectedValue(form.clientSecret),
            scope: form.scope.trim(),
            tokenEndpoint: form.tokenEndpoint.trim(),
          },
        }
      : form.authentication === "none"
        ? base
        : {
            ...base,
            sasl: {
              mechanism: form.authentication,
              username: form.saslUsername,
              password: createProtectedField(form.saslPassword),
            },
          };
  if (form.transport === "plaintext") {
    return { ...withOAuth, transport: "plaintext" };
  }
  return {
    ...withOAuth,
    ...(form.clientIdentity.enabled
      ? { clientIdentity: buildClientIdentity(form.clientIdentity, createProtectedField)! }
      : {}),
    ...(form.apiCa === undefined ? {} : { apiCa: form.apiCa }),
    ...(binding === undefined ? {} : { binding }),
    transport: "tls",
    trust: {
      kind: form.trustKind,
      label: form.trustLabel.trim(),
      material: createTrustValue(form.trustMaterialMode, form.trustMaterial, acquisition),
      password:
        form.trustKind === "pem"
          ? ({ mode: "clear" } as const)
          : createTrustValue(form.trustPasswordMode, form.trustPassword, acquisition),
    },
  };
}

export function buildUpdateInput(
  form: ProfileForm,
  profile: ProfileSummary,
  acquisition: RemoteTrustAcquisitionSummary | null,
  expectedRevision: number,
  binding?: ProfileBindingInput,
): ProfileUpdateInput {
  const base = {
    expectedRevision,
    brokers: splitBrokers(form.brokers),
    name: form.name.trim(),
    ...(buildServices(form, updateProtectedField) === undefined
      ? {}
      : { services: buildServices(form, updateProtectedField)! }),
  };
  const withOAuth =
    form.authentication === "oauth"
      ? {
          ...base,
          oauth: {
            clientId: form.clientId.trim(),
            clientSecret: updateProtectedValue(
              form.clientSecret,
              profile.oauth?.clientSecretPresent === true,
            ),
            scope: form.scope.trim(),
            tokenEndpoint: form.tokenEndpoint.trim(),
          },
        }
      : form.authentication === "none"
        ? base
        : {
            ...base,
            sasl: {
              mechanism: form.authentication,
              username: form.saslUsername,
              password: updateProtectedField(form.saslPassword),
            },
          };
  if (form.transport === "plaintext") {
    return { ...withOAuth, transport: "plaintext" };
  }
  return {
    ...withOAuth,
    ...(form.clientIdentity.enabled
      ? { clientIdentity: buildClientIdentity(form.clientIdentity, updateProtectedField)! }
      : {}),
    ...(form.apiCa === undefined ? {} : { apiCa: form.apiCa }),
    ...(binding === undefined ? {} : { binding }),
    transport: "tls",
    trust: {
      kind: form.trustKind,
      label: form.trustLabel.trim(),
      material: updateTrustValue(form.trustMaterialMode, form.trustMaterial, acquisition),
      password:
        form.trustKind === "pem"
          ? ({ mode: "clear" } as const)
          : updateTrustValue(form.trustPasswordMode, form.trustPassword, acquisition),
    },
  };
}
