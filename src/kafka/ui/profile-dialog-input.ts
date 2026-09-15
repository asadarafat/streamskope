import type {
  ClusterServiceEndpointsInput,
  ProfileCreateInput,
  ProfileSummary,
  ProfileTrustCreateValueInput,
  ProfileTrustUpdateValueInput,
  ProfileUpdateInput,
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

function buildServices(form: ProfileForm): ClusterServiceEndpointsInput | undefined {
  const schemaRegistry = form.schemaRegistryUrl.trim();
  const redpandaAdmin = form.redpandaAdminUrl.trim();
  if (schemaRegistry.length === 0 && redpandaAdmin.length === 0) return undefined;
  return {
    ...(schemaRegistry.length === 0
      ? {}
      : {
          schemaRegistry: {
            authentication: form.schemaRegistryAuthentication,
            baseUrl: schemaRegistry,
          },
        }),
    ...(redpandaAdmin.length === 0
      ? {}
      : {
          redpandaAdmin: {
            authentication: form.redpandaAdminAuthentication,
            baseUrl: redpandaAdmin,
          },
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
    ...(form.apiCa === undefined ? {} : { apiCa: form.apiCa }),
    ...(binding === undefined ? {} : { binding }),
    brokers: splitBrokers(form.brokers),
    name: form.name.trim(),
    ...(buildServices(form) === undefined ? {} : { services: buildServices(form)! }),
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
  return form.oauthEnabled
    ? {
        ...base,
        oauth: {
          clientId: form.clientId.trim(),
          clientSecret: createProtectedValue(form.clientSecret),
          scope: form.scope.trim(),
          tokenEndpoint: form.tokenEndpoint.trim(),
        },
      }
    : base;
}

export function buildUpdateInput(
  form: ProfileForm,
  profile: ProfileSummary,
  acquisition: RemoteTrustAcquisitionSummary | null,
  expectedRevision: number,
  binding?: ProfileBindingInput,
): ProfileUpdateInput {
  const base = {
    ...(form.apiCa === undefined ? {} : { apiCa: form.apiCa }),
    ...(binding === undefined ? {} : { binding }),
    expectedRevision,
    brokers: splitBrokers(form.brokers),
    name: form.name.trim(),
    ...(buildServices(form) === undefined ? {} : { services: buildServices(form)! }),
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
  return form.oauthEnabled
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
    : base;
}
