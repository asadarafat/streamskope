import type {
  ProfileCreateInput,
  ProfileUpdateInput,
  ProtectedValueCreateInput,
  ProtectedValueUpdateInput,
} from "../contracts";

import { KafkaProfileValidationError } from "./profile-errors";
import type { KafkaProfileRecord } from "./profile-types";
import {
  KafkaTrustAcquisitionIncompleteError,
  KafkaTrustAcquisitionUnavailableError,
} from "./trust-acquisition-errors";
import type {
  KafkaResolvedTrustAcquisition,
  KafkaTrustAcquisitionResolver,
} from "./trust-acquisition-types";

export interface ResolvedProfileTrustValues {
  readonly lifetimeSignal?: AbortSignal;
  readonly acquisitionId?: string;
  readonly material: string | undefined;
  readonly password: string | undefined;
}

type TrustValue = ProfileCreateInput["trust"]["material"] | ProfileUpdateInput["trust"]["material"];

function acquisitionId(material: TrustValue, password: TrustValue): string | undefined {
  const materialId = material.mode === "acquired" ? material.acquisitionId : undefined;
  const passwordId = password.mode === "acquired" ? password.acquisitionId : undefined;
  if (materialId !== undefined && passwordId !== undefined && materialId !== passwordId) {
    throw new KafkaProfileValidationError([
      {
        field: "trust.password",
        message: "Acquired trust material and password must use one acquisition.",
      },
    ]);
  }
  return materialId ?? passwordId;
}

function resolveAcquisition(
  id: string | undefined,
  kind: ProfileCreateInput["trust"]["kind"],
  acquisitions: KafkaTrustAcquisitionResolver | undefined,
  editorId?: string,
): KafkaResolvedTrustAcquisition | undefined {
  if (id === undefined) {
    return undefined;
  }
  if (acquisitions === undefined) {
    throw new KafkaTrustAcquisitionUnavailableError();
  }
  return acquisitions.resolve(id, kind, editorId);
}

function acquisitionEditor(material: TrustValue, password: TrustValue): string | undefined {
  const materialEditor = material.mode === "acquired" ? material.editorId : undefined;
  const passwordEditor = password.mode === "acquired" ? password.editorId : undefined;
  if (
    material.mode === "acquired" &&
    password.mode === "acquired" &&
    materialEditor !== passwordEditor
  )
    throw new KafkaProfileValidationError([
      { field: "trust.password", message: "Acquired trust values must belong to the same editor." },
    ]);
  return materialEditor ?? passwordEditor;
}

function createValue(
  input: ProtectedValueCreateInput | { readonly acquisitionId: string; readonly mode: "acquired" },
  acquired: string | undefined,
  acquisition: KafkaResolvedTrustAcquisition | undefined,
): string | undefined {
  switch (input.mode) {
    case "clear":
      return undefined;
    case "replace":
      return input.value;
    case "acquired":
      if (acquired === undefined) {
        throw new KafkaTrustAcquisitionIncompleteError(acquisition?.id);
      }
      return acquired;
  }
}

function updateValue(
  input: ProtectedValueUpdateInput | { readonly acquisitionId: string; readonly mode: "acquired" },
  existing: string | undefined,
  acquired: string | undefined,
  acquisition: KafkaResolvedTrustAcquisition | undefined,
): string | undefined {
  return input.mode === "retain" ? existing : createValue(input, acquired, acquisition);
}

export function resolveCreateProfileTrust(
  trust: ProfileCreateInput["trust"],
  acquisitions: KafkaTrustAcquisitionResolver | undefined,
): ResolvedProfileTrustValues {
  const id = acquisitionId(trust.material, trust.password);
  const acquisition = resolveAcquisition(
    id,
    trust.kind,
    acquisitions,
    acquisitionEditor(trust.material, trust.password),
  );
  return {
    ...(id === undefined ? {} : { acquisitionId: id }),
    ...(acquisition?.lifetimeSignal === undefined
      ? {}
      : { lifetimeSignal: acquisition.lifetimeSignal }),
    material: createValue(trust.material, acquisition?.material, acquisition),
    password: createValue(trust.password, acquisition?.password, acquisition),
  };
}

export function resolveUpdateProfileTrust(
  trust: ProfileUpdateInput["trust"],
  existing: KafkaProfileRecord["trust"],
  acquisitions: KafkaTrustAcquisitionResolver | undefined,
): ResolvedProfileTrustValues {
  const id = acquisitionId(trust.material, trust.password);
  const acquisition = resolveAcquisition(
    id,
    trust.kind,
    acquisitions,
    acquisitionEditor(trust.material, trust.password),
  );
  return {
    ...(id === undefined ? {} : { acquisitionId: id }),
    ...(acquisition?.lifetimeSignal === undefined
      ? {}
      : { lifetimeSignal: acquisition.lifetimeSignal }),
    material: updateValue(trust.material, existing.material, acquisition?.material, acquisition),
    password: updateValue(trust.password, existing.password, acquisition?.password, acquisition),
  };
}
