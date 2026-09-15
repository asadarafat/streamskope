import type {
  HostSecureConnectionInput,
  ProfileTrustKind,
  SecureConnectionInput,
} from "../contracts";
import type { KafkaTrustAcquisitionResolver } from "../application";

import type { StructuredOperationFailure } from "./facade-support";

export interface ResolvedHostConnection {
  readonly lifetimeSignal?: AbortSignal;
  readonly acquisitionId?: string;
  readonly connection: SecureConnectionInput;
}

class TrustAcquisitionOwnerUnavailableError extends Error implements StructuredOperationFailure {
  readonly code = "BACKEND_UNAVAILABLE" as const;
  readonly recovery = "Restart StreamSkope to restore the remote trust acquisition service.";
  readonly retryable = true;
  readonly stage = "backend" as const;
  readonly target = "remote-trust-acquisition";

  constructor() {
    super("The StreamSkope host cannot resolve the selected remote trust acquisition.");
    this.name = "TrustAcquisitionOwnerUnavailableError";
  }
}

function canonicalConnection(
  input: HostSecureConnectionInput,
  caPem: string,
): SecureConnectionInput {
  const base = {
    brokers: input.brokers,
    name: input.name,
    tls: {
      caPem,
      enabled: true as const,
    },
  };
  return input.oauth === undefined ? base : { ...base, oauth: input.oauth };
}

export function acquiredTrustKind(input: HostSecureConnectionInput): ProfileTrustKind | undefined {
  return "acquisitionId" in input.tls ? input.tls.kind : undefined;
}

export function resolveHostConnection(
  input: HostSecureConnectionInput,
  acquisitions: KafkaTrustAcquisitionResolver | undefined,
): ResolvedHostConnection {
  if ("caPem" in input.tls) {
    return {
      connection: canonicalConnection(input, input.tls.caPem),
    };
  }
  if (acquisitions === undefined) {
    throw new TrustAcquisitionOwnerUnavailableError();
  }
  const acquisition = acquisitions.resolve(
    input.tls.acquisitionId,
    input.tls.kind,
    input.tls.editorId,
  );
  return {
    acquisitionId: acquisition.id,
    ...(acquisition.lifetimeSignal === undefined
      ? {}
      : { lifetimeSignal: acquisition.lifetimeSignal }),
    connection: canonicalConnection(input, acquisition.caPem),
  };
}
