import { vi } from "vitest";

import { InMemoryKafkaTrustRecipeStore } from "../../src/features/kafka/application/trust-recipe-store";
import type {
  ProfileTrustKind,
  RemoteSshTargetInput,
  RemoteTrustMaterialFetchInput,
} from "../../src/features/kafka/contracts";
import {
  KafkaTrustAcquisitionService,
  type KafkaProfileTrustDecoder,
  type KafkaRemoteHostKeyRequest,
  type KafkaRemoteMaterialRequest,
  type KafkaRemotePasswordRequest,
  type KafkaRemoteTrustPort,
} from "../../src/features/kafka/application";

import { createRecipeLibrary } from "./recipe-library";

export const target = {
  host: "kafka-lab.example.test",
  hostKeyFingerprint: `SHA256:${"A".repeat(43)}`,
  password: "ssh-password",
  port: 22,
  username: "operator",
} as const satisfies RemoteSshTargetInput;

export function materialRecipe(
  kind: ProfileTrustKind,
): NonNullable<RemoteTrustMaterialFetchInput["recipe"]> {
  return { mode: "replace", recipeId: `fixture-${kind}`, recipeRevision: 1, overrides: {} };
}

class FakeRemoteTrustPort implements KafkaRemoteTrustPort {
  discoveryCalls: KafkaRemoteHostKeyRequest[] = [];
  fingerprintResult = target.hostKeyFingerprint;
  materialCalls: KafkaRemoteMaterialRequest[] = [];
  materialRejection: Error | undefined;
  materialResult = new Uint8Array([1, 2, 3]);
  passwordCalls: KafkaRemotePasswordRequest[] = [];
  passwordRejection: Error | undefined;
  passwordResult = "remote-store-password\r\n";
  rejection: Error | undefined;

  discoverHostKey(request: KafkaRemoteHostKeyRequest, signal?: AbortSignal): Promise<string> {
    signal?.throwIfAborted();
    this.discoveryCalls.push(request);
    return this.rejection === undefined
      ? Promise.resolve(this.fingerprintResult)
      : Promise.reject(this.rejection);
  }

  fetchMaterial(request: KafkaRemoteMaterialRequest, signal?: AbortSignal): Promise<Uint8Array> {
    signal?.throwIfAborted();
    this.materialCalls.push(request);
    const rejection = this.materialRejection ?? this.rejection;
    return rejection === undefined
      ? Promise.resolve(this.materialResult.slice())
      : Promise.reject(rejection);
  }

  fetchPassword(request: KafkaRemotePasswordRequest, signal?: AbortSignal): Promise<string> {
    signal?.throwIfAborted();
    this.passwordCalls.push(request);
    const rejection = this.passwordRejection ?? this.rejection;
    return rejection === undefined
      ? Promise.resolve(this.passwordResult)
      : Promise.reject(rejection);
  }
}

interface AcquisitionHarness {
  readonly templates: ReturnType<typeof createRecipeLibrary>;
  readonly advance: (ms: number) => void;
  readonly decode: KafkaProfileTrustDecoder["decode"];
  readonly remote: FakeRemoteTrustPort;
  readonly service: KafkaTrustAcquisitionService;
}

export async function scopedMaterialInput(
  service: KafkaTrustAcquisitionService,
  editor: { id: string; generation: number },
  label = "trust",
): Promise<RemoteTrustMaterialFetchInput> {
  const discovered = await service.discoverHostKey({ editor, target });
  if (discovered.review === undefined) throw new Error("Expected host identity review");
  return {
    recipe: materialRecipe("jks"),
    kind: "jks" as const,
    label,
    target,
    editor,
    identityId: discovered.review.id,
    acceptIdentity: true,
  };
}

export function deferred<T>(): {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

export function createHarness(
  options: {
    readonly nowMs?: number;
    readonly https?: import("../../src/features/kafka/application/https-trust-port").HttpsTrustAcquisitionPort;
  } = {},
): AcquisitionHarness {
  let nowMs = options.nowMs ?? Date.parse("2026-07-26T13:00:00.000Z");
  let nextId = 0;
  const remote = new FakeRemoteTrustPort();
  const decode: KafkaProfileTrustDecoder["decode"] = vi.fn(
    (
      input: {
        readonly kind: ProfileTrustKind;
        readonly material: string;
        readonly password?: string;
      },
      signal?: AbortSignal,
    ): Promise<{ readonly caPem: string; readonly kind: ProfileTrustKind }> => {
      signal?.throwIfAborted();
      return Promise.resolve({
        caPem: `decoded-${input.kind}-ca`,
        kind: input.kind,
      });
    },
  );
  const decoder: KafkaProfileTrustDecoder = { decode };
  const templateService = createRecipeLibrary(undefined, {
    store: new InMemoryKafkaTrustRecipeStore(
      { durability: "session", state: "ready" },
      {
        version: 1,
        recipes: (["jks", "pkcs12", "pem"] as const).map((kind) => ({
          id: `fixture-${kind}`,
          revision: 1,
          kind,
          name: kind === "jks" ? "Remote trust" : `Fixture ${kind}`,
          syntax: "legacy-v1" as const,
          method: "ssh" as const,
          parameters: [],
          timeoutSeconds: 60,
          ssh: {
            source: "legacy-tempfile" as const,
            value: "copy --password {storepass} --directory {destDir} source {truststorePath}",
            password:
              kind === "pem"
                ? { source: "none" as const }
                : { source: "command" as const, command: "fetch-password --quiet" },
          },
        })),
      },
    ),
  });
  const service = new KafkaTrustAcquisitionService(templateService, remote, decoder, {
    ...(options.https === undefined ? {} : { https: options.https }),
    createId: (): string => `acquisition-${String(++nextId)}`,
    createRemotePath: (): string => `/tmp/streamskope-${String(nextId + 1)}.trust`,
    now: (): Date => new Date(nowMs),
  });
  return {
    templates: templateService,
    advance(ms: number): void {
      nowMs += ms;
    },
    decode,
    remote,
    service,
  };
}
