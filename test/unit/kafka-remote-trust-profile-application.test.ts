import { describe, expect, it } from "vitest";

import type {
  ProfileCreateInput,
  ProfileStoreCapability,
  ProfileUpdateInput,
} from "../../src/features/kafka/contracts";
import {
  KafkaProfileService,
  KafkaProfileValidationError,
  KafkaTrustAcquisitionIncompleteError,
  type KafkaProfileRecord,
  type KafkaProfileStore,
  type KafkaProfileTrustDecoder,
  type KafkaResolvedTrustAcquisition,
  type KafkaTrustAcquisitionResolver,
} from "../../src/features/kafka/application";

const capability: ProfileStoreCapability = {
  durability: "session",
  protection: "memory",
  state: "ready",
};

const acquisition: KafkaResolvedTrustAcquisition = {
  caPem: "-----BEGIN CERTIFICATE-----\nvalidated\n-----END CERTIFICATE-----",
  id: "acquisition-1",
  kind: "jks",
  label: "remote.truststore.jks",
  material: "AQID",
  password: "remote-password",
};

const createInput: ProfileCreateInput = {
  brokers: ["kafka-lab.example.test:9093"],
  name: "Remote Kafka lab",
  trust: {
    kind: "jks",
    label: "remote.truststore.jks",
    material: { acquisitionId: acquisition.id, mode: "acquired" },
    password: { acquisitionId: acquisition.id, mode: "acquired" },
  },
};

class RecordingResolver implements KafkaTrustAcquisitionResolver {
  consumeCalls: string[] = [];
  failure: Error | undefined;
  resolution = acquisition;
  resolveCalls: Array<{ readonly id: string; readonly kind: string }> = [];

  consume(acquisitionId: string): void {
    this.consumeCalls.push(acquisitionId);
  }

  resolve(
    acquisitionId: string,
    expectedKind: "jks" | "pem" | "pkcs12",
  ): KafkaResolvedTrustAcquisition {
    this.resolveCalls.push({ id: acquisitionId, kind: expectedKind });
    if (this.failure !== undefined) {
      throw this.failure;
    }
    if (this.resolution.kind !== expectedKind) {
      throw new KafkaTrustAcquisitionIncompleteError(acquisitionId);
    }
    return this.resolution;
  }
}

class RecordingDecoder implements KafkaProfileTrustDecoder {
  readonly calls: Array<{
    readonly kind: "jks" | "pem" | "pkcs12";
    readonly material: string;
    readonly password?: string;
  }> = [];

  decode(
    input: {
      readonly kind: "jks" | "pem" | "pkcs12";
      readonly material: string;
      readonly password?: string;
    },
    signal?: AbortSignal,
  ): Promise<{ readonly caPem: string; readonly kind: "jks" | "pem" | "pkcs12" }> {
    signal?.throwIfAborted();
    this.calls.push(input);
    return Promise.resolve({ caPem: acquisition.caPem, kind: input.kind });
  }
}

class RecordingStore implements KafkaProfileStore {
  readonly commits: Array<readonly KafkaProfileRecord[]> = [];
  rejection: Error | undefined;

  constructor(private records: readonly KafkaProfileRecord[] = []) {}

  capability(): ProfileStoreCapability {
    return capability;
  }

  commit(records: readonly KafkaProfileRecord[], signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    this.commits.push(records);
    if (this.rejection !== undefined) {
      return Promise.reject(this.rejection);
    }
    this.records = records;
    return Promise.resolve();
  }

  load(): Promise<readonly KafkaProfileRecord[]> {
    return Promise.resolve(this.records);
  }
}

function existingJksProfile(): KafkaProfileRecord {
  return {
    brokers: ["kafka-lab.example.test:9093"],
    createdAt: "2026-07-25T18:00:00.000Z",
    id: "profile-1",
    name: "Remote Kafka lab",
    trust: {
      kind: "jks",
      label: "existing.jks",
      material: "existing-material",
      password: "existing-password",
    },
    updatedAt: "2026-07-25T18:00:00.000Z",
  };
}

function service(
  store: KafkaProfileStore,
  decoder: KafkaProfileTrustDecoder,
  acquisitions: KafkaTrustAcquisitionResolver,
): KafkaProfileService {
  return new KafkaProfileService(store, decoder, {
    createId: (): string => "profile-created",
    now: (): Date => new Date("2026-07-25T18:05:00.000Z"),
    trustAcquisitions: acquisitions,
  });
}

describe("Kafka acquired-trust profile mutation", () => {
  it("carries editor cancellation through the resolved draft test context", async () => {
    const owner = new AbortController();
    const resolver = new RecordingResolver();
    resolver.resolution = { ...acquisition, lifetimeSignal: owner.signal };
    const profiles = service(new RecordingStore(), new RecordingDecoder(), resolver);
    const context = await profiles.resolveTestContext({ mode: "create", profile: createInput });
    expect(context.lifetimeSignal).toBe(owner.signal);
    owner.abort();
    expect(context.lifetimeSignal?.aborted).toBe(true);
    expect(resolver.consumeCalls).toEqual([]);
  });
  it.each(["create", "update"] as const)(
    "does not %s after the owning editor closes during decoding",
    async (mode) => {
      const controller = new AbortController();
      const resolver = new RecordingResolver();
      resolver.resolution = { ...acquisition, lifetimeSignal: controller.signal };
      const store = new RecordingStore(mode === "update" ? [existingJksProfile()] : []);
      const profiles = service(
        store,
        {
          decode: () => {
            controller.abort(new Error("Editor closed"));
            return Promise.resolve({ kind: "jks", caPem: acquisition.caPem });
          },
        },
        resolver,
      );
      await expect(
        mode === "create"
          ? profiles.create(createInput)
          : profiles.update("profile-1", createInput),
      ).rejects.toThrow("Editor closed");
      expect(store.commits).toEqual([]);
      expect(resolver.consumeCalls).toEqual([]);
    },
  );
  it("resolves acquired trust for a draft test without committing or consuming it", async () => {
    const acquisitions = new RecordingResolver();
    const decoder = new RecordingDecoder();
    const store = new RecordingStore();
    const profiles = service(store, decoder, acquisitions);

    await expect(
      profiles
        .resolveTestContext({
          mode: "create",
          profile: createInput,
        })
        .then((context) => context.connection),
    ).resolves.toEqual({
      brokers: createInput.brokers,
      name: createInput.name,
      tls: {
        caPem: acquisition.caPem,
        enabled: true,
      },
    });

    expect(store.commits).toEqual([]);
    expect(acquisitions.resolveCalls).toEqual([{ id: acquisition.id, kind: "jks" }]);
    expect(acquisitions.consumeCalls).toEqual([]);
  });

  it("commits acquired JKS values before consuming and returns only a safe summary", async () => {
    const acquisitions = new RecordingResolver();
    const decoder = new RecordingDecoder();
    const store = new RecordingStore();
    const profiles = service(store, decoder, acquisitions);

    const snapshot = await profiles.create(createInput);

    expect(store.commits[0]).toMatchObject([
      {
        id: "profile-created",
        trust: {
          kind: "jks",
          label: "remote.truststore.jks",
          material: "AQID",
          password: "remote-password",
        },
      },
    ]);
    expect(decoder.calls).toEqual([
      {
        kind: "jks",
        material: "AQID",
        password: "remote-password",
      },
    ]);
    expect(acquisitions.resolveCalls).toEqual([{ id: "acquisition-1", kind: "jks" }]);
    expect(acquisitions.consumeCalls).toEqual(["acquisition-1"]);
    expect(snapshot.profiles).toMatchObject([
      {
        id: "profile-created",
        trust: {
          materialPresent: true,
          passwordPresent: true,
        },
      },
    ]);
    expect(JSON.stringify(snapshot)).not.toMatch(/AQID|remote-password|validated/);
  });

  it("retains material while replacing only the password from one acquisition", async () => {
    const existing = existingJksProfile();
    const acquisitions = new RecordingResolver();
    const store = new RecordingStore([existing]);
    const profiles = service(store, new RecordingDecoder(), acquisitions);
    const update: ProfileUpdateInput = {
      brokers: existing.brokers,
      name: existing.name,
      trust: {
        kind: "jks",
        label: existing.trust.label,
        material: { mode: "retain" },
        password: { acquisitionId: acquisition.id, mode: "acquired" },
      },
    };

    await profiles.update(existing.id, update);

    expect(store.commits[0]?.[0]?.trust).toEqual({
      kind: "jks",
      label: "existing.jks",
      material: "existing-material",
      password: "remote-password",
    });
    expect(acquisitions.consumeCalls).toEqual(["acquisition-1"]);
  });

  it("rejects different material and password acquisition owners before resolution", async () => {
    const acquisitions = new RecordingResolver();
    const store = new RecordingStore();
    const profiles = service(store, new RecordingDecoder(), acquisitions);

    await expect(
      profiles.create({
        ...createInput,
        trust: {
          ...createInput.trust,
          password: { acquisitionId: "acquisition-2", mode: "acquired" },
        },
      }),
    ).rejects.toBeInstanceOf(KafkaProfileValidationError);

    expect(acquisitions.resolveCalls).toEqual([]);
    expect(acquisitions.consumeCalls).toEqual([]);
    expect(store.commits).toEqual([]);
  });

  it("rejects a declared-kind mismatch without committing or consuming", async () => {
    const acquisitions = new RecordingResolver();
    const store = new RecordingStore();
    const profiles = service(store, new RecordingDecoder(), acquisitions);

    await expect(
      profiles.create({
        ...createInput,
        trust: {
          kind: "pem",
          label: "remote.pem",
          material: { acquisitionId: acquisition.id, mode: "acquired" },
          password: { mode: "clear" },
        },
      }),
    ).rejects.toMatchObject({
      code: "ACQUISITION_INCOMPLETE",
      stage: "acquisition",
    });

    expect(store.commits).toEqual([]);
    expect(acquisitions.consumeCalls).toEqual([]);
  });

  it("retains a resolved acquisition when protected storage rejects the commit", async () => {
    const acquisitions = new RecordingResolver();
    const store = new RecordingStore();
    store.rejection = new Error("protected store unavailable");
    const profiles = service(store, new RecordingDecoder(), acquisitions);

    await expect(profiles.create(createInput)).rejects.toThrow("protected store unavailable");

    expect(acquisitions.resolveCalls).toHaveLength(1);
    expect(acquisitions.consumeCalls).toEqual([]);
  });

  it("does not resolve or consume acquired trust when canonical validation fails", async () => {
    const acquisitions = new RecordingResolver();
    const store = new RecordingStore();
    const profiles = service(store, new RecordingDecoder(), acquisitions);

    await expect(
      profiles.create({
        ...createInput,
        brokers: ["not-a-broker"],
      }),
    ).rejects.toBeInstanceOf(KafkaProfileValidationError);

    expect(acquisitions.resolveCalls).toEqual([]);
    expect(acquisitions.consumeCalls).toEqual([]);
    expect(store.commits).toEqual([]);
  });

  it("does not resolve or consume trust while the profile is active", async () => {
    const existing = existingJksProfile();
    const acquisitions = new RecordingResolver();
    const store = new RecordingStore([existing]);
    const profiles = service(store, new RecordingDecoder(), acquisitions);
    await profiles.markActive(existing.id);

    await expect(
      profiles.update(existing.id, {
        brokers: existing.brokers,
        name: existing.name,
        trust: {
          kind: "jks",
          label: existing.trust.label,
          material: { mode: "retain" },
          password: { acquisitionId: acquisition.id, mode: "acquired" },
        },
      }),
    ).rejects.toMatchObject({ code: "PROFILE_ACTIVE" });

    expect(acquisitions.resolveCalls).toEqual([]);
    expect(acquisitions.consumeCalls).toEqual([]);
    expect(store.commits).toEqual([]);
  });

  it("propagates a missing or expired acquisition without profile mutation", async () => {
    const acquisitions = new RecordingResolver();
    acquisitions.failure = Object.assign(new Error("The acquisition expired."), {
      code: "ACQUISITION_EXPIRED",
      recovery: "Acquire trust again.",
      retryable: false,
      stage: "acquisition",
      target: acquisition.id,
    });
    const store = new RecordingStore();
    const profiles = service(store, new RecordingDecoder(), acquisitions);

    await expect(profiles.create(createInput)).rejects.toMatchObject({
      code: "ACQUISITION_EXPIRED",
      stage: "acquisition",
    });
    expect(store.commits).toEqual([]);
    expect(acquisitions.consumeCalls).toEqual([]);
  });
});
