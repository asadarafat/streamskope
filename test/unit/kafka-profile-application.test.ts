import { describe, expect, it, vi } from "vitest";

import {
  PROFILE_LIMITS,
  type ProfileCreateInput,
  type ProfileStoreCapability,
  type ProfileTestInput,
  type ProfileUpdateInput,
} from "../../src/features/kafka/contracts";
import {
  DuplicateKafkaProfileError,
  ActiveKafkaProfileMutationError,
  InMemoryKafkaProfileStore,
  KafkaProfileCapacityError,
  KafkaProfileNotFoundError,
  KafkaProfileService,
  KafkaProfileStoreUnavailableError,
  KafkaProfileValidationError,
  UnavailableKafkaProfileStore,
  type KafkaProfileRecord,
  type KafkaProfileStore,
  type KafkaProfileTrustDecoder,
} from "../../src/features/kafka/application";

const capability: ProfileStoreCapability = {
  durability: "session",
  protection: "memory",
  state: "ready",
};

const createInput: ProfileCreateInput = {
  brokers: ["127.0.0.1:19093"],
  name: " Local validation ",
  oauth: {
    clientId: "admin",
    clientSecret: { mode: "replace", value: "fixture-secret" },
    scope: "kafka",
    tokenEndpoint: "http://127.0.0.1:15000/rest-gateway/rest/api/v1/auth/token",
  },
  trust: {
    kind: "pem",
    label: "ca.pem",
    material: {
      mode: "replace",
      value: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
    },
    password: { mode: "clear" },
  },
};

it("validates and retains an independent API CA without adding it to Kafka connection input", async () => {
  const store = new InMemoryKafkaProfileStore(capability);
  const decoder = new AcceptingTrustDecoder();
  const service = new KafkaProfileService(store, decoder, {
    createId: (): string => "api-profile",
  });
  await service.create({ ...createInput, apiCa: { mode: "replace", value: "api-ca-input" } });
  const saved = (await store.load())[0];
  expect(saved?.apiCaPem).toBe("-----BEGIN CERTIFICATE-----\nvalidated\n-----END CERTIFICATE-----");
  expect(saved?.trust.material).toBe(
    createInput.trust.material.mode === "replace" ? createInput.trust.material.value : "",
  );
  expect(decoder.calls).toContainEqual({ kind: "pem", material: "api-ca-input" });
  expect(await service.bindingDetail("api-profile")).toMatchObject({ apiCaPresent: true });
  expect(await service.resolveAcquisitionApiCa("api-profile", 1)).toBe(saved?.apiCaPem);
  await expect(service.resolveAcquisitionApiCa("api-profile", 2)).rejects.toThrow();
  await service.update("api-profile", { ...createInput, expectedRevision: 1 });
  expect((await store.load())[0]?.apiCaPem).toBe(saved?.apiCaPem);
  await service.update("api-profile", {
    ...createInput,
    expectedRevision: 2,
    apiCa: { mode: "clear" },
  });
  expect((await store.load())[0]?.apiCaPem).toBeUndefined();
  await expect(service.resolveAcquisitionApiCa("api-profile", 3)).rejects.toThrow();
});

class AcceptingTrustDecoder implements KafkaProfileTrustDecoder {
  readonly calls: Array<{
    readonly kind: "jks" | "pem" | "pkcs12";
    readonly material: string;
    readonly password?: string;
  }> = [];

  decode(input: {
    readonly kind: "jks" | "pem" | "pkcs12";
    readonly material: string;
    readonly password?: string;
  }): Promise<{ readonly caPem: string; readonly kind: "jks" | "pem" | "pkcs12" }> {
    this.calls.push(input);
    return Promise.resolve({
      caPem: "-----BEGIN CERTIFICATE-----\nvalidated\n-----END CERTIFICATE-----",
      kind: input.kind,
    });
  }
}

function record(
  id: string,
  name: string,
  createdAt = "2026-07-25T18:00:00.000Z",
): KafkaProfileRecord {
  return {
    brokers: [`${id}.example.test:9093`],
    createdAt,
    id,
    name,
    trust: {
      kind: "pem",
      label: `${id}.pem`,
      material: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
    },
    updatedAt: createdAt,
  };
}

interface Deferred<Value> {
  readonly promise: Promise<Value>;
  reject(reason: Error): void;
  resolve(value: Value): void;
}

function deferred<Value>(): Deferred<Value> {
  let resolvePromise: ((value: Value) => void) | undefined;
  let rejectPromise: ((reason: Error) => void) | undefined;
  const promise = new Promise<Value>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });
  return {
    promise,
    reject: (reason): void => {
      rejectPromise?.(reason);
    },
    resolve: (value): void => {
      resolvePromise?.(value);
    },
  };
}

class ControlledProfileStore implements KafkaProfileStore {
  readonly commits: Array<readonly KafkaProfileRecord[]> = [];
  nextCommit: Promise<void> | undefined;

  constructor(private currentRecords: readonly KafkaProfileRecord[] = []) {}

  capability(): ProfileStoreCapability {
    return capability;
  }

  async commit(records: readonly KafkaProfileRecord[], signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    this.commits.push(records);
    await this.nextCommit;
    signal?.throwIfAborted();
    this.currentRecords = records;
  }

  load(): Promise<readonly KafkaProfileRecord[]> {
    return Promise.resolve(this.currentRecords);
  }
}

describe("Kafka profile application", () => {
  it("does not let a versioned profile bypass conflict checks by omitting the revision", async () => {
    const store = new InMemoryKafkaProfileStore(capability, [
      { ...record("one", "Original"), revision: 2 },
    ]);
    const decoder = new AcceptingTrustDecoder();
    const service = new KafkaProfileService(store, decoder);
    await expect(service.update("one", createInput)).rejects.toMatchObject({ code: "VALIDATION" });
    expect(decoder.calls).toHaveLength(0);
    expect(store.commitCount).toBe(0);
  });

  it("rejects an outdated editor revision before decoding or committing", async () => {
    const store = new InMemoryKafkaProfileStore(capability, [record("one", "Original")]);
    const decoder = new AcceptingTrustDecoder();
    const service = new KafkaProfileService(store, decoder);
    const input = { ...createInput, name: "First edit", expectedRevision: 1 };
    await service.update("one", input);
    const committed = store.records();
    const decodeCount = decoder.calls.length;

    await expect(service.update("one", { ...input, name: "Stale edit" })).rejects.toMatchObject({
      code: "VALIDATION",
    });
    expect(decoder.calls).toHaveLength(decodeCount);
    expect(store.records()).toEqual(committed);
    expect((await service.list()).profiles[0]).toMatchObject({ name: "First edit", revision: 2 });
  });

  it("allows only one of two queued updates from the same revision", async () => {
    const store = new InMemoryKafkaProfileStore(capability, [record("one", "Original")]);
    const service = new KafkaProfileService(store, new AcceptingTrustDecoder());
    const input = { ...createInput, expectedRevision: 1 };
    const results = await Promise.allSettled([
      service.update("one", { ...input, name: "First" }),
      service.update("one", { ...input, name: "Second" }),
    ]);
    expect(results.map((result) => result.status)).toEqual(["fulfilled", "rejected"]);
    expect(store.commitCount).toBe(1);
  });

  it("does not advance the revision after a failed commit", async () => {
    const store = new ControlledProfileStore([record("one", "Original")]);
    const service = new KafkaProfileService(store, new AcceptingTrustDecoder());
    const input = { ...createInput, expectedRevision: 1 };
    const failure = deferred<void>();
    store.nextCommit = failure.promise;
    const update = service.update("one", input);
    const assertion = expect(update).rejects.toThrow("fixture commit failed");
    failure.reject(new Error("fixture commit failed"));
    await assertion;
    store.nextCommit = undefined;
    await expect(service.update("one", input)).resolves.toMatchObject({
      profiles: [{ revision: 2 }],
    });
  });

  it("normalizes optional cluster-service endpoints and resolves them without duplicating OAuth", async () => {
    const store = new InMemoryKafkaProfileStore(capability);
    const service = new KafkaProfileService(store, new AcceptingTrustDecoder(), {
      createId: (): string => "profile-services",
      now: (): Date => new Date("2026-08-12T12:00:00.000Z"),
    });
    const input = {
      ...createInput,
      services: {
        redpandaAdmin: {
          authentication: "oauth",
          baseUrl: "https://redpanda.example.test:9644/",
        },
        schemaRegistry: {
          authentication: "none",
          baseUrl: "https://schema.example.test:8081/registry/",
        },
      },
    } as const;

    const snapshot = await service.create(input);

    expect(snapshot.profiles[0]).toMatchObject({
      services: {
        redpandaAdmin: {
          authentication: "oauth",
          baseUrl: "https://redpanda.example.test:9644",
        },
        schemaRegistry: {
          authentication: "none",
          baseUrl: "https://schema.example.test:8081/registry",
        },
      },
    });
    await service.markActive("profile-services");
    await expect(service.resolveConnection("profile-services")).resolves.toMatchObject({
      services: snapshot.profiles[0]?.services,
    });
    expect(JSON.stringify(snapshot)).not.toContain("fixture-secret");
  });

  it("rejects OAuth service authentication when the profile has no OAuth owner", async () => {
    const service = new KafkaProfileService(
      new InMemoryKafkaProfileStore(capability),
      new AcceptingTrustDecoder(),
    );
    const input = {
      ...createInput,
      oauth: undefined,
      services: {
        schemaRegistry: {
          authentication: "oauth",
          baseUrl: "https://schema.example.test:8081",
        },
      },
    } as const;

    try {
      await service.create(input as unknown as ProfileCreateInput);
      throw new Error("Expected profile validation to fail.");
    } catch (error) {
      expect(error).toBeInstanceOf(KafkaProfileValidationError);
      if (!(error instanceof KafkaProfileValidationError)) throw error;
      expect(
        error.issues.some((issue) => issue.field === "services.schemaRegistry.authentication"),
      ).toBe(true);
    }
  });

  it("lists safe summaries in persisted creation order without protected values", async () => {
    const store = new InMemoryKafkaProfileStore(capability, [
      {
        ...record("profile-b", "Second", "2026-07-25T18:01:00.000Z"),
        oauth: {
          clientId: "admin",
          clientSecret: "never-return-this",
          scope: "kafka",
          tokenEndpoint: "http://127.0.0.1:15000/token",
        },
      },
      record("profile-a", "First"),
    ]);
    const service = new KafkaProfileService(store, new AcceptingTrustDecoder());

    await expect(service.list()).resolves.toEqual({
      profiles: [
        {
          active: false,
          brokers: ["profile-a.example.test:9093"],
          createdAt: "2026-07-25T18:00:00.000Z",
          id: "profile-a",
          name: "First",
          trust: {
            kind: "pem",
            label: "profile-a.pem",
            materialPresent: true,
            passwordPresent: false,
          },
          updatedAt: "2026-07-25T18:00:00.000Z",
        },
        {
          active: false,
          brokers: ["profile-b.example.test:9093"],
          createdAt: "2026-07-25T18:01:00.000Z",
          id: "profile-b",
          name: "Second",
          oauth: {
            clientId: "admin",
            clientSecretPresent: true,
            scope: "kafka",
            tokenEndpoint: "http://127.0.0.1:15000/token",
          },
          trust: {
            kind: "pem",
            label: "profile-b.pem",
            materialPresent: true,
            passwordPresent: false,
          },
          updatedAt: "2026-07-25T18:01:00.000Z",
        },
      ],
      store: capability,
    });
  });

  it("fails closed when loaded profiles violate canonical domain invariants", async () => {
    const store = new InMemoryKafkaProfileStore(capability, [
      record("profile-a", "Duplicate"),
      {
        ...record("profile-b", " duplicate "),
        brokers: ["broker-user:broker-secret@broker.example.test:9093"],
      },
    ]);
    const service = new KafkaProfileService(store, new AcceptingTrustDecoder());

    await expect(service.list()).rejects.toBeInstanceOf(KafkaProfileStoreUnavailableError);
    expect(service.currentSnapshot()).toEqual({
      profiles: [],
      store: {
        durability: "session",
        protection: "unavailable",
        recovery:
          "Preserve the profile data, correct it outside the running application, then restart StreamSkope.",
        state: "unavailable",
      },
    });
  });

  it("creates one canonical profile with injected identity and time", async () => {
    const store = new InMemoryKafkaProfileStore(capability);
    const decoder = new AcceptingTrustDecoder();
    const service = new KafkaProfileService(store, decoder, {
      createId: (): string => "01983770-8f0c-77e2-8edc-51426a9e2450",
      now: (): Date => new Date("2026-07-25T18:02:00.000Z"),
    });

    const snapshot = await service.create(createInput);

    expect(snapshot.profiles).toEqual([
      {
        revision: 1,
        active: false,
        brokers: ["127.0.0.1:19093"],
        createdAt: "2026-07-25T18:02:00.000Z",
        id: "01983770-8f0c-77e2-8edc-51426a9e2450",
        name: "Local validation",
        oauth: {
          clientId: "admin",
          clientSecretPresent: true,
          scope: "kafka",
          tokenEndpoint: "http://127.0.0.1:15000/rest-gateway/rest/api/v1/auth/token",
        },
        trust: {
          kind: "pem",
          label: "ca.pem",
          materialPresent: true,
          passwordPresent: false,
        },
        updatedAt: "2026-07-25T18:02:00.000Z",
      },
    ]);
    expect(store.records()).toMatchObject([
      {
        id: "01983770-8f0c-77e2-8edc-51426a9e2450",
        name: "Local validation",
        oauth: { clientSecret: "fixture-secret" },
        trust: {
          material: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
        },
      },
    ]);
    expect(decoder.calls).toEqual([
      {
        kind: "pem",
        material: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
      },
    ]);
  });

  it("rejects a duplicate normalized name without committing either profile", async () => {
    const store = new InMemoryKafkaProfileStore(capability, [
      record("existing-id", "Local Validation"),
    ]);
    const service = new KafkaProfileService(store, new AcceptingTrustDecoder());

    await expect(service.create(createInput)).rejects.toBeInstanceOf(DuplicateKafkaProfileError);
    expect(store.commitCount).toBe(0);
    expect(store.records()).toHaveLength(1);
  });

  it("reports canonical validation issues before trust parsing or persistence", async () => {
    const store = new InMemoryKafkaProfileStore(capability);
    const decoder = new AcceptingTrustDecoder();
    const service = new KafkaProfileService(store, decoder);

    const creation = service.create({
      ...createInput,
      brokers: ["not-a-broker", "not-a-broker"],
      name: " ",
      oauth: {
        ...createInput.oauth!,
        tokenEndpoint: "file:///tmp/token",
      },
      trust: {
        ...createInput.trust,
        material: { mode: "clear" },
      },
    });
    let failure: unknown;
    try {
      await creation;
    } catch (error: unknown) {
      failure = error;
    }
    expect(failure).toBeInstanceOf(KafkaProfileValidationError);
    if (!(failure instanceof KafkaProfileValidationError)) {
      throw new Error("Expected KafkaProfileValidationError.");
    }
    expect(failure.issues.map((issue) => issue.field)).toEqual(
      expect.arrayContaining([
        "name",
        "brokers[0]",
        "brokers[1]",
        "oauth.tokenEndpoint",
        "trust.material",
      ]),
    );
    expect(decoder.calls).toEqual([]);
    expect(store.commitCount).toBe(0);
  });

  it("rejects credentials embedded in renderer-visible broker and OAuth endpoint metadata", async () => {
    const store = new InMemoryKafkaProfileStore(capability);
    const decoder = new AcceptingTrustDecoder();
    const service = new KafkaProfileService(store, decoder);
    const creation = service.create({
      ...createInput,
      brokers: ["broker-user:broker-secret@broker.example.test:9093"],
      oauth: {
        ...createInput.oauth!,
        tokenEndpoint: "https://oauth-user:oauth-secret@identity.example.test/token",
      },
    });
    let failure: unknown;
    try {
      await creation;
    } catch (error: unknown) {
      failure = error;
    }

    expect(failure).toBeInstanceOf(KafkaProfileValidationError);
    if (!(failure instanceof KafkaProfileValidationError)) {
      throw new Error("Expected KafkaProfileValidationError.");
    }
    expect(failure.issues.map((issue) => issue.field)).toEqual([
      "brokers[0]",
      "oauth.tokenEndpoint",
    ]);
    expect(failure.message).not.toMatch(/broker-secret|oauth-secret/);
    expect(decoder.calls).toEqual([]);
    expect(store.commitCount).toBe(0);
  });

  it("rejects creation at the profile bound without parsing or committing", async () => {
    const records = Array.from({ length: PROFILE_LIMITS.profiles }, (_, index) =>
      record(`profile-${index}`, `Profile ${index}`),
    );
    const store = new InMemoryKafkaProfileStore(capability, records);
    const decoder = new AcceptingTrustDecoder();
    const service = new KafkaProfileService(store, decoder);

    await expect(service.create(createInput)).rejects.toBeInstanceOf(KafkaProfileCapacityError);
    expect(decoder.calls).toEqual([]);
    expect(store.commitCount).toBe(0);
  });

  it("retains protected values while updating canonical metadata and timestamps", async () => {
    const existing: KafkaProfileRecord = {
      ...record("profile-1", "Local validation"),
      oauth: {
        clientId: "admin",
        clientSecret: "existing-secret",
        scope: "kafka",
        tokenEndpoint: "http://127.0.0.1:15000/token",
      },
    };
    const store = new InMemoryKafkaProfileStore(capability, [existing]);
    const decoder = new AcceptingTrustDecoder();
    const service = new KafkaProfileService(store, decoder, {
      now: (): Date => new Date("2026-07-25T18:03:00.000Z"),
    });
    const update: ProfileUpdateInput = {
      brokers: ["127.0.0.1:29093"],
      name: " Local renamed ",
      oauth: {
        clientId: "renamed-client",
        clientSecret: { mode: "retain" },
        scope: "kafka.read",
        tokenEndpoint: "http://127.0.0.1:15000/token",
      },
      trust: {
        kind: "pem",
        label: "renamed-ca.pem",
        material: { mode: "retain" },
        password: { mode: "retain" },
      },
    };

    await service.update("profile-1", update);

    expect(store.records()).toEqual([
      {
        brokers: ["127.0.0.1:29093"],
        revision: 2,
        createdAt: "2026-07-25T18:00:00.000Z",
        id: "profile-1",
        name: "Local renamed",
        oauth: {
          clientId: "renamed-client",
          clientSecret: "existing-secret",
          scope: "kafka.read",
          tokenEndpoint: "http://127.0.0.1:15000/token",
        },
        trust: {
          kind: "pem",
          label: "renamed-ca.pem",
          material: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
        },
        updatedAt: "2026-07-25T18:03:00.000Z",
      },
    ]);
    expect(decoder.calls[0]).toMatchObject({
      kind: "pem",
      material: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
    });
  });

  it("replaces trust material and explicitly clears optional OAuth configuration", async () => {
    const existing: KafkaProfileRecord = {
      ...record("profile-1", "Local validation"),
      oauth: {
        clientId: "admin",
        clientSecret: "existing-secret",
        scope: "kafka",
        tokenEndpoint: "http://127.0.0.1:15000/token",
      },
    };
    const store = new InMemoryKafkaProfileStore(capability, [existing]);
    const service = new KafkaProfileService(store, new AcceptingTrustDecoder());

    await service.update("profile-1", {
      brokers: existing.brokers,
      name: existing.name,
      trust: {
        kind: "pem",
        label: "replacement.pem",
        material: {
          mode: "replace",
          value: "-----BEGIN CERTIFICATE-----\nreplacement\n-----END CERTIFICATE-----",
        },
        password: { mode: "clear" },
      },
    });

    expect(store.records()[0]).toMatchObject({
      id: "profile-1",
      trust: {
        label: "replacement.pem",
        material: "-----BEGIN CERTIFICATE-----\nreplacement\n-----END CERTIFICATE-----",
      },
    });
    expect(store.records()[0]?.oauth).toBeUndefined();
  });

  it("serializes competing creates so a normalized duplicate cannot race persistence", async () => {
    const pendingCommit = deferred<void>();
    const store = new ControlledProfileStore();
    store.nextCommit = pendingCommit.promise;
    const service = new KafkaProfileService(store, new AcceptingTrustDecoder(), {
      createId: (): string => "profile-created",
    });

    const first = service.create(createInput);
    const duplicate = service.create({ ...createInput, name: "LOCAL VALIDATION" });
    await vi.waitFor(() => {
      expect(store.commits).toHaveLength(1);
    });
    pendingCommit.resolve();

    await expect(first).resolves.toMatchObject({
      profiles: [{ id: "profile-created" }],
    });
    await expect(duplicate).rejects.toBeInstanceOf(DuplicateKafkaProfileError);
    expect(store.commits).toHaveLength(1);
  });

  it("does not commit work cancelled before mutation", async () => {
    const store = new ControlledProfileStore();
    const service = new KafkaProfileService(store, new AcceptingTrustDecoder());
    const controller = new AbortController();
    controller.abort();

    await expect(service.create(createInput, controller.signal)).rejects.toMatchObject({
      name: "AbortError",
    });
    expect(store.commits).toEqual([]);
  });

  it("keeps the previously published snapshot when persistence rejects a mutation", async () => {
    const store = new ControlledProfileStore([record("existing", "Existing")]);
    store.nextCommit = Promise.reject(new Error("disk full"));
    const service = new KafkaProfileService(store, new AcceptingTrustDecoder());

    await expect(service.create(createInput)).rejects.toThrow("disk full");
    await expect(service.list()).resolves.toMatchObject({
      profiles: [{ id: "existing", name: "Existing" }],
    });
  });

  it("rejects an update for a profile that no longer exists", async () => {
    const store = new InMemoryKafkaProfileStore(capability);
    const service = new KafkaProfileService(store, new AcceptingTrustDecoder());

    await expect(
      service.update("missing-profile", {
        brokers: createInput.brokers,
        name: createInput.name,
        oauth: {
          ...createInput.oauth!,
          clientSecret: { mode: "retain" },
        },
        trust: {
          ...createInput.trust,
          material: { mode: "retain" },
          password: { mode: "retain" },
        },
      }),
    ).rejects.toBeInstanceOf(KafkaProfileNotFoundError);
    expect(store.commitCount).toBe(0);
  });

  it("rejects editing the active profile until it is cleared", async () => {
    const existing = record("active-profile", "Active");
    const store = new InMemoryKafkaProfileStore(capability, [existing]);
    const service = new KafkaProfileService(store, new AcceptingTrustDecoder());
    await service.markActive(existing.id);

    await expect(
      service.update(existing.id, {
        brokers: existing.brokers,
        name: "Renamed",
        trust: {
          kind: "pem",
          label: existing.trust.label,
          material: { mode: "retain" },
          password: { mode: "retain" },
        },
      }),
    ).rejects.toBeInstanceOf(ActiveKafkaProfileMutationError);
    expect(store.commitCount).toBe(0);
    await expect(service.list()).resolves.toMatchObject({
      profiles: [{ active: true, id: existing.id, name: "Active" }],
    });
  });

  it("deletes exactly one inactive profile and its protected record", async () => {
    const first = {
      ...record("profile-1", "First"),
      oauth: {
        clientId: "admin",
        clientSecret: "first-secret",
        scope: "kafka",
        tokenEndpoint: "http://127.0.0.1/token",
      },
    };
    const second = record("profile-2", "Second", "2026-07-25T18:01:00.000Z");
    const store = new InMemoryKafkaProfileStore(capability, [first, second]);
    const service = new KafkaProfileService(store, new AcceptingTrustDecoder());

    await expect(service.delete(first.id)).resolves.toMatchObject({
      profiles: [{ id: second.id }],
    });
    expect(store.records()).toEqual([second]);
  });

  it("rejects deletion of the active profile without changing persistent state", async () => {
    const existing = record("active-profile", "Active");
    const store = new InMemoryKafkaProfileStore(capability, [existing]);
    const service = new KafkaProfileService(store, new AcceptingTrustDecoder());
    await service.markActive(existing.id);

    await expect(service.delete(existing.id)).rejects.toBeInstanceOf(
      ActiveKafkaProfileMutationError,
    );
    expect(store.commitCount).toBe(0);
    expect(store.records()).toEqual([existing]);
  });

  it("retains a profile when its persistent deletion fails", async () => {
    const existing = record("profile-1", "First");
    const store = new ControlledProfileStore([existing]);
    const failedCommit = deferred<void>();
    store.nextCommit = failedCommit.promise;
    const service = new KafkaProfileService(store, new AcceptingTrustDecoder());

    const deletion = service.delete(existing.id);
    await vi.waitFor(() => {
      expect(store.commits).toHaveLength(1);
    });
    failedCommit.reject(new Error("read-only filesystem"));
    await expect(deletion).rejects.toThrow("read-only filesystem");
    await expect(service.list()).resolves.toMatchObject({
      profiles: [{ id: existing.id }],
    });
  });

  it("clears active profile identity without deleting the stored profile", async () => {
    const existing = record("active-profile", "Active");
    const store = new InMemoryKafkaProfileStore(capability, [existing]);
    const service = new KafkaProfileService(store, new AcceptingTrustDecoder());
    await service.markActive(existing.id);

    expect(service.clearActive()).toMatchObject({
      profiles: [{ active: false, id: existing.id }],
    });
    expect(store.commitCount).toBe(0);
  });

  it("resolves a selected profile to the existing secure connection contract only in the host", async () => {
    const existing: KafkaProfileRecord = {
      ...record("profile-1", "Local validation"),
      oauth: {
        clientId: "admin",
        clientSecret: "existing-secret",
        scope: "kafka",
        tokenEndpoint: "http://127.0.0.1:15000/token",
      },
    };
    const decoder = new AcceptingTrustDecoder();
    const service = new KafkaProfileService(
      new InMemoryKafkaProfileStore(capability, [existing]),
      decoder,
    );

    await expect(service.resolveConnection(existing.id)).resolves.toEqual({
      brokers: existing.brokers,
      name: existing.name,
      oauth: existing.oauth,
      tls: {
        caPem: "-----BEGIN CERTIFICATE-----\nvalidated\n-----END CERTIFICATE-----",
        enabled: true,
      },
    });
    expect(decoder.calls).toEqual([
      {
        kind: "pem",
        material: existing.trust.material,
      },
    ]);
  });

  it("resolves a new binary-trust draft for testing without committing it", async () => {
    const decoder = new AcceptingTrustDecoder();
    const store = new InMemoryKafkaProfileStore(capability);
    const service = new KafkaProfileService(store, decoder);
    const draft: ProfileTestInput = {
      mode: "create",
      profile: {
        brokers: ["broker.example.test:9093"],
        name: "Binary draft",
        oauth: {
          clientId: "draft-client",
          clientSecret: { mode: "replace", value: "draft-secret" },
          scope: "kafka",
          tokenEndpoint: "http://127.0.0.1:15000/token",
        },
        trust: {
          kind: "jks",
          label: "draft.jks",
          material: { mode: "replace", value: "AQID" },
          password: { mode: "replace", value: "changeit" },
        },
      },
    };

    await expect(
      service.resolveTestContext(draft).then((context) => context.connection),
    ).resolves.toEqual({
      brokers: ["broker.example.test:9093"],
      name: "Binary draft",
      oauth: {
        clientId: "draft-client",
        clientSecret: "draft-secret",
        scope: "kafka",
        tokenEndpoint: "http://127.0.0.1:15000/token",
      },
      tls: {
        caPem: "-----BEGIN CERTIFICATE-----\nvalidated\n-----END CERTIFICATE-----",
        enabled: true,
      },
    });
    expect(decoder.calls).toEqual([
      {
        kind: "jks",
        material: "AQID",
        password: "changeit",
      },
    ]);
    expect(store.commitCount).toBe(0);
    expect(store.records()).toEqual([]);
  });

  it("resolves retained update values for testing without changing the stored profile", async () => {
    const existing: KafkaProfileRecord = {
      ...record("profile-retained", "Retained profile"),
      oauth: {
        clientId: "stored-client",
        clientSecret: "stored-secret",
        scope: "stored-scope",
        tokenEndpoint: "http://127.0.0.1:15000/token",
      },
      trust: {
        kind: "jks",
        label: "stored.jks",
        material: "stored-material",
        password: "stored-password",
      },
    };
    const decoder = new AcceptingTrustDecoder();
    const store = new InMemoryKafkaProfileStore(capability, [existing]);
    const service = new KafkaProfileService(store, decoder);
    const draft: ProfileTestInput = {
      mode: "update",
      profile: {
        brokers: ["changed.example.test:9093"],
        name: "Changed only for test",
        oauth: {
          clientId: "changed-client",
          clientSecret: { mode: "retain" },
          scope: "changed-scope",
          tokenEndpoint: "http://127.0.0.1:15000/changed-token",
        },
        trust: {
          kind: "jks",
          label: "stored.jks",
          material: { mode: "retain" },
          password: { mode: "retain" },
        },
      },
      profileId: existing.id,
    };

    await expect(
      service.resolveTestContext(draft).then((context) => context.connection),
    ).resolves.toEqual({
      brokers: ["changed.example.test:9093"],
      name: "Changed only for test",
      oauth: {
        clientId: "changed-client",
        clientSecret: "stored-secret",
        scope: "changed-scope",
        tokenEndpoint: "http://127.0.0.1:15000/changed-token",
      },
      tls: {
        caPem: "-----BEGIN CERTIFICATE-----\nvalidated\n-----END CERTIFICATE-----",
        enabled: true,
      },
    });
    expect(decoder.calls).toEqual([
      {
        kind: "jks",
        material: "stored-material",
        password: "stored-password",
      },
    ]);
    expect(store.commitCount).toBe(0);
    expect(store.records()).toEqual([existing]);
  });

  it("reports an unavailable protected store and rejects mutation before trust parsing", async () => {
    const unavailableCapability: ProfileStoreCapability = {
      durability: "durable",
      protection: "unavailable",
      recovery: "Unlock the operating-system credential service.",
      state: "unavailable",
    };
    const decoder = new AcceptingTrustDecoder();
    const service = new KafkaProfileService(
      new UnavailableKafkaProfileStore(unavailableCapability),
      decoder,
    );

    await expect(service.list()).resolves.toEqual({
      profiles: [],
      store: unavailableCapability,
    });
    await expect(service.create(createInput)).rejects.toBeInstanceOf(
      KafkaProfileStoreUnavailableError,
    );
    expect(decoder.calls).toEqual([]);
  });
});
