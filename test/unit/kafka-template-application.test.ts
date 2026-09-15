import { describe, expect, it } from "vitest";

import {
  type ConnectionTemplateSnapshot,
  type ConnectionTemplateStoreCapability,
} from "../../src/kafka/contracts";
import {
  DEFAULT_CONNECTION_TEMPLATE_DOCUMENT,
  DuplicateKafkaConnectionTemplateError,
  InMemoryKafkaConnectionTemplateStore,
  KafkaConnectionTemplateCapacityError,
  KafkaConnectionTemplateNotFoundError,
  KafkaConnectionTemplateService,
  KafkaConnectionTemplateStoreUnavailableError,
  KafkaConnectionTemplateValidationError,
  type KafkaConnectionTemplateDocument,
  type KafkaConnectionTemplateStore,
} from "../../src/kafka/application";

const sessionCapability: ConnectionTemplateStoreCapability = {
  durability: "session",
  state: "ready",
};

function documentWith(
  catalog: "truststore-fetch" | "truststore-password" | "oauth-endpoint",
  entries: ReadonlyArray<{ readonly name: string; readonly template: string }>,
  selectedName: string | null,
): KafkaConnectionTemplateDocument {
  return {
    catalogs: DEFAULT_CONNECTION_TEMPLATE_DOCUMENT.catalogs.map((current) =>
      current.catalog === catalog ? { catalog, entries, selectedName } : current,
    ),
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

class ControlledTemplateStore implements KafkaConnectionTemplateStore {
  readonly commitStarted = deferred<void>();
  readonly commits: KafkaConnectionTemplateDocument[] = [];
  nextCommit: Promise<void> | undefined;

  constructor(private current: KafkaConnectionTemplateDocument | undefined) {}

  capability(): ConnectionTemplateStoreCapability {
    return sessionCapability;
  }

  async commit(document: KafkaConnectionTemplateDocument, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    this.commits.push(document);
    this.commitStarted.resolve();
    await this.nextCommit;
    signal?.throwIfAborted();
    this.current = document;
  }

  load(): Promise<KafkaConnectionTemplateDocument | undefined> {
    return Promise.resolve(this.current);
  }
}

describe("Kafka connection-template application", () => {
  it("atomically seeds the exact inspected source defaults only when the document is missing", async () => {
    const store = new InMemoryKafkaConnectionTemplateStore(sessionCapability);
    const service = new KafkaConnectionTemplateService(store);

    await expect(service.list()).resolves.toEqual({
      catalogs: [
        {
          catalog: "truststore-fetch",
          entries: [
            {
              name: "nsp-25-4",
              template:
                "kubectl cp nsp-psa-restricted/$(kubectl get pods -n nsp-psa-restricted -l app=nsp-tomcat -o name | awk -F/ '{print $2}'):/opt/nsp/os/ssl/nsp.truststore {truststorePath}",
            },
            {
              name: "nsp-25-11",
              template:
                "kubectl exec -n nsp-psa-restricted $(kubectl get pods -n nsp-psa-restricted -o name | grep -m1 nspos-tomcat | awk -F/ '{print $2}') -- cat /opt/nsp/os/ssl/nsp.truststore > {truststorePath}",
            },
          ],
          selectedName: "nsp-25-4",
        },
        {
          catalog: "truststore-password",
          entries: [
            {
              name: "nsp-25-11",
              template:
                "kubectl get secret -o jsonpath='{.data.truststore-pass}' -n nsp-psa-restricted nsp-tls-truststore-pass-nspdeployer | base64 -d; echo",
            },
          ],
          selectedName: "nsp-25-11",
        },
        {
          catalog: "oauth-endpoint",
          entries: [
            {
              name: "nsp-25-4",
              template: "https://{kafka-cluseter-server}/rest-gateway/rest/api/v1/auth/token",
            },
          ],
          selectedName: "nsp-25-4",
        },
      ],
      store: sessionCapability,
    });
    expect(store.commitCount).toBe(1);
    expect(store.document()).toEqual(DEFAULT_CONNECTION_TEMPLATE_DOCUMENT);
  });

  it("preserves an intentionally empty persisted catalog without reseeding it", async () => {
    const empty = documentWith("truststore-fetch", [], null);
    const store = new InMemoryKafkaConnectionTemplateStore(sessionCapability, empty);
    const service = new KafkaConnectionTemplateService(store);

    const snapshot = await service.list();

    expect(snapshot.catalogs[0]).toEqual({
      catalog: "truststore-fetch",
      entries: [],
      selectedName: null,
    });
    expect(store.commitCount).toBe(0);
  });

  it("creates a canonical entry last without changing the current selection", async () => {
    const store = new InMemoryKafkaConnectionTemplateStore(
      sessionCapability,
      DEFAULT_CONNECTION_TEMPLATE_DOCUMENT,
    );
    const service = new KafkaConnectionTemplateService(store);

    const snapshot = await service.create({
      catalog: "truststore-fetch",
      name: "  Custom fetch  ",
      template: "copy source {truststorePath}",
    });

    expect(snapshot.catalogs[0]).toMatchObject({
      entries: [
        { name: "nsp-25-4" },
        { name: "nsp-25-11" },
        { name: "Custom fetch", template: "copy source {truststorePath}" },
      ],
      selectedName: "nsp-25-4",
    });
  });

  it("rejects normalized duplicates and invalid placeholders without committing", async () => {
    const store = new InMemoryKafkaConnectionTemplateStore(
      sessionCapability,
      DEFAULT_CONNECTION_TEMPLATE_DOCUMENT,
    );
    const service = new KafkaConnectionTemplateService(store);

    await expect(
      service.create({
        catalog: "truststore-fetch",
        name: " NSP-25-4 ",
        template: "copy source {truststorePath}",
      }),
    ).rejects.toBeInstanceOf(DuplicateKafkaConnectionTemplateError);
    await expect(
      service.create({
        catalog: "truststore-fetch",
        name: "Broken",
        template: "copy source {unknown}",
      }),
    ).rejects.toBeInstanceOf(KafkaConnectionTemplateValidationError);
    expect(store.commitCount).toBe(0);
  });

  it("rejects a catalog beyond its entry bound without committing", async () => {
    const full = documentWith(
      "truststore-password",
      Array.from({ length: 100 }, (_, index) => ({
        name: `Password ${index}`,
        template: `command ${index}`,
      })),
      "Password 0",
    );
    const store = new InMemoryKafkaConnectionTemplateStore(sessionCapability, full);
    const service = new KafkaConnectionTemplateService(store);

    await expect(
      service.create({
        catalog: "truststore-password",
        name: "Overflow",
        template: "command overflow",
      }),
    ).rejects.toBeInstanceOf(KafkaConnectionTemplateCapacityError);
    expect(store.commitCount).toBe(0);
  });

  it("updates in place and follows the selected entry across a rename", async () => {
    const store = new InMemoryKafkaConnectionTemplateStore(
      sessionCapability,
      DEFAULT_CONNECTION_TEMPLATE_DOCUMENT,
    );
    const service = new KafkaConnectionTemplateService(store);

    const snapshot = await service.update("truststore-fetch", "nsp-25-4", {
      name: "NSP Primary",
      template: "fetch new {truststorePath}",
    });

    expect(snapshot.catalogs[0]).toMatchObject({
      entries: [
        { name: "NSP Primary", template: "fetch new {truststorePath}" },
        { name: "nsp-25-11" },
      ],
      selectedName: "NSP Primary",
    });
  });

  it("selects only an existing entry and retains state after a stale selection", async () => {
    const store = new InMemoryKafkaConnectionTemplateStore(
      sessionCapability,
      DEFAULT_CONNECTION_TEMPLATE_DOCUMENT,
    );
    const service = new KafkaConnectionTemplateService(store);

    const selected = await service.select("truststore-fetch", "nsp-25-11");
    await expect(service.select("truststore-fetch", "missing")).rejects.toBeInstanceOf(
      KafkaConnectionTemplateNotFoundError,
    );

    expect(selected.catalogs[0]?.selectedName).toBe("nsp-25-11");
    expect(service.currentSnapshot().catalogs[0]?.selectedName).toBe("nsp-25-11");
  });

  it("uses a deterministic selection fallback after deleting selected entries", async () => {
    const store = new InMemoryKafkaConnectionTemplateStore(
      sessionCapability,
      DEFAULT_CONNECTION_TEMPLATE_DOCUMENT,
    );
    const service = new KafkaConnectionTemplateService(store);

    const firstDeletion = await service.delete("truststore-fetch", "nsp-25-4");
    expect(firstDeletion.catalogs[0]).toMatchObject({
      entries: [{ name: "nsp-25-11" }],
      selectedName: "nsp-25-11",
    });

    const finalDeletion = await service.delete("truststore-fetch", "nsp-25-11");
    expect(finalDeletion.catalogs[0]).toEqual({
      catalog: "truststore-fetch",
      entries: [],
      selectedName: null,
    });
  });

  it("leaves the selected entry unchanged when deleting a different entry", async () => {
    const store = new InMemoryKafkaConnectionTemplateStore(
      sessionCapability,
      DEFAULT_CONNECTION_TEMPLATE_DOCUMENT,
    );
    const service = new KafkaConnectionTemplateService(store);

    const snapshot = await service.delete("truststore-fetch", "nsp-25-11");

    expect(snapshot.catalogs[0]).toMatchObject({
      entries: [{ name: "nsp-25-4" }],
      selectedName: "nsp-25-4",
    });
  });

  it("serializes concurrent mutations against the last committed document", async () => {
    const gate = deferred<void>();
    const store = new ControlledTemplateStore(DEFAULT_CONNECTION_TEMPLATE_DOCUMENT);
    store.nextCommit = gate.promise;
    const service = new KafkaConnectionTemplateService(store);

    const first = service.create({
      catalog: "truststore-password",
      name: "First",
      template: "first command",
    });
    const second = service.create({
      catalog: "truststore-password",
      name: "Second",
      template: "second command",
    });
    await store.commitStarted.promise;
    expect(store.commits).toHaveLength(1);

    gate.resolve();
    await first;
    await second;

    expect(service.currentSnapshot().catalogs[1]?.entries.slice(-2)).toEqual([
      { name: "First", template: "first command" },
      { name: "Second", template: "second command" },
    ]);
  });

  it("retains the last committed snapshot when persistence fails or is cancelled", async () => {
    const failureGate = deferred<void>();
    const failureStore = new ControlledTemplateStore(DEFAULT_CONNECTION_TEMPLATE_DOCUMENT);
    failureStore.nextCommit = failureGate.promise;
    const failureService = new KafkaConnectionTemplateService(failureStore);
    await failureService.list();

    const failedCreation = failureService.create({
      catalog: "truststore-password",
      name: "Not committed",
      template: "command",
    });
    await Promise.resolve();
    failureGate.reject(new Error("disk full"));
    await expect(failedCreation).rejects.toBeInstanceOf(
      KafkaConnectionTemplateStoreUnavailableError,
    );
    expect(failureService.currentSnapshot().catalogs[1]?.entries).toHaveLength(1);

    const cancellationGate = deferred<void>();
    const cancellationStore = new ControlledTemplateStore(DEFAULT_CONNECTION_TEMPLATE_DOCUMENT);
    cancellationStore.nextCommit = cancellationGate.promise;
    const cancellationService = new KafkaConnectionTemplateService(cancellationStore);
    await cancellationService.list();
    const controller = new AbortController();
    const selection = cancellationService.select(
      "truststore-fetch",
      "nsp-25-11",
      controller.signal,
    );
    controller.abort();
    cancellationGate.resolve();
    await expect(selection).rejects.toMatchObject({ name: "AbortError" });
    expect(cancellationService.currentSnapshot().catalogs[0]?.selectedName).toBe("nsp-25-4");
  });

  it("fails closed when a loaded document violates canonical invariants", async () => {
    const invalid = documentWith(
      "oauth-endpoint",
      [
        { name: "Duplicate", template: "https://{host}/one" },
        { name: " duplicate ", template: "https://{host}/two" },
      ],
      "Duplicate",
    );
    const store = new InMemoryKafkaConnectionTemplateStore(sessionCapability, invalid);
    const service = new KafkaConnectionTemplateService(store);

    await expect(service.list()).rejects.toBeInstanceOf(
      KafkaConnectionTemplateStoreUnavailableError,
    );
    expect(service.currentSnapshot()).toEqual({
      catalogs: [
        { catalog: "truststore-fetch", entries: [], selectedName: null },
        { catalog: "truststore-password", entries: [], selectedName: null },
        { catalog: "oauth-endpoint", entries: [], selectedName: null },
      ],
      store: {
        durability: "session",
        recovery:
          "Preserve the template data, correct it outside the running application, then restart StreamSkope.",
        state: "unavailable",
      },
    } satisfies ConnectionTemplateSnapshot);
  });
});
