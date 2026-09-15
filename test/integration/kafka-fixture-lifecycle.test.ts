import { describe, expect, it } from "vitest";

import {
  KafkaFixtureLifecycle,
  FixtureRuntimeError,
  type FixtureConnection,
  type FixtureOwnershipStore,
  type FixtureRuntime,
  type OwnedFixtureRecord,
  type OwnedFixtureRequest,
} from "../../tools/kafka-fixture/lifecycle";

const ownedRequest: OwnedFixtureRequest = {
  caPath: "/workspace/aio-kafka/config/kafka-broker/certs/ca.pem",
  kafkaPort: 19_093,
  name: "streamskope-test-a",
  oauthImage: "streamskope-aio-kafka-oauth:1.0.0",
  oauthPort: 15_000,
  schemaRegistryImage:
    "ghcr.io/aiven-open/karapace:5.0.3@sha256:4cf3dbea61eebb6c85a5198ea8f0b6c32ca3e7fe330cf3b9843cf3837fcdb868",
  schemaRegistryPort: 18_081,
  topologyPath: "/workspace/aio-kafka/topology.clab.yml",
};

class FakeFixtureRuntime implements FixtureRuntime {
  readonly calls: string[] = [];
  readonly unavailable = new Set<number>();
  readinessFailure: Error | undefined;

  resumeOwned(record: OwnedFixtureRecord): Promise<void> {
    this.calls.push(`resume:${record.name}`);
    return Promise.resolve();
  }

  findUnavailablePorts(ports: readonly number[]): Promise<readonly number[]> {
    this.calls.push(`ports:${ports.join(",")}`);
    return Promise.resolve(ports.filter((port) => this.unavailable.has(port)));
  }

  buildOAuthImage(request: OwnedFixtureRequest): Promise<void> {
    this.calls.push(`build:${request.oauthImage}`);
    return Promise.resolve();
  }

  generateCertificates(request: OwnedFixtureRequest): Promise<void> {
    this.calls.push(`certs:${request.name}`);
    return Promise.resolve();
  }

  deploy(request: OwnedFixtureRequest): Promise<void> {
    this.calls.push(`deploy:${request.name}`);
    return Promise.resolve();
  }

  waitUntilReady(connection: FixtureConnection): Promise<void> {
    this.calls.push(`ready:${connection.ownership}`);
    if (this.readinessFailure !== undefined) {
      return Promise.reject(this.readinessFailure);
    }
    return Promise.resolve();
  }

  destroy(request: OwnedFixtureRecord): Promise<void> {
    this.calls.push(`destroy:${request.name}`);
    return Promise.resolve();
  }
}

class InMemoryOwnershipStore implements FixtureOwnershipStore {
  readonly records = new Map<string, OwnedFixtureRecord>();

  load(name: string): Promise<OwnedFixtureRecord | undefined> {
    return Promise.resolve(this.records.get(name));
  }

  save(record: OwnedFixtureRecord): Promise<void> {
    this.records.set(record.name, record);
    return Promise.resolve();
  }

  remove(name: string): Promise<void> {
    this.records.delete(name);
    return Promise.resolve();
  }
}

describe("Kafka fixture lifecycle", () => {
  it("ensures an existing fixture without recreating data or certificates", async () => {
    const runtime = new FakeFixtureRuntime();
    const store = new InMemoryOwnershipStore();
    store.records.set(ownedRequest.name, { ...ownedRequest, ownership: "owned" });
    const lifecycle = new KafkaFixtureLifecycle(runtime, store);
    const connection = await lifecycle.ensureOwned(ownedRequest);
    expect(connection.kafkaEndpoint).toBe("127.0.0.1:19093");
    expect(runtime.calls).toEqual([`resume:${ownedRequest.name}`, "ready:external"]);
    expect(store.records.size).toBe(1);
  });

  it("starts an absent fixture once and reuses it on the next ensure", async () => {
    const runtime = new FakeFixtureRuntime();
    const store = new InMemoryOwnershipStore();
    const lifecycle = new KafkaFixtureLifecycle(runtime, store);
    await lifecycle.ensureOwned(ownedRequest);
    await lifecycle.ensureOwned(ownedRequest);
    expect(runtime.calls.filter((call) => call.startsWith("deploy:"))).toHaveLength(1);
    expect(runtime.calls.filter((call) => call.startsWith("certs:"))).toHaveLength(1);
  });

  it("does not rebuild a recorded fixture on readiness failure or configuration mismatch", async () => {
    const runtime = new FakeFixtureRuntime();
    const store = new InMemoryOwnershipStore();
    store.records.set(ownedRequest.name, { ...ownedRequest, ownership: "owned" });
    const lifecycle = new KafkaFixtureLifecycle(runtime, store);
    await expect(lifecycle.ensureOwned({ ...ownedRequest, kafkaPort: 29093 })).rejects.toThrow();
    expect(runtime.calls).toEqual([]);
    runtime.readinessFailure = new Error("OAuth unavailable");
    await expect(lifecycle.ensureOwned(ownedRequest)).rejects.toThrow("OAuth unavailable");
    expect(runtime.calls).not.toContain(`destroy:${ownedRequest.name}`);
  });
  it("rejects an occupied port before creating resources", async () => {
    const runtime = new FakeFixtureRuntime();
    const store = new InMemoryOwnershipStore();
    runtime.unavailable.add(ownedRequest.kafkaPort);
    const lifecycle = new KafkaFixtureLifecycle(runtime, store);

    await expect(lifecycle.startOwned(ownedRequest)).rejects.toMatchObject({
      code: "PORT_IN_USE",
      unavailablePorts: [ownedRequest.kafkaPort],
    });
    expect(runtime.calls).toEqual([
      `ports:${ownedRequest.kafkaPort},${ownedRequest.oauthPort},${ownedRequest.schemaRegistryPort}`,
    ]);
    expect(store.records.size).toBe(0);
  });

  it("cleans the invocation-owned lab when readiness fails", async () => {
    const runtime = new FakeFixtureRuntime();
    const store = new InMemoryOwnershipStore();
    runtime.readinessFailure = new FixtureRuntimeError(
      "Fixture readiness failed.",
      "Kafka metadata unavailable.",
    );
    const lifecycle = new KafkaFixtureLifecycle(runtime, store);

    await expect(lifecycle.startOwned(ownedRequest)).rejects.toMatchObject({
      code: "STARTUP_FAILED",
      diagnostic: "Kafka metadata unavailable.",
    });
    expect(runtime.calls).toContain(`destroy:${ownedRequest.name}`);
    expect(store.records.size).toBe(0);
  });

  it("destroys only a recorded owned fixture and removes its record after success", async () => {
    const runtime = new FakeFixtureRuntime();
    const store = new InMemoryOwnershipStore();
    const lifecycle = new KafkaFixtureLifecycle(runtime, store);

    await expect(lifecycle.stopOwned(ownedRequest.name)).rejects.toMatchObject({
      code: "NOT_OWNED",
    });
    expect(runtime.calls).toEqual([]);

    const connection = await lifecycle.startOwned(ownedRequest);
    expect(connection).toMatchObject({
      ownership: "owned",
      schemaRegistryEndpoint: "http://127.0.0.1:18081",
    });
    await lifecycle.stopOwned(ownedRequest.name);

    expect(runtime.calls.at(-1)).toBe(`destroy:${ownedRequest.name}`);
    expect(store.records.has(ownedRequest.name)).toBe(false);
  });

  it("verifies an external lab without taking ownership", async () => {
    const runtime = new FakeFixtureRuntime();
    const store = new InMemoryOwnershipStore();
    const lifecycle = new KafkaFixtureLifecycle(runtime, store);

    const connection = await lifecycle.attachExternal({
      caPath: "/external/ca.pem",
      kafkaEndpoint: "127.0.0.1:9093",
      oauthEndpoint: "http://127.0.0.1:5000/rest-gateway/rest/api/v1/auth/token",
    });

    expect(connection.ownership).toBe("external");
    expect(runtime.calls).toEqual(["ready:external"]);
    expect(store.records.size).toBe(0);
  });
});
