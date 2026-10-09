import { afterEach, expect, it, vi } from "vitest";

import {
  KafkaConnectionScopes,
  type ConnectionScopeContext,
} from "../../src/features/kafka/application/connection-scope";
import {
  InMemoryKafkaQueryStore,
  KafkaQueryLibrary,
} from "../../src/features/kafka/application/query-library";
import { TopicCatalogService } from "../../src/features/kafka/application/topic-catalog-service";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseCorrelatedHostResponse,
  type HostCommand,
  type KafkaTopicAnnotation,
} from "../../src/features/kafka/contracts";
import {
  command,
  createFacade,
  RecordingActiveConnection,
  RecordingConnectionPort,
} from "../support/kafka-backend-facade-fixture";

const identity = {
  clusterId: "cluster-a",
  topicId: "12345678-1234-1234-1234-123456789abc",
  topic: "orders",
};
const annotation: KafkaTopicAnnotation = {
  identity,
  description: "Customer order events",
  owner: "Payments",
  labels: ["critical"],
  links: [{ title: "Runbook", url: "https://example.com/runbook" }],
};
const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const run of cleanup.splice(0)) await run();
});

function fixture(): {
  store: InMemoryKafkaQueryStore;
  library: KafkaQueryLibrary;
  service: TopicCatalogService;
  metadata: ReturnType<typeof vi.fn<() => Promise<typeof identity & { partitions: number }>>>;
  scopes: KafkaConnectionScopes;
  disconnect(): void;
  reconnect(): void;
} {
  const store = new InMemoryKafkaQueryStore();
  const library = new KafkaQueryLibrary(store);
  const metadata = vi.fn(() => Promise.resolve({ ...identity, partitions: 1 }));
  const connection = Object.assign(new RecordingActiveConnection(), {
    describeTopicIdentity: metadata,
  });
  let context: ConnectionScopeContext | null = {
    connection,
    generation: 1,
    connectionName: "Cluster A",
  };
  const scopes = new KafkaConnectionScopes(() => context);
  const service = new TopicCatalogService(library, () => scopes.topicCatalog());
  return {
    store,
    library,
    service,
    metadata,
    scopes,
    disconnect: (): void => {
      context = null;
    },
    reconnect: (): void => {
      context = { connection, generation: 2, connectionName: "Cluster A" };
    },
  };
}
function gate(): { promise: Promise<void>; resolve(): void } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  cleanup.push(() => {
    resolve();
    return Promise.resolve();
  });
  return { promise, resolve };
}

it("stores notes against verified metadata, and locally reviews/removes them while disconnected", async () => {
  const f = fixture();
  expect(await f.service.load(identity.topic)).toMatchObject({ identity, annotation: null });
  expect(await f.service.put(annotation, null)).toMatchObject({ annotation });
  expect(f.metadata).toHaveBeenCalledWith(identity.topic);
  f.disconnect();
  expect(await f.library.listTopics()).toMatchObject({ topics: [annotation] });
  expect(await f.library.deleteTopic(identity, annotation)).toMatchObject({ annotation: null });
  await expect(f.service.put(annotation, null)).rejects.toThrow("Connect to Kafka");
});

it.each([{ clusterId: "cluster-b" }, { topicId: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" }])(
  "refuses notes reassociation after identity replacement %j",
  async (replacement) => {
    const f = fixture();
    await f.service.put(annotation, null);
    f.metadata.mockResolvedValue({ ...identity, ...replacement, partitions: 1 });
    expect(await f.service.load(identity.topic)).toMatchObject({
      identity: replacement,
      annotation: null,
    });
    await expect(
      f.service.put({ ...annotation, description: "Do not attach" }, annotation),
    ).rejects.toThrow("different cluster or was recreated");
    expect(await f.library.listTopics()).toMatchObject({ topics: [annotation] });
  },
);

it("suppresses late metadata after same-adapter reconnection", async () => {
  const f = fixture();
  const pending = gate();
  f.metadata.mockImplementation(async () => {
    await pending.promise;
    return { ...identity, partitions: 1 };
  });
  const load = f.service.load(identity.topic);
  const rejected = expect(load).rejects.toThrow("could not be verified");
  f.reconnect();
  pending.resolve();
  await rejected;
  expect((await f.library.listTopics()).topics).toEqual([]);
});

it("checks connection authority after the storage queue and before admitting a write", async () => {
  const f = fixture();
  const pending = gate();
  const read = vi.spyOn(f.store, "load").mockImplementationOnce(async () => {
    await pending.promise;
    return { queries: [], topics: [] };
  });
  const write = vi.spyOn(f.store, "commit");
  const put = f.service.put(annotation, null);
  const rejected = expect(put).rejects.toThrow("connection changed");
  await vi.waitFor(() => expect(read).toHaveBeenCalledOnce());
  f.disconnect();
  pending.resolve();
  await rejected;
  expect(write).not.toHaveBeenCalled();
});

it("keeps the admitted local commit receipt when the connection changes during disk I/O", async () => {
  const f = fixture();
  const pending = gate();
  const commit = f.store.commit.bind(f.store);
  const write = vi.spyOn(f.store, "commit").mockImplementation(async (state) => {
    await pending.promise;
    await commit(state);
  });
  const put = f.service.put(annotation, null);
  await vi.waitFor(() => expect(write).toHaveBeenCalledOnce());
  f.reconnect();
  pending.resolve();
  expect(await put).toMatchObject({ identity, annotation });
  expect(await f.library.listTopics()).toMatchObject({ topics: [annotation] });
});

it.each([
  { clusterId: "", topicId: identity.topicId, partitions: 1 },
  { clusterId: identity.clusterId, topicId: "00000000-0000-0000-0000-000000000000", partitions: 1 },
])("never substitutes a name for unsupported identity %j", async (metadata) => {
  const f = fixture();
  f.metadata.mockResolvedValue({ ...identity, ...metadata });
  await expect(f.service.put(annotation, null)).rejects.toThrow("could not be verified");
  expect((await f.library.listTopics()).topics).toEqual([]);
});

it("returns actionable metadata errors without disclosing adapter details", async () => {
  const f = fixture();
  f.metadata.mockRejectedValue(new Error("denied password=secret-token user=internal-host"));
  await expect(f.service.load(identity.topic)).rejects.toThrow("Describe permissions");
  await expect(f.service.load(identity.topic)).rejects.not.toThrow("secret-token");
});

it("wires catalog commands through the composed host and preserves CAS conflicts", async () => {
  const port = new RecordingConnectionPort();
  const metadata = vi.fn(() => Promise.resolve({ ...identity, partitions: 1 }));
  port.openOperations.push(() =>
    Promise.resolve(
      Object.assign(new RecordingActiveConnection(), { describeTopicIdentity: metadata }),
    ),
  );
  const facade = createFacade(port);
  cleanup.push(() => facade.shutdown());
  const execute = async (
    name: HostCommand["command"],
    payload: unknown,
  ): ReturnType<typeof facade.execute> => {
    const input = parseHostCommand({
      command: name,
      payload,
      id: name,
      version: HOST_PROTOCOL_VERSION,
    });
    return parseCorrelatedHostResponse(await facade.execute(input), input);
  };
  expect(await execute("catalog.load", { topic: identity.topic })).toMatchObject({
    ok: false,
    error: { code: "QUERY_UNAVAILABLE" },
  });
  expect(await facade.execute(command("connection.connect", "connect"))).toMatchObject({
    ok: true,
  });
  expect(await execute("catalog.put", { annotation, expected: null })).toMatchObject({
    ok: true,
    result: { snapshot: { annotation } },
  });
  expect(
    await execute("catalog.put", { annotation: { ...annotation, owner: "stale" }, expected: null }),
  ).toMatchObject({ ok: false, error: { code: "QUERY_UNAVAILABLE" } });
  expect(await execute("catalog.list", {})).toMatchObject({
    ok: true,
    result: { snapshot: { topics: [annotation] } },
  });
  expect(await facade.execute(command("connection.disconnect", "disconnect"))).toMatchObject({
    ok: true,
  });
  expect(await execute("catalog.delete", { identity, expected: annotation })).toMatchObject({
    ok: true,
    result: { snapshot: { annotation: null } },
  });
});

it("rejects incomplete CAS inputs, unrelated identities and mismatched host receipts", () => {
  const base = { version: HOST_PROTOCOL_VERSION, id: "catalog" };
  for (const payload of [
    { annotation },
    { annotation, expected: { ...annotation, identity: { ...identity, clusterId: "other" } } },
    { annotation, expected: null, credential: "secret" },
  ])
    expect(() => parseHostCommand({ ...base, command: "catalog.put", payload })).toThrow();
  const put = parseHostCommand({
    ...base,
    command: "catalog.put",
    payload: { annotation, expected: null },
  });
  for (const snapshot of [
    { durability: "durable", identity, annotation: null },
    { durability: "durable", identity, annotation: { ...annotation, owner: "different" } },
  ])
    expect(() =>
      parseCorrelatedHostResponse(
        {
          ...base,
          command: "catalog.put",
          ok: true,
          result: { correlationId: "result", snapshot },
        },
        put,
      ),
    ).toThrow("must match");
});
