import { expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEvent,
} from "../../src/features/kafka/contracts";
import {
  KafkaApplicationSession,
  type KafkaClusterServiceContext,
  type SchemaRegistryPort,
} from "../../src/features/kafka/application";
import { SchemaRegistryFacadeController } from "../../src/features/kafka/facade/schema-registry-facade";
import { AclFacadeController } from "../../src/features/kafka/facade/acl-facade";
import {
  RecordingActiveConnection,
  RecordingConnectionPort,
} from "../support/kafka-backend-facade-fixture";

const schema = {
  normalize: true,
  references: [],
  schema: '{"type":"record","name":"Order","fields":[]}',
  schemaType: "AVRO",
  subject: "orders",
  version: "latest",
} as const;
const acl = {
  host: "*",
  operation: "READ",
  patternType: "LITERAL",
  permission: "ALLOW",
  principal: "User:reader",
  resourceName: "orders",
  resourceType: "TOPIC",
} as const;

function schemaFixture(): {
  controller: SchemaRegistryFacadeController;
  events: HostEvent[];
  port: SchemaRegistryPort;
  recordActivity: ReturnType<typeof vi.fn>;
} {
  const events: HostEvent[] = [];
  const recordActivity = vi.fn();
  const port: SchemaRegistryPort = {
    checkCompatibility: () => Promise.resolve({ compatible: true, messages: [] }),
    register: () => Promise.resolve({ id: 42 }),
    delete: () => Promise.resolve([1]),
    listSubjects: () => Promise.reject(new Error("Refresh unavailable")),
    loadLatestSubject: () => Promise.reject(new Error("Refresh unavailable")),
    loadSubject: () => Promise.reject(new Error("Refresh unavailable")),
  };
  const session = {
    clusterServiceContext: (): KafkaClusterServiceContext => ({
      baseUrl: "http://registry",
      authorization: () => Promise.resolve(undefined),
    }),
    snapshot: () => ({ state: "connected", connectionName: "Cluster A" }),
  } as unknown as KafkaApplicationSession;
  const controller = new SchemaRegistryFacadeController({
    nextSequence: (): number => events.length + 1,
    now: (): Date => new Date(),
    port,
    publish: (event): void => {
      events.push(event);
    },
    recordActivity,
    session,
  });
  return { controller, events, port, recordActivity };
}

it.each(["schemas.register", "schemas.delete"] as const)(
  "retains %s acknowledgement if the refresh fails",
  async (name) => {
    const fixture = schemaFixture();
    const command: HostCommand =
      name === "schemas.register"
        ? { command: name, id: "register", version: HOST_PROTOCOL_VERSION, payload: schema }
        : {
            command: name,
            id: "delete",
            version: HOST_PROTOCOL_VERSION,
            payload: {
              target: { kind: "subject", subject: "orders" },
              mode: "soft",
              confirmation: "orders",
            },
          };
    expect(await fixture.controller.execute(command, "mutation")).toMatchObject({ ok: true });
    expect(fixture.recordActivity).toHaveBeenLastCalledWith(
      expect.objectContaining({
        outcome: "succeeded",
        severity: "warning",
        detail: expect.stringContaining("acknowledged") as unknown,
      }),
    );
    expect(fixture.events.at(-1)).toMatchObject({
      payload: { state: "stale", error: { retryable: false } },
    });
  },
);

it("retains registration acknowledgement across invalidation without publishing into a new connection", async () => {
  const fixture = schemaFixture();
  let acknowledge!: (value: { id: number }) => void;
  const register = vi.fn(
    () =>
      new Promise<{ id: number }>((resolve) => {
        acknowledge = resolve;
      }),
  );
  fixture.port.register = register;
  const request = fixture.controller.execute(
    {
      command: "schemas.register",
      id: "register",
      version: HOST_PROTOCOL_VERSION,
      payload: schema,
    },
    "mutation",
  );
  await vi.waitFor(() => expect(register).toHaveBeenCalledTimes(1));
  fixture.controller.invalidate();
  const count = fixture.events.length;
  acknowledge({ id: 42 });
  expect(await request).toMatchObject({ ok: true });
  expect(fixture.events).toHaveLength(count);
  expect(fixture.recordActivity).toHaveBeenLastCalledWith(
    expect.objectContaining({
      outcome: "succeeded",
      detail: expect.stringContaining("42") as unknown,
    }),
  );
});

it("reports an unacknowledged registration without inviting a blind retry", async () => {
  const fixture = schemaFixture();
  fixture.port.register = (): Promise<never> => Promise.reject(new Error("Response lost"));
  expect(
    await fixture.controller.execute(
      {
        command: "schemas.register",
        id: "register",
        version: HOST_PROTOCOL_VERSION,
        payload: schema,
      },
      "mutation",
    ),
  ).toMatchObject({
    ok: false,
    error: {
      retryable: false,
      recovery: expect.stringContaining("Inspect the subject") as unknown,
    },
  });
});

it("preserves an ACL acknowledgement after inventory failure", async () => {
  const events: HostEvent[] = [];
  const activity = vi.fn();
  const createAcl = vi.fn(() => Promise.resolve());
  const session = {
    createAcl,
    listAcls: () => Promise.reject(new Error("Read denied")),
    snapshot: () => ({ state: "connected", connectionName: "Cluster A" }),
  } as unknown as KafkaApplicationSession;
  const controller = new AclFacadeController({
    available: (): boolean => true,
    nextSequence: (): number => events.length + 1,
    now: (): Date => new Date(),
    publish: (event): void => {
      events.push(event);
    },
    recordActivity: activity,
    session,
  });
  expect(
    await controller.execute(
      { command: "acls.create", id: "create", version: HOST_PROTOCOL_VERSION, payload: acl },
      "mutation",
    ),
  ).toMatchObject({ ok: true });
  expect(createAcl).toHaveBeenCalledTimes(1);
  expect(activity).toHaveBeenLastCalledWith(
    expect.objectContaining({ outcome: "succeeded", severity: "warning" }),
  );
  expect(events.at(-1)).toMatchObject({ payload: { state: "stale" } });
});

it("does not claim an exact binding was reconciled just because inventory loaded", async () => {
  const activity = vi.fn();
  const session = {
    createAcl: () => Promise.resolve(),
    listAcls: () => Promise.resolve([{ ...acl, resourceName: "different-topic" }]),
    snapshot: () => ({ state: "connected", connectionName: "Cluster A" }),
  } as unknown as KafkaApplicationSession;
  const controller = new AclFacadeController({
    available: (): boolean => true,
    nextSequence: (): number => 1,
    now: (): Date => new Date(),
    publish: (): void => undefined,
    recordActivity: activity,
    session,
  });
  expect(
    await controller.execute(
      { command: "acls.create", id: "create", version: HOST_PROTOCOL_VERSION, payload: acl },
      "mutation",
    ),
  ).toMatchObject({ ok: true });
  expect(activity).toHaveBeenLastCalledWith(
    expect.objectContaining({
      severity: "warning",
      detail: expect.stringContaining("does not match") as unknown,
    }),
  );
});

it("does not cancel an in-flight mutation when a metadata read starts, and keeps its late acknowledgement", async () => {
  const connection = new RecordingActiveConnection();
  let finish!: () => void;
  let signal: AbortSignal | undefined;
  connection.alterTopicConfiguration = vi.fn(
    (_topic?: string, _changes?: unknown, _validate?: boolean, supplied?: AbortSignal) => {
      signal = supplied;
      return new Promise<void>((resolve) => {
        finish = resolve;
      });
    },
  );
  const port = new RecordingConnectionPort();
  port.openOperations.push(() => Promise.resolve(connection));
  const session = new KafkaApplicationSession(port);
  await session.connect({
    name: "Cluster A",
    brokers: ["localhost:9092"],
    tls: { enabled: false },
  });
  const mutation = session.alterTopicConfiguration(
    "orders",
    [{ name: "retention.ms", value: "1000", isSensitive: false }],
    false,
  );
  await session.listTopics();
  expect(signal?.aborted).toBe(false);
  const disconnect = session.disconnect();
  expect(signal?.aborted).toBe(true);
  finish();
  await expect(mutation).resolves.toBeUndefined();
  await disconnect;
});

it.each([409, 422])(
  "reports Registry rejection HTTP %s as a non-retryable validation failure",
  async (status) => {
    const fixture = schemaFixture();
    fixture.port.register = (): Promise<never> =>
      Promise.reject(Object.assign(new Error("rejected"), { status }));
    expect(
      await fixture.controller.execute(
        {
          command: "schemas.register",
          id: "register",
          version: HOST_PROTOCOL_VERSION,
          payload: schema,
        },
        "validation",
      ),
    ).toMatchObject({
      ok: false,
      error: {
        code: "VALIDATION",
        retryable: false,
        recovery: expect.stringContaining("dependent subjects") as unknown,
      },
    });
  },
);
