import { afterEach, expect, it, vi, type Mock, type MockInstance } from "vitest";
import { Admin, ResponseError } from "@platformatic/kafka";

import { ClientQuotaService } from "../../src/features/kafka/application/client-quota-service";
import { KafkaConnectionScopes } from "../../src/features/kafka/application/connection-scope";
import {
  parseClientQuotaInput,
  parseClientQuotaEntity,
  clientQuotaExpected,
  type ClientQuotaEntity,
  type ClientQuotaInput,
  type ClientQuotaSnapshot,
  type ClientQuotaOutcome,
} from "../../src/features/kafka/contracts/client-quotas";
import { PlatformaticClientQuotas } from "../../src/features/kafka/engine/platformatic-client-quotas";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
  parseCorrelatedHostResponse,
} from "../../src/features/kafka/contracts";
import { RecordingActiveConnection } from "../support/kafka-backend-facade-fixture";

const entity: ClientQuotaEntity = [{ type: "user", name: "quota-user" }];
const input: ClientQuotaInput = { entity, changes: [{ key: "producer_byte_rate", value: 512 }] };
const baseline: ClientQuotaSnapshot = {
  entity,
  clusterId: "owned",
  alterSupported: true,
  values: [
    { key: "consumer_byte_rate", value: 900 },
    { key: "producer_byte_rate", value: 1000 },
  ],
};
const receipt: ClientQuotaOutcome = {
  input,
  state: "acknowledged",
  verification: "verified",
  cleanup: "confirmed",
  observed: [
    { key: "consumer_byte_rate", value: 900 },
    { key: "producer_byte_rate", value: 512 },
  ],
  detail: "Independent exact receipt",
};
function deferred<T>(): { promise: Promise<T>; resolve(value: T): void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function setup(): {
  snapshot: ClientQuotaSnapshot;
  read: Mock<() => Promise<ClientQuotaSnapshot>>;
  write: Mock<() => Promise<ClientQuotaOutcome>>;
  service: ClientQuotaService;
  revoke(): void;
  expire(): void;
} {
  let generation = 0,
    now = 0;
  const snapshot = structuredClone(baseline);
  const read = vi.fn(() => Promise.resolve(structuredClone(snapshot))),
    write = vi.fn(() => Promise.resolve(structuredClone(receipt)));
  const connection = Object.assign(new RecordingActiveConnection(), {
    clientQuotaSnapshot: read,
    applyClientQuotas: write,
  });
  const scopes = new KafkaConnectionScopes(() => ({
    connection,
    generation,
    connectionName: "quota fixture",
  }));
  return {
    snapshot,
    read,
    write,
    service: new ClientQuotaService(
      () => scopes.clientQuotas(),
      () => now,
    ),
    revoke: (): void => {
      generation++;
    },
    expire: (): void => {
      now = 120000;
    },
  };
}
afterEach(() => vi.restoreAllMocks());
it("distinguishes named and default entities; refuses zero as a set and removes only explicit keys", () => {
  expect(parseClientQuotaEntity([{ type: "user", name: null }])).not.toEqual(entity);
  expect(parseClientQuotaEntity([{ type: "user", name: "(default)" }])).not.toEqual([
    { type: "user", name: null },
  ]);
  expect(() =>
    parseClientQuotaInput({ entity, changes: [{ key: "producer_byte_rate", value: 0 }] }),
  ).toThrow("positive");
  expect(
    parseClientQuotaInput({ entity, changes: [{ key: "request_percentage", value: 0.5 }] })
      .changes[0]!.value,
  ).toBe(0.5);
  expect(
    clientQuotaExpected(baseline.values, [{ key: "producer_byte_rate", value: null }]),
  ).toEqual([{ key: "consumer_byte_rate", value: 900 }]);
  expect(clientQuotaExpected(baseline.values, input.changes)).toEqual(receipt.observed);
});
it.each([
  { entity: [] },
  { entity: [{ type: "ip", name: "127.0.0.1" }] },
  { entity: [{ type: "user", name: "" }] },
  { entity: [{ type: "user", name: "a\nb" }] },
  { entity: [{ type: "user", name: "\ud800" }] },
  {
    entity: [
      { type: "user", name: null },
      { type: "user", name: "other" },
    ],
  },
  { changes: [] },
  { changes: [{ key: "producer_byte_rate", value: 0 }] },
  { changes: [{ key: "consumer_byte_rate", value: 0.5 }] },
  { changes: [{ key: "producer_byte_rate", value: Number.MAX_SAFE_INTEGER + 1 }] },
  { changes: [{ key: "producer_byte_rate", value: NaN }] },
  { changes: [{ key: "producer_byte_rate", value: Infinity }] },
  { changes: [{ key: "producer_byte_rate", value: -1 }] },
  { changes: [{ key: "connection_creation_rate", value: 2 }] },
  {
    changes: [
      { key: "producer_byte_rate", value: 2 },
      { key: "producer_byte_rate", value: null },
    ],
  },
  { unexpected: "not-admitted" },
])("refuses malformed or unsupported exact quota input %#", (patch) => {
  expect(() => parseClientQuotaInput({ ...input, ...patch })).toThrow();
});
it("inspect/review send no write; exact confirmation coalesces one attempt", async () => {
  const f = setup();
  expect(await f.service.inspect(entity)).toEqual(baseline);
  const review = await f.service.review(input);
  expect(f.write).not.toHaveBeenCalled();
  await expect(f.service.apply(review.planId, "quota-user")).rejects.toThrow("exact");
  const first = f.service.apply(review.planId, review.confirmation);
  expect(f.service.apply(review.planId, review.confirmation)).toBe(first);
  expect(await first).toEqual(receipt);
  expect(f.write).toHaveBeenCalledOnce();
});
it.each(["cluster", "values", "capability", "entity"])(
  "refuses changed %s before dispatch",
  async (kind) => {
    const f = setup(),
      review = await f.service.review(input);
    Object.assign(
      f.snapshot,
      kind === "cluster"
        ? { clusterId: "replacement" }
        : kind === "values"
          ? { values: [{ key: "producer_byte_rate", value: 10 }] }
          : kind === "capability"
            ? { alterSupported: false }
            : { entity: [{ type: "user", name: null }] },
    );
    expect(await f.service.apply(review.planId, review.confirmation)).toMatchObject({
      state: "unsent",
    });
    expect(f.write).not.toHaveBeenCalled();
  },
);
it.each(["revoke", "expire"] as const)("refuses %s reviews", async (kind) => {
  const f = setup(),
    review = await f.service.review(input);
  f[kind]();
  await expect(f.service.apply(review.planId, review.confirmation)).rejects.toThrow();
  expect(f.write).not.toHaveBeenCalled();
});
it("retains an admitted actual receipt after revocation and rejects mismatched/lost replies without resending", async () => {
  const f = setup(),
    pending = deferred<ClientQuotaOutcome>(),
    review = await f.service.review(input);
  f.write.mockImplementation(() => pending.promise);
  const result = f.service.apply(review.planId, review.confirmation);
  await vi.waitFor(() => expect(f.write).toHaveBeenCalledOnce());
  f.revoke();
  pending.resolve(receipt);
  expect(await result).toEqual(receipt);
  const bad = setup(),
    badReview = await bad.service.review(input);
  bad.write.mockResolvedValue({
    ...receipt,
    input: { ...input, entity: [{ type: "user", name: "foreign" }] },
  });
  expect(await bad.service.apply(badReview.planId, badReview.confirmation)).toMatchObject({
    state: "unknown",
    cleanup: "unresolved",
  });
  await bad.service.apply(badReview.planId, badReview.confirmation);
  expect(bad.write).toHaveBeenCalledOnce();
});
it("refuses unsupported and unchanged quota reviews and fences a stale inspection", async () => {
  const f = setup();
  await expect(
    f.service.review({ entity, changes: [{ key: "producer_byte_rate", value: 1000 }] }),
  ).rejects.toThrow("already");
  Object.assign(f.snapshot, { alterSupported: false });
  await expect(f.service.review(input)).rejects.toThrow("unavailable");
  const pending = deferred<ClientQuotaSnapshot>();
  f.read.mockImplementation(() => pending.promise);
  const inspection = f.service.inspect(entity);
  f.revoke();
  pending.resolve(baseline);
  await expect(inspection).rejects.toThrow();
});
it("validates closed wire responses and correlates inspection/review with the submitted exact entity", async () => {
  const f = setup(),
    review = await f.service.review(input);
  const command = parseHostCommand({
    command: "quotas.change.review",
    id: "quota",
    version: HOST_PROTOCOL_VERSION,
    payload: input,
  });
  const response = parseHostCommandResponse({
    command: command.command,
    id: command.id,
    version: command.version,
    ok: true,
    result: { correlationId: "c", review },
  });
  expect(parseCorrelatedHostResponse(response, command)).toEqual(response);
  expect(() =>
    parseHostCommand({ ...command, payload: { ...input, rawCredentials: "forbidden" } }),
  ).toThrow("not declared");
  expect(() =>
    parseHostCommandResponse({
      ...response,
      result: { correlationId: "c", review: { ...review, confirmation: "other" } },
    }),
  ).toThrow();
  const other = parseHostCommand({
    ...command,
    payload: { ...input, entity: [{ type: "user", name: "foreign" }] },
  });
  expect(() => parseCorrelatedHostResponse(response, other)).toThrow("submitted exact");
});
function adapterReads(): {
  metadata: Awaited<ReturnType<Admin["metadata"]>>;
  apis: MockInstance<Admin["listApis"]>;
  read: MockInstance<Admin["describeClientQuotas"]>;
  close: MockInstance<Admin["close"]>;
} {
  const metadata: Awaited<ReturnType<Admin["metadata"]>> = {
    id: "owned",
    topics: new Map(),
    brokers: new Map(),
    controllerId: 1,
    lastUpdate: 0,
  };
  vi.spyOn(Admin.prototype, "metadata").mockResolvedValue(metadata);
  const apis = vi.spyOn(Admin.prototype, "listApis").mockResolvedValue([
    { apiKey: 48, name: "DescribeClientQuotas", minVersion: 0, maxVersion: 1 },
    { apiKey: 49, name: "AlterClientQuotas", minVersion: 0, maxVersion: 1 },
  ]);
  const read = vi.spyOn(Admin.prototype, "describeClientQuotas").mockResolvedValue([
    {
      entity: [{ entityType: "user", entityName: "quota-user" }],
      values: [
        { key: "consumer_byte_rate", value: 900 },
        { key: "producer_byte_rate", value: 1000 },
      ],
    },
  ]);
  const close = vi.spyOn(Admin.prototype, "close").mockResolvedValue();
  return { metadata, apis, read, close };
}
const client = { brokers: ["127.0.0.1:1"], tlsEnabled: false as const, operationTimeoutMs: 5000 };
it("requires actual DescribeClientQuotas support and an exact strict entity; empty is not inferred from unavailable data", async () => {
  const f = adapterReads(),
    owner = new PlatformaticClientQuotas(client, new AbortController().signal);
  expect(await owner.snapshot(entity)).toEqual(baseline);
  expect(f.read).toHaveBeenLastCalledWith({
    strict: true,
    components: [{ entityType: "user", matchType: 0, match: "quota-user" }],
  });
  f.read.mockResolvedValue([{ entity: [{ entityType: "user", entityName: null }], values: [] }]);
  await expect(owner.snapshot(entity)).rejects.toThrow("exact");
  f.apis.mockResolvedValue([]);
  await expect(owner.snapshot(entity)).rejects.toThrow("unavailable");
  await owner.close();
});
it("interprets an actual SDK per-entity rejection without leaking server text or inventing an ACK", async () => {
  adapterReads();
  const response = {
    throttleTimeMs: 0,
    entries: [
      {
        errorCode: 31,
        errorMessage: "credential-sentinel",
        entity: [{ entityType: "user", entityName: "quota-user" }],
      },
    ],
  };
  const write = vi
    .spyOn(Admin.prototype, "alterClientQuotas")
    .mockRejectedValue(
      new ResponseError(49, 1, { "/entries/0": [31, "credential-sentinel"] }, response),
    );
  const owner = new PlatformaticClientQuotas(client, new AbortController().signal);
  const result = await owner.apply(input, baseline);
  expect(result).toMatchObject({
    state: "rejected",
    cleanup: "confirmed",
    verification: "unavailable",
  });
  expect(JSON.stringify(result)).not.toContain("credential-sentinel");
  expect(write).toHaveBeenCalledOnce();
  await owner.close();
});
it.each(["empty", "foreign", "lost", "server-unknown"] as const)(
  "retains %s mutation replies as unknown without retry",
  async (kind) => {
    adapterReads();
    const write = vi.spyOn(Admin.prototype, "alterClientQuotas");
    if (kind === "lost") write.mockRejectedValue(new Error("Reply lost after send"));
    else if (kind === "server-unknown")
      write.mockRejectedValue(
        new ResponseError(
          49,
          1,
          { "/entries/0": [-1, "credential-sentinel"] },
          {
            throttleTimeMs: 0,
            entries: [
              {
                errorCode: -1,
                errorMessage: "credential-sentinel",
                entity: [{ entityType: "user", entityName: "quota-user" }],
              },
            ],
          },
        ),
      );
    else
      write.mockResolvedValue(
        kind === "empty"
          ? []
          : [
              {
                errorCode: 0,
                errorMessage: null,
                entity: [{ entityType: "user", entityName: "foreign" }],
              },
            ],
      );
    const owner = new PlatformaticClientQuotas(client, new AbortController().signal);
    expect(await owner.apply(input, baseline)).toMatchObject({
      state: "unknown",
      verification: "unavailable",
    });
    expect(write).toHaveBeenCalledOnce();
    await owner.close();
  },
);
it("retains a late ACK, makes close join the original mutation, and fences unresolved original cleanup", async () => {
  const f = adapterReads(),
    lifetime = new AbortController(),
    pending = deferred<Awaited<ReturnType<Admin["alterClientQuotas"]>>>();
  const write = vi
    .spyOn(Admin.prototype, "alterClientQuotas")
    .mockImplementation(() => pending.promise);
  const owner = new PlatformaticClientQuotas(client, lifetime.signal);
  const result = owner.apply(input, baseline);
  await vi.waitFor(() => expect(write).toHaveBeenCalledOnce());
  lifetime.abort();
  let closed = false;
  const drain = owner.close().then(() => {
    closed = true;
  });
  await Promise.resolve();
  expect(closed).toBe(false);
  pending.resolve([
    {
      errorCode: 0,
      errorMessage: null,
      entity: [{ entityType: "user", entityName: "quota-user" }],
    },
  ]);
  expect(await result).toMatchObject({
    state: "acknowledged",
    verification: "unavailable",
    cleanup: "confirmed",
  });
  await drain;
  expect(f.close).toHaveBeenCalledOnce();
  vi.restoreAllMocks();
  const second = adapterReads();
  second.close.mockRejectedValueOnce(new Error("Original close failure"));
  vi.spyOn(Admin.prototype, "alterClientQuotas").mockResolvedValue([
    {
      errorCode: 0,
      errorMessage: null,
      entity: [{ entityType: "user", entityName: "quota-user" }],
    },
  ]);
  const blocked = new PlatformaticClientQuotas(client, new AbortController().signal);
  expect(await blocked.apply(input, baseline)).toMatchObject({
    state: "acknowledged",
    cleanup: "unresolved",
  });
  expect(await blocked.apply(input, baseline)).toMatchObject({
    state: "unsent",
    cleanup: "unresolved",
  });
  await blocked.close();
  expect(second.close).toHaveBeenCalledTimes(2);
});
it("rechecks the exact mutation client's baseline and keeps ACK separate from differing readback", async () => {
  const f = adapterReads(),
    write = vi.spyOn(Admin.prototype, "alterClientQuotas").mockResolvedValue([
      {
        errorCode: 0,
        errorMessage: null,
        entity: [{ entityType: "user", entityName: "quota-user" }],
      },
    ]);
  const owner = new PlatformaticClientQuotas(client, new AbortController().signal);
  Object.assign(f.metadata, { id: "replacement" });
  expect(await owner.apply(input, baseline)).toMatchObject({ state: "unsent" });
  expect(write).not.toHaveBeenCalled();
  Object.assign(f.metadata, { id: "owned" });
  expect(await owner.apply(input, baseline)).toMatchObject({
    state: "acknowledged",
    verification: "different",
    observed: baseline.values,
  });
  expect(write).toHaveBeenCalledOnce();
  await owner.close();
});
