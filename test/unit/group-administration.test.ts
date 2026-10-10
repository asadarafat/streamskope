import { afterEach, expect, it, vi } from "vitest";
import { Admin, AclOperations } from "@platformatic/kafka";

import { GroupAdministrationService } from "../../src/features/kafka/application/group-administration-service";
import { KafkaConnectionScopes } from "../../src/features/kafka/application/connection-scope";
import type {
  GroupAdministrationSnapshot,
  GroupAdministrationOutcome,
} from "../../src/features/kafka/contracts/group-administration";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
  parseCorrelatedHostResponse,
} from "../../src/features/kafka/contracts";
import { RecordingActiveConnection } from "../support/kafka-backend-facade-fixture";
import { PlatformaticGroupAdministration } from "../../src/features/kafka/engine/platformatic-group-administration";
import { PlatformaticOffsetReset } from "../../src/features/kafka/engine/platformatic-offset-reset";

const baseline: GroupAdministrationSnapshot = {
  groupId: "orders",
  clusterId: "owned",
  protocolType: "",
  state: "Empty",
  members: 0,
  offsetsSha256: "a".repeat(64),
  deletePermission: "allowed",
  deleteSupported: true,
};
const receipt: GroupAdministrationOutcome = {
  groupId: "orders",
  state: "acknowledged",
  verification: "verified",
  cleanup: "confirmed",
  detail: "Actual fixture receipt",
};
function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function setup(): {
  service: GroupAdministrationService;
  snapshot: GroupAdministrationSnapshot;
  read: ReturnType<typeof vi.fn>;
  write: ReturnType<typeof vi.fn>;
  revoke(): void;
  expire(): void;
} {
  let generation = 0,
    now = 0;
  const snapshot = structuredClone(baseline);
  const read = vi.fn(() => Promise.resolve(structuredClone(snapshot))),
    write = vi.fn(() => Promise.resolve(receipt));
  const connection = Object.assign(new RecordingActiveConnection(), {
    groupAdministrationSnapshot: read,
    deleteConsumerGroup: write,
  });
  const scopes = new KafkaConnectionScopes(() => ({
    connection,
    generation,
    connectionName: "Review fixture",
  }));
  return {
    service: new GroupAdministrationService(
      () => scopes.groupAdministration(),
      () => now,
    ),
    snapshot,
    read,
    write,
    revoke: (): void => {
      generation++;
    },
    expire: (): void => {
      now = 120_000;
    },
  };
}
afterEach(() => vi.restoreAllMocks());
it("requires exact deletion scope and coalesces one admitted attempt", async () => {
  const f = setup(),
    review = await f.service.review({ groupId: "orders" });
  expect(f.write).not.toHaveBeenCalled();
  await expect(f.service.apply(review.planId, "orders")).rejects.toThrow("exact");
  const first = f.service.apply(review.planId, review.confirmation),
    duplicate = f.service.apply(review.planId, review.confirmation);
  expect(first).toBe(duplicate);
  expect(await first).toEqual(receipt);
  expect(f.write).toHaveBeenCalledOnce();
});
it.each(["active", "members", "offsets", "cluster", "denied", "unsupported"])(
  "refuses changed %s before dispatch",
  async (kind) => {
    const f = setup(),
      review = await f.service.review({ groupId: "orders" });
    Object.assign(
      f.snapshot,
      kind === "active"
        ? { state: "Stable" }
        : kind === "members"
          ? { members: 1 }
          : kind === "offsets"
            ? { offsetsSha256: "b".repeat(64) }
            : kind === "cluster"
              ? { clusterId: "different" }
              : kind === "denied"
                ? { deletePermission: "denied" }
                : { deleteSupported: false },
    );
    expect(await f.service.apply(review.planId, review.confirmation)).toMatchObject({
      state: "unsent",
    });
    expect(f.write).not.toHaveBeenCalled();
  },
);
it.each(["revoke", "expire"] as const)("refuses %s reviews", async (action) => {
  const f = setup(),
    review = await f.service.review({ groupId: "orders" });
  f[action]();
  await expect(f.service.apply(review.planId, review.confirmation)).rejects.toThrow();
  expect(f.write).not.toHaveBeenCalled();
});
it("keeps the original admitted deletion receipt after reconnection", async () => {
  const f = setup(),
    review = await f.service.review({ groupId: "orders" }),
    admitted = deferred<GroupAdministrationOutcome>();
  f.write.mockReturnValueOnce(admitted.promise);
  const pending = f.service.apply(review.planId, review.confirmation);
  await vi.waitFor(() => expect(f.write).toHaveBeenCalledOnce());
  f.revoke();
  admitted.resolve(receipt);
  expect(await pending).toEqual(receipt);
  expect(f.service.apply(review.planId, review.confirmation)).toBe(pending);
});
it("rejects stale reads and retains unknown dispatch without replay", async () => {
  const f = setup(),
    review = await f.service.review({ groupId: "orders" });
  f.write.mockRejectedValueOnce(new Error("Reply lost"));
  const first = await f.service.apply(review.planId, review.confirmation);
  expect(first).toMatchObject({ state: "unknown", cleanup: "unresolved" });
  expect(await f.service.apply(review.planId, review.confirmation)).toEqual(first);
  expect(f.write).toHaveBeenCalledOnce();
});
it("validates closed group commands and correlates the selected group", async () => {
  const review = await setup().service.review({ groupId: "orders" }),
    command = parseHostCommand({
      command: "consumerGroups.delete.review",
      id: "r",
      version: HOST_PROTOCOL_VERSION,
      payload: { groupId: "orders" },
    });
  const response = {
    command: command.command,
    id: command.id,
    version: command.version,
    ok: true,
    result: { correlationId: "c", review },
  };
  expect(parseCorrelatedHostResponse(response, command).ok).toBe(true);
  expect(() =>
    parseHostCommand({ ...command, payload: { groupId: "orders", force: true } }),
  ).toThrow();
  expect(() =>
    parseHostCommandResponse({
      ...response,
      result: { correlationId: "c", review: { ...review, confirmation: "DELETE GROUP different" } },
    }),
  ).toThrow();
  expect(() =>
    parseCorrelatedHostResponse(
      {
        ...response,
        result: {
          correlationId: "c",
          review: {
            ...review,
            baseline: { ...review.baseline, groupId: "different" },
            confirmation: "DELETE GROUP different",
          },
        },
      },
      command,
    ),
  ).toThrow("submitted group");
});
const resetTarget = { topic: "events", partition: 0, offset: "2" };
const resetBaseline: import("../../src/features/kafka/contracts/offset-reset").OffsetResetSnapshot =
  {
    clusterId: "owned",
    topics: [{ topic: "events", topicId: "11111111-1111-1111-1111-111111111111" }],
    inactive: true,
    state: "Empty",
    groupRead: "allowed",
    partitions: [{ ...resetTarget, before: "0", low: "0", high: "10", replayUpperBound: "0" }],
  };
function resetReads(): Awaited<ReturnType<Admin["metadata"]>> {
  vi.spyOn(Admin.prototype, "listGroups").mockResolvedValue(
    new Map([["orders", { id: "orders", protocolType: "", state: "Empty" }]]),
  );
  const metadata: Awaited<ReturnType<Admin["metadata"]>> = {
    id: "owned",
    topics: new Map([
      [
        "events",
        { id: resetBaseline.topics[0]!.topicId, partitions: [], partitionsCount: 1, lastUpdate: 0 },
      ],
    ]),
    brokers: new Map(),
    controllerId: 1,
    lastUpdate: 0,
  };
  vi.spyOn(Admin.prototype, "metadata").mockResolvedValue(metadata);
  vi.spyOn(Admin.prototype, "describeGroups").mockResolvedValue(
    new Map([
      [
        "orders",
        {
          id: "orders",
          protocolType: "",
          state: "Empty",
          protocol: "",
          members: new Map(),
          authorizedOperations: 1 << AclOperations.READ,
        },
      ],
    ]),
  );
  vi.spyOn(Admin.prototype, "listConsumerGroupOffsets").mockResolvedValue([
    {
      groupId: "orders",
      topics: [
        {
          name: "events",
          partitions: [
            { partitionIndex: 0, committedOffset: 0n, committedLeaderEpoch: 0, metadata: null },
          ],
        },
      ],
    },
  ]);
  vi.spyOn(Admin.prototype, "listOffsets").mockImplementation((options) =>
    Promise.resolve([
      {
        name: "events",
        partitions: [
          {
            partitionIndex: 0,
            offset: options.topics[0]!.partitions[0]!.timestamp === -2n ? 0n : 10n,
            timestamp: 0n,
            leaderEpoch: 0,
          },
        ],
      },
    ]),
  );
  return metadata;
}
const clientInput = {
  brokers: ["127.0.0.1:1"],
  tlsEnabled: false as const,
  operationTimeoutMs: 5000,
};
it("keeps an actual offset ACK after revocation and makes connection close join the original client", async () => {
  const lifetime = new AbortController(),
    adapter = new PlatformaticOffsetReset(clientInput, lifetime.signal),
    ack = deferred<void>(),
    cleanup = deferred<void>();
  resetReads();
  const writes = vi
    .spyOn(Admin.prototype, "alterConsumerGroupOffsets")
    .mockReturnValue(ack.promise);

  const close = vi.spyOn(Admin.prototype, "close").mockReturnValue(cleanup.promise);
  const pending = adapter.apply("orders", resetTarget, resetBaseline);
  await vi.waitFor(() => expect(writes).toHaveBeenCalledOnce());
  lifetime.abort();
  const draining = adapter.close();
  let drained = false;
  void draining.then(() => {
    drained = true;
  });
  await Promise.resolve();
  expect(close).not.toHaveBeenCalled();
  expect(drained).toBe(false);
  ack.resolve();
  await vi.waitFor(() => expect(close).toHaveBeenCalledOnce());
  expect(drained).toBe(false);
  cleanup.resolve();
  expect(await pending).toMatchObject({
    state: "acknowledged",
    verified: false,
    cleanup: "confirmed",
  });
  await draining;
});
it("retains ACK despite failed close, fences new resets and retries original cleanup on connection drain", async () => {
  const adapter = new PlatformaticOffsetReset(clientInput, new AbortController().signal);
  resetReads();
  const writes = vi.spyOn(Admin.prototype, "alterConsumerGroupOffsets").mockResolvedValue();

  const close = vi
    .spyOn(Admin.prototype, "close")
    .mockRejectedValueOnce(new Error("Unresolved close"))
    .mockResolvedValue();
  const target = { topic: "events", partition: 0, offset: "2" };
  expect(await adapter.apply("orders", target, resetBaseline)).toMatchObject({
    state: "acknowledged",
    cleanup: "unresolved",
  });
  expect(await adapter.apply("orders", target, resetBaseline)).toMatchObject({
    state: "unsent",
    cleanup: "unresolved",
  });
  expect(writes).toHaveBeenCalledOnce();
  await adapter.close();
  expect(close).toHaveBeenCalledTimes(2);
});
it("group connection drain owns an in-flight inspection even after revocation", async () => {
  const lifetime = new AbortController(),
    adapter = new PlatformaticGroupAdministration(clientInput, lifetime.signal);
  const groups = deferred<Awaited<ReturnType<Admin["listGroups"]>>>();
  vi.spyOn(Admin.prototype, "listGroups").mockReturnValue(groups.promise);
  const close = vi.spyOn(Admin.prototype, "close").mockResolvedValue();
  const pending = adapter.snapshot("orders");
  const refused = expect(pending).rejects.toThrow();
  lifetime.abort();
  const draining = adapter.close();
  await Promise.resolve();
  expect(close).not.toHaveBeenCalled();
  groups.resolve(new Map());
  await refused;
  await draining;
  expect(close).toHaveBeenCalledOnce();
});

it("refuses a topic replaced on the exact mutation client after the application preview", async () => {
  const current = resetReads();
  current.topics.get("events")!.id = "22222222-2222-2222-2222-222222222222";
  const writes = vi.spyOn(Admin.prototype, "alterConsumerGroupOffsets").mockResolvedValue();
  vi.spyOn(Admin.prototype, "close").mockResolvedValue();
  const adapter = new PlatformaticOffsetReset(clientInput, new AbortController().signal);
  expect(await adapter.apply("orders", resetTarget, resetBaseline)).toMatchObject({
    state: "unsent",
    cleanup: "confirmed",
  });
  expect(writes).not.toHaveBeenCalled();
  await adapter.close();
});

it.each(["dead", "recreated", "denied"] as const)(
  "uses exact coordinator and offset readback after deletion: %s",
  async (kind) => {
    resetReads();
    vi.spyOn(Admin.prototype, "listApis").mockResolvedValue([
      { apiKey: 42, minVersion: 0, maxVersion: 2, name: "DeleteGroups" },
    ]);
    const empty = {
      id: "orders",
      protocolType: "",
      protocol: "",
      state: "Empty" as const,
      members: new Map(),
      authorizedOperations: -1,
    };
    const groups = vi
      .spyOn(Admin.prototype, "describeGroups")
      .mockResolvedValue(new Map([["orders", empty]]));
    const offsets = vi
      .spyOn(Admin.prototype, "listConsumerGroupOffsets")
      .mockResolvedValue([{ groupId: "orders", topics: [] }]);
    const writes = vi.spyOn(Admin.prototype, "deleteGroups").mockResolvedValue();
    vi.spyOn(Admin.prototype, "close").mockResolvedValue();
    const adapter = new PlatformaticGroupAdministration(clientInput, new AbortController().signal),
      review = await adapter.snapshot("orders");
    groups
      .mockResolvedValueOnce(new Map([["orders", empty]]))
      .mockResolvedValueOnce(
        new Map([["orders", { ...empty, state: kind === "recreated" ? "Empty" : "Dead" }]]),
      );
    if (kind === "denied")
      offsets
        .mockResolvedValueOnce([{ groupId: "orders", topics: [] }])
        .mockRejectedValueOnce(new Error("Offset read denied"));
    const result = await adapter.apply(review);
    expect(result).toMatchObject({
      state: "acknowledged",
      verification:
        kind === "dead" ? "verified" : kind === "recreated" ? "different" : "unavailable",
      cleanup: "confirmed",
    });
    expect(writes).toHaveBeenCalledOnce();
    await adapter.close();
  },
);
