import { expect, it, vi } from "vitest";
import { ProtocolError } from "@platformatic/kafka";

import { TopicAdministrationService } from "../../src/features/kafka/application/topic-administration-service";
import type {
  TopicAdministrationScope,
  MutationDispatch,
} from "../../src/features/kafka/application/connection-scope";
import { PlatformaticTopicAdministration } from "../../src/features/kafka/engine/platformatic-topic-administration";
import {
  parseTopicAdministrationInput,
  parseTopicAdministrationReview,
  type TopicAdministrationInput,
  type TopicAdministrationSnapshot,
  type TopicAdministrationOutcome,
} from "../../src/features/kafka/contracts/topic-administration";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
  parseCorrelatedHostResponse,
} from "../../src/features/kafka/contracts";

const baseline: TopicAdministrationSnapshot = {
  identity: {
    clusterId: "fixture-cluster",
    topicId: "01234567-89ab-cdef-0123-456789abcdef",
    topic: "orders",
  },
  partitions: 2,
  replicasSha256: "a".repeat(64),
  internal: false,
  deleteSupported: true,
  deletePermission: "allowed",
  expandPermission: "allowed",
};
const expanded = { kind: "expand", topic: "orders", partitions: 4 } as const;
const accepted: TopicAdministrationOutcome = {
  input: expanded,
  state: "acknowledged",
  verification: "verified",
  cleanup: "confirmed",
  detail: "Actual acknowledgement and readback",
};
function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
} {
  let resolve!: (value: T) => void, reject!: (error: unknown) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}
function fixture(): {
  scope: TopicAdministrationScope;
  current: { value: boolean };
  snapshot: ReturnType<typeof vi.fn>;
  apply: ReturnType<typeof vi.fn>;
  service: TopicAdministrationService;
  clock: { value: number };
} {
  const current = { value: true },
    clock = { value: 1_000 };
  const snapshot = vi.fn(() => Promise.resolve(structuredClone(baseline)));
  const apply = vi.fn(() => ({ started: true as const, result: Promise.resolve(accepted) }));
  const scope: TopicAdministrationScope = {
    connectionName: "Selected connection",
    isCurrent: () => current.value,
    snapshot,
    tryApply: apply,
  };
  return {
    scope,
    current,
    snapshot,
    apply,
    service: new TopicAdministrationService(
      () => scope,
      () => clock.value,
    ),
    clock,
  };
}
it("closes typed payloads, reserves internal names, bounds counts and pairs review identity", () => {
  expect(parseTopicAdministrationInput(expanded)).toEqual(expanded);
  for (const input of [
    { ...expanded, partitions: 0 },
    { ...expanded, partitions: 4097 },
    { ...expanded, partitions: 2.5 },
    { ...expanded, topic: "__consumer_offsets" },
    { kind: "delete", topic: "orders", partitions: 4 },
  ])
    expect(() => parseTopicAdministrationInput(input)).toThrow();
  const command = parseHostCommand({
    command: "topics.change.review",
    id: "review",
    version: HOST_PROTOCOL_VERSION,
    payload: expanded,
  });
  const review = {
    planId: "plan",
    connectionName: "A",
    expiresAt: "2026-10-10T10:00:00Z",
    input: expanded,
    baseline,
    confirmation: "EXPAND orders TO 4",
  };
  expect(
    parseHostCommandResponse({
      command: command.command,
      id: command.id,
      version: command.version,
      ok: true,
      result: { correlationId: "r", review },
    }),
  ).toMatchObject({ ok: true });
  expect(() =>
    parseTopicAdministrationReview({
      ...review,
      baseline: { ...baseline, identity: { ...baseline.identity, topic: "other" } },
    }),
  ).toThrow();
  expect(() =>
    parseCorrelatedHostResponse(
      {
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "r",
          review: {
            ...review,
            input: { ...expanded, partitions: 5 },
            confirmation: "EXPAND orders TO 5",
          },
        },
      },
      command,
    ),
  ).toThrow();
  expect(() => parseHostCommand({ ...command, version: 67 })).toThrow();
});
it.each([
  { partitions: 3 },
  { replicasSha256: "b".repeat(64) },
  { identity: { ...baseline.identity, topicId: "11234567-89ab-cdef-0123-456789abcdef" } },
  { expandPermission: "denied" as const },
])("refuses a changed topic baseline without dispatch: %j", async (changed) => {
  const f = fixture(),
    review = await f.service.review(expanded);
  f.snapshot.mockResolvedValue({ ...baseline, ...changed });
  expect(await f.service.apply(review.planId, review.confirmation)).toMatchObject({
    state: "unsent",
  });
  expect(f.apply).not.toHaveBeenCalled();
});
it("refuses shrinking/internal/unsupported/denied targets, wrong confirmation, expiry and revoked scope", async () => {
  const f = fixture();
  await expect(f.service.review({ ...expanded, partitions: 2 })).rejects.toThrow();
  for (const change of [
    { internal: true },
    { deleteSupported: false },
    { deletePermission: "denied" as const },
  ]) {
    f.snapshot.mockResolvedValue({ ...baseline, ...change });
    await expect(f.service.review({ kind: "delete", topic: "orders" })).rejects.toThrow();
  }
  f.snapshot.mockResolvedValue(baseline);
  const review = await f.service.review(expanded);
  await expect(f.service.apply(review.planId, "orders")).rejects.toThrow();
  f.clock.value += 120_000;
  await expect(f.service.apply(review.planId, review.confirmation)).rejects.toThrow();
  f.clock.value = 1_000;
  f.current.value = false;
  await expect(f.service.apply(review.planId, review.confirmation)).rejects.toThrow();
  expect(f.apply).not.toHaveBeenCalled();
});
it("joins duplicate apply and retains acknowledgement after the original connection is revoked", async () => {
  const f = fixture(),
    pending = deferred<TopicAdministrationOutcome>();
  f.apply.mockReturnValue({ started: true, result: pending.promise });
  const review = await f.service.review(expanded),
    one = f.service.apply(review.planId, review.confirmation),
    two = f.service.apply(review.planId, review.confirmation);
  expect(one).toBe(two);
  await vi.waitFor(() => expect(f.apply).toHaveBeenCalledOnce());
  f.current.value = false;
  pending.resolve(accepted);
  expect(await one).toEqual(accepted);
});
function client(): {
  snapshot: ReturnType<typeof vi.fn<(topic: string) => Promise<TopicAdministrationSnapshot>>>;
  tryChange: ReturnType<
    typeof vi.fn<
      (
        input: TopicAdministrationInput,
        baseline: TopicAdministrationSnapshot,
        signal: AbortSignal,
      ) => MutationDispatch<void>
    >
  >;
  deletionVisible: (id: string) => Promise<boolean>;
  close: ReturnType<typeof vi.fn<() => Promise<void>>>;
} {
  return {
    snapshot: vi.fn(() => Promise.resolve(baseline)),
    tryChange: vi.fn(() => ({ started: true, result: Promise.resolve() })),
    deletionVisible: () => Promise.resolve(true),
    close: vi.fn(() => Promise.resolve()),
  };
}
const connection = {
  brokers: ["127.0.0.1:9092"],
  tlsEnabled: false,
  operationTimeoutMs: 1000,
} as const;
it("distinguishes an actual negative Kafka acknowledgement from transport uncertainty without retrying", async () => {
  const c = client();
  c.tryChange.mockReturnValue({
    started: true,
    result: Promise.reject(
      new ProtocolError("TOPIC_AUTHORIZATION_FAILED", "fixture-private-server-detail"),
    ),
  });
  const owner = new PlatformaticTopicAdministration(
    connection,
    new AbortController().signal,
    () => c,
  );
  const result = await owner.apply(expanded, baseline);
  expect(result).toMatchObject({
    state: "rejected",
    verification: "unavailable",
    cleanup: "confirmed",
  });
  expect(JSON.stringify(result)).not.toContain("fixture-private-server-detail");
  expect(c.tryChange).toHaveBeenCalledOnce();
  await owner.close();
});
it("adapter checks fresh identity again, owns cleanup and never mutates a replaced topic", async () => {
  const c = client();
  c.snapshot.mockResolvedValue({
    ...baseline,
    identity: { ...baseline.identity, topicId: "11234567-89ab-cdef-0123-456789abcdef" },
  });
  const owner = new PlatformaticTopicAdministration(
    connection,
    new AbortController().signal,
    () => c,
  );
  expect(await owner.apply(expanded, baseline)).toMatchObject({
    state: "unsent",
    cleanup: "confirmed",
  });
  expect(c.tryChange).not.toHaveBeenCalled();
  expect(c.close).toHaveBeenCalledOnce();
  await owner.close();
});
it("original owner drains delayed acknowledgement across revoke and preserves unresolved cleanup fences", async () => {
  const controller = new AbortController(),
    receipt = deferred<void>(),
    cleanup = deferred<void>(),
    c = client();
  c.tryChange.mockReturnValue({ started: true, result: receipt.promise });
  c.close.mockImplementation(() => cleanup.promise);
  const owner = new PlatformaticTopicAdministration(connection, controller.signal, () => c),
    operation = owner.apply(expanded, baseline);
  await vi.waitFor(() => expect(c.tryChange).toHaveBeenCalledOnce());
  controller.abort();
  const originalClose = owner.close();
  expect(owner.close()).toBe(originalClose);
  let settled = false;
  const closed = originalClose.then(() => {
    settled = true;
  });
  await Promise.resolve();
  expect(settled).toBe(false);
  receipt.resolve();
  await vi.waitFor(() => expect(c.close).toHaveBeenCalledOnce());
  expect(settled).toBe(false);
  cleanup.resolve();
  expect(await operation).toMatchObject({
    state: "acknowledged",
    verification: "unavailable",
    cleanup: "confirmed",
  });
  await closed;
  expect(settled).toBe(true);
  expect(c.snapshot).toHaveBeenCalledOnce();
  const failed = client();
  failed.close.mockRejectedValue(new Error("fixture-private-cleanup"));
  const blocked = new PlatformaticTopicAdministration(
    connection,
    new AbortController().signal,
    () => failed,
  );
  expect(await blocked.apply({ kind: "delete", topic: "orders" }, baseline)).toMatchObject({
    state: "acknowledged",
    verification: "verified",
    cleanup: "unresolved",
  });
  await expect(blocked.snapshot("orders")).rejects.toThrow(/cleanup/u);
  await expect(blocked.close()).rejects.toThrow();
});
it("retains honest unknown receipt loss and acknowledged but different readback without a retry", async () => {
  const c = client();
  c.tryChange.mockReturnValue({
    started: true,
    result: Promise.reject(new Error("secret transport failure")),
  });
  const owner = new PlatformaticTopicAdministration(
    connection,
    new AbortController().signal,
    () => c,
  );
  const unknown = await owner.apply(expanded, baseline);
  expect(unknown).toMatchObject({ state: "unknown", cleanup: "confirmed" });
  expect(JSON.stringify(unknown)).not.toContain("secret transport failure");
  expect(c.tryChange).toHaveBeenCalledOnce();
  await owner.close();
  const other = client(),
    acknowledged = new PlatformaticTopicAdministration(
      connection,
      new AbortController().signal,
      () => other,
    );
  expect(await acknowledged.apply(expanded, baseline)).toMatchObject({
    state: "acknowledged",
    verification: "different",
  });
  expect(other.tryChange).toHaveBeenCalledOnce();
  await acknowledged.close();
});
