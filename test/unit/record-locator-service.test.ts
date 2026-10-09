import { randomUUID } from "node:crypto";

import { afterEach, expect, it, vi, type Mock } from "vitest";

import {
  KAFKA_RECORD_PROTECTION_DEFAULTS,
  type KafkaMessage,
  type KafkaReadCoverage,
} from "../../src/features/kafka/contracts";
import type {
  KafkaRecordLocator,
  KafkaRecordLocatorLoadInput,
} from "../../src/features/kafka/contracts/record-locator";
import type { RecordReadSettings } from "../../src/features/kafka/contracts/finite-record-read";
import type { KafkaMessageStream } from "../../src/features/kafka/application/types";
import type { RecordReadScope } from "../../src/features/kafka/application/connection-scope";
import { RecordLocatorService } from "../../src/features/kafka/application/record-locator-service";
import {
  KafkaRecordLocatorError,
  RecordLocatorOperationError,
  UnknownRecordLocatorRequestError,
} from "../../src/features/kafka/application/record-locator-errors";
import { KafkaReadOpenCleanupError } from "../../src/features/kafka/application/read-open-cleanup";

const locator: KafkaRecordLocator = {
  schemaVersion: 1,
  clusterId: "fixture-cluster",
  topicId: "12345678-1234-1234-1234-123456789abc",
  topic: "events",
  partition: 0,
  offset: "7",
  leaderEpoch: 3,
};
const input = (): KafkaRecordLocatorLoadInput => ({
  requestId: randomUUID(),
  locator: { ...locator },
});
const message = (): KafkaMessage => ({
  id: "events:0:7",
  topic: "events",
  partition: 0,
  offset: "7",
  timestamp: "2026-10-09T00:00:00.000Z",
  key: null,
  payload: "[MASKED]",
  preview: "[MASKED]",
  headers: {},
  originalByteSize: 8,
  original: { state: "unavailable", reason: "masked" },
  truncated: false,
  provenance: {
    clusterId: locator.clusterId,
    topicId: locator.topicId,
    leaderEpoch: locator.leaderEpoch,
  },
});
const settings = (): RecordReadSettings => ({
  codecs: { key: "auto" as const, value: "auto" as const },
  protection: KAFKA_RECORD_PROTECTION_DEFAULTS,
});
function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
} {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
interface Fixture {
  readonly reader: KafkaMessageStream;
  readonly scope: RecordReadScope;
  readonly close: Mock<() => Promise<void>>;
  readonly open: Mock<RecordReadScope["openMessageStream"]>;
  readonly service: RecordLocatorService;
  readonly revoke: () => void;
  readonly complete: () => void;
}
function fixture(values: readonly KafkaMessage[] = [message()]): Fixture {
  let current = true;
  let complete = false;
  const close = vi.fn(() => Promise.resolve());
  const coverage = (): KafkaReadCoverage => ({
    reason: complete ? "range-complete" : "reading",
    scannedRecords: values.length,
    scannedBytes: 8 * values.length,
    matchedRecords: values.length,
    unavailableRecords: 0,
    partitions: [
      { partition: 0, startOffset: "7", endOffset: "8", nextOffset: complete ? "8" : "7" },
    ],
  });
  const reader: KafkaMessageStream = {
    close,
    coverage,
    checkpoint: () => ({
      clusterId: locator.clusterId,
      topicId: locator.topicId,
      partitionCount: 1,
      coverage: coverage(),
    }),
    async *[Symbol.asyncIterator]() {
      for (const value of values) {
        complete = true;
        yield await Promise.resolve(value);
      }
      complete = true;
    },
  };
  const open = vi.fn<RecordReadScope["openMessageStream"]>(() => Promise.resolve(reader));
  const scope: RecordReadScope = {
    connectionName: "test",
    isCurrent: (): boolean => current,
    openMessageStream: open,
  };
  const service = new RecordLocatorService({ scope: (): RecordReadScope => scope, settings });
  return {
    reader,
    open,
    scope,
    close,
    service,
    revoke: (): void => {
      current = false;
    },
    complete: (): void => {
      complete = true;
    },
  };
}
afterEach(() => vi.useRealTimers());

it("reloads the exact locator with current protected bytes and confirms close before returning", async () => {
  const f = fixture();
  const submitted = input();
  const closing = deferred<void>();
  f.close.mockImplementation(() => closing.promise);
  let returned = false;
  const work = f.service.load(submitted).then((result) => {
    returned = true;
    return result;
  });
  await vi.waitFor(() => expect(f.close).toHaveBeenCalledOnce());
  expect(returned).toBe(false);
  closing.resolve();
  expect(await work).toMatchObject({
    state: "loaded",
    message: { payload: "[MASKED]", original: { reason: "masked" } },
  });
  expect(f.open.mock.calls[0]?.[0]).toMatchObject({
    mode: "earliest",
    maxMessages: 1,
    search: { partition: 0, offsetExact: "7", key: "", value: "" },
  });
  expect(f.open.mock.calls[0]?.[1]).toBeInstanceOf(AbortSignal);
  expect(f.open.mock.calls[0]?.[2]).toBeUndefined();
  expect(f.open.mock.calls[0]?.[3]).toEqual(submitted.locator);
});

it("joins only exact in-flight duplicates, rejects overlap, and never caches a completed payload", async () => {
  const f = fixture();
  const submitted = input();
  const work = f.service.load(submitted);
  expect(f.service.load(submitted)).toBe(work);
  expect(() => f.service.load(input())).toThrow(RecordLocatorOperationError);
  expect(() => f.service.load({ ...submitted, locator: { ...locator, offset: "8" } })).toThrow(
    RecordLocatorOperationError,
  );
  await work;
  expect(() => f.service.load(submitted)).toThrow(RecordLocatorOperationError);
  expect(await f.service.load(input())).toMatchObject({ state: "loaded" });
  expect(f.open).toHaveBeenCalledTimes(2);
});

const unprovenMessage = message();
Reflect.deleteProperty(unprovenMessage, "provenance");

it.each([
  [
    "resource-replaced",
    {
      ...message(),
      provenance: { ...message().provenance!, topicId: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" },
    },
  ],
  ["record-replaced", { ...message(), provenance: { ...message().provenance!, leaderEpoch: 4 } }],
  ["unavailable", { ...message(), offset: "8" }],
  ["unavailable", unprovenMessage],
] as const)("withholds %s evidence through the application boundary", async (state, candidate) => {
  const f = fixture([candidate]);
  expect(await f.service.load(input())).toMatchObject({ state });
  expect(f.close).toHaveBeenCalled();
});

it.each(["expired", "resource-replaced", "topic-missing", "inaccessible"] as const)(
  "preserves safe typed %s and never exposes nested broker text",
  async (state) => {
    const f = fixture();
    vi.mocked(f.open).mockRejectedValue(
      new AggregateError([new Error("password=private"), new KafkaRecordLocatorError(state)]),
    );
    const result = await f.service.load(input());
    expect(result).toMatchObject({ state });
    expect(JSON.stringify(result)).not.toContain("private");
  },
);

it("distinguishes a conclusively traversed offset gap from incomplete empty fetches", async () => {
  const gap = fixture([]);
  expect(await gap.service.load(input())).toMatchObject({ state: "record-missing" });
  const incomplete = fixture([]);
  incomplete.reader[Symbol.asyncIterator] = async function* (): AsyncIterator<KafkaMessage> {
    yield* await Promise.resolve<readonly KafkaMessage[]>([]);
  };
  expect(await incomplete.service.load(input())).toMatchObject({ state: "unavailable" });
});

it("retains failed-close ownership, blocks new loads, and retries only the original reader", async () => {
  const f = fixture();
  const submitted = input();
  f.close.mockRejectedValueOnce(new Error("close private failure"));
  await expect(f.service.load(submitted)).rejects.toThrow(RecordLocatorOperationError);
  await expect(f.service.idle()).rejects.toThrow(RecordLocatorOperationError);
  expect(() => f.service.load(input())).toThrow(expect.objectContaining({ reason: "busy" }));
  expect(() => f.service.load(submitted)).toThrow(RecordLocatorOperationError);
  await expect(f.service.cancel(randomUUID())).rejects.toThrow(UnknownRecordLocatorRequestError);
  expect(f.close).toHaveBeenCalledOnce();
  await f.service.cancel(submitted.requestId);
  expect(f.close).toHaveBeenCalledTimes(2);
  await f.service.idle();
  expect(await f.service.load(input())).toMatchObject({ state: "loaded" });
});

it("joins concurrent cleanup retries and retains the same reader when a retry fails", async () => {
  const f = fixture();
  const submitted = input();
  f.close.mockRejectedValueOnce(new Error("first close failed"));
  await expect(f.service.load(submitted)).rejects.toThrow(RecordLocatorOperationError);
  const retry = deferred<void>();
  f.close.mockReturnValueOnce(retry.promise);
  const first = f.service.cancel(submitted.requestId);
  const second = f.service.cancel(submitted.requestId);
  const outcomes = Promise.allSettled([first, second]);
  await vi.waitFor(() => expect(f.close).toHaveBeenCalledTimes(2));
  retry.reject(new Error("retry failed"));
  expect((await outcomes).map((result) => result.status)).toEqual(["rejected", "rejected"]);
  await expect(f.service.idle()).rejects.toThrow(RecordLocatorOperationError);
  await f.service.cancel(submitted.requestId);
  expect(f.close).toHaveBeenCalledTimes(3);
  expect(f.open).toHaveBeenCalledOnce();
  await f.service.idle();
});

it("cancels a late-opened reader and returns only after its close is confirmed", async () => {
  const f = fixture();
  const submitted = input();
  const opening = deferred<KafkaMessageStream>();
  const closing = deferred<void>();
  vi.mocked(f.open).mockReturnValue(opening.promise);
  f.close.mockReturnValue(closing.promise);
  const load = f.service.load(submitted);
  await vi.waitFor(() => expect(f.open).toHaveBeenCalledOnce());
  let stopped = false;
  const cancel = f.service.cancel(submitted.requestId).then(() => {
    stopped = true;
  });
  opening.resolve(f.reader);
  await vi.waitFor(() => expect(f.close).toHaveBeenCalledOnce());
  expect(stopped).toBe(false);
  closing.resolve();
  await cancel;
  expect(await load).toMatchObject({ state: "cancelled" });
  await f.service.cancel(submitted.requestId);
});

it("withholds an already decoded record when authority is revoked during close", async () => {
  const f = fixture();
  const closing = deferred<void>();
  f.close.mockReturnValue(closing.promise);
  const load = f.service.load(input());
  await vi.waitFor(() => expect(f.close).toHaveBeenCalledOnce());
  f.service.invalidate();
  f.revoke();
  closing.resolve();
  expect(await load).toMatchObject({ state: "revoked" });
});

it("keeps cleanup failure authoritative even when the original read indicates retention expiry", async () => {
  const f = fixture();
  const submitted = input();
  vi.mocked(f.open).mockRejectedValue(
    Object.assign(new KafkaRecordLocatorError("expired"), {
      cleanupCause: new Error("private cleanup"),
    }),
  );
  await expect(f.service.load(submitted)).rejects.toThrow(RecordLocatorOperationError);
  await expect(f.service.cancel(submitted.requestId)).rejects.toThrow(RecordLocatorOperationError);
  expect(() => f.service.load(input())).toThrow(RecordLocatorOperationError);
});

it("retries a failed-open close-only capability instead of losing or recreating its owner", async () => {
  const f = fixture();
  const submitted = input();
  const cleanup = {
    close: vi.fn().mockRejectedValueOnce(new Error("still closing")).mockResolvedValue(undefined),
  };
  vi.mocked(f.open).mockRejectedValueOnce(
    new KafkaReadOpenCleanupError(
      new KafkaRecordLocatorError("expired"),
      new Error("original close failed"),
      cleanup,
    ),
  );
  await expect(f.service.load(submitted)).rejects.toThrow(RecordLocatorOperationError);
  await expect(f.service.cancel(submitted.requestId)).rejects.toThrow(RecordLocatorOperationError);
  expect(() => f.service.load(input())).toThrow(RecordLocatorOperationError);
  await f.service.cancel(submitted.requestId);
  expect(cleanup.close).toHaveBeenCalledTimes(2);
  expect(f.open).toHaveBeenCalledOnce();
  expect(await f.service.load(input())).toMatchObject({ state: "loaded" });
});

it("enforces the admission deadline during a pending open without claiming a missing record", async () => {
  vi.useFakeTimers();
  const f = fixture();
  const opening = deferred<KafkaMessageStream>();
  vi.mocked(f.open).mockReturnValue(opening.promise);
  const service = new RecordLocatorService({
    scope: (): RecordReadScope => f.scope,
    settings,
    durationMs: 20,
  });
  const load = service.load(input());
  await vi.advanceTimersByTimeAsync(21);
  expect(vi.mocked(f.open).mock.calls[0]?.[1].aborted).toBe(true);
  opening.resolve(f.reader);
  expect(await load).toMatchObject({ state: "unavailable" });
  expect(f.close).toHaveBeenCalledOnce();
});
