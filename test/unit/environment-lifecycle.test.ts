import { expect, it, vi, type Mock } from "vitest";

import type { KafkaActiveConnection } from "../../src/features/kafka/application";
import {
  EnvironmentService,
  captureEnvironment,
} from "../../src/features/kafka/application/environment-service";
import type {
  ReplayDestination,
  ReplayDestinationPort,
} from "../../src/features/kafka/application/replay-destination";
import type {
  KafkaTopicConfigurationChange,
  KafkaTopicConfigurationEntry,
} from "../../src/features/kafka/contracts";
import type { EnvironmentInput } from "../../src/features/kafka/contracts/environment-snapshot";

function deferred<T>(): {
  readonly promise: Promise<T>;
  resolve(value: T): void;
  reject(error: Error): void;
} {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

interface EnvironmentConnection extends KafkaActiveConnection {
  readonly values: Record<string, string>;
  readonly writes: string[];
  readonly close: Mock<KafkaActiveConnection["close"]>;
  readonly describeClusterMetadata: Mock<KafkaActiveConnection["describeClusterMetadata"]>;
  readonly describeTopicIdentity: Mock<NonNullable<KafkaActiveConnection["describeTopicIdentity"]>>;
  readonly describeTopicConfiguration: Mock<KafkaActiveConnection["describeTopicConfiguration"]>;
  readonly alterTopicConfiguration: Mock<KafkaActiveConnection["alterTopicConfiguration"]>;
}

function connection(): EnvironmentConnection {
  const values: Record<string, string> = { a: "1000", b: "1000" };
  const writes: string[] = [];
  return {
    values,
    writes,
    close: vi.fn<() => Promise<void>>().mockResolvedValue(undefined),
    describeBrokerConfiguration: (): never => {
      throw new Error("Unexpected broker query.");
    },
    listTopics: (): Promise<readonly string[]> => Promise.resolve(Object.keys(values)),
    openMessageStream: (): never => {
      throw new Error("Unexpected message stream.");
    },
    describeClusterMetadata: vi.fn<KafkaActiveConnection["describeClusterMetadata"]>(
      (_signal?: AbortSignal) =>
        Promise.resolve({ clusterId: "target-cluster", brokers: [], controllerId: null }),
    ),
    describeTopicIdentity: vi.fn<NonNullable<KafkaActiveConnection["describeTopicIdentity"]>>(
      (topic: string) =>
        Promise.resolve({ clusterId: "target-cluster", topicId: `stable-${topic}`, partitions: 1 }),
    ),
    describeTopicConfiguration: vi.fn<KafkaActiveConnection["describeTopicConfiguration"]>(
      (topic: string, _signal?: AbortSignal): Promise<readonly KafkaTopicConfigurationEntry[]> =>
        Promise.resolve([
          {
            name: "retention.ms",
            value: values[topic]!,
            isDefault: false,
            isSensitive: false,
            readOnly: false,
            source: "topic",
            type: "long",
            synonyms: [],
            documentation: null,
          },
        ]),
    ),
    alterTopicConfiguration: vi.fn<KafkaActiveConnection["alterTopicConfiguration"]>(
      (
        topic: string,
        changes: readonly KafkaTopicConfigurationChange[],
        validate: boolean,
        _signal?: AbortSignal,
      ): Promise<void> => {
        if (!validate) {
          writes.push(topic);
          values[topic] = changes[0]!.value;
        }
        return Promise.resolve();
      },
    ),
  };
}

async function fixture(): Promise<{
  readonly source: EnvironmentConnection;
  readonly target: EnvironmentConnection;
  readonly close: Mock<ReplayDestination["close"]>;
  readonly destination: ReplayDestination;
  readonly open: Mock<ReplayDestinationPort["open"]>;
  readonly service: EnvironmentService;
  readonly input: EnvironmentInput;
}> {
  const source = connection();
  const target = connection();
  const close = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const destination: ReplayDestination = {
    connection: target,
    name: "Independent destination",
    current: () => true,
    close,
  };
  const open = vi.fn<ReplayDestinationPort["open"]>().mockResolvedValue(destination);
  const service = new EnvironmentService(
    () => ({ connection: source, generation: 1, connectionName: "Source" }),
    { open },
  );
  const snapshot = await captureEnvironment(target, ["a", "b"], AbortSignal.timeout(1000));
  const input: EnvironmentInput = {
    source: {
      ...snapshot,
      clusterId: "source-cluster",
      topics: snapshot.topics.map((topic) => ({
        ...topic,
        configs: topic.configs.map((config) =>
          config.key === "retention.ms" ? { ...config, value: "2000" } : config,
        ),
      })),
    },
    target: snapshot,
    targetProfile: { id: "saved-destination", revision: 1 },
    selected: [
      { topic: "a", key: "retention.ms" },
      { topic: "b", key: "retention.ms" },
    ],
  };
  return { source, target, close, destination, open, service, input };
}

it("drains a cancelled destination open through its late return and deferred close", async () => {
  const f = await fixture();
  const opening = deferred<ReplayDestination>();
  const opened = deferred<AbortSignal>();
  const closing = deferred<void>();
  const closeStarted = deferred<void>();
  f.open.mockImplementation((_id, _revision, signal) => {
    opened.resolve(signal);
    return opening.promise;
  });
  f.close.mockImplementation(() => {
    closeStarted.resolve();
    return closing.promise;
  });
  f.target.describeClusterMetadata.mockClear();
  const capture = f.service.capture(["a"], f.input.targetProfile);
  const captureFailed = expect(capture).rejects.toMatchObject({ name: "AbortError" });
  const signal = await opened.promise;
  f.service.cancel();
  expect(signal.aborted).toBe(true);
  const drained = vi.fn();
  const idle = f.service.idle().then(drained);
  opening.resolve(f.destination);
  await closeStarted.promise;
  expect(drained).not.toHaveBeenCalled();
  expect(f.target.describeClusterMetadata).not.toHaveBeenCalled();
  closing.resolve();
  await Promise.all([captureFailed, idle]);
  expect(f.close).toHaveBeenCalledOnce();
  expect(f.source.close).not.toHaveBeenCalled();
});

it("waits for failed capture cleanup and reports its failure once after the work has settled", async () => {
  const f = await fixture();
  const closing = deferred<void>();
  const closeStarted = deferred<void>();
  const failure = new Error("Destination close failed.");
  f.target.describeClusterMetadata.mockRejectedValue(new Error("Capture failed."));
  f.close.mockImplementation(() => {
    closeStarted.resolve();
    return closing.promise;
  });
  const capture = f.service.capture(["a"], f.input.targetProfile);
  const captureFailed = expect(capture).rejects.toBe(failure);
  await closeStarted.promise;
  f.service.cancel();
  const idle = f.service.idle();
  const idleFailed = expect(idle).rejects.toMatchObject({ errors: [failure] });
  closing.reject(failure);
  await Promise.all([captureFailed, idleFailed]);
  await expect(f.service.idle()).resolves.toBeUndefined();
});

it("prevents a cancelled review from publishing a plan after capture cleanup completes", async () => {
  const f = await fixture();
  const closing = deferred<void>();
  const closeStarted = deferred<void>();
  f.close.mockImplementation(() => {
    closeStarted.resolve();
    return closing.promise;
  });
  const review = f.service.review(f.input);
  const reviewFailed = expect(review).rejects.toMatchObject({ name: "AbortError" });
  await closeStarted.promise;
  f.service.cancel();
  const drained = vi.fn();
  const idle = f.service.idle().then(drained);
  expect(drained).not.toHaveBeenCalled();
  closing.resolve();
  await Promise.all([reviewFailed, idle]);
  f.close.mockResolvedValue(undefined);
  const next = await f.service.review(f.input);
  expect(next.planId.length).toBeGreaterThan(0);
  expect(f.target.writes).toEqual([]);
});

it.each(["capture", "review"] as const)(
  "rejects a %s when the saved destination profile changes during deferred cleanup",
  async (kind) => {
    const f = await fixture();
    const closing = deferred<void>();
    const closeStarted = deferred<void>();
    let current = true;
    f.open.mockResolvedValue({ ...f.destination, current: () => current });
    f.close.mockImplementation(() => {
      closeStarted.resolve();
      return closing.promise;
    });
    const operation =
      kind === "capture"
        ? f.service.capture(["a"], f.input.targetProfile)
        : f.service.review(f.input);
    const failed = expect(operation).rejects.toThrow("Connection changed.");
    await closeStarted.promise;
    current = false;
    const idle = f.service.idle();
    closing.resolve();
    await Promise.all([failed, idle]);
    expect(f.target.writes).toEqual([]);
  },
);

it("stops after a non-cancellable topic identity query returns late", async () => {
  const f = await fixture();
  const identity = deferred<{ clusterId: string; topicId: string; partitions: number }>();
  const identityStarted = deferred<void>();
  f.target.describeTopicIdentity.mockImplementation(() => {
    identityStarted.resolve();
    return identity.promise;
  });
  f.target.describeTopicConfiguration.mockClear();
  const capture = f.service.capture(["a"], f.input.targetProfile);
  const failed = expect(capture).rejects.toMatchObject({ name: "AbortError" });
  await identityStarted.promise;
  f.service.cancel();
  const idle = f.service.idle();
  identity.resolve({ clusterId: "target-cluster", topicId: "stable-a", partitions: 1 });
  await Promise.all([failed, idle]);
  expect(f.target.describeTopicConfiguration).not.toHaveBeenCalled();
  expect(f.close).toHaveBeenCalledOnce();
});

it("invalidates an undispatched review even when the source connection generation is unchanged", async () => {
  const f = await fixture();
  const review = await f.service.review(f.input);
  f.service.cancel();
  await expect(f.service.apply(review.planId, review.confirmation)).rejects.toThrow(
    "Review was cancelled.",
  );
  await f.service.idle();
  expect(f.target.writes).toEqual([]);
  expect(f.open).toHaveBeenCalledOnce();
});

it("drains an accepted apply cancelled before its dispatch callback starts", async () => {
  const f = await fixture();
  const review = await f.service.review(f.input);
  const apply = f.service.apply(review.planId, review.confirmation);
  const failed = expect(apply).rejects.toThrow("Review was cancelled.");
  f.service.cancel();
  await Promise.all([failed, f.service.idle()]);
  expect(f.target.writes).toEqual([]);
  expect(f.open).toHaveBeenCalledOnce();
});

it.each(["complete", "failed"] as const)(
  "retains a late write acknowledgement and drains %s cleanup without dispatching or retrying more topics",
  async (cleanup) => {
    const f = await fixture();
    const review = await f.service.review(f.input);
    const dispatch = deferred<void>();
    const dispatched = deferred<AbortSignal>();
    const closing = deferred<void>();
    const closeStarted = deferred<void>();
    const cleanupFailure = new Error("Destination close failed.");
    f.target.alterTopicConfiguration.mockImplementation((topic, _changes, validate, signal) => {
      if (validate) return Promise.resolve();
      f.target.writes.push(topic);
      dispatched.resolve(signal!);
      return dispatch.promise;
    });
    f.close.mockImplementation(() => {
      closeStarted.resolve();
      return closing.promise;
    });
    const apply = f.service.apply(review.planId, review.confirmation);
    const signal = await dispatched.promise;
    expect(f.service.apply(review.planId, review.confirmation)).toBe(apply);
    f.service.cancel();
    expect(signal.aborted).toBe(true);
    const drained = vi.fn();
    const idle = f.service.idle();
    const idleResult =
      cleanup === "failed"
        ? expect(idle).rejects.toMatchObject({ errors: [cleanupFailure] })
        : idle.then(drained);
    dispatch.resolve();
    await closeStarted.promise;
    expect(drained).not.toHaveBeenCalled();
    if (cleanup === "failed") closing.reject(cleanupFailure);
    else closing.resolve();
    const outcome = await apply;
    await idleResult;
    expect(outcome.results).toEqual([
      { topic: "a", state: "acknowledged", verified: false },
      { topic: "b", state: "unsent", verified: false },
    ]);
    if (cleanup === "failed") expect(outcome.detail).toContain("cleanup could not be confirmed");
    expect(await f.service.apply(review.planId, review.confirmation)).toEqual(outcome);
    await f.service.idle();
    expect(f.target.writes).toEqual(["a"]);
    expect(f.source.close).not.toHaveBeenCalled();
  },
);

it.each(["unknown", "rejected"] as const)(
  "preserves a cancelled dispatched write's %s result and does not retry it",
  async (state) => {
    const f = await fixture();
    const review = await f.service.review(f.input);
    const dispatch = deferred<void>();
    const dispatched = deferred<void>();
    f.target.alterTopicConfiguration.mockImplementation((topic, _changes, validate) => {
      if (validate) return Promise.resolve();
      f.target.writes.push(topic);
      dispatched.resolve();
      return dispatch.promise;
    });
    const apply = f.service.apply(review.planId, review.confirmation);
    await dispatched.promise;
    f.service.cancel();
    dispatch.reject(
      state === "rejected"
        ? Object.assign(new Error("Write denied."), { code: "AUTHORIZATION_DENIED" })
        : new Error("Acknowledgement unavailable."),
    );
    const outcome = await apply;
    await f.service.idle();
    expect(outcome.results).toEqual([
      { topic: "a", state, verified: false },
      { topic: "b", state: "unsent", verified: false },
    ]);
    expect(await f.service.apply(review.planId, review.confirmation)).toEqual(outcome);
    expect(f.target.writes).toEqual(["a"]);
  },
);

it.each(["ordinary", "cleanup"] as const)(
  "drains an open's %s failure and reports only unconfirmed owned cleanup",
  async (kind) => {
    const f = await fixture();
    const opening = deferred<ReplayDestination>();
    const opened = deferred<void>();
    const cleanup = new Error("Late-open destination close failed.");
    const failure =
      kind === "cleanup"
        ? Object.assign(new Error("Destination cleanup unavailable."), { cleanupCause: cleanup })
        : new Error("Destination open failed.");
    f.open.mockImplementation(() => {
      opened.resolve();
      return opening.promise;
    });
    const capture = f.service.capture(["a"], f.input.targetProfile);
    const captureFailed = expect(capture).rejects.toBe(failure);
    await opened.promise;
    f.service.cancel();
    const idle = f.service.idle();
    const idleResult =
      kind === "cleanup"
        ? expect(idle).rejects.toMatchObject({ errors: [cleanup] })
        : expect(idle).resolves.toBeUndefined();
    opening.reject(failure);
    await Promise.all([captureFailed, idleResult]);
    expect(f.close).not.toHaveBeenCalled();
    await expect(f.service.idle()).resolves.toBeUndefined();
  },
);
