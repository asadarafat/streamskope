import { afterEach, describe, expect, it, vi } from "vitest";

import {
  PLUGIN_TRANSITION_WAIT_MS,
  PluginTransitions,
  type PluginTransitionHandle,
} from "../../src/platform/node/plugins/transition";
import type { PluginTransition } from "../../src/plugins/contracts";

function deferred<T>(): {
  readonly promise: Promise<T>;
  resolve(value: T): void;
  reject(reason: Error): void;
} {
  let resolve!: (value: T) => void;
  let reject!: (reason: Error) => void;
  const promise = new Promise<T>((done, failed) => {
    resolve = done;
    reject = failed;
  });
  return { promise, resolve, reject };
}
function fixture(): {
  readonly transitions: PluginTransitions;
  readonly snapshots: Array<readonly PluginTransition[]>;
} {
  const snapshots: Array<readonly PluginTransition[]> = [];
  const transitions = new PluginTransitions({
    changed: (): void => {
      snapshots.push(transitions.snapshot());
    },
  });
  return { transitions, snapshots };
}
const spec = { pluginId: "example.capture", operation: "install" } as const;

afterEach(() => {
  vi.useRealTimers();
});

describe("owned plugin transitions", () => {
  it("reports a stalled hook without settling it, releasing dispatch or poisoning its intent", async () => {
    vi.useFakeTimers();
    const { transitions, snapshots } = fixture();
    const hook = deferred<void>();
    const entered = deferred<void>();
    let settled = false;
    const changing = transitions
      .withIntent(spec.pluginId, (assertCurrent) =>
        transitions.run(
          spec,
          async (owner) => {
            entered.resolve();
            await owner.wait("prepare-unload", () => hook.promise);
            assertCurrent();
            return "activated";
          },
          assertCurrent,
        ),
      )
      .finally(() => {
        settled = true;
      });
    await entered.promise;
    expect(transitions.isChanging(spec.pluginId)).toBe(true);
    await vi.advanceTimersByTimeAsync(PLUGIN_TRANSITION_WAIT_MS);
    expect(transitions.snapshot()[0]).toMatchObject({
      state: "waiting",
      stage: "prepare-unload",
      commit: "not-started",
    });
    expect(settled).toBe(false);
    const refused = vi.fn(() => Promise.resolve());
    await expect(transitions.withIntent(spec.pluginId, refused)).rejects.toThrow("busy");
    expect(refused).not.toHaveBeenCalled();
    expect(transitions.isChanging(spec.pluginId)).toBe(true);
    hook.resolve();
    await expect(changing).resolves.toBe("activated");
    expect(transitions.snapshot()).toEqual([]);
    expect(transitions.isChanging(spec.pluginId)).toBe(false);
    expect(snapshots.filter((snapshot) => snapshot[0]?.state === "waiting")).toHaveLength(1);
    const count = snapshots.length;
    await vi.advanceTimersByTimeAsync(PLUGIN_TRANSITION_WAIT_MS * 2);
    expect(snapshots).toHaveLength(count);
  });

  it("keeps latest acquisition intent before local admission, preventing a late older install", async () => {
    const { transitions } = fixture();
    const downloaded = deferred<void>();
    const install = vi.fn(() => Promise.resolve());
    const old = transitions.withIntent(spec.pluginId, async (assertCurrent) => {
      await downloaded.promise;
      assertCurrent();
      await transitions.run(spec, install, assertCurrent);
    });
    const rejected = expect(old).rejects.toThrow("superseded");
    await transitions.withIntent(spec.pluginId, (assertCurrent) =>
      transitions.run({ ...spec, operation: "remove" }, () => Promise.resolve(), assertCurrent),
    );
    downloaded.resolve();
    await rejected;
    expect(install).not.toHaveBeenCalled();
  });

  it("publishes another plugin as queued while preserving its dispatch until execution begins", async () => {
    const { transitions } = fixture();
    const first = deferred<void>();
    const second = deferred<void>();
    const secondStarted = deferred<void>();
    const running = transitions.run(spec, (owner) =>
      owner.wait("load-candidate", () => first.promise),
    );
    const nextSpec = { pluginId: "example.second", operation: "remove" } as const;
    const queued = transitions.run(nextSpec, (owner) => {
      secondStarted.resolve();
      return owner.wait("prepare-unload", () => second.promise);
    });
    expect(transitions.snapshot().map((entry) => entry.state)).toEqual(["queued", "queued"]);
    await Promise.resolve();
    expect(transitions.snapshot()[1]).toMatchObject({ stage: "queued", state: "queued" });
    expect(transitions.isChanging(nextSpec.pluginId)).toBe(false);
    first.resolve();
    await running;
    await secondStarted.promise;
    expect(transitions.isChanging(nextSpec.pluginId)).toBe(true);
    second.resolve();
    await queued;
    expect(transitions.snapshot()).toEqual([]);
  });

  it.each(["review-install", "review-retry", "review-remove", "review-exit"] as const)(
    "keeps plugin stop/recovery dispatch available during stalled %s",
    async (operation) => {
      vi.useFakeTimers();
      const { transitions } = fixture();
      const hook = deferred<void>();
      const running = transitions.run({ ...spec, operation }, (owner) =>
        owner.wait(
          operation === "review-exit" ? "review-exit" : "review-change",
          () => hook.promise,
        ),
      );
      await vi.advanceTimersByTimeAsync(PLUGIN_TRANSITION_WAIT_MS);
      expect(transitions.snapshot()[0]?.state).toBe("waiting");
      expect(transitions.isChanging(spec.pluginId)).toBe(false);
      await expect(transitions.run(spec, () => Promise.resolve())).rejects.toThrow("busy");
      hook.resolve();
      await running;
    },
  );

  it("closes new admission while retaining active cleanup and refusing queued mutations", async () => {
    const { transitions } = fixture();
    const load = deferred<void>();
    const cleanup = deferred<void>();
    const entered = deferred<void>();
    const cleaning = deferred<void>();
    const mutation = transitions.run(spec, async (owner) => {
      entered.resolve();
      try {
        await owner.wait("load-candidate", () => load.promise);
        owner.assertCurrent();
      } finally {
        cleaning.resolve();
        await owner.wait("close-candidate", () => cleanup.promise);
      }
    });
    const failedMutation = expect(mutation).rejects.toThrow("closing");
    const other = { pluginId: "example.second", operation: "remove" } as const;
    const mustNotRun = vi.fn(() => Promise.resolve());
    const queued = transitions.run(other, mustNotRun);
    const failedQueued = expect(queued).rejects.toThrow("closing");
    await entered.promise;
    transitions.beginClose();
    transitions.beginClose();
    await expect(transitions.withIntent("example.third", mustNotRun)).rejects.toThrow("closing");
    await expect(
      transitions.run({ ...other, pluginId: "example.third" }, mustNotRun),
    ).rejects.toThrow("closing");
    load.resolve();
    await cleaning.promise;
    expect(transitions.snapshot()[0]?.stage).toBe("close-candidate");
    expect(transitions.isChanging(spec.pluginId)).toBe(true);
    await expect(
      transitions.observeShutdown({ ...spec, operation: "shutdown" }, mustNotRun),
    ).rejects.toThrow("settled");
    cleanup.resolve();
    await failedMutation;
    await failedQueued;
    await transitions.settled();
    expect(mustNotRun).not.toHaveBeenCalled();
    expect(transitions.snapshot()).toEqual([]);
  });

  it("attempts shutdown owners concurrently and retains the stalled sibling", async () => {
    vi.useFakeTimers();
    const { transitions } = fixture();
    const first = deferred<void>();
    const close = vi.fn(() => Promise.resolve());
    await expect(
      transitions.observeShutdown({ ...spec, operation: "shutdown" }, close),
    ).rejects.toThrow("closed admission");
    transitions.beginClose();
    const pending = transitions.observeShutdown({ ...spec, operation: "shutdown" }, (owner) =>
      owner.wait("close-backend", () => first.promise),
    );
    const completed = transitions.observeShutdown(
      { pluginId: "example.second", operation: "shutdown" },
      close,
    );
    await completed;
    expect(close).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(PLUGIN_TRANSITION_WAIT_MS);
    expect(transitions.snapshot()).toMatchObject([
      { pluginId: spec.pluginId, state: "waiting", operation: "shutdown" },
    ]);
    first.resolve();
    await pending;
    expect(transitions.snapshot()).toEqual([]);
  });

  it("updates actual outstanding counts without changing commit or stage authority", async () => {
    vi.useFakeTimers();
    const { transitions } = fixture();
    const commit = deferred<void>();
    const entered = deferred<void>();
    let requests = 2;
    let connections = 1;
    const running = transitions.run(spec, async (owner) => {
      entered.resolve();
      await owner.wait("commit-storage", () => commit.promise, {
        commit: "in-progress",
        counts: () => ({ requests, connections }),
      });
      owner.phase("activate-candidate", { commit: "confirmed" });
    });
    await entered.promise;
    requests = 1;
    connections = 0;
    transitions.refresh(spec.pluginId);
    await vi.advanceTimersByTimeAsync(PLUGIN_TRANSITION_WAIT_MS);
    expect(transitions.snapshot()[0]).toMatchObject({
      stage: "commit-storage",
      state: "waiting",
      commit: "in-progress",
      outstandingRequests: 1,
      outstandingConnections: 0,
    });
    commit.resolve();
    await running;
    expect(transitions.snapshot()).toEqual([]);
  });

  it("keeps coherent immutable snapshots and rejects late owner updates", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-10-08T12:00:00.000Z"));
    const { transitions } = fixture();
    const entered = deferred<void>();
    const finish = deferred<void>();
    let handle: PluginTransitionHandle | undefined;
    const running = transitions.run(spec, (owner) => {
      handle = owner;
      entered.resolve();
      return owner.wait("prepare-unload", () => finish.promise);
    });
    await entered.promise;
    const before = transitions.snapshot();
    expect(Object.isFrozen(before)).toBe(true);
    expect(Object.isFrozen(before[0])).toBe(true);
    vi.setSystemTime(new Date("2026-10-08T11:00:00.000Z"));
    handle!.phase("drain-requests");
    expect(transitions.snapshot()[0]!.stageStartedAt).toBe(before[0]!.stageStartedAt);
    expect(before[0]!.stage).toBe("prepare-unload");
    finish.resolve();
    await running;
    expect(() => handle!.phase("commit-storage")).toThrow("already settled");
  });

  it("does not let progress observer failures replace a hook rejection", async () => {
    const transitions = new PluginTransitions({
      changed: (): void => {
        throw new Error("Observer failed");
      },
    });
    const failure = new Error("Actual backend cleanup failure");
    await expect(
      transitions.run(spec, (owner) =>
        owner.wait("close-candidate", () => Promise.reject(failure)),
      ),
    ).rejects.toBe(failure);
    expect(transitions.snapshot()).toEqual([]);
    await expect(transitions.run(spec, () => Promise.resolve("recovered"))).resolves.toBe(
      "recovered",
    );
  });

  it("bounds admitted records without abandoning already queued owners", async () => {
    const { transitions } = fixture();
    const finish = deferred<void>();
    const running = Array.from({ length: 64 }, (_, index) =>
      transitions.run(
        { pluginId: `example.plugin-${index}`, operation: "install" },
        () => finish.promise,
      ),
    );
    await expect(
      transitions.run({ pluginId: "example.excess", operation: "install" }, () =>
        Promise.resolve(),
      ),
    ).rejects.toThrow("Too many");
    expect(transitions.snapshot()).toHaveLength(64);
    finish.resolve();
    await Promise.all(running);
    expect(transitions.snapshot()).toEqual([]);
  });

  it("refuses an exit cohort while a new plugin activation is queued, then reviews it after settlement", async () => {
    const { transitions } = fixture();
    const finish = deferred<void>();
    const first = transitions.run(spec, () => finish.promise);
    const install = transitions.run({ pluginId: "example.new", operation: "install" }, () =>
      Promise.resolve(),
    );
    const review = vi.fn(() => Promise.resolve());
    await expect(
      transitions.reviewExit([{ pluginId: spec.pluginId, operation: "review-exit" }], review),
    ).rejects.toThrow("busy");
    expect(review).not.toHaveBeenCalled();
    expect(transitions.snapshot().find((entry) => entry.pluginId === "example.new")?.stage).toBe(
      "queued",
    );
    finish.resolve();
    await Promise.all([first, install]);
    await transitions.reviewExit(
      [
        { pluginId: spec.pluginId, operation: "review-exit" },
        { pluginId: "example.new", operation: "review-exit" },
      ],
      async (handles) => {
        expect([...handles.keys()]).toEqual([spec.pluginId, "example.new"]);
        await handles.get("example.new")!.wait("review-exit", review);
      },
    );
    expect(review).toHaveBeenCalledTimes(1);
  });

  it("holds one nonblocking exit-review cohort across hooks and does not interleave new mutations", async () => {
    vi.useFakeTimers();
    const { transitions, snapshots } = fixture();
    const first = deferred<void>();
    const second = deferred<void>();
    const reviewingSecond = deferred<void>();
    const reviewed = transitions.reviewExit(
      [
        { ...spec, operation: "review-exit" },
        { pluginId: "example.second", operation: "review-exit" },
      ],
      async (handles) => {
        await handles.get(spec.pluginId)!.wait("review-exit", () => first.promise);
        reviewingSecond.resolve();
        await handles.get("example.second")!.wait("review-exit", () => second.promise);
      },
    );
    expect(snapshots[0]).toHaveLength(2);
    const mutation = vi.fn(() => Promise.resolve());
    const queued = transitions.run({ pluginId: "example.new", operation: "install" }, mutation);
    await vi.advanceTimersByTimeAsync(PLUGIN_TRANSITION_WAIT_MS);
    expect(transitions.isChanging(spec.pluginId)).toBe(false);
    expect(transitions.isChanging("example.second")).toBe(false);
    expect(transitions.snapshot()[0]?.state).toBe("waiting");
    expect(mutation).not.toHaveBeenCalled();
    first.resolve();
    await reviewingSecond.promise;
    expect(mutation).not.toHaveBeenCalled();
    second.resolve();
    await reviewed;
    await queued;
    expect(mutation).toHaveBeenCalledTimes(1);
    expect(transitions.snapshot()).toEqual([]);
  });

  it("admits no partial invalid exit cohort and still owns an empty cohort's actual work", async () => {
    const { transitions } = fixture();
    await expect(
      transitions.reviewExit(
        [
          { ...spec, operation: "review-exit" },
          { pluginId: "invalid id", operation: "review-exit" },
        ],
        () => Promise.resolve(),
      ),
    ).rejects.toThrow();
    expect(transitions.snapshot()).toEqual([]);
    const finish = deferred<void>();
    const entered = deferred<void>();
    const reviewed = transitions.reviewExit([], () => {
      entered.resolve();
      return finish.promise;
    });
    await entered.promise;
    transitions.beginClose();
    await expect(
      transitions.observeShutdown({ ...spec, operation: "shutdown" }, () => Promise.resolve()),
    ).rejects.toThrow("settled");
    finish.resolve();
    await reviewed;
    await transitions.settled();
    await transitions.observeShutdown({ ...spec, operation: "shutdown" }, () => Promise.resolve());
  });
});
