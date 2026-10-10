import { expect, it, vi } from "vitest";

import { ConnectOffsetsService } from "../../src/features/kafka/application/connect-offset-service";
import { ConnectWriteAdmission } from "../../src/features/kafka/application/connect-write-admission";
import type { ConnectReviewScope } from "../../src/features/kafka/application/connection-scope";
import type {
  ConnectOffsetState,
  ConnectOffsetsPort,
} from "../../src/features/kafka/application/connect-offset-types";
import {
  parseConnectOffsetsInput,
  parseConnectOffsetsReview,
  parseConnectOffsetsSnapshot,
  parseConnectOffsetsOutcome,
} from "../../src/features/kafka/contracts/connect-offsets";
import type { KafkaClusterServiceContext } from "../../src/features/kafka/application/types";
import {
  ConnectService,
  type ConnectPort,
  type ConnectMutationReceipt,
} from "../../src/features/kafka/application/connect-service";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseCorrelatedHostResponse,
} from "../../src/features/kafka/contracts";

function fixture(source = false): {
  readonly service: ConnectOffsetsService;
  readonly admission: ConnectWriteAdmission;
  readonly apply: ReturnType<typeof vi.fn<ConnectOffsetsPort["applyOffsets"]>>;
  readonly state: ConnectOffsetState;
  readonly scope: () => ConnectReviewScope | null;
  revoke(): void;
  expire(): void;
  change(state: ConnectOffsetState): void;
  cluster(value: string | null): void;
} {
  let alive = true,
    now = 1000,
    clusterId: string | null = "cluster-A";
  let state: ConnectOffsetState = {
    status: "available",
    clusterId: "cluster-A",
    workerVersion: "4.3.1",
    mapping: source ? "file-source" : "kafka-sink",
    connector: {
      config: {
        name: "orders",
        "connector.class": source
          ? "org.apache.kafka.connect.file.FileStreamSourceConnector"
          : "org.apache.kafka.connect.file.FileStreamSinkConnector",
        file: "/protected/private-source",
        password: "protected-host-password",
      },
      detail: {
        name: "orders",
        state: "STOPPED",
        tasks: [],
        config: {},
        dlq: null,
        observedAt: new Date(now).toISOString(),
      },
    },
    offsets: [
      {
        partition: source
          ? { filename: "/protected/private-source" }
          : { kafka_topic: "orders", kafka_partition: 0 },
        offset: source ? { position: 5 } : { kafka_offset: 5 },
        label: source ? "Source partition" : "orders · partition 0",
        position: 5,
      },
    ],
  };
  const context: KafkaClusterServiceContext = {
    baseUrl: "https://connect.example",
    authorization: (): Promise<undefined> => Promise.resolve(undefined),
  };
  const scope: ConnectReviewScope = {
    connectionName: "Original profile",
    isCurrent: () => alive,
    cleanupUnresolved: () => false,
    brokerClusterId: (): Promise<string | null> => Promise.resolve(clusterId),
    read: async <T>(
      run: (c: KafkaClusterServiceContext, s: AbortSignal) => Promise<T>,
      signal: AbortSignal,
    ): Promise<T> => {
      signal.throwIfAborted();
      if (!alive) throw new Error("revoked");
      const result = await run(context, signal);
      if (!alive) throw new Error("revoked");
      return result;
    },
    tryDispatch: <T>(run: (c: KafkaClusterServiceContext) => Promise<T>) =>
      alive ? { started: true as const, result: run(context) } : { started: false as const },
  };
  const apply = vi
    .fn<ConnectOffsetsPort["applyOffsets"]>()
    .mockImplementation((_context, input) => {
      state = {
        ...state,
        offsets:
          input.action === "reset"
            ? []
            : state.offsets.flatMap((item) =>
                JSON.stringify(item.partition) !== JSON.stringify(input.partition)
                  ? [item]
                  : input.offset === null
                    ? []
                    : [
                        {
                          ...item,
                          offset: input.offset,
                          position: Object.values(input.offset)[0]!,
                        },
                      ],
              ),
      };
      return Promise.resolve({
        state: "acknowledged",
        dispatch: "attempted",
        cleanup: "confirmed",
        detail: "Accepted once",
      });
    });
  const admission = new ConnectWriteAdmission();
  return {
    service: new ConnectOffsetsService(
      () => (alive ? scope : null),
      { inspectOffsets: () => Promise.resolve(structuredClone(state)), applyOffsets: apply },
      () => now,
      admission,
    ),
    admission,
    apply,
    scope: (): ConnectReviewScope | null => (alive ? scope : null),
    get state(): ConnectOffsetState {
      return state;
    },
    revoke: (): void => {
      alive = false;
    },
    expire: (): void => {
      now += 120001;
    },
    change: (next): void => {
      state = next;
    },
    cluster: (value): void => {
      clusterId = value;
    },
  };
}
it.each([false, true])(
  "reviews and verifies exact observed sink/source positions without exposing host configuration (source=%s)",
  async (source) => {
    for (const action of ["set", "remove", "reset"] as const) {
      const f = fixture(source),
        snapshot = await f.service.inspect("orders");
      expect(snapshot.status).toBe("available");
      expect(JSON.stringify(snapshot)).not.toContain("/protected/");
      const review = await f.service.review({
        snapshotId: snapshot.snapshotId!,
        action,
        partitionRef: action === "reset" ? null : snapshot.positions[0]!.partitionRef,
        position: action === "set" ? 9 : null,
      });
      expect(JSON.stringify(review)).not.toContain("protected-host-password");
      expect(JSON.stringify(review)).not.toContain("/protected/");
      expect(parseConnectOffsetsReview(review)).toEqual(review);
      expect(f.apply).not.toHaveBeenCalled();
      const outcome = await f.service.apply(review.planId, review.confirmation);
      expect(outcome).toMatchObject({
        state: "acknowledged",
        dispatch: "attempted",
        verification: "verified",
        cleanup: "confirmed",
      });
      expect(await f.service.apply(review.planId, review.confirmation)).toEqual(outcome);
      expect(f.apply).toHaveBeenCalledTimes(1);
    }
  },
);
it.each([
  "paused",
  "running-task",
  "identity",
  "config",
  "offsets",
  "task",
  "expire",
  "revoke",
  "confirmation",
])("refuses %s without sending an offset mutation", async (mode) => {
  const f = fixture(),
    snapshot = await f.service.inspect("orders"),
    input = {
      snapshotId: snapshot.snapshotId!,
      action: "set" as const,
      partitionRef: snapshot.positions[0]!.partitionRef,
      position: 9,
    };
  if (mode === "paused" || mode === "running-task") {
    f.change({
      ...f.state,
      connector: {
        ...f.state.connector,
        detail: {
          ...f.state.connector.detail,
          state: mode === "paused" ? "PAUSED" : "STOPPED",
          tasks: mode === "running-task" ? [{ id: 0, state: "RUNNING", failure: "" }] : [],
        },
      },
    });
    await expect(f.service.review(input)).rejects.toThrow();
  } else {
    const review = await f.service.review(input);
    if (mode === "identity") f.cluster("different-cluster");
    if (mode === "config")
      f.change({
        ...f.state,
        connector: {
          ...f.state.connector,
          config: { ...f.state.connector.config, password: "changed-host-password" },
        },
      });
    if (mode === "offsets")
      f.change({
        ...f.state,
        offsets: [{ ...f.state.offsets[0]!, offset: { kafka_offset: 7 }, position: 7 }],
      });
    if (mode === "task")
      f.change({
        ...f.state,
        connector: {
          ...f.state.connector,
          detail: {
            ...f.state.connector.detail,
            tasks: [{ id: 1, state: "STOPPED", failure: "" }],
          },
        },
      });
    if (mode === "expire") f.expire();
    if (mode === "revoke") f.revoke();
    if (["expire", "revoke", "confirmation"].includes(mode))
      await expect(
        f.service.apply(review.planId, mode === "confirmation" ? "yes" : review.confirmation),
      ).rejects.toThrow();
    else
      expect(await f.service.apply(review.planId, review.confirmation)).toMatchObject({
        state: "rejected",
        dispatch: "not-sent",
      });
  }
  expect(f.apply).not.toHaveBeenCalled();
});
it("refuses a foreign partition and an unavailable broker identity instead of inventing position zero", async () => {
  const f = fixture(),
    snapshot = await f.service.inspect("orders");
  await expect(
    f.service.review({
      snapshotId: snapshot.snapshotId!,
      action: "set",
      partitionRef: "foreign",
      position: 0,
    }),
  ).rejects.toThrow();
  f.cluster(null);
  expect(await f.service.inspect("orders")).toMatchObject({
    status: "unavailable",
    positions: [],
    snapshotId: null,
  });
  expect(f.apply).not.toHaveBeenCalled();
});
it("shares write admission and preserves an original late ACK after revocation", async () => {
  const f = fixture(),
    snapshot = await f.service.inspect("orders"),
    review = await f.service.review({
      snapshotId: snapshot.snapshotId!,
      action: "set",
      partitionRef: snapshot.positions[0]!.partitionRef,
      position: 9,
    });
  let finishLifecycle!: (value: ConnectMutationReceipt) => void;
  const lifecycleApply = vi.fn<ConnectPort["apply"]>().mockImplementation(
    () =>
      new Promise((resolve) => {
        finishLifecycle = resolve;
      }),
  );
  const lifecycle = new ConnectService(
    f.scope,
    {
      list: (): Promise<{ names: readonly string[]; plugins: readonly string[] }> =>
        Promise.resolve({ names: ["orders"], plugins: [] }),
      load: (): ReturnType<ConnectPort["load"]> => Promise.resolve(f.state.connector),
      validate: (): ReturnType<ConnectPort["validate"]> => Promise.resolve({ issues: [] }),
      apply: lifecycleApply,
    },
    () => 1000,
    f.admission,
  );
  const lifecycleReview = await lifecycle.review({ name: "orders", action: "pause", config: {} });
  const pendingLifecycle = lifecycle.apply(lifecycleReview.planId, lifecycleReview.confirmation);
  await vi.waitFor(() => expect(lifecycleApply).toHaveBeenCalledTimes(1));
  expect((await f.service.apply(review.planId, review.confirmation)).dispatch).toBe("not-sent");
  finishLifecycle({
    state: "acknowledged",
    dispatch: "attempted",
    cleanup: "confirmed",
    detail: "Actual lifecycle ACK",
  });
  await pendingLifecycle;
  expect(f.apply).not.toHaveBeenCalled();
  const next = await f.service.review({
    snapshotId: snapshot.snapshotId!,
    action: "set",
    partitionRef: snapshot.positions[0]!.partitionRef,
    position: 9,
  });
  let finish!: (value: ConnectMutationReceipt) => void;
  f.apply.mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const pending = f.service.apply(next.planId, next.confirmation);
  await vi.waitFor(() => expect(f.apply).toHaveBeenCalledTimes(1));
  f.revoke();
  finish({
    state: "acknowledged",
    dispatch: "attempted",
    cleanup: "confirmed",
    detail: "Actual ACK",
  });
  expect(await pending).toMatchObject({
    state: "acknowledged",
    verification: "unavailable",
    cleanup: "confirmed",
  });
  expect(await f.service.apply(next.planId, next.confirmation)).toEqual(await pending);
  expect(f.apply).toHaveBeenCalledTimes(1);
});
it("keeps unknown/failed cleanup separate and refuses unsafe or ambiguous closed contracts", async () => {
  const f = fixture(),
    snapshot = await f.service.inspect("orders"),
    input = {
      snapshotId: snapshot.snapshotId!,
      action: "remove" as const,
      partitionRef: snapshot.positions[0]!.partitionRef,
      position: null,
    };
  const review = await f.service.review(input);
  f.apply.mockResolvedValue({
    state: "unknown",
    dispatch: "attempted",
    cleanup: "unresolved",
    detail: "No reply",
  });
  const uncertain = await f.service.apply(review.planId, review.confirmation);
  expect(uncertain).toMatchObject({
    state: "unknown",
    verification: "not-applicable",
    cleanup: "unresolved",
  });
  expect(parseConnectOffsetsOutcome(uncertain)).toEqual(uncertain);
  const submitted = parseHostCommand({
    command: "connect.offsets.apply",
    id: "original-offset",
    version: HOST_PROTOCOL_VERSION,
    payload: { planId: review.planId, confirmation: review.confirmation },
  });
  for (const foreign of [
    { ...uncertain, planId: "foreign" },
    { ...uncertain, confirmation: "remove OFFSETS other" },
  ])
    expect(() =>
      parseCorrelatedHostResponse(
        {
          id: submitted.id,
          command: submitted.command,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: { correlationId: "corr", outcome: foreign },
        },
        submitted,
      ),
    ).toThrow();
  for (const position of [-1, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, "5"])
    expect(() => parseConnectOffsetsInput({ ...input, action: "set", position })).toThrow();
  expect(() => parseConnectOffsetsInput({ ...input, extra: "unreviewed" })).toThrow();
  expect(() =>
    parseConnectOffsetsSnapshot({
      ...snapshot,
      positions: [...snapshot.positions, ...snapshot.positions],
    }),
  ).toThrow();
  const command = parseHostCommand({
    command: "connect.offsets.review",
    id: "correlated",
    version: HOST_PROTOCOL_VERSION,
    payload: input,
  });
  expect(() =>
    parseCorrelatedHostResponse(
      {
        id: command.id,
        command: command.command,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: {
          correlationId: "corr",
          review: { ...review, input: { ...input, partitionRef: "foreign" } },
        },
      },
      command,
    ),
  ).toThrow();
});
it("an old repeated release cannot release a subsequent Connect writer", () => {
  const admission = new ConnectWriteAdmission(),
    old = admission.acquire()!;
  old();
  const current = admission.acquire()!;
  old();
  expect(admission.acquire()).toBeNull();
  current();
  expect(admission.acquire()).not.toBeNull();
});
