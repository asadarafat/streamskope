import { afterEach, describe, expect, it, vi } from "vitest";

import type { NatsProfileCreateInput } from "../../src/features/nats/contracts";
import {
  createNatsWorkspaceOwner,
  type NatsWorkspaceOwner,
} from "../../src/features/nats/ui/workspace-controller";
import { initialNatsWorkspaceSnapshot } from "../../src/features/nats/ui/workspace-state";
import {
  natsUiHostFixture,
  uiNatsProfile,
  uiNatsProfiles,
  uiNatsRecord,
  uiNatsSubscription,
} from "../support/nats-ui-host-fixture";

const owners: NatsWorkspaceOwner[] = [];
afterEach(() => {
  for (const owner of owners.splice(0)) owner.dispose();
});
function fixture(interactive: () => boolean = (): boolean => true): {
  readonly host: ReturnType<typeof natsUiHostFixture>;
  readonly owner: NatsWorkspaceOwner;
} {
  const host = natsUiHostFixture();
  const owner = createNatsWorkspaceOwner(host.host, interactive);
  owners.push(owner);
  owner.start();
  return { host, owner };
}
async function ready(f: ReturnType<typeof fixture>): Promise<void> {
  f.host.bootstrap();
  await vi.waitFor(() => expect(f.owner.snapshot().loading).toBe(false));
}
function subscription(f: ReturnType<typeof fixture>, snapshot = uiNatsSubscription()): void {
  f.host.emit({
    event: "subscription.changed",
    operation: "subscription.start",
    correlationId: "subscription-request",
    payload: snapshot,
  });
}
const input: NatsProfileCreateInput = {
  name: "Created",
  servers: ["nats://127.0.0.1:4222"],
  authentication: { mode: "none" },
  tls: { mode: "plaintext" },
};

describe("NATS mounted workspace authority", () => {
  it("observes profiles and control revisions without retaining or publishing live records in management mode", async () => {
    const host = natsUiHostFixture();
    const owner = createNatsWorkspaceOwner(host.host, () => true, "control-only");
    owners.push(owner);
    owner.start();
    host.bootstrap();
    await vi.waitFor(() => expect(owner.snapshot().loading).toBe(false));
    host.emit({
      event: "subscription.changed",
      operation: "subscription.start",
      correlationId: "start",
      payload: uiNatsSubscription(),
    });
    const before = owner.snapshot();
    const listener = vi.fn();
    owner.subscribe(listener);
    host.emit({
      event: "records.batch",
      operation: "subscription.start",
      correlationId: "start",
      payload: {
        generation: "generation-1",
        records: [uiNatsRecord()],
        counters: uiNatsSubscription("streaming", "generation-1", 1).counters,
      },
    });
    expect(owner.snapshot()).toBe(before);
    expect(listener).not.toHaveBeenCalled();
    expect(owner.snapshot().records).toEqual([]);
    host.emit({
      event: "connection.state",
      operation: "connection.disconnect",
      correlationId: "disconnect",
      payload: { revision: 2, state: "disconnected", profile: null },
    });
    host.emit({
      event: "connection.state",
      operation: "profiles.connect",
      correlationId: "late-connect",
      payload: {
        revision: 1,
        state: "connected",
        profile: { id: uiNatsProfile.id, revision: 1, name: uiNatsProfile.name },
      },
    });
    expect(owner.snapshot().connection).toMatchObject({ revision: 2, state: "disconnected" });
  });

  it("subscribes before bootstrap and removes exactly its admitted listener", () => {
    const f = fixture();
    expect(f.host.calls).toEqual(["subscribe", "profiles.list"]);
    expect(f.host.listenerCount()).toBe(1);
    f.owner.dispose();
    f.owner.dispose();
    expect(f.host.calls).toEqual(["subscribe", "profiles.list", "unsubscribe"]);
    expect(f.host.listenerCount()).toBe(0);
  });
  it("fences each bootstrap section independently against newer events", async () => {
    const f = fixture();
    const newer = { ...uiNatsProfile, name: "Newer profile", revision: 2 };
    f.host.emit({
      event: "profiles.changed",
      operation: "profiles.update",
      correlationId: "update-request",
      payload: uiNatsProfiles([newer], 1),
    });
    f.host.emit({
      event: "connection.state",
      operation: "profiles.connect",
      correlationId: "connect-request",
      payload: {
        revision: 1,
        state: "connected",
        profile: { id: newer.id, revision: newer.revision, name: newer.name },
      },
    });
    subscription(f);
    f.host.bootstrap();
    await vi.waitFor(() => expect(f.owner.snapshot().loading).toBe(false));
    expect(f.owner.snapshot().profiles?.profiles[0]?.name).toBe("Newer profile");
    expect(f.owner.snapshot().connection.state).toBe("connected");
    expect(f.owner.snapshot().subscription.state).toBe("streaming");
  });
  it("keeps a newer overlapping read receipt without requiring an event", async () => {
    const f = fixture();
    const newerRead = f.owner.refresh();
    const initial = initialNatsWorkspaceSnapshot();
    f.host.requests[1]!.answer({
      profiles: uiNatsProfiles([{ ...uiNatsProfile, name: "Newest" }], 1),
      connection: initial.connection,
      subscription: initial.subscription,
    });
    expect(await newerRead).toBe(true);
    f.host.bootstrap();
    await vi.waitFor(() => expect(f.owner.snapshot().pending).toEqual([]));
    expect(f.owner.snapshot().profiles?.profiles[0]?.name).toBe("Newest");
    expect(f.owner.snapshot().loading).toBe(false);
  });
  it("retains same-generation transport omissions across global list replies", async () => {
    const f = fixture();
    await ready(f);
    subscription(f, uiNatsSubscription("streaming", "generation-1", 10, 6));
    const read = f.owner.refresh();
    f.host.requests[1]!.answer({
      profiles: uiNatsProfiles(),
      connection: initialNatsWorkspaceSnapshot().connection,
      subscription: uiNatsSubscription("streaming", "generation-1", 5, 0),
    });
    await read;
    expect(f.owner.snapshot().subscription.counters).toMatchObject({
      receivedRecords: 10,
      publishedRecords: 10,
      transportOmittedRecords: 6,
    });
  });
  it("keeps newer queue gauges from a batch when a same-revision snapshot reply arrives late", async () => {
    const f = fixture();
    await ready(f);
    subscription(f);
    const read = f.owner.refresh();
    f.host.emit({
      event: "records.batch",
      operation: "subscription.start",
      correlationId: "start",
      payload: {
        generation: "generation-1",
        records: [uiNatsRecord()],
        counters: {
          ...uiNatsSubscription("streaming", "generation-1", 4).counters,
          publishedRecords: 1,
          queuedRecords: 3,
          queuedBytes: 300,
        },
      },
    });
    f.host.requests[1]!.answer({
      profiles: uiNatsProfiles(),
      connection: initialNatsWorkspaceSnapshot().connection,
      subscription: {
        ...uiNatsSubscription("streaming", "generation-1", 2),
        counters: {
          ...uiNatsSubscription("streaming", "generation-1", 2).counters,
          publishedRecords: 1,
          queuedRecords: 1,
          queuedBytes: 100,
        },
      },
    });
    expect(await read).toBe(true);
    expect(f.owner.snapshot().subscription.counters).toMatchObject({
      receivedRecords: 4,
      publishedRecords: 1,
      queuedRecords: 3,
      queuedBytes: 300,
    });
  });
  it("ignores late batches and terminal events from a replaced generation", async () => {
    const f = fixture();
    await ready(f);
    subscription(f);
    f.host.emit({
      event: "records.batch",
      operation: "subscription.start",
      correlationId: "old-start",
      payload: {
        generation: "generation-1",
        records: [uiNatsRecord()],
        counters: uiNatsSubscription("streaming", "generation-1", 1).counters,
      },
    });
    f.owner.selectRecord("record-1");
    subscription(f, uiNatsSubscription("loading", "generation-2", 0, 0, 6));
    subscription(f, uiNatsSubscription("stopped", "generation-1", 1));
    f.host.emit({
      event: "records.batch",
      operation: "subscription.start",
      correlationId: "old-start",
      payload: {
        generation: "generation-1",
        records: [uiNatsRecord("late")],
        counters: uiNatsSubscription("streaming", "generation-1", 2).counters,
      },
    });
    expect(f.owner.snapshot().subscription).toMatchObject({
      state: "loading",
      generation: "generation-2",
    });
    expect(f.owner.snapshot().records).toEqual([]);
    expect(f.owner.snapshot().selectedRecord).toBeNull();
  });
  it("retains stopped evidence and refuses to revive it with late streaming events", async () => {
    const f = fixture();
    await ready(f);
    subscription(f);
    const record = uiNatsRecord();
    f.host.emit({
      event: "records.batch",
      operation: "subscription.start",
      correlationId: "start",
      payload: {
        generation: "generation-1",
        records: [record],
        counters: uiNatsSubscription("streaming", "generation-1", 1).counters,
      },
    });
    subscription(f, uiNatsSubscription("stopped", "generation-1", 1));
    subscription(f, uiNatsSubscription("streaming", "generation-1", 1));
    expect(f.owner.snapshot().subscription.state).toBe("stopped");
    expect(f.owner.snapshot().records).toEqual([record]);
  });
  it("keeps a new subscription receipt when old loading and streaming events arrive later", async () => {
    const f = fixture();
    await ready(f);
    subscription(f);
    const starting = f.owner.startSubscription("next.*");
    f.host.requests[1]!.answer({
      subscription: uiNatsSubscription("streaming", "generation-2", 0, 0, 6),
    });
    expect(await starting).toBe(true);
    subscription(f, uiNatsSubscription("loading", "generation-1"));
    subscription(f, uiNatsSubscription("streaming", "generation-1"));
    f.host.emit({
      event: "records.batch",
      operation: "subscription.start",
      correlationId: "old-start",
      payload: {
        generation: "generation-1",
        records: [uiNatsRecord("late")],
        counters: uiNatsSubscription().counters,
      },
    });
    expect(f.owner.snapshot().subscription).toMatchObject({
      revision: 6,
      state: "streaming",
      generation: "generation-2",
    });
    expect(f.owner.snapshot().records).toEqual([]);
  });
  it("does not revive a disconnected connection with control events queued before its receipt", async () => {
    const f = fixture();
    await ready(f);
    const profile = {
      id: uiNatsProfile.id,
      name: uiNatsProfile.name,
      revision: uiNatsProfile.revision,
    };
    f.host.emit({
      event: "connection.state",
      operation: "profiles.connect",
      correlationId: "old-connect",
      payload: { revision: 2, state: "connected", profile },
    });
    const disconnecting = f.owner.disconnect();
    f.host.requests[1]!.answer({
      connection: { revision: 4, state: "disconnected", profile: null },
      subscription: { ...initialNatsWorkspaceSnapshot().subscription, revision: 2 },
    });
    expect(await disconnecting).toBe(true);
    f.host.emit({
      event: "connection.state",
      operation: "profiles.connect",
      correlationId: "old-connect",
      payload: { revision: 1, state: "connecting", profile },
    });
    f.host.emit({
      event: "connection.state",
      operation: "profiles.connect",
      correlationId: "old-connect",
      payload: { revision: 2, state: "connected", profile },
    });
    expect(f.owner.snapshot().connection).toEqual({
      revision: 4,
      state: "disconnected",
      profile: null,
    });
  });
  it("keeps a newer asynchronous failure when an older successful receipt arrives", async () => {
    const f = fixture();
    await ready(f);
    const starting = f.owner.startSubscription("qualification.*");
    subscription(f, {
      ...uiNatsSubscription("failed", "generation-1", 0, 0, 3),
      failure: {
        code: "connection",
        summary: "The server closed the connection.",
        recovery: "Reconnect the profile.",
      },
    });
    f.host.requests[1]!.answer({
      subscription: uiNatsSubscription("streaming", "generation-1", 0, 0, 2),
    });
    expect(await starting).toBe(true);
    expect(f.owner.snapshot().subscription).toMatchObject({
      revision: 3,
      state: "failed",
      failure: { summary: "The server closed the connection." },
    });
  });
  it("retains a pre-stop batch delivered after the stop receipt without reviving queue or subscription", async () => {
    const f = fixture();
    await ready(f);
    subscription(f);
    const stopping = f.owner.stopSubscription();
    f.host.requests[1]!.answer({ subscription: uiNatsSubscription("stopped", "generation-1", 2) });
    expect(await stopping).toBe(true);
    const record = uiNatsRecord("pre-stop");
    f.host.emit({
      event: "records.batch",
      operation: "subscription.start",
      correlationId: "old-start",
      payload: {
        generation: "generation-1",
        records: [record],
        counters: {
          ...uiNatsSubscription("streaming", "generation-1", 2).counters,
          publishedRecords: 1,
          queuedRecords: 1,
          queuedBytes: 100,
        },
      },
    });
    expect(f.owner.snapshot().records).toEqual([record]);
    expect(f.owner.snapshot().subscription).toMatchObject({
      state: "stopped",
      counters: {
        queuedRecords: 0,
        queuedBytes: 0,
        applicationOmittedRecords: 0,
        transportOmittedRecords: 0,
      },
    });
    expect(f.owner.snapshot().viewerOmittedRecords).toBe(0);
  });
  it("bounds viewer records and clears an evicted selection with an explicit notice", async () => {
    const f = fixture();
    await ready(f);
    subscription(f);
    for (let group = 0; group < 6; group += 1) {
      const records = Array.from({ length: 200 }, (_, index) =>
        uiNatsRecord(`record-${String(group * 200 + index)}`),
      );
      f.host.emit({
        event: "records.batch",
        operation: "subscription.start",
        correlationId: "start",
        payload: {
          generation: "generation-1",
          records,
          counters: uiNatsSubscription("streaming", "generation-1", (group + 1) * 200).counters,
        },
      });
      if (group === 0) f.owner.selectRecord("record-0");
    }
    expect(f.owner.snapshot().records).toHaveLength(1000);
    expect(f.owner.snapshot().viewerOmittedRecords).toBe(200);
    expect(f.owner.snapshot().selectedRecord).toBeNull();
    expect(f.owner.snapshot().selectionNotice).toContain("left the live window");
    expect(f.owner.snapshot().subscription.counters.applicationOmittedRecords).toBe(0);
  });
  it("allows cleanup events while inactive but refuses new commands and selection", async () => {
    let interactive = true;
    const f = fixture(() => interactive);
    await ready(f);
    subscription(f);
    interactive = false;
    expect(await f.owner.createProfile(input)).toBe(false);
    expect(f.host.requests).toHaveLength(1);
    subscription(f, uiNatsSubscription("stopped"));
    expect(f.owner.snapshot().subscription.state).toBe("stopped");
  });
  it("preserves an admitted committed receipt after disposing its view", async () => {
    const f = fixture();
    await ready(f);
    const operation = f.owner.createProfile(input);
    const snapshot = f.owner.snapshot();
    f.owner.dispose();
    f.host.requests[1]!.answer({
      profiles: uiNatsProfiles([{ ...uiNatsProfile, name: "Created" }]),
    });
    expect(await operation).toBe(true);
    expect(f.owner.snapshot()).toBe(snapshot);
    expect(f.host.listenerCount()).toBe(0);
  });
  it("re-reads authoritative state when host availability recovers", async () => {
    const f = fixture();
    await ready(f);
    f.host.emit({
      event: "backend.availability",
      payload: { state: "unavailable", recovery: "Reconnect to the host." },
    });
    expect(f.owner.snapshot().available).toBe(false);
    expect(await f.owner.connectProfile(uiNatsProfile)).toBe(false);
    f.host.emit({ event: "backend.availability", payload: { state: "ready" } });
    expect(f.host.requests).toHaveLength(2);
    f.host.bootstrap(f.host.requests[1]);
    await vi.waitFor(() => expect(f.owner.snapshot().loading).toBe(false));
    expect(f.owner.snapshot().available).toBe(true);
  });
  it("does not echo an unexpected host rejection into the public view", async () => {
    const f = fixture();
    await ready(f);
    const operation = f.owner.connectProfile(uiNatsProfile);
    const privateDetail = globalThis.crypto.randomUUID();
    f.host.requests[1]!.reject(new Error(privateDetail));
    expect(await operation).toBe(false);
    expect(JSON.stringify(f.owner.snapshot()).includes(privateDetail)).toBe(false);
    expect(f.owner.snapshot().failure?.summary).toBe("The NATS request could not be completed.");
  });
});
