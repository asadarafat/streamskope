import { randomUUID } from "node:crypto";

import { afterEach, expect, it, vi } from "vitest";

import { KafkaOperationalPreferenceService } from "../../src/features/kafka/application";
import type { RecordReadScope } from "../../src/features/kafka/application/connection-scope";
import { InMemoryKafkaOperationalPreferenceStore } from "../../src/features/kafka/application/in-memory-operational-preference-store";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommandResponse,
  type HostCommand,
  type HostEvent,
} from "../../src/features/kafka/contracts";
import type { KafkaRecordLocator } from "../../src/features/kafka/contracts/record-locator";
import { RecordRangeFacade } from "../../src/features/kafka/facade/record-range-facade";
import {
  command,
  ControlledMessageStream,
  createFacade,
  message,
  RecordingActiveConnection,
  RecordingConnectionPort,
  settleAsyncIteration,
} from "../support/kafka-backend-facade-fixture";

const locator: KafkaRecordLocator = {
  schemaVersion: 1,
  clusterId: "fixture-cluster",
  topicId: "12345678-1234-1234-1234-123456789abc",
  topic: "orders",
  partition: 0,
  offset: "7",
  leaderEpoch: 3,
};
const cleanups: Array<() => Promise<void>> = [];
const releases: Array<() => void> = [];
afterEach(async () => {
  for (const release of releases.splice(0)) release();
  for (const cleanup of cleanups.splice(0)) await cleanup();
});
function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  releases.push(() => resolve());
  return { promise, resolve };
}
async function flush(): Promise<void> {
  for (let index = 0; index < 12; index += 1) await settleAsyncIteration();
}
function load(requestId = randomUUID()): Extract<HostCommand, { command: "records.locator.load" }> {
  return {
    command: "records.locator.load",
    id: randomUUID(),
    version: HOST_PROTOCOL_VERSION,
    payload: { requestId, locator },
  };
}
function cancel(requestId: string): Extract<HostCommand, { command: "records.locator.cancel" }> {
  return {
    command: "records.locator.cancel",
    id: randomUUID(),
    version: HOST_PROTOCOL_VERSION,
    payload: { requestId },
  };
}
function fixture(preferences?: KafkaOperationalPreferenceService): {
  facade: ReturnType<typeof createFacade>;
  port: RecordingConnectionPort;
  active: RecordingActiveConnection;
  stream: ControlledMessageStream;
  opened: ReturnType<typeof vi.fn<() => Promise<ControlledMessageStream>>>;
  events: HostEvent[];
} {
  const port = new RecordingConnectionPort();
  const active = new RecordingActiveConnection();
  const stream = new ControlledMessageStream();
  const opened = vi.fn(() => Promise.resolve(stream));
  active.messageStreamOperations.push(opened);
  port.openOperations.push(() => Promise.resolve(active));
  const facade = createFacade(port, undefined, undefined, undefined, preferences);
  const events: HostEvent[] = [];
  facade.subscribe((event) => events.push(event));
  cleanups.push(() => facade.shutdown());
  return { facade, port, active, stream, opened, events };
}

it("returns the protected record only after reader cleanup without adding it to the grid", async () => {
  const { facade, stream, events } = fixture();
  await facade.execute(command("connection.connect", "connect"));
  const closing = deferred();
  const close = vi.spyOn(stream, "close").mockImplementation(() => {
    stream.end();
    return closing.promise;
  });
  let settled = false;
  const pending = facade.execute(load()).then((response) => {
    settled = true;
    return response;
  });
  await flush();
  stream.push({
    ...message("7", "[MASKED]"),
    topic: locator.topic,
    original: { state: "unavailable", reason: "masked" },
    provenance: {
      clusterId: locator.clusterId,
      topicId: locator.topicId,
      leaderEpoch: locator.leaderEpoch,
    },
  });
  await vi.waitFor(() => expect(close).toHaveBeenCalled());
  expect(settled).toBe(false);
  closing.resolve();
  const response = await pending;
  expect(response).toMatchObject({
    ok: true,
    result: { outcome: { state: "loaded", message: { payload: "[MASKED]" } } },
  });
  expect(parseHostCommandResponse(response)).toEqual(response);
  expect(events.some((event) => event.event === "messages.batch")).toBe(false);
});

it("joins reload cleanup before reconnect and suppresses a decoded result from the old connection", async () => {
  const { facade, port, stream } = fixture();
  await facade.execute(command("connection.connect", "connect"));
  const closing = deferred();
  const close = vi.spyOn(stream, "close").mockImplementation(() => {
    stream.end();
    return closing.promise;
  });
  const pending = facade.execute(load());
  await flush();
  const replacement = vi.fn(() => Promise.resolve(new RecordingActiveConnection()));
  port.openOperations.push(replacement);
  const reconnect = facade.execute(command("connection.connect", "replace"));
  await vi.waitFor(() => expect(close).toHaveBeenCalled());
  expect(replacement).not.toHaveBeenCalled();
  expect(await facade.execute(load())).toMatchObject({ ok: false, error: { code: "VALIDATION" } });
  closing.resolve();
  expect(await pending).toMatchObject({ ok: true, result: { outcome: { state: "revoked" } } });
  expect(await reconnect).toMatchObject({ ok: true });
  expect(replacement).toHaveBeenCalledOnce();
});

it("reports a competing reload as unavailable without creating cleanup debt for its unowned ID", async () => {
  const { facade, opened } = fixture();
  await facade.execute(command("connection.connect", "connect"));
  const original = load();
  const pending = facade.execute(original);
  await vi.waitFor(() => expect(opened).toHaveBeenCalledOnce());
  const competing = load();
  const response = await facade.execute(competing);
  expect(response).toMatchObject({
    ok: true,
    result: {
      outcome: {
        state: "unavailable",
        requestId: competing.payload.requestId,
      },
    },
  });
  expect(parseHostCommandResponse(response)).toEqual(response);
  expect(opened).toHaveBeenCalledOnce();
  // Its normal no-reader response may be delayed in transport after a user clicks Stop.
  expect(await facade.execute(cancel(competing.payload.requestId))).toMatchObject({
    ok: true,
    result: { stopped: true },
  });
  expect(await facade.execute(cancel(original.payload.requestId))).toMatchObject({ ok: true });
  expect(await pending).toMatchObject({ ok: true, result: { outcome: { state: "cancelled" } } });
});

it("confirms exact Stop after failed protection admission without fabricating an owned reader", async () => {
  const preferences = new KafkaOperationalPreferenceService(
    new InMemoryKafkaOperationalPreferenceStore({ durability: "session", state: "ready" }),
  );
  const { facade, opened } = fixture(preferences);
  await facade.execute(command("connection.connect", "connect"));
  const snapshot = preferences.currentSnapshot();
  vi.spyOn(preferences, "get").mockResolvedValueOnce({
    ...snapshot,
    store: { ...snapshot.store, state: "unavailable" },
  });
  const request = load();
  expect(await facade.execute(request)).toMatchObject({ ok: false });
  expect(opened).not.toHaveBeenCalled();
  expect(await facade.execute(cancel(request.payload.requestId))).toMatchObject({
    ok: true,
    result: { stopped: true },
  });
  expect(await facade.execute(cancel(randomUUID()))).toMatchObject({ ok: false });
});

it("keeps failed-close authority and permits only the exact retry to confirm cleanup", async () => {
  const { facade, stream, events, opened } = fixture();
  let fail = true;
  releases.push(() => {
    fail = false;
  });
  const close = vi.spyOn(stream, "close").mockImplementation(() => {
    stream.end();
    return fail
      ? Promise.reject(new Error("password=private-secret; broker=private-host"))
      : Promise.resolve();
  });
  await facade.execute(command("connection.connect", "connect"));
  const request = load();
  const pending = facade.execute(request);
  await vi.waitFor(() => expect(opened).toHaveBeenCalledOnce());
  const stop = await facade.execute(cancel(request.payload.requestId));
  expect(stop).toMatchObject({ ok: false });
  const failed = await pending;
  expect(failed).toMatchObject({ ok: false });
  expect(await facade.execute(load())).toMatchObject({
    ok: true,
    result: { outcome: { state: "unavailable" } },
  });
  const calls = close.mock.calls.length;
  expect(await facade.execute(cancel(randomUUID()))).toMatchObject({ ok: false });
  expect(close.mock.calls.length).toBe(calls);
  fail = false;
  const stopped = await facade.execute(cancel(request.payload.requestId));
  expect(stopped).toMatchObject({ ok: true, result: { stopped: true } });
  expect(close.mock.calls.length).toBeGreaterThan(calls);
  expect(parseHostCommandResponse(stopped)).toEqual(stopped);
  expect(JSON.stringify([events, stop, failed, stopped])).not.toMatch(
    /private-secret|private-host/u,
  );
});

it("waits for the reload owner at a codec change and never proves cleanup for an unknown ID", async () => {
  const stream = new ControlledMessageStream();
  const closing = deferred();
  const close = vi.spyOn(stream, "close").mockImplementation(() => {
    stream.end();
    return closing.promise;
  });
  const preferences = new KafkaOperationalPreferenceService(
    new InMemoryKafkaOperationalPreferenceStore({ durability: "session", state: "ready" }),
  );
  await preferences.get();
  const ranges = new RecordRangeFacade(
    {
      recordReadScope: (): RecordReadScope => ({
        connectionName: "fixture",
        isCurrent: (): boolean => true,
        openMessageStream: (): Promise<ControlledMessageStream> => Promise.resolve(stream),
      }),
    },
    preferences,
    undefined,
    () => 1,
    () => undefined,
    () => undefined,
  );
  cleanups.push(async () => {
    ranges.invalidate();
    await ranges.idle();
  });
  expect(await ranges.execute(cancel(randomUUID()), "unknown")).toMatchObject({ ok: false });
  const pending = ranges.execute(load(), "load");
  await flush();
  let changed = false;
  const settings = ranges
    .preparePreferences({
      command: "preferences.update",
      id: "settings",
      version: HOST_PROTOCOL_VERSION,
      payload: { patch: { codecs: { key: "auto", value: "json" } } },
    })
    .then(() => {
      changed = true;
    });
  await vi.waitFor(() => expect(close).toHaveBeenCalled());
  expect(changed).toBe(false);
  closing.resolve();
  await settings;
  expect(await pending).toMatchObject({ ok: true, result: { outcome: { state: "revoked" } } });
});
