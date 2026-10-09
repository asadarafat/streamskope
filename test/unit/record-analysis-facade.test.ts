import { randomUUID } from "node:crypto";

import { afterEach, expect, it, vi } from "vitest";

import { KafkaOperationalPreferenceService } from "../../src/features/kafka/application";
import { InMemoryKafkaOperationalPreferenceStore } from "../../src/features/kafka/application/in-memory-operational-preference-store";
import type { RecordReadScope } from "../../src/features/kafka/application/connection-scope";
import type {
  RecordExportArtifacts,
  RecordExportSink,
} from "../../src/features/kafka/application/record-export-artifacts";
import {
  HOST_PROTOCOL_VERSION,
  KAFKA_RECORD_PROTECTION_DEFAULTS,
  parseHostCommandResponse,
  parseHostEvent,
  type HostCommand,
  type HostEvent,
  type RecordAnalysisInput,
} from "../../src/features/kafka/contracts";
import { RecordRangeFacade } from "../../src/features/kafka/facade/record-range-facade";
import {
  command,
  ControlledMessageStream,
  createFacade,
  RecordingActiveConnection,
  RecordingConnectionPort,
  settleAsyncIteration,
} from "../support/kafka-backend-facade-fixture";

const cleanup: Array<() => Promise<void>> = [];
const unblock: Array<() => void> = [];
const captured: HostEvent[] = [];
afterEach(async () => {
  for (const release of unblock.splice(0)) release();
  await Promise.all(cleanup.splice(0).map((close) => close()));
  for (const event of captured.splice(0)) expect(parseHostEvent(event)).toEqual(event);
});
function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  unblock.push(() => resolve());
  return { promise, resolve };
}
async function flush(): Promise<void> {
  for (let index = 0; index < 10; index += 1) await settleAsyncIteration();
}
function input(): RecordAnalysisInput {
  return {
    requestId: randomUUID(),
    topic: "orders",
    range: { mode: "earliest" },
    maxRecords: 10,
    search: { key: "", value: "", offset: "", timestamp: "", partition: null },
    columns: [],
    groupBy: null,
  };
}
function start(payload = input()): Extract<HostCommand, { command: "records.analysis.start" }> {
  return {
    command: "records.analysis.start",
    id: randomUUID(),
    payload,
    version: HOST_PROTOCOL_VERSION,
  };
}
function exportStart(): Extract<HostCommand, { command: "records.export.start" }> {
  const { requestId, topic, range, maxRecords, search } = input();
  return {
    command: "records.export.start",
    id: randomUUID(),
    payload: { requestId, topic, range, maxRecords, search, format: "jsonl" },
    version: HOST_PROTOCOL_VERSION,
  };
}
function status(): Extract<HostCommand, { command: "records.analysis.status" }> {
  return {
    command: "records.analysis.status",
    id: randomUUID(),
    payload: {},
    version: HOST_PROTOCOL_VERSION,
  };
}
function artifactOwner(discard: () => Promise<void>): {
  readonly artifacts: RecordExportArtifacts;
  readonly revoke: ReturnType<typeof vi.fn<RecordExportArtifacts["revoke"]>>;
} {
  const sink: RecordExportSink = {
    write: () => Promise.resolve(),
    discard,
    seal: () => Promise.reject(new Error("Fixture expects revocation before sealing.")),
  };
  const revoke = vi.fn<RecordExportArtifacts["revoke"]>();
  return {
    artifacts: {
      create: vi.fn(() => Promise.resolve(sink)),
      revoke,
      drain: () => Promise.resolve(),
    },
    revoke,
  };
}
function fixture(artifacts?: RecordExportArtifacts): {
  facade: ReturnType<typeof createFacade>;
  port: RecordingConnectionPort;
  active: RecordingActiveConnection;
  stream: ControlledMessageStream;
  opened: ReturnType<typeof vi.fn<() => Promise<ControlledMessageStream>>>;
} {
  const port = new RecordingConnectionPort();
  const active = new RecordingActiveConnection();
  const stream = new ControlledMessageStream();
  const opened = vi.fn(() => Promise.resolve(stream));
  active.messageStreamOperations.push(opened);
  port.openOperations.push(() => Promise.resolve(active));
  const facade = createFacade(
    port,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    artifacts,
  );
  facade.subscribe((event) => captured.push(event));
  cleanup.push(() => facade.shutdown());
  return { facade, port, active, stream, opened };
}

it("admits analysis without a file owner and coalesces retries using its dedicated request identity", async () => {
  const { facade, opened } = fixture();
  expect(await facade.execute(command("connection.connect", "connect"))).toMatchObject({
    ok: true,
  });
  expect(
    await facade.execute({
      command: "records.export.status",
      id: "export-status",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    }),
  ).toMatchObject({
    ok: true,
    result: { snapshot: { available: false } },
  });
  const payload = input();
  const first = await facade.execute(start(payload));
  await flush();
  expect(first).toMatchObject({
    ok: true,
    result: { snapshot: { operation: { input: payload } } },
  });
  expect(parseHostCommandResponse(first)).toEqual(first);
  const retry = await facade.execute(start(payload));
  expect(retry).toMatchObject({ ok: true });
  if (
    !first.ok ||
    first.command !== "records.analysis.start" ||
    !retry.ok ||
    retry.command !== "records.analysis.start"
  )
    throw new Error("Missing analysis admission.");
  expect(retry.result.snapshot.operation?.jobId).toBe(first.result.snapshot.operation?.jobId);
  expect(opened).toHaveBeenCalledOnce();
  expect(await facade.execute(start({ ...payload, maxRecords: 9 }))).toMatchObject({
    ok: false,
    error: { code: "VALIDATION" },
  });
  expect(opened).toHaveBeenCalledOnce();
});

it("revokes and joins both range owners before reconnect and fences reentrant analysis admission", async () => {
  const filesClosed = deferred();
  const readerClosed = deferred();
  const { artifacts, revoke } = artifactOwner(() => filesClosed.promise);
  const { facade, port, active, stream, opened } = fixture(artifacts);
  const closeAnalysis = vi.spyOn(stream, "close").mockImplementation(() => {
    stream.end();
    return readerClosed.promise;
  });
  const exportStream = new ControlledMessageStream();
  active.messageStreamOperations.push(() => Promise.resolve(exportStream));
  await facade.execute(command("connection.connect", "connect"));
  await facade.execute(start());
  await facade.execute(exportStart());
  await flush();
  const replacement = vi.fn(() => Promise.resolve(new RecordingActiveConnection()));
  port.openOperations.push(replacement);
  let reentrant: ReturnType<typeof facade.execute> | undefined;
  facade.subscribe((event) => {
    if (
      event.event === "records.analysis.changed" &&
      event.payload.operation?.reason === "revoked" &&
      reentrant === undefined
    )
      reentrant = facade.execute(start());
  });
  let settled = false;
  const reconnect = facade.execute(command("connection.connect", "replace")).then((response) => {
    settled = true;
    return response;
  });
  await flush();
  expect(closeAnalysis).toHaveBeenCalled();
  expect(exportStream.closeCalls).toBeGreaterThan(0);
  expect(revoke).toHaveBeenCalled();
  expect(await reentrant).toMatchObject({
    ok: false,
    error: {
      code: "VALIDATION",
      summary: "Wait for the connection change before starting an analysis.",
    },
  });
  expect(opened).toHaveBeenCalledOnce();
  expect(await facade.execute(status())).toMatchObject({
    ok: true,
    result: { snapshot: { operation: { result: null } } },
  });
  filesClosed.resolve();
  await flush();
  expect(settled).toBe(false);
  expect(replacement).not.toHaveBeenCalled();
  readerClosed.resolve();
  expect(await reconnect).toMatchObject({ ok: true });
  expect(replacement).toHaveBeenCalledOnce();
});

it("keeps analysis cleanup debt across disconnect and retries the original reader without exposing its error", async () => {
  const { facade, port, stream } = fixture();
  let fail = true;
  unblock.push(() => {
    fail = false;
  });
  const close = vi.spyOn(stream, "close").mockImplementation(() => {
    stream.end();
    return fail
      ? Promise.reject(new Error("credential=private-password; endpoint=/private/cluster"))
      : Promise.resolve();
  });
  await facade.execute(command("connection.connect", "connect"));
  await facade.execute(start());
  await flush();
  const disconnected = await facade.execute(command("connection.disconnect", "disconnect"));
  expect(disconnected).toMatchObject({ ok: false });
  const current = await facade.execute(status());
  expect(current).toMatchObject({
    ok: true,
    result: { snapshot: { operation: { reason: "cleanup-failed", result: null } } },
  });
  expect(parseHostCommandResponse(current)).toEqual(current);
  if (
    !current.ok ||
    current.command !== "records.analysis.status" ||
    !current.result.snapshot.operation
  )
    throw new Error("Missing analysis cleanup state.");
  const replacement = vi.fn(() => Promise.resolve(new RecordingActiveConnection()));
  port.openOperations.push(replacement);
  expect(await facade.execute(command("connection.connect", "blocked"))).toMatchObject({
    ok: false,
  });
  expect(replacement).not.toHaveBeenCalled();
  const priorCalls = close.mock.calls.length;
  fail = false;
  const discarded = await facade.execute({
    command: "records.analysis.discard",
    id: "discard",
    payload: { jobId: current.result.snapshot.operation.jobId },
    version: HOST_PROTOCOL_VERSION,
  });
  expect(discarded).toMatchObject({ ok: true, result: { snapshot: { operation: null } } });
  expect(close.mock.calls.length).toBeGreaterThan(priorCalls);
  expect(await facade.execute(command("connection.connect", "retry"))).toMatchObject({ ok: true });
  expect(replacement).toHaveBeenCalledOnce();
  expect(JSON.stringify([captured, disconnected, current, discarded])).not.toMatch(
    /private-password|\/private\/cluster/u,
  );
});

it.each(["codecs", "protection", "reset"] as const)(
  "joins both range owners before the %s settings boundary proceeds",
  async (kind) => {
    const filesClosed = deferred();
    const readerClosed = deferred();
    const { artifacts, revoke } = artifactOwner(() => filesClosed.promise);
    const analysisStream = new ControlledMessageStream();
    const closeAnalysis = vi.spyOn(analysisStream, "close").mockImplementation(() => {
      analysisStream.end();
      return readerClosed.promise;
    });
    const exportStream = new ControlledMessageStream();
    const readers = [analysisStream, exportStream];
    const preferences = new KafkaOperationalPreferenceService(
      new InMemoryKafkaOperationalPreferenceStore({ durability: "session", state: "ready" }),
    );
    await preferences.get();
    let sequence = 0;
    // The outer command guard requires disconnection before settings changes. This
    // seam independently proves the shared facade joins each already-owned reader.
    const ranges = new RecordRangeFacade(
      {
        recordReadScope: (): RecordReadScope => ({
          connectionName: "fixture",
          isCurrent: (): boolean => true,
          openMessageStream: (): Promise<ControlledMessageStream> =>
            Promise.resolve(readers.shift()!),
        }),
      },
      preferences,
      artifacts,
      () => ++sequence,
      (event) => captured.push(event),
      () => undefined,
    );
    cleanup.push(async () => {
      ranges.invalidate();
      await ranges.idle();
    });
    expect(await ranges.execute(start(), "analysis")).toMatchObject({ ok: true });
    expect(await ranges.execute(exportStart(), "export")).toMatchObject({ ok: true });
    await flush();
    const settings: Extract<HostCommand, { command: "preferences.update" | "preferences.reset" }> =
      kind === "reset"
        ? { command: "preferences.reset", id: "reset", payload: {}, version: HOST_PROTOCOL_VERSION }
        : {
            command: "preferences.update",
            id: "update",
            payload: {
              patch:
                kind === "codecs"
                  ? { codecs: { key: "auto", value: "json" } }
                  : { protection: { ...KAFKA_RECORD_PROTECTION_DEFAULTS, maskKey: true } },
            },
            version: HOST_PROTOCOL_VERSION,
          };
    let settled = false;
    const changing = ranges.preparePreferences(settings).then(() => {
      settled = true;
    });
    await flush();
    expect(closeAnalysis).toHaveBeenCalled();
    expect(exportStream.closeCalls).toBeGreaterThan(0);
    expect(revoke).toHaveBeenCalled();
    expect(await ranges.execute(status(), "status")).toMatchObject({
      ok: true,
      result: { snapshot: { operation: { result: null } } },
    });
    readerClosed.resolve();
    await flush();
    expect(settled).toBe(false);
    filesClosed.resolve();
    await changing;
    expect(settled).toBe(true);
  },
);
