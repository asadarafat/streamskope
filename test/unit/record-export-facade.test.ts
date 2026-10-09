import { randomUUID } from "node:crypto";

import { expect, it, vi, type Mock } from "vitest";

import type {
  RecordExportArtifacts,
  RecordExportSink,
} from "../../src/features/kafka/application/record-export-artifacts";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommandResponse,
  parseHostEvent,
  type HostEvent,
  type RecordExportInput,
} from "../../src/features/kafka/contracts";
import {
  command,
  ControlledMessageStream,
  createFacade,
  RecordingActiveConnection,
  RecordingConnectionPort,
  settleAsyncIteration,
} from "../support/kafka-backend-facade-fixture";

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function startInput(): RecordExportInput {
  return {
    requestId: randomUUID(),
    topic: "orders",
    range: { mode: "earliest" },
    format: "jsonl",
    maxRecords: 10,
    search: { key: "", value: "", offset: "", timestamp: "", partition: null },
  };
}
function fixture(artifacts?: RecordExportArtifacts): {
  facade: ReturnType<typeof createFacade>;
  port: RecordingConnectionPort;
  active: RecordingActiveConnection;
  stream: ControlledMessageStream;
  opened: Mock<() => Promise<ControlledMessageStream>>;
  events: HostEvent[];
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
  const events: HostEvent[] = [];
  facade.subscribe((event) => {
    events.push(event);
  });
  return { facade, port, active, stream, opened, events };
}
function memoryOwner(discardOperation: () => Promise<void> = () => Promise.resolve()): {
  artifacts: RecordExportArtifacts;
  sink: RecordExportSink;
  create: Mock<RecordExportArtifacts["create"]>;
  revoke: Mock<RecordExportArtifacts["revoke"]>;
  discard: Mock<RecordExportSink["discard"]>;
} {
  const discard = vi.fn(discardOperation);
  const sink: RecordExportSink = {
    write: () => Promise.resolve(),
    discard,
    seal: () => Promise.reject(new Error("No completion expected")),
  };
  const create = vi.fn<RecordExportArtifacts["create"]>(() => Promise.resolve(sink));
  const revoke = vi.fn<RecordExportArtifacts["revoke"]>();
  const artifacts: RecordExportArtifacts = { create, revoke, drain: () => Promise.resolve() };
  return { artifacts, sink, create, revoke, discard };
}

it("reports unavailable export without a file owner and provides a safe actionable admission error", async () => {
  const { facade } = fixture();
  const status = await facade.execute({
    command: "records.export.status",
    id: "status",
    payload: {},
    version: HOST_PROTOCOL_VERSION,
  });
  expect(status).toMatchObject({
    ok: true,
    result: { snapshot: { available: false, operation: null } },
  });
  expect(parseHostCommandResponse(status)).toEqual(status);
  const failed = await facade.execute({
    command: "records.export.start",
    id: "start",
    payload: startInput(),
    version: HOST_PROTOCOL_VERSION,
  });
  expect(failed).toMatchObject({
    ok: false,
    error: { code: "VALIDATION", summary: "Range export is unavailable in this host." },
  });
  await facade.shutdown();
});

it("returns admission before the reader finishes, coalesces a request retry, and fences replacement until cleanup settles", async () => {
  const cleanup = deferred();
  const { artifacts, create, revoke } = memoryOwner(() => cleanup.promise);
  const { facade, port, opened, events } = fixture(artifacts);
  await facade.execute(command("connection.connect", "connect"));
  const payload = startInput();
  const start = await facade.execute({
    command: "records.export.start",
    id: "first",
    payload,
    version: HOST_PROTOCOL_VERSION,
  });
  await settleAsyncIteration();
  expect(opened).toHaveBeenCalledOnce();
  const retry = await facade.execute({
    command: "records.export.start",
    id: "retry",
    payload,
    version: HOST_PROTOCOL_VERSION,
  });
  expect(start).toMatchObject({ ok: true });
  expect(retry).toMatchObject({ ok: true });
  expect(create).toHaveBeenCalledOnce();
  const replacement = vi.fn(() => Promise.resolve(new RecordingActiveConnection()));
  port.openOperations.push(replacement);
  let reentrant: Promise<unknown> | undefined;
  facade.subscribe((event) => {
    if (
      event.event === "records.export.changed" &&
      event.payload.operation?.reason === "revoked" &&
      reentrant === undefined
    )
      reentrant = facade.execute({
        command: "records.export.start",
        id: "reentrant",
        payload: startInput(),
        version: HOST_PROTOCOL_VERSION,
      });
  });
  let settled = false;
  const reconnect = facade.execute(command("connection.connect", "replace")).then((result) => {
    settled = true;
    return result;
  });
  await settleAsyncIteration();
  expect(revoke).toHaveBeenCalled();
  expect(replacement).not.toHaveBeenCalled();
  expect(settled).toBe(false);
  expect(await reentrant).toMatchObject({
    ok: false,
    error: {
      code: "VALIDATION",
      summary: "Wait for the connection change before starting an export.",
    },
  });
  expect(create).toHaveBeenCalledOnce();
  const status = await facade.execute({
    command: "records.export.status",
    id: "status",
    payload: {},
    version: HOST_PROTOCOL_VERSION,
  });
  expect(status).toMatchObject({
    ok: true,
    result: { snapshot: { operation: { artifact: null } } },
  });
  cleanup.resolve();
  expect(await reconnect).toMatchObject({ ok: true });
  expect(replacement).toHaveBeenCalledOnce();
  for (const event of events.filter((entry) => entry.event === "records.export.changed"))
    expect(parseHostEvent(event)).toEqual(event);
  await facade.shutdown();
});

it("preserves failed cleanup on disconnect, blocks replacement, and retries the original sink through Discard", async () => {
  let fail = true;
  const { artifacts, discard } = memoryOwner(() =>
    fail
      ? Promise.reject(new Error("secret-upstream-path=/private/storage/password"))
      : Promise.resolve(),
  );
  const { facade, port, events } = fixture(artifacts);
  await facade.execute(command("connection.connect", "connect"));
  await facade.execute({
    command: "records.export.start",
    id: "first",
    payload: startInput(),
    version: HOST_PROTOCOL_VERSION,
  });
  await settleAsyncIteration();
  const disconnected = await facade.execute(command("connection.disconnect", "disconnect"));
  expect(disconnected.ok).toBe(false);
  const status = await facade.execute({
    command: "records.export.status",
    id: "status",
    payload: {},
    version: HOST_PROTOCOL_VERSION,
  });
  if (!status.ok || status.command !== "records.export.status" || !status.result.snapshot.operation)
    throw new Error("Missing export status");
  expect(status.result.snapshot.operation.reason).toBe("cleanup-failed");
  const replacement = vi.fn(() => Promise.resolve(new RecordingActiveConnection()));
  port.openOperations.push(replacement);
  expect(await facade.execute(command("connection.connect", "blocked"))).toMatchObject({
    ok: false,
  });
  expect(replacement).not.toHaveBeenCalled();
  fail = false;
  expect(
    await facade.execute({
      command: "records.export.discard",
      id: "discard",
      payload: { jobId: status.result.snapshot.operation.jobId },
      version: HOST_PROTOCOL_VERSION,
    }),
  ).toMatchObject({ ok: true, result: { snapshot: { operation: null } } });
  expect(discard).toHaveBeenCalled();
  expect(await facade.execute(command("connection.connect", "retry"))).toMatchObject({ ok: true });
  expect(JSON.stringify([events, status, disconnected])).not.toContain("secret-upstream-path");
  await facade.shutdown();
});
