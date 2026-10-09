// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, render, renderHook, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  type HostCommand,
  type HostEvent,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import {
  RECORD_EXPORT_LIMITS,
  type RecordExportInput,
  type RecordExportOperation,
  type RecordExportSnapshot,
} from "../../src/features/kafka/contracts/record-export";
import { RecordExportDialog } from "../../src/features/kafka/ui/RecordExportDialog";
import { RecordExportStatus } from "../../src/features/kafka/ui/RecordExportStatus";
import { initialKafkaMessageFilters } from "../../src/features/kafka/ui/message-operations";
import { createArtifactTransfer } from "../../src/features/kafka/ui/artifact-transfer";
import {
  useRecordExport,
  type RecordExportController,
} from "../../src/features/kafka/ui/use-record-export";
import { DESKTOP_PLATFORM_VERSION, type StreamSkopeDesktop } from "../../src/platform/desktop";
import { testHostExecute } from "../support/host-response";

const id = "d325ebd1-de91-4a95-b75f-5fb35ad20818";
const input: RecordExportInput = {
  requestId: id,
  topic: "orders",
  range: { mode: "earliest" },
  search: { key: "", value: "", offset: "", timestamp: "", partition: null },
  format: "jsonl",
  maxRecords: 100,
};
const operation: RecordExportOperation = {
  jobId: id,
  state: "completed",
  input,
  source: { connectionName: "Test", clusterId: null, topicId: null },
  settings: {
    codecs: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.codecs,
    protection: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.protection,
  },
  limits: RECORD_EXPORT_LIMITS,
  startedAt: "2026-01-01T00:00:00.000Z",
  completedAt: "2026-01-01T00:00:01.000Z",
  counts: {
    passes: 1,
    scannedRecords: 1,
    scannedBytes: 10,
    writtenRecords: 1,
    writtenBytes: 10,
    unavailableRecords: 0,
    decodeErrorRecords: 0,
    originalUnavailableRecords: 0,
  },
  coverage: {
    reason: "range-complete",
    scannedRecords: 1,
    scannedBytes: 10,
    matchedRecords: 1,
    unavailableRecords: 0,
    partitions: [{ partition: 0, startOffset: "0", endOffset: "1", nextOffset: "1" }],
  },
  reason: "range-complete",
  artifact: {
    artifactId: id,
    output: { format: "jsonl", fileName: "orders.jsonl", bytes: 10, sha256: "a".repeat(64) },
    receiptBytes: 100,
    receiptSha256: "b".repeat(64),
    expiresAt: "2099-01-01T00:00:00.000Z",
  },
  error: null,
};
const snapshot = (revision = 0, op: RecordExportOperation | null = null): RecordExportSnapshot => ({
  scopeId: id,
  revision,
  available: true,
  operation: op,
});
function response(command: HostCommand, data: RecordExportSnapshot): unknown {
  return {
    command: command.command,
    id: command.id,
    version: HOST_PROTOCOL_VERSION,
    ok: true,
    result: { correlationId: "export-test", snapshot: data },
  };
}
function setup(): {
  result: ReturnType<
    typeof renderHook<RecordExportController, Parameters<typeof useRecordExport>[0]>
  >["result"];
  rerender: (props: Parameters<typeof useRecordExport>[0]) => void;
  unmount: () => void;
  props: Parameters<typeof useRecordExport>[0];
  dispatch: ReturnType<typeof vi.fn<(command: HostCommand) => Promise<unknown>>>;
  transfer: {
    download: ReturnType<
      typeof vi.fn<Parameters<typeof useRecordExport>[0]["transfer"]["download"]>
    >;
  };
  emit: (snapshot: RecordExportSnapshot) => void;
} {
  let listener: ((event: HostEvent) => void) | undefined;
  const dispatch = vi.fn<(command: HostCommand) => Promise<unknown>>((command) =>
    Promise.resolve(response(command, snapshot())),
  );
  const host: StreamSkopeHost = {
    execute: testHostExecute(dispatch),
    subscribe: (next) => {
      listener = next;
      return () => {
        listener = undefined;
      };
    },
    openExternalUrl: () => Promise.reject(new Error("Not used in export UI")),
  };
  const transfer = {
    download: vi.fn<Parameters<typeof useRecordExport>[0]["transfer"]["download"]>(() =>
      Promise.resolve("started"),
    ),
  };
  const props: Parameters<typeof useRecordExport>[0] = {
    host,
    connected: true,
    backendAvailable: true,
    transfer,
  };
  const hook = renderHook(useRecordExport, { initialProps: props });
  return {
    ...hook,
    props,
    dispatch,
    transfer,
    emit: (next: RecordExportSnapshot): void => {
      act(() =>
        listener?.({
          event: "records.export.changed",
          payload: next,
          sequence: next.revision,
          version: HOST_PROTOCOL_VERSION,
        }),
      );
    },
  };
}
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

it("keeps newer event evidence when a status response arrives late", async () => {
  const { result, emit, dispatch } = setup();
  await waitFor(() => expect(result.current.snapshot?.available).toBe(true));
  let resolve!: (value: unknown) => void;
  dispatch.mockImplementationOnce(
    () =>
      new Promise((accept) => {
        resolve = accept;
      }),
  );
  let pending!: Promise<void>;
  act(() => {
    pending = result.current.refresh();
  });
  emit(snapshot(5, operation));
  const command = dispatch.mock.calls.at(-1)![0];
  await act(async () => {
    resolve(response(command, snapshot(1)));
    await pending;
  });
  expect(result.current.snapshot?.revision).toBe(5);
  expect(result.current.snapshot?.operation?.artifact?.artifactId).toBe(id);
});

it("retries an uncertain start with the same request identity and immutable input", async () => {
  const { result, dispatch } = setup();
  await waitFor(() => expect(result.current.snapshot?.available).toBe(true));
  dispatch.mockImplementationOnce(() => Promise.reject(new Error("transport lost")));
  await act(async () => {
    expect(await result.current.start(input)).toBe(false);
  });
  expect(result.current.uncertainStart).toBe(true);
  dispatch.mockImplementationOnce((command) =>
    Promise.resolve(response(command, snapshot(2, operation))),
  );
  await act(async () => {
    expect(await result.current.retryStart()).toBe(true);
  });
  const starts = dispatch.mock.calls
    .map(([command]) => command)
    .filter((command) => command.command === "records.export.start");
  expect(starts).toHaveLength(2);
  expect(starts[0]!.payload).toEqual(input);
  expect(starts[1]!.payload).toEqual(input);
  expect(result.current.uncertainStart).toBe(false);
});

it("retains unresolved cleanup and sends only its owning job identity on retry", async () => {
  const { result, emit, dispatch } = setup();
  await waitFor(() => expect(result.current.snapshot?.available).toBe(true));
  emit(
    snapshot(1, {
      ...operation,
      state: "reading",
      reason: null,
      completedAt: null,
      artifact: null,
      coverage: null,
    }),
  );
  dispatch.mockImplementationOnce(() => Promise.reject(new Error("timeout")));
  await act(() => result.current.cancel());
  expect(result.current.snapshot?.operation?.state).toBe("reading");
  expect(result.current.error).toContain("did not confirm export cleanup");
  dispatch.mockImplementationOnce((command) => Promise.resolve(response(command, snapshot(2))));
  await act(() => result.current.discard());
  expect(dispatch.mock.calls.at(-1)![0]).toMatchObject({
    command: "records.export.discard",
    payload: { jobId: id },
  });
  expect(result.current.snapshot?.operation).toBeNull();
});

it("distinguishes browser download initiation, native cancellation and a saved file", async () => {
  const { result, emit, transfer } = setup();
  await waitFor(() => expect(result.current.snapshot?.available).toBe(true));
  emit(snapshot(1, operation));
  await act(() => result.current.download("data"));
  expect(transfer.download).toHaveBeenCalledWith({ artifactId: id, part: "data" });
  expect(result.current.notice).toContain("download started");
  expect(result.current.notice).not.toContain("saved");
  transfer.download.mockResolvedValueOnce("cancelled");
  await act(() => result.current.download("receipt"));
  expect(result.current.notice).toBe("Receipt save cancelled.");
  transfer.download.mockResolvedValueOnce("saved");
  await act(() => result.current.download("data"));
  expect(result.current.notice).toBe("Export saved.");
});

it("does not download an expired artifact or advertise completion after revocation", async () => {
  const { result, emit, transfer } = setup();
  await waitFor(() => expect(result.current.snapshot?.available).toBe(true));
  emit(
    snapshot(1, {
      ...operation,
      artifact: { ...operation.artifact!, expiresAt: "2020-01-01T00:00:00.000Z" },
    }),
  );
  await act(() => result.current.download("data"));
  expect(transfer.download).not.toHaveBeenCalled();
  emit(snapshot(2, operation));
  let complete!: () => void;
  transfer.download.mockImplementationOnce(
    () =>
      new Promise((accept) => {
        complete = (): void => accept("saved");
      }),
  );
  let pending!: Promise<void>;
  act(() => {
    pending = result.current.download("data");
  });
  emit(snapshot(3));
  await act(async () => {
    complete();
    await pending;
  });
  expect(result.current.notice).toBeUndefined();
});

function controller(overrides: Partial<RecordExportController> = {}): RecordExportController {
  return {
    snapshot: snapshot(),
    connected: true,
    busy: false,
    error: undefined,
    notice: undefined,
    uncertainStart: false,
    expired: false,
    start: vi.fn(() => Promise.resolve(true)),
    retryStart: vi.fn(() => Promise.resolve(true)),
    refresh: vi.fn(() => Promise.resolve()),
    cancel: vi.fn(() => Promise.resolve()),
    discard: vi.fn(() => Promise.resolve()),
    download: vi.fn(() => Promise.resolve()),
    ...overrides,
  };
}
it("starts an explicit bounded range with the current filter and no rule annotation filter", async () => {
  const user = userEvent.setup(),
    control = controller(),
    close = vi.fn();
  render(
    <RecordExportDialog
      topic="orders"
      filters={{ ...initialKafkaMessageFilters, value: "approved" }}
      controller={control}
      onClose={close}
    />,
  );
  await user.click(screen.getByRole("button", { name: "Start export" }));
  expect(control.start).toHaveBeenCalledWith(
    expect.objectContaining({
      topic: "orders",
      range: { mode: "earliest" },
      format: "jsonl",
      maxRecords: 100_000,
    }),
  );
  expect(control.start).toHaveBeenCalledWith(
    expect.objectContaining({
      search: { key: "", value: "approved", offset: "", timestamp: "", partition: null },
    }),
  );
  expect(close).toHaveBeenCalledOnce();
});
it("blocks range export when the unsupported rule-only filter would change results", () => {
  render(
    <RecordExportDialog
      topic="orders"
      filters={{ ...initialKafkaMessageFilters, activeRuleMatchesOnly: true }}
      controller={controller()}
      onClose={vi.fn()}
    />,
  );
  expect(screen.getByRole("button", { name: "Start export" })).toBeDisabled();
  expect(screen.getByRole("alert")).toHaveTextContent("Turn off “Rule matches only”");
});
it("requires explicit replacement of a previously prepared download", async () => {
  const user = userEvent.setup();
  render(
    <RecordExportDialog
      topic="orders"
      filters={initialKafkaMessageFilters}
      controller={controller({ snapshot: snapshot(1, operation) })}
      onClose={vi.fn()}
    />,
  );
  expect(screen.getByRole("button", { name: "Start export" })).toBeDisabled();
  await user.click(
    screen.getByRole("checkbox", { name: "Replace the previous prepared download" }),
  );
  expect(screen.getByRole("button", { name: "Start export" })).toBeEnabled();
});
it("labels cancelled output as partial and keeps its captured topic visible", () => {
  render(
    <RecordExportStatus
      controller={controller({
        snapshot: snapshot(1, { ...operation, state: "partial", reason: "cancelled" }),
      })}
    />,
  );
  expect(screen.getByRole("region", { name: "Range export" })).toHaveTextContent(
    "Partial range export · orders · JSONL",
  );
  expect(screen.getByText(/incomplete prefix/u)).toBeVisible();
  expect(screen.getByRole("button", { name: "Download receipt" })).toBeEnabled();
});

it("uses authenticated HEAD plus an anchor without buffering range artifacts", async () => {
  const fetcher = vi.fn(() => Promise.resolve({ ok: true }));
  vi.stubGlobal("fetch", fetcher);
  let href = "";
  vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(function (
    this: HTMLAnchorElement,
  ) {
    href = this.getAttribute("href") ?? "";
  });
  expect(await createArtifactTransfer().download({ artifactId: id, part: "data" })).toBe("started");
  expect(fetcher).toHaveBeenCalledWith(
    `/__streamskope_host/exports/${id}/data`,
    expect.objectContaining({ method: "HEAD", credentials: "same-origin", redirect: "error" }),
  );
  expect(href).toBe(`/__streamskope_host/exports/${id}/data`);
  expect(document.querySelectorAll("a")).toHaveLength(0);
});
it("refuses an expired browser download before clicking an anchor", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(() => Promise.resolve({ ok: false, status: 410 })),
  );
  const click = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => undefined);
  await expect(
    createArtifactTransfer().download({ artifactId: id, part: "receipt" }),
  ).rejects.toThrow("no longer available");
  expect(click).not.toHaveBeenCalled();
});
it("passes an opaque reference to native Save and preserves its actual result", async () => {
  const saveArtifact = vi.fn<StreamSkopeDesktop["saveArtifact"]>(() =>
    Promise.resolve({ state: "cancelled", version: DESKTOP_PLATFORM_VERSION }),
  );
  const desktop: StreamSkopeDesktop = {
    saveArtifact,
    saveTextDocument: () => Promise.reject(new Error("not used")),
    subscribeActions: () => () => undefined,
  };
  expect(await createArtifactTransfer(desktop).download({ artifactId: id, part: "receipt" })).toBe(
    "cancelled",
  );
  expect(saveArtifact).toHaveBeenCalledWith({ artifactId: id, part: "receipt" });
});
