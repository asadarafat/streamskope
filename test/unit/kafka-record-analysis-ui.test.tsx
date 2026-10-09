// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, renderHook, screen, waitFor, within } from "@testing-library/react";
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
  RECORD_ANALYSIS_LIMITS,
  type RecordAnalysisCell,
  type RecordAnalysisInput,
  type RecordAnalysisOperation,
  type RecordAnalysisSnapshot,
} from "../../src/features/kafka/contracts/record-analysis";
import { RecordAnalysisDialog } from "../../src/features/kafka/ui/RecordAnalysisDialog";
import { RecordAnalysisResults } from "../../src/features/kafka/ui/RecordAnalysisResults";
import { analysisCell } from "../../src/features/kafka/ui/record-analysis-presentation";
import { initialKafkaMessageFilters } from "../../src/features/kafka/ui/message-operations";
import {
  useRecordAnalysis,
  type RecordAnalysisController,
} from "../../src/features/kafka/ui/use-record-analysis";
import { testHostExecute } from "../support/host-response";

const id = "d325ebd1-de91-4a95-b75f-5fb35ad20818";
const input: RecordAnalysisInput = {
  requestId: id,
  topic: "orders",
  range: { mode: "earliest" },
  search: { key: "", value: "", offset: "", timestamp: "", partition: null },
  maxRecords: 100,
  columns: [{ id: "status", label: "Status", source: "value", path: "$.status" }],
  groupBy: "status",
};
const operation: RecordAnalysisOperation = {
  jobId: id,
  input,
  state: "completed",
  source: { connectionName: "Test", clusterId: null, topicId: null },
  settings: {
    codecs: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.codecs,
    protection: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.protection,
  },
  limits: RECORD_ANALYSIS_LIMITS,
  startedAt: "2026-01-01T00:00:00.000Z",
  completedAt: "2026-01-01T00:00:01.000Z",
  counts: {
    passes: 1,
    scannedRecords: 5,
    scannedBytes: 500,
    countedRecords: 5,
    unavailableRecords: 0,
  },
  coverage: {
    reason: "range-complete",
    scannedRecords: 5,
    scannedBytes: 500,
    matchedRecords: 5,
    unavailableRecords: 0,
    partitions: [{ partition: 0, startOffset: "0", endOffset: "5", nextOffset: "5" }],
  },
  reason: "range-complete",
  error: null,
  result: {
    columns: [
      {
        columnId: "status",
        scalar: 2,
        missing: 1,
        nullKey: 0,
        tombstone: 0,
        masked: 1,
        unavailable: 1,
      },
    ],
    grouping: {
      groups: [
        { key: { state: "scalar", value: 1 }, count: 1 },
        { key: { state: "scalar", value: "1" }, count: 1 },
        { key: { state: "missing" }, count: 1 },
      ],
      groupedRecords: 3,
      excluded: { masked: 1, unavailable: 1 },
    },
    preview: [
      {
        partition: 0,
        offset: "0",
        timestamp: "2026-01-01T00:00:00.000Z",
        cells: [{ state: "scalar", value: 1 }],
      },
      {
        partition: 0,
        offset: "1",
        timestamp: "2026-01-01T00:00:00.000Z",
        cells: [{ state: "scalar", value: "1" }],
      },
    ],
    previewOmittedRecords: 3,
    previewBytes: 256,
    workUnits: 10,
  },
};
function snapshot(op: RecordAnalysisOperation | null = null, revision = 0): RecordAnalysisSnapshot {
  return { scopeId: id, revision, operation: op };
}
function controller(overrides: Partial<RecordAnalysisController> = {}): RecordAnalysisController {
  return {
    snapshot: snapshot(),
    connected: true,
    busy: false,
    error: undefined,
    notice: undefined,
    uncertainStart: false,
    start: vi.fn(() => Promise.resolve(true)),
    retryStart: vi.fn(() => Promise.resolve(true)),
    refresh: vi.fn(() => Promise.resolve()),
    cancel: vi.fn(() => Promise.resolve()),
    discard: vi.fn(() => Promise.resolve()),
    ...overrides,
  };
}
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

it.each<[RecordAnalysisCell, string, string]>([
  [{ state: "scalar", value: 1 }, "number", "1"],
  [{ state: "scalar", value: "1" }, "string", '"1"'],
  [{ state: "scalar", value: null }, "JSON null", "null"],
  [{ state: "scalar", value: true }, "boolean", "true"],
  [{ state: "scalar", value: "" }, "string", '""'],
  [{ state: "null-key" }, "Null key", "Null key"],
  [{ state: "tombstone" }, "Tombstone", "Tombstone"],
  [{ state: "missing" }, "Missing", "Missing path"],
  [{ state: "masked" }, "Masked", "Masked"],
  [{ state: "scalar", value: "[MASKED]" }, "string", '"[MASKED]"'],
  [{ state: "unavailable", reason: "object" }, "Unavailable", "Unsupported: object"],
  [
    { state: "unavailable", reason: "value-limit" },
    "Unavailable",
    "Value exceeds the scalar byte limit",
  ],
])("preserves typed protected cell %j in presentation", (cell, type, text) => {
  expect(analysisCell(cell)).toEqual({ type, text });
});

it("separates a complete match count, excluded groups and an omitted preview", () => {
  render(<RecordAnalysisResults operation={operation} />);
  expect(screen.getByRole("status", { name: "Analysis count" })).toHaveTextContent(
    "Count for captured range complete: 5 matching records.",
  );
  const grouping = screen.getByRole("region", { name: "Count by results" });
  expect(grouping).toHaveTextContent("3 grouped of 5 counted records.");
  expect(grouping).toHaveTextContent(
    "Grouping excludes 2 records: 1 masked; 1 unavailable or unsupported.",
  );
  expect(within(grouping).getByRole("table", { name: "Analysis groups" })).toHaveTextContent('"1"');
  expect(screen.getByRole("region", { name: "Projection preview" })).toHaveTextContent(
    "First 2 of 5 counted records shown; 3 omitted",
  );
  expect(
    within(screen.getByRole("table", { name: "Analysis preview" })).getAllByRole("row"),
  ).toHaveLength(3);
});

it("labels cancelled counts as partial without disguising a small preview as all records", () => {
  render(
    <RecordAnalysisResults operation={{ ...operation, state: "partial", reason: "cancelled" }} />,
  );
  expect(screen.getByRole("status", { name: "Analysis count" })).toHaveTextContent(
    "Partial count: 5 matching records counted.",
  );
  expect(
    screen.getByText("Analysis stopped after the confirmed records were counted."),
  ).toBeVisible();
  expect(screen.getByRole("region", { name: "Projection preview" })).toHaveTextContent(
    "The preview limit does not stop counting.",
  );
});

it("permits count-only with zero fields and preserves explicit finite bounds and filters", async () => {
  const user = userEvent.setup(),
    control = controller();
  render(
    <RecordAnalysisDialog
      topic="orders"
      filters={{ ...initialKafkaMessageFilters, key: "invoice" }}
      controller={control}
      onClose={vi.fn()}
    />,
  );
  await user.click(screen.getByRole("button", { name: "Start analysis" }));
  expect(control.start).toHaveBeenCalledWith(
    expect.objectContaining({
      topic: "orders",
      columns: [],
      groupBy: null,
      range: { mode: "earliest" },
      maxRecords: 100_000,
    }),
  );
  expect(control.start).toHaveBeenCalledWith(
    expect.objectContaining({
      search: { key: "invoice", value: "", offset: "", timestamp: "", partition: null },
    }),
  );
});

it("rejects fan-out path syntax before start and allows a supported scalar path", async () => {
  const user = userEvent.setup(),
    control = controller();
  render(
    <RecordAnalysisDialog
      topic="orders"
      filters={initialKafkaMessageFilters}
      controller={control}
      onClose={vi.fn()}
    />,
  );
  await user.click(screen.getByRole("button", { name: "Add field" }));
  const path = screen.getByRole("textbox", { name: "Field 1 path" });
  await user.clear(path);
  await user.type(path, "$.*");
  expect(screen.getByRole("button", { name: "Start analysis" })).toBeDisabled();
  await user.clear(path);
  await user.type(path, "$.status");
  expect(screen.getByRole("button", { name: "Start analysis" })).toBeEnabled();
  await user.click(screen.getByRole("combobox", { name: "Count by" }));
  await user.click(screen.getByRole("option", { name: /value \$\.status/u }));
  await user.click(screen.getByRole("button", { name: "Remove field 1" }));
  expect(screen.getByRole("combobox", { name: "Count by" })).toHaveTextContent(
    "None — count matching records",
  );
});

it("blocks rule-only analysis and requires confirmation to replace a retained result", async () => {
  const user = userEvent.setup(),
    control = controller({ snapshot: snapshot(operation, 1) });
  const { rerender } = render(
    <RecordAnalysisDialog
      topic="orders"
      filters={{ ...initialKafkaMessageFilters, activeRuleMatchesOnly: true }}
      controller={control}
      onClose={vi.fn()}
    />,
  );
  await user.click(screen.getByRole("checkbox", { name: "Replace the previous analysis result" }));
  expect(screen.getByRole("button", { name: "Start analysis" })).toBeDisabled();
  rerender(
    <RecordAnalysisDialog
      topic="orders"
      filters={initialKafkaMessageFilters}
      controller={control}
      onClose={vi.fn()}
    />,
  );
  expect(screen.getByRole("button", { name: "Start analysis" })).toBeEnabled();
});

it("keeps the captured topic while viewing results and sends cancellation without closing ownership", async () => {
  const user = userEvent.setup(),
    control = controller({
      snapshot: snapshot(
        { ...operation, state: "reading", result: null, reason: null, completedAt: null },
        1,
      ),
    });
  render(
    <RecordAnalysisDialog
      topic="different-topic"
      filters={initialKafkaMessageFilters}
      controller={control}
      initialTab="results"
      onClose={vi.fn()}
    />,
  );
  const panel = screen.getByRole("tabpanel", { name: "Results" });
  expect(panel).toHaveTextContent("orders · Test");
  expect(panel).not.toHaveTextContent("different-topic");
  await user.click(screen.getByRole("button", { name: "Cancel analysis" }));
  expect(control.cancel).toHaveBeenCalledOnce();
  expect(screen.getByRole("dialog", { name: "Analyze a topic range" })).toBeVisible();
});

it("uses the typed analysis transport and retries an uncertain start with the same identity", async () => {
  let listener: ((event: HostEvent) => void) | undefined;
  const response = (command: HostCommand, data: RecordAnalysisSnapshot): unknown => ({
    command: command.command,
    id: command.id,
    version: HOST_PROTOCOL_VERSION,
    ok: true,
    result: { correlationId: "analysis-test", snapshot: data },
  });
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
    openExternalUrl: () => Promise.reject(new Error("Not used")),
  };
  const props: Parameters<typeof useRecordAnalysis>[0] = {
    host,
    connected: true,
    backendAvailable: true,
  };
  const { result } = renderHook(useRecordAnalysis, { initialProps: props });
  await waitFor(() => expect(result.current.snapshot).not.toBeNull());
  dispatch.mockRejectedValueOnce(new Error("lost response"));
  await act(async () => {
    expect(await result.current.start(input)).toBe(false);
  });
  expect(result.current.uncertainStart).toBe(true);
  dispatch.mockImplementationOnce((command) => Promise.resolve(response(command, snapshot())));
  await act(async () => {
    expect(await result.current.retryStart()).toBe(true);
  });
  const starts = dispatch.mock.calls
    .map(([command]) => command)
    .filter((command) => command.command === "records.analysis.start");
  expect(starts.map((command) => command.payload)).toEqual([input, input]);
  act(() =>
    listener?.({
      event: "records.analysis.changed",
      version: HOST_PROTOCOL_VERSION,
      sequence: 10,
      payload: snapshot(operation, 10),
    }),
  );
  expect(result.current.snapshot?.operation?.input.topic).toBe("orders");
  act(() =>
    listener?.({
      event: "records.analysis.changed",
      version: HOST_PROTOCOL_VERSION,
      sequence: 11,
      payload: snapshot({ ...operation, state: "revoked", reason: "revoked", result: null }, 11),
    }),
  );
  expect(result.current.snapshot?.operation?.result).toBeNull();
  act(() =>
    listener?.({
      event: "records.analysis.changed",
      version: HOST_PROTOCOL_VERSION,
      sequence: 9,
      payload: snapshot(operation, 9),
    }),
  );
  expect(result.current.snapshot?.operation?.result).toBeNull();
});

it("refreshes active analysis with a newer full snapshot without accepting an older progress event", async () => {
  const user = userEvent.setup();
  let listener: ((event: HostEvent) => void) | undefined;
  let next = snapshot();
  const host: StreamSkopeHost = {
    execute: testHostExecute((command) =>
      Promise.resolve({
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId: "active-analysis-test", snapshot: next },
      }),
    ),
    subscribe: (callback) => {
      listener = callback;
      return () => {
        listener = undefined;
      };
    },
    openExternalUrl: () => Promise.reject(new Error("Not used")),
  };
  function Harness(): React.JSX.Element {
    const control = useRecordAnalysis({ host, connected: true, backendAvailable: true });
    return (
      <RecordAnalysisDialog
        topic="orders"
        filters={initialKafkaMessageFilters}
        controller={control}
        initialTab="results"
        onClose={vi.fn()}
      />
    );
  }
  render(<Harness />);
  await waitFor(() => expect(listener).toBeDefined());
  const active: RecordAnalysisOperation = {
    ...operation,
    state: "reading",
    completedAt: null,
    reason: null,
    result: null,
    counts: { ...operation.counts, scannedRecords: 4, countedRecords: 4 },
    coverage: {
      ...operation.coverage!,
      reason: "reading",
      scannedRecords: 4,
      matchedRecords: 4,
      partitions: [{ partition: 0, startOffset: "0", endOffset: "10", nextOffset: "4" }],
    },
  };
  act(() =>
    listener?.({
      event: "records.analysis.changed",
      version: HOST_PROTOCOL_VERSION,
      sequence: 1,
      payload: snapshot(active, 1),
    }),
  );
  expect(screen.getByRole("status", { name: "Analysis count" })).toHaveTextContent(
    "4 matching records counted so far",
  );
  expect(screen.queryByRole("table", { name: "Analysis preview" })).not.toBeInTheDocument();
  next = snapshot(
    {
      ...active,
      counts: operation.counts,
      coverage: {
        ...active.coverage!,
        scannedRecords: 5,
        matchedRecords: 5,
        partitions: [{ partition: 0, startOffset: "0", endOffset: "10", nextOffset: "5" }],
      },
      result: {
        ...operation.result!,
        previewBytes: new TextEncoder().encode(JSON.stringify(operation.result!.preview))
          .byteLength,
      },
    },
    2,
  );
  await user.click(screen.getByRole("button", { name: "Refresh analysis status" }));
  expect(screen.getByRole("status", { name: "Analysis count" })).toHaveTextContent(
    "5 matching records counted so far",
  );
  expect(
    within(screen.getByRole("table", { name: "Analysis preview" })).getAllByRole("row"),
  ).toHaveLength(3);
  expect(screen.getByRole("region", { name: "Count by results" })).toHaveTextContent(
    "3 grouped of 5 counted records",
  );
  act(() =>
    listener?.({
      event: "records.analysis.changed",
      version: HOST_PROTOCOL_VERSION,
      sequence: 2,
      payload: snapshot(active, 1),
    }),
  );
  expect(screen.getByRole("status", { name: "Analysis count" })).toHaveTextContent(
    "5 matching records counted so far",
  );
  expect(screen.getByRole("table", { name: "Analysis preview" })).toBeVisible();
});
