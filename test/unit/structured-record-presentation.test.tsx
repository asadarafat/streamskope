// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import type { KafkaExploredMessage } from "../../src/features/kafka/contracts";
import type { StructuredRecord } from "../../src/features/kafka/contracts/structured-record";
import { MessageInspector } from "../../src/features/kafka/ui/MessageInspector";
import { MessageDataGrid } from "../../src/features/kafka/ui/MessageDataGrid";
import { RecordComparisonPanel } from "../../src/features/kafka/ui/RecordComparisonPanel";
import {
  createKafkaMessageExportDocument,
  initialKafkaMessageFilters,
  selectKafkaQueryMessages,
} from "../../src/features/kafka/ui/message-operations";
import { StreamSkopeThemeProvider } from "../../src/platform/ui/StreamSkopeThemeProvider";

const protectedJson = '{"id":9007199254740993,"email":"[MASKED]","status":"paid"}';
const structured: StructuredRecord = {
  version: 1,
  headersState: "complete",
  protection: "masked",
  key: { state: "decoded", codec: "utf8", text: "order-1", json: null, writerSchema: null },
  value: {
    state: "decoded",
    codec: "avro",
    text: protectedJson,
    json: protectedJson,
    writerSchema: { id: 8, format: "avro", messageType: null, registry: "https://registry.test/" },
  },
  headers: [
    { key: "correlation-id", value: "first", error: null },
    { key: "correlation-id", value: "second", error: null },
    { key: "token", value: "[MASKED]", error: null },
    { key: "nullable", value: null, error: null },
  ],
};
const message: KafkaExploredMessage = {
  id: "orders:0:1",
  topic: "orders",
  partition: 0,
  offset: "1",
  timestamp: "2026-10-09T00:00:00Z",
  headers: { "correlation-id": "second", token: "[MASKED]", nullable: "" },
  key: "order-1",
  payload: protectedJson,
  preview: protectedJson,
  originalByteSize: 100,
  recordByteSize: 220,
  truncated: false,
  original: { state: "unavailable", reason: "masked" },
  structured,
  ruleEvaluation: {
    state: "evaluated",
    activeMatchCount: 0,
    activeMatches: [],
    suppressedMatchCount: 0,
    suppressedMatches: [],
    durationMicros: 0,
    errorCount: 0,
    errors: [],
    evaluatedRules: 0,
    omittedEvidence: 0,
    omittedRules: 0,
  },
};
afterEach(cleanup);

it("shows ordered duplicate headers and copies the same protected exact projection used by filters and export", async () => {
  const user = userEvent.setup();
  const copy = vi.fn().mockResolvedValue(undefined);
  render(
    <MessageInspector
      message={message}
      onClose={() => undefined}
      transfer={{ copy, download: vi.fn().mockResolvedValue(undefined) }}
    />,
  );
  const headers = within(screen.getByLabelText("Message headers"));
  expect(headers.getAllByText("correlation-id")).toHaveLength(2);
  expect(headers.getByText("first")).toBeVisible();
  expect(headers.getByText("second")).toBeVisible();
  expect(headers.getByText("Kafka null header")).toBeVisible();
  await user.click(screen.getByRole("tab", { name: /^Value$/u }));
  expect(screen.getByRole("region", { name: "Value evidence" })).toHaveTextContent(
    "9007199254740993",
  );
  await user.click(screen.getByRole("button", { name: "Copy value" }));
  expect(copy).toHaveBeenCalledWith(protectedJson);
  const filters = {
    ...initialKafkaMessageFilters,
    expression: '$.status == "paid"',
    value: "[MASKED]",
  };
  const selected = selectKafkaQueryMessages([message], filters);
  expect(selected.messages).toEqual([message]);
  expect(
    selectKafkaQueryMessages([message], {
      ...initialKafkaMessageFilters,
      value: "private@example.test",
    }).messages,
  ).toEqual([]);
  const document = createKafkaMessageExportDocument({
    messages: selected.messages,
    filters,
    retainedMessageCount: 1,
    stale: false,
    topic: "orders",
  });
  const exported = JSON.parse(document.content) as {
    schemaVersion: number;
    messages: {
      payload: string;
      recordByteSize: number;
      structured: StructuredRecord;
      original: { reason: string };
    }[];
  };
  expect(exported.schemaVersion).toBe(3);
  expect(exported.messages[0]!.payload).toBe(protectedJson);
  expect(exported.messages[0]!.recordByteSize).toBe(220);
  expect(exported.messages[0]!.structured).toEqual(structured);
  expect(exported.messages[0]!.original.reason).toBe("masked");
  expect(document.content).not.toContain("private@example.test");
});

it("compares protected projections from mixed writer schemas without rereading secrets or losing large integers", async () => {
  const user = userEvent.setup();
  const nextJson = protectedJson.replace("9007199254740993", "9007199254740994");
  const current: KafkaExploredMessage = {
    ...message,
    id: "orders:0:2",
    offset: "2",
    payload: nextJson,
    preview: nextJson,
    structured: {
      ...structured,
      value: {
        state: "decoded",
        codec: "protobuf",
        text: nextJson,
        json: nextJson,
        writerSchema: {
          id: 12,
          format: "protobuf",
          messageType: ".fixture.Event",
          registry: "https://registry.test/",
        },
      },
    },
  };
  render(
    <RecordComparisonPanel current={current} baseline={message} onPin={() => undefined} enabled />,
  );
  await user.click(screen.getByRole("button", { name: "Compare records" }));
  expect(screen.getByRole("table", { name: "Differences" })).toHaveTextContent("9007199254740993");
  expect(screen.getByRole("table", { name: "Differences" })).toHaveTextContent("9007199254740994");
  await user.click(screen.getByRole("combobox", { name: "Comparison representation" }));
  await user.click(screen.getByRole("option", { name: "Original bytes (Base64)" }));
  expect(screen.getByRole("button", { name: "Compare records" })).toBeDisabled();
});

it("shows the canonical protected value in the grid", () => {
  render(
    <StreamSkopeThemeProvider>
      <div style={{ height: 400, width: 1200 }}>
        <MessageDataGrid
          messages={[message]}
          selectedMessageId={null}
          onSelectMessage={() => undefined}
        />
      </div>
    </StreamSkopeThemeProvider>,
  );
  expect(screen.getByRole("gridcell", { name: protectedJson })).toBeVisible();
});

it("distinguishes a tombstone from decoded JSON null and blocks malformed projection comparison", async () => {
  const user = userEvent.setup();
  const before: KafkaExploredMessage = {
    ...message,
    payload: null,
    preview: "",
    structured: { ...structured, value: { state: "null", codec: "auto", writerSchema: null } },
  };
  const after: KafkaExploredMessage = {
    ...message,
    payload: "null",
    preview: "null",
    structured: {
      ...structured,
      value: { state: "decoded", codec: "json", text: "null", json: "null", writerSchema: null },
    },
  };
  const view = render(
    <RecordComparisonPanel current={after} baseline={before} onPin={() => undefined} enabled />,
  );
  await user.click(screen.getByRole("button", { name: "Compare records" }));
  expect(screen.getByRole("table", { name: "Differences" })).toHaveTextContent("Kafka null");
  expect(screen.getByRole("table", { name: "Differences" })).toHaveTextContent("decoded");
  view.rerender(
    <RecordComparisonPanel
      current={{
        ...after,
        structured: {
          ...structured,
          value: {
            state: "error",
            codec: "avro",
            writerSchema: structured.value.writerSchema,
            code: "malformed",
            detail: "Invalid payload.",
          },
        },
      }}
      baseline={before}
      onPin={() => undefined}
      enabled
    />,
  );
  expect(screen.getByRole("button", { name: "Compare records" })).toBeDisabled();
  expect(screen.getByText(/without decoding errors/u)).toBeVisible();
});

it.each([
  { codec: "utf8" as const, text: '{"looks":"json","number":9007199254740993}' },
  { codec: "bytes" as const, text: "null" },
])(
  "honors a manual $codec choice without a second JSON interpretation",
  async ({ codec, text }) => {
    const user = userEvent.setup();
    const copy = vi.fn().mockResolvedValue(undefined);
    const manual: KafkaExploredMessage = {
      ...message,
      payload: text,
      preview: text,
      structured: {
        ...structured,
        value: { state: "decoded", codec, text, json: null, writerSchema: null },
      },
    };
    render(
      <MessageInspector
        message={manual}
        onClose={() => undefined}
        transfer={{ copy, download: vi.fn().mockResolvedValue(undefined) }}
      />,
    );
    await user.click(screen.getByRole("tab", { name: /^Value$/u }));
    expect(screen.queryByRole("tab", { name: "Formatted JSON" })).not.toBeInTheDocument();
    expect(screen.getByRole("region", { name: "Value evidence" })).toHaveTextContent(text);
    await user.click(screen.getByRole("button", { name: "Copy value" }));
    expect(copy).toHaveBeenCalledWith(text);
  },
);

it("distinguishes malformed records, tombstones and empty text in the grid", () => {
  const make = (
    id: string,
    value: StructuredRecord["value"],
    preview: string,
  ): KafkaExploredMessage => ({ ...message, id, preview, structured: { ...structured, value } });
  const records = [
    make(
      "malformed",
      {
        state: "error",
        codec: "json",
        writerSchema: null,
        code: "malformed",
        detail: "Invalid JSON payload.",
      },
      "",
    ),
    make("tombstone", { state: "null", codec: "auto", writerSchema: null }, ""),
    make(
      "empty",
      { state: "decoded", codec: "utf8", writerSchema: null, text: "", json: null },
      "",
    ),
  ];
  render(
    <StreamSkopeThemeProvider>
      <div style={{ height: 400, width: 1200 }}>
        <MessageDataGrid
          messages={records}
          selectedMessageId={null}
          onSelectMessage={() => undefined}
        />
      </div>
    </StreamSkopeThemeProvider>,
  );
  expect(screen.getByRole("gridcell", { name: "Decoding unavailable" })).toBeVisible();
  expect(screen.getByTitle("Invalid JSON payload.")).toBeVisible();
  expect(screen.getByRole("gridcell", { name: "Kafka null (tombstone)" })).toBeVisible();
});

it("does not claim absent headers when original header evidence was unavailable", () => {
  render(
    <MessageInspector
      message={{
        ...message,
        structured: { ...structured, headers: [], headersState: "unavailable" },
      }}
      onClose={() => undefined}
    />,
  );
  expect(screen.getByText(/Ordered header evidence is unavailable/u)).toBeVisible();
  expect(screen.queryByText("This record has no retained headers.")).not.toBeInTheDocument();
});
