// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useMemo, useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type {
  KafkaExploredMessage,
  KafkaFetchRequest,
  KafkaLiveRuleEvaluation,
} from "../../src/kafka/contracts";
import { MessageWorkspace } from "../../src/kafka/ui/MessageWorkspace";
import {
  initialKafkaMessageFilters,
  selectFilteredKafkaMessages,
  selectKafkaMessageById,
  withKafkaMessageTextFilter,
  type KafkaMessageFilters,
  type KafkaMessageTextFilterField,
  type TextDocumentTransferPort,
} from "../../src/kafka/ui";

const evaluated: KafkaLiveRuleEvaluation = {
  activeMatchCount: 0,
  activeMatches: [],
  durationMicros: 0,
  errorCount: 0,
  errors: [],
  evaluatedRules: 1,
  omittedEvidence: 0,
  omittedRules: 0,
  state: "evaluated",
  suppressedMatchCount: 0,
  suppressedMatches: [],
};

const activeEvaluation: KafkaLiveRuleEvaluation = {
  ...evaluated,
  activeMatchCount: 1,
  activeMatches: [{ level: "warn", name: "Matched" }],
  highestActiveSeverity: "warn",
};

function message(id: string, overrides: Partial<KafkaExploredMessage> = {}): KafkaExploredMessage {
  return {
    headers: {},
    id,
    key: `key-${id}`,
    offset: id,
    originalByteSize: 30,
    partition: 0,
    payload: `{"record":"${id}"}`,
    preview: `{"record":"${id}"}`,
    ruleEvaluation: evaluated,
    timestamp: "2026-07-25T10:40:00.000Z",
    topic: "orders",
    truncated: false,
    ...overrides,
  };
}

const retained = [
  message("10", {
    key: "Order-Alpha",
    offset: "101",
    partition: 2,
    payload: '{"status":"APPROVED"}',
    preview: '{"status":"APPROVED"}',
    ruleEvaluation: activeEvaluation,
    timestamp: "2026-07-25T10:41:00.000Z",
  }),
  message("20", {
    key: "Order-Beta",
    offset: "202",
    partition: 3,
    payload: '{"status":"rejected"}',
    preview: '{"status":"rejected"}',
    timestamp: "2026-07-25T10:42:00.000Z",
  }),
  message("30", {
    key: null,
    offset: "303",
    originalByteSize: 2_000_000,
    partition: 3,
    payload: null,
    preview: "retained preview",
    timestamp: "2026-07-25T10:43:00.000Z",
    truncated: true,
  }),
] as const;

const request: KafkaFetchRequest = {
  maxMessages: 1_000,
  mode: "tail",
  topic: "orders",
};

function Harness({
  download = (): Promise<void> => Promise.resolve(),
}: {
  readonly download?: TextDocumentTransferPort["download"];
}): React.JSX.Element {
  const [filters, setFilters] = useState<KafkaMessageFilters>(initialKafkaMessageFilters);
  const [selectedMessageId, setSelectedMessageId] = useState<string | null>(null);
  const messages = useMemo(() => selectFilteredKafkaMessages(retained, filters), [filters]);
  const selectedMessage = selectKafkaMessageById(messages, selectedMessageId);
  const selectionNotice =
    selectedMessageId !== null &&
    retained.some((item) => item.id === selectedMessageId) &&
    selectedMessage === null
      ? "The selected message is hidden by the current filters."
      : undefined;
  const transfer: TextDocumentTransferPort = {
    copy: () => Promise.resolve(),
    download,
  };
  function changeText(field: KafkaMessageTextFilterField, value: string): void {
    setFilters((current) => withKafkaMessageTextFilter(current, field, value));
  }
  return (
    <div style={{ height: 650, width: 1_000 }}>
      <MessageWorkspace
        connectionAvailable
        consumptionError={null}
        consumptionRequest={request}
        consumptionState="streaming"
        consumptionStopping={false}
        droppedMessages={0}
        fetchMaximum={1_000}
        fetchMode="tail"
        filters={filters}
        liveRuleCapability={{ applicableRules: 1, omittedRules: 0, state: "ready" }}
        messages={messages}
        messagesStale={false}
        onClearFilters={() => {
          setFilters(initialKafkaMessageFilters);
        }}
        onClearSelection={() => {
          setSelectedMessageId(null);
        }}
        onFetchMaximumChange={() => undefined}
        onFetchModeChange={() => undefined}
        onPartitionFilterChange={(partition) => {
          setFilters((current) => ({ ...current, partition }));
        }}
        onRuleFilterChange={(activeRuleMatchesOnly) => {
          setFilters((current) => ({ ...current, activeRuleMatchesOnly }));
        }}
        onSelectMessage={setSelectedMessageId}
        onStart={() => undefined}
        onStop={() => undefined}
        onTextFilterChange={changeText}
        retainedMessageCount={retained.length}
        savedProfileCount={0}
        selectedMessage={selectedMessage}
        selectedMessageId={selectedMessageId}
        selectedTopic="orders"
        selectionNotice={selectionNotice}
        transfer={transfer}
      />
    </div>
  );
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("Kafka message operation Material UI workflow", () => {
  it("combines all field controls and the rule filter with exact visible counts", async () => {
    const user = userEvent.setup();
    render(<Harness />);

    expect(await screen.findByLabelText("Showing 3 of 3 retained messages")).toHaveTextContent(
      "3 / 3",
    );
    expect(screen.getByRole("status", { name: "Consumption status" })).toHaveTextContent(
      "Streaming",
    );
    await user.click(screen.getByRole("button", { name: "Show message filters" }));
    expect(screen.getByRole("button", { name: "Hide message filters" })).toBeVisible();
    await user.type(screen.getByRole("textbox", { name: "Timestamp contains" }), "10:41");
    await user.type(screen.getByRole("spinbutton", { name: "Partition" }), "2");
    await user.type(screen.getByRole("textbox", { name: "Offset contains" }), "01");
    await user.type(screen.getByRole("textbox", { name: "Key contains" }), "alpha");
    await user.type(
      screen.getByRole("textbox", { name: "Value or retained preview contains" }),
      "approved",
    );
    await user.click(screen.getByRole("checkbox", { name: "Rule matches only" }));

    const grid = await screen.findByRole("grid", { name: "Kafka messages" });
    expect(within(grid).getByText("Order-Alpha")).toBeVisible();
    expect(within(grid).queryByText("Order-Beta")).not.toBeInTheDocument();
    expect(screen.getByLabelText("Showing 1 of 3 retained messages")).toHaveTextContent("1 / 3");
    expect(screen.getByText("6 active filters")).toBeVisible();
  });

  it("distinguishes no filter matches, clears atomically, and restores retained selection", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    let grid = await screen.findByRole("grid", { name: "Kafka messages" }, { timeout: 5_000 });
    await user.click(within(grid).getByText("Order-Beta"));
    expect(await screen.findByRole("complementary", { name: "Message inspector" })).toBeVisible();
    expect(
      within(grid)
        .getAllByRole("columnheader")
        .map((header) => header.textContent),
    ).toEqual(["Timestamp", "Key", "Rules"]);

    await user.click(screen.getByRole("button", { name: "Show message filters" }));
    await user.type(screen.getByRole("textbox", { name: "Key contains" }), "does-not-exist");
    expect(await screen.findByText("No messages match the current filters")).toBeVisible();
    expect(
      screen.getByText("The selected message is hidden by the current filters."),
    ).toBeVisible();
    expect(
      screen.queryByRole("complementary", { name: "Message inspector" }),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Export filtered JSON" })).toBeDisabled();

    await user.click(screen.getByRole("button", { name: "Clear message filters" }));
    grid = await screen.findByRole("grid", { name: "Kafka messages" });
    expect(within(grid).getByText("Order-Beta")).toBeVisible();
    const inspector = await screen.findByRole("complementary", {
      name: "Message inspector",
    });
    await user.click(within(inspector).getByRole("tab", { name: "Key" }));
    expect(inspector).toHaveTextContent("Order-Beta");
    expect(screen.getByLabelText("Showing 3 of 3 retained messages")).toHaveTextContent("3 / 3");
  });

  it("downloads the exact current filtered array and reports completion only afterward", async () => {
    const download = vi.fn<TextDocumentTransferPort["download"]>(() => Promise.resolve());
    const user = userEvent.setup();
    render(<Harness download={download} />);

    await user.click(screen.getByRole("button", { name: "Show message filters" }));
    await user.type(screen.getByRole("textbox", { name: "Key contains" }), "alpha");
    await user.click(screen.getByRole("button", { name: "Export filtered JSON" }));

    await waitFor(() => {
      expect(download).toHaveBeenCalledOnce();
    });
    const exported = JSON.parse(download.mock.calls[0]![0].content) as {
      readonly exportedMessageCount: number;
      readonly filters: { readonly key: string };
      readonly messages: readonly { readonly key: string | null }[];
      readonly retainedMessageCount: number;
    };
    expect(exported).toMatchObject({
      exportedMessageCount: 1,
      filters: { key: "alpha" },
      retainedMessageCount: 3,
    });
    expect(exported.messages.map((item) => item.key)).toEqual(["Order-Alpha"]);
    expect(screen.getByRole("status", { name: "Message operation status" })).toHaveTextContent(
      "Filtered message JSON download started.",
    );
  });

  it("reports download failure without losing filters or claiming success", async () => {
    const user = userEvent.setup();
    render(
      <Harness download={() => Promise.reject(new Error("blocked: private message content"))} />,
    );

    await user.click(screen.getByRole("button", { name: "Show message filters" }));
    await user.type(screen.getByRole("textbox", { name: "Key contains" }), "alpha");
    await user.click(screen.getByRole("button", { name: "Export filtered JSON" }));

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "The filtered JSON export failed. No file was saved. Retry the export.",
    );
    expect(screen.getByRole("alert")).not.toHaveTextContent("private message content");
    expect(screen.getByRole("textbox", { name: "Key contains" })).toHaveValue("alpha");
    expect(screen.queryByText(/download started/u)).not.toBeInTheDocument();
  });

  it("keeps the filter region and primary actions keyboard reachable at 1000x650", async () => {
    const user = userEvent.setup();
    render(<Harness />);

    const toggle = screen.getByRole("button", { name: "Show message filters" });
    toggle.focus();
    await user.keyboard("{Enter}");
    expect(screen.getByRole("button", { name: "Hide message filters" })).toHaveFocus();
    expect(screen.getByRole("textbox", { name: "Key contains" })).toBeVisible();

    await user.keyboard("{Enter}");
    expect(screen.getByRole("button", { name: "Show message filters" })).toHaveFocus();
    expect(screen.queryByRole("textbox", { name: "Key contains" })).not.toBeInTheDocument();

    await user.keyboard("{Enter}");
    const timestamp = screen.getByRole("textbox", { name: "Timestamp contains" });
    timestamp.focus();
    await user.keyboard("10:41");
    expect(timestamp).toHaveValue("10:41");
    expect(screen.getByRole("button", { name: "Clear message filters" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Export filtered JSON" })).toBeEnabled();
  });
});
