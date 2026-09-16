// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import type {
  KafkaExploredMessage,
  KafkaLiveRuleEvaluation,
} from "../../src/features/kafka/contracts";
import { MessageDataGrid } from "../../src/features/kafka/ui/MessageDataGrid";
import { MessageInspector } from "../../src/features/kafka/ui/MessageInspector";
import { StreamSkopeThemeProvider } from "../../src/platform/ui/StreamSkopeThemeProvider";

const evaluated: KafkaLiveRuleEvaluation = {
  activeMatchCount: 0,
  activeMatches: [],
  durationMicros: 0,
  errorCount: 0,
  errors: [],
  evaluatedRules: 0,
  omittedEvidence: 0,
  omittedRules: 0,
  state: "evaluated",
  suppressedMatchCount: 0,
  suppressedMatches: [],
};

function message(id: string, payload = `{"sequence":${id}}`): KafkaExploredMessage {
  return {
    headers: { source: "synthetic" },
    id,
    key: `key-${id}`,
    offset: id,
    originalByteSize: payload.length + id.length + 4,
    partition: Number(id) % 3,
    payload,
    preview: payload,
    ruleEvaluation: evaluated,
    timestamp: "2026-07-25T15:00:00.000Z",
    topic: "test",
    truncated: false,
  };
}

afterEach(() => {
  cleanup();
});

describe("message presentation", () => {
  it("preserves Value and Raw when selecting another record", async () => {
    const user = userEvent.setup();
    const view = render(<MessageInspector message={message("1")} onClose={() => undefined} />);
    await user.click(screen.getByRole("tab", { name: "Value" }));
    await user.click(screen.getByRole("tab", { name: "Raw" }));
    for (const id of ["2", "3", "4"]) {
      view.rerender(<MessageInspector message={message(id)} onClose={() => undefined} />);
      expect(screen.getByRole("tab", { name: "Value" })).toHaveAttribute("aria-selected", "true");
      expect(screen.getByRole("tab", { name: "Raw" })).toHaveAttribute("aria-selected", "true");
      expect(screen.getByRole("region", { name: "Value evidence" })).toHaveTextContent(
        `{"sequence":${id}}`,
      );
    }
    view.rerender(
      <MessageInspector
        message={{ ...message("5"), payload: null, preview: "" }}
        onClose={() => undefined}
      />,
    );
    expect(screen.getByRole("tab", { name: "Value" })).toHaveAttribute("aria-selected", "true");
    expect(screen.queryByRole("tab", { name: "Raw" })).not.toBeInTheDocument();
    view.rerender(<MessageInspector message={message("6")} onClose={() => undefined} />);
    expect(screen.getByRole("tab", { name: "Raw" })).toHaveAttribute("aria-selected", "true");
  });
  it("virtualizes 10,000 deterministic rows and keeps keyboard selection operable", async () => {
    const messages = Array.from({ length: 10_000 }, (_value, index) => message(String(index)));
    const onSelectMessage = vi.fn<(id: string | null) => void>();
    const user = userEvent.setup();
    render(
      <div style={{ height: 600, width: 1_200 }}>
        <MessageDataGrid
          messages={messages}
          onSelectMessage={onSelectMessage}
          selectedMessageId={null}
        />
      </div>,
    );

    const grid = screen.getByRole("grid", { name: "Kafka messages" });
    expect(within(grid).getAllByRole("row").length).toBeLessThan(250);
    expect(
      within(grid)
        .getAllByRole("columnheader")
        .map((header) => header.getAttribute("aria-label") ?? header.textContent),
    ).toEqual(["Timestamp", "Key", "Value", "Partition", "Offset", "Rules"]);
    const firstKey = within(grid).getByRole("gridcell", { name: "key-0" });
    await user.click(firstKey);
    expect(onSelectMessage).toHaveBeenLastCalledWith("0");

    fireEvent.keyDown(firstKey, { key: "ArrowDown" });
    fireEvent.keyDown(grid, { key: " " });
    expect(within(grid).getAllByRole("row").length).toBeLessThan(250);
  });

  it("renders active markup and JavaScript URLs as inert payload text", async () => {
    const payload =
      '<script>globalThis.__payloadExecuted=true</script><a href="javascript:alert(1)">open</a>';
    const user = userEvent.setup();
    render(<MessageInspector message={message("1", payload)} onClose={() => undefined} />);

    const inspector = screen.getByRole("complementary", { name: "Message inspector" });
    await user.click(within(inspector).getByRole("tab", { name: "Value" }));
    expect(inspector).toHaveTextContent(payload);
    expect(document.querySelector("script")).toBeNull();
    expect(document.querySelector('a[href^="javascript:"]')).toBeNull();
    expect((globalThis as { __payloadExecuted?: boolean }).__payloadExecuted).toBeUndefined();
  });

  it("keeps malformed JSON available as raw text without a parsing error state", async () => {
    const payload = '{"status": invalid';
    const user = userEvent.setup();
    render(<MessageInspector message={message("2", payload)} onClose={() => undefined} />);

    const inspector = screen.getByRole("complementary", { name: "Message inspector" });
    await user.click(within(inspector).getByRole("tab", { name: "Value" }));
    expect(inspector).toHaveTextContent(payload);
    expect(
      within(inspector).queryByRole("tab", { name: "Formatted JSON" }),
    ).not.toBeInTheDocument();
    expect(within(inspector).queryByRole("alert")).not.toBeInTheDocument();
  });

  it("organizes selected-record evidence as one Studio-shaped Message details inspector", async () => {
    const user = userEvent.setup();
    render(<MessageInspector message={message("3")} onClose={() => undefined} />);

    const inspector = screen.getByRole("complementary", { name: "Message inspector" });
    const panelHeading = within(inspector).getByRole("heading", { name: "Message details" });
    expect(getComputedStyle(panelHeading.parentElement as HTMLElement).minHeight).toBe("44px");
    const summary = within(inspector).getByRole("region", { name: "Selected message summary" });
    expect(summary).toHaveTextContent("Partition 0 · Offset 3");
    expect(summary).toHaveTextContent("test");
    expect(summary).not.toHaveTextContent("key-3");
    const evidenceTabs = within(inspector).getByRole("tablist", { name: "Message evidence" });
    expect(
      within(inspector).getByRole("region", { name: "Message evidence content" }),
    ).toHaveAttribute("tabindex", "0");
    expect(
      within(evidenceTabs)
        .getAllByRole("tab")
        .map((tab) => tab.textContent),
    ).toEqual(["Metadata", "Key", "Value", "Rules"]);
    expect(within(evidenceTabs).getByRole("tab", { name: "Metadata" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    const metadata = within(inspector).getByRole("region", { name: "Metadata evidence" });
    expect(within(metadata).getByRole("heading", { name: "Record" })).toBeVisible();
    expect(within(metadata).getByRole("heading", { name: "Headers" })).toBeVisible();
    expect(within(metadata).getByText("synthetic")).toBeVisible();
    expect(within(inspector).getByText("test")).toBeVisible();
    expect(within(metadata).queryByText("key-3")).not.toBeInTheDocument();
    expect(within(metadata).queryByText('{"sequence":3}')).not.toBeInTheDocument();

    await user.click(within(evidenceTabs).getByRole("tab", { name: "Key" }));
    expect(within(inspector).getByRole("region", { name: "Key evidence" })).toHaveTextContent(
      "key-3",
    );

    await user.click(within(evidenceTabs).getByRole("tab", { name: "Value" }));
    expect(within(inspector).getByRole("region", { name: "Value evidence" })).toHaveTextContent(
      '"sequence": 3',
    );
    expect(within(inspector).getByRole("tablist", { name: "Value representation" })).toBeVisible();

    await user.click(within(evidenceTabs).getByRole("tab", { name: "Rules" }));
    expect(within(inspector).getByText("Evaluated")).toBeVisible();
    expect(
      within(inspector).getByRole("heading", { name: "Rule evaluation" }).parentElement,
    ).toHaveTextContent("0 active matches");
  });

  it("uses deliberate peer metrics for definition rows and Message Key/Value evidence", async () => {
    const user = userEvent.setup();
    render(
      <StreamSkopeThemeProvider>
        <MessageInspector message={message("8")} onClose={() => undefined} />
      </StreamSkopeThemeProvider>,
    );

    const inspector = screen.getByRole("complementary", { name: "Message inspector" });
    const metadata = within(inspector).getByRole("region", { name: "Metadata evidence" });
    const topicLabel = within(metadata).getByText("Topic", { exact: true });
    const topicValue = within(metadata).getByText("test", { exact: true });
    const labelStyle = getComputedStyle(topicLabel);
    const valueStyle = getComputedStyle(topicValue);
    expect(labelStyle.fontSize).toBe("0.75rem");
    expect(valueStyle.fontSize).toBe("0.75rem");
    expect(labelStyle.lineHeight).toBe("1.5");
    expect(valueStyle.lineHeight).toBe("1.5");
    expect(labelStyle.fontFamily).toContain("system-ui");
    expect(valueStyle.fontFamily).toContain("ui-monospace");

    const evidenceTabs = within(inspector).getByRole("tablist", { name: "Message evidence" });
    await user.click(within(evidenceTabs).getByRole("tab", { name: "Key" }));
    const keyBlock = within(inspector).getByText("key-8", { exact: true });
    const keyStyle = getComputedStyle(keyBlock);
    expect(keyStyle.fontFamily).toContain("ui-monospace");
    expect(keyStyle.fontSize).toBe("0.75rem");
    expect(keyStyle.lineHeight).toBe("1.5");
    await user.click(within(evidenceTabs).getByRole("tab", { name: "Value" }));
    const valueBlock = within(inspector).getByText(/"sequence": 8/u);
    const valueBlockStyle = getComputedStyle(valueBlock);
    expect(valueBlockStyle.fontFamily).toBe(keyStyle.fontFamily);
    expect(valueBlockStyle.fontSize).toBe(keyStyle.fontSize);
    expect(valueBlockStyle.lineHeight).toBe(keyStyle.lineHeight);
  });

  it("makes rule evidence requiring attention explicit in its stable mode", async () => {
    const user = userEvent.setup();
    render(
      <MessageInspector
        message={{
          ...message("4"),
          ruleEvaluation: {
            ...evaluated,
            activeMatchCount: 1,
            activeMatches: [{ level: "warn", name: "Slow order" }],
            evaluatedRules: 1,
          },
        }}
        onClose={() => undefined}
      />,
    );

    const inspector = screen.getByRole("complementary", { name: "Message inspector" });
    const evidenceTabs = within(inspector).getByRole("tablist", { name: "Message evidence" });
    expect(within(evidenceTabs).getByRole("tab", { name: /^Rules$/u })).toBeVisible();
    await user.click(within(evidenceTabs).getByRole("tab", { name: /^Rules$/u }));
    expect(within(inspector).getByText("Slow order · Warn")).toBeVisible();
  });
});
