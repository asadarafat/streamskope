// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { ActivityEntry, HostTextDocument } from "../../src/kafka/contracts";
import { ActivityLogDrawer } from "../../src/kafka/ui/ActivityLogDrawer";

const failedTrust: ActivityEntry = {
  correlationId: "correlation-trust-1",
  detail: 'Stage: Trust material\nCategory: TRUST_MATERIAL\nNext action: Select "JKS" and retry.',
  id: "activity-trust-1",
  object: "kafka-lab.example.test:22",
  operation: "Fetch remote trust material",
  outcome: "failed",
  severity: "error",
  timestamp: "2026-07-27T12:20:34.000Z",
};

const loadedProfiles: ActivityEntry = {
  correlationId: "correlation-profiles-1",
  detail: "The safe profile inventory was refreshed.",
  id: "activity-profiles-1",
  object: "Kafka profiles",
  operation: "Load profiles",
  outcome: "succeeded",
  severity: "info",
  timestamp: "2026-07-27T12:19:00.000Z",
};

afterEach(() => {
  cleanup();
});

describe("Material UI Activity log panel", () => {
  it("keeps follow accessible and preserves the reader position until explicitly resumed", async () => {
    const user = userEvent.setup();
    const view = render(
      <ActivityLogDrawer entries={[loadedProfiles]} onClose={() => undefined} open />,
    );
    expect(screen.getByRole("button", { name: "Follow latest" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    const raw = screen.getByRole("log");
    Object.defineProperties(raw, {
      scrollHeight: { configurable: true, value: 1000 },
      clientHeight: { configurable: true, value: 200 },
      scrollTop: { configurable: true, writable: true, value: 100 },
    });
    const scrollTo = vi.fn();
    raw.scrollTo = scrollTo;
    fireEvent.scroll(raw);
    expect(screen.getByRole("button", { name: "Resume live" })).toHaveAttribute(
      "aria-pressed",
      "false",
    );
    view.rerender(
      <ActivityLogDrawer entries={[loadedProfiles, failedTrust]} onClose={() => undefined} open />,
    );
    expect(scrollTo).not.toHaveBeenCalled();
    expect(raw.scrollTop).toBe(100);
    await user.click(screen.getByRole("button", { name: "Resume live" }));
    expect(scrollTo).toHaveBeenCalledWith({ top: 1000 });
    await user.click(screen.getByRole("button", { name: "Filter" }));
    await user.type(screen.getByRole("searchbox", { name: "Search activity" }), "profiles");
    await user.click(screen.getByRole("button", { name: "Filter (1)" }));
    expect(screen.getByRole("button", { name: "Filter (1)" })).toHaveAttribute(
      "aria-expanded",
      "false",
    );
  });
  it("opens the existing raw log filtered to the originating operation correlation", () => {
    render(
      <ActivityLogDrawer
        entries={[loadedProfiles, failedTrust]}
        onClose={() => undefined}
        open
        initialQuery="correlation-trust-1"
      />,
    );
    const raw = screen.getByRole("log", { name: "Raw activity log" });
    expect(raw).toHaveTextContent("correlation-trust-1");
    expect(raw).not.toHaveTextContent("correlation-profiles-1");
    expect(screen.getByRole("searchbox", { name: "Search activity" })).toHaveValue(
      "correlation-trust-1",
    );
  });
  it("presents, filters, and transfers one bounded redacted raw log", async () => {
    const onClose = vi.fn();
    const copy = vi.fn<(content: string) => Promise<void>>(() => Promise.resolve());
    const download = vi.fn<(document: HostTextDocument) => Promise<void>>(() => Promise.resolve());
    const user = userEvent.setup();
    render(
      <ActivityLogDrawer
        entries={[loadedProfiles, failedTrust]}
        onClose={onClose}
        open
        transfer={{ copy, download }}
      />,
    );

    expect(screen.queryByRole("dialog", { name: "Activity log" })).not.toBeInTheDocument();
    const session = screen.getByRole("region", { name: "Activity dock" });
    const activity = within(session).getByRole("complementary", { name: "Activity log" });
    expect(session).toHaveTextContent("2 entries · Error");
    expect(within(session).getByRole("heading", { name: "Raw logs" })).toBeVisible();
    expect(within(session).queryByRole("tablist")).not.toBeInTheDocument();
    expect(within(session).queryByRole("tab", { name: "Events" })).not.toBeInTheDocument();
    expect(within(session).queryByRole("tab", { name: "Raw log" })).not.toBeInTheDocument();
    expect(within(activity).queryByLabelText("Activity entry detail")).not.toBeInTheDocument();
    expect(within(activity).queryByLabelText("Activity log entries")).not.toBeInTheDocument();

    const raw = within(activity).getByRole("log", { name: "Raw activity log" });
    expect(raw).toHaveStyle({ whiteSpace: "pre" });
    expect(getComputedStyle(raw).fontFamily).toContain("ui-monospace");
    expect(getComputedStyle(raw).fontSize).toBe("0.75rem");
    expect(getComputedStyle(raw).lineHeight).toBe("1.5");
    expect(raw).toHaveTextContent('operation="Load profiles"');
    expect(raw).toHaveTextContent('operation="Fetch remote trust material"');

    await user.click(within(session).getByRole("button", { name: "Filter" }));
    const wrapLines = within(activity).getByRole("checkbox", { name: "Wrap lines" });
    expect(wrapLines).not.toBeChecked();
    await user.click(wrapLines);
    expect(raw).toHaveStyle({ whiteSpace: "pre-wrap" });
    await user.click(within(activity).getByRole("combobox", { name: "Severity" }));
    await user.click(screen.getByRole("option", { name: "Error" }));
    expect(raw).not.toHaveTextContent('operation="Load profiles"');
    expect(raw).toHaveTextContent('time="2026-07-27T12:20:34.000Z"');
    expect(raw).toHaveTextContent("level=error");
    expect(raw).toHaveTextContent('correlation_id="correlation-trust-1"');
    expect(raw).toHaveTextContent(
      'msg="Stage: Trust material\\nCategory: TRUST_MATERIAL\\nNext action: Select \\"JKS\\" and retry."',
    );

    await user.click(within(session).getByRole("button", { name: "Copy raw logs" }));
    expect(copy).toHaveBeenCalledWith(expect.stringContaining("correlation-trust-1"));
    expect(copy).not.toHaveBeenCalledWith(expect.stringContaining("correlation-profiles-1"));
    await user.click(within(session).getByRole("button", { name: "Export visible" }));
    expect(download).toHaveBeenCalledOnce();
    const exportedDocument = download.mock.calls[0]?.[0];
    expect(exportedDocument?.content).toContain("correlation-trust-1");
    expect(exportedDocument?.content).not.toContain("correlation-profiles-1");
    expect(exportedDocument?.fileName).toMatch(/streamskope-activity-.+\.json/u);
    expect(exportedDocument?.mediaType).toBe("application/json");

    await user.click(within(session).getByRole("button", { name: "Collapse Activity" }));
    expect(onClose).toHaveBeenCalledOnce();
  });

  it("searches visible operational fields and keeps the empty condition explicit", async () => {
    const user = userEvent.setup();
    const view = render(
      <ActivityLogDrawer entries={[loadedProfiles, failedTrust]} onClose={() => undefined} open />,
    );
    await user.click(
      within(screen.getByRole("region", { name: "Activity dock" })).getByRole("button", {
        name: "Filter",
      }),
    );
    const activity = screen.getByRole("complementary", { name: "Activity log" });
    const raw = within(activity).getByRole("log", { name: "Raw activity log" });
    await user.type(
      within(activity).getByRole("searchbox", { name: "Search activity" }),
      "profiles",
    );
    expect(raw).toHaveTextContent('operation="Load profiles"');
    expect(raw).not.toHaveTextContent('operation="Fetch remote trust material"');

    view.rerender(<ActivityLogDrawer entries={[]} onClose={() => undefined} open />);
    expect(screen.getByText("No activity recorded.")).toBeVisible();
  });

  it("resizes the bounded dock with keyboard commands", async () => {
    const onHeightChange = vi.fn<(height: number) => void>();
    const user = userEvent.setup();
    render(
      <ActivityLogDrawer
        entries={[]}
        height={240}
        onClose={() => undefined}
        onHeightChange={onHeightChange}
        open
      />,
    );

    const separator = screen.getByRole("separator", { name: "Resize Activity dock" });
    const dock = screen.getByRole("region", { name: "Activity dock" });
    expect(getComputedStyle(dock).borderTopWidth).toBe("0px");
    expect(getComputedStyle(separator).height).toBe("9px");
    expect(within(separator).getByTestId("activity-resize-grip")).toBeVisible();
    expect(separator).toHaveAttribute("aria-valuemin", "160");
    expect(separator).toHaveAttribute("aria-valuemax", "420");
    expect(separator).toHaveAttribute("aria-valuenow", "240");

    await user.type(separator, "{ArrowUp}{ArrowDown}{Home}{End}");
    expect(onHeightChange.mock.calls.map(([height]) => height)).toEqual([256, 224, 160, 420]);
  });
});
