// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type KafkaExploredMessage,
  type KafkaLiveRuleCapability,
  type KafkaLiveRuleEvaluation,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { StreamSkopeApp as StreamSkopeWorkbench } from "../../src/features/kafka/ui/StreamSkopeApp";

class FakeHost implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  private readonly listeners = new Set<HostEventListener>();

  emit(event: HostEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    return Promise.resolve({
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: `correlation-${command.id}` },
      version: HOST_PROTOCOL_VERSION,
    });
  }

  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("External URL action was not expected."));
  }

  subscribe(listener: HostEventListener): () => void {
    this.listeners.add(listener);
    return (): void => {
      this.listeners.delete(listener);
    };
  }
}

const zeroEvaluation: KafkaLiveRuleEvaluation = {
  activeMatchCount: 0,
  activeMatches: [],
  durationMicros: 18,
  errorCount: 0,
  errors: [],
  evaluatedRules: 2,
  omittedEvidence: 0,
  omittedRules: 0,
  state: "evaluated",
  suppressedMatchCount: 0,
  suppressedMatches: [],
};

function message(
  id: string,
  key: string,
  ruleEvaluation: KafkaLiveRuleEvaluation,
): KafkaExploredMessage {
  return {
    headers: { "content-type": "application/json" },
    id,
    key,
    offset: id,
    originalByteSize: 28,
    partition: 0,
    payload: `{"record":"${key}"}`,
    preview: `{"record":"${key}"}`,
    ruleEvaluation,
    timestamp: "2026-07-25T22:00:00.000Z",
    topic: "orders",
    truncated: false,
  };
}

const active = message("1", "active-key", {
  ...zeroEvaluation,
  activeMatchCount: 2,
  activeMatches: [
    { level: "warn", name: "Slow order" },
    { level: "error", name: "<script>alert(1)</script>" },
  ],
  highestActiveSeverity: "error",
});

const suppressed = message("2", "suppressed-key", {
  ...zeroEvaluation,
  activeMatchCount: 0,
  activeMatches: [],
  evaluatedRules: 1,
  suppressedMatchCount: 1,
  suppressedMatches: [{ level: "warn", name: "Cooldown rule" }],
});

const partial = message("3", "partial-key", {
  ...zeroEvaluation,
  activeMatchCount: 0,
  activeMatches: [],
  errorCount: 1,
  errors: [{ diagnostic: "Rule evaluation failed.", name: "Broken rule" }],
  evaluatedRules: 1,
  state: "partial",
});

const unavailable = message("4", "unavailable-key", {
  activeMatchCount: 0,
  activeMatches: [],
  durationMicros: 0,
  errorCount: 0,
  errors: [],
  evaluatedRules: 0,
  omittedEvidence: 0,
  omittedRules: 0,
  reason: "payload-limit-exceeded",
  state: "unavailable",
  suppressedMatchCount: 0,
  suppressedMatches: [],
});

const plain = message("5", "plain-key", zeroEvaluation);

afterEach(() => {
  cleanup();
});

async function renderMessages(
  capability: KafkaLiveRuleCapability = {
    applicableRules: 52,
    omittedRules: 2,
    state: "partial",
  },
): Promise<{
  readonly grid: HTMLElement;
  readonly host: FakeHost;
  readonly user: ReturnType<typeof userEvent.setup>;
}> {
  const host = new FakeHost();
  const user = userEvent.setup();
  render(<StreamSkopeWorkbench host={host} />);
  act(() => {
    host.emit({
      event: "connection.state",
      payload: { connectionName: "Local validation", state: "connected" },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });
    host.emit({
      event: "topics.changed",
      payload: {
        refreshedAt: "2026-07-25T22:00:00.000Z",
        state: "ready",
        topics: ["orders"],
      },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    });
  });
  await user.click(await screen.findByRole("button", { name: "orders" }));
  act(() => {
    host.emit({
      event: "consumption.state",
      payload: {
        droppedMessages: 0,
        receivedMessages: 5,
        request: {
          maxMessages: 1_000,
          mode: "tail",
          topic: "orders",
        },
        ruleEvaluation: capability,
        state: "streaming",
      },
      sequence: 3,
      version: HOST_PROTOCOL_VERSION,
    });
    host.emit({
      event: "messages.batch",
      payload: {
        droppedMessages: 0,
        messages: [active, suppressed, partial, unavailable, plain],
        topic: "orders",
      },
      sequence: 4,
      version: HOST_PROTOCOL_VERSION,
    });
  });
  const grid = await screen.findByRole("grid", { name: "Kafka messages" }, { timeout: 15_000 });
  return { grid, host, user };
}

describe("Kafka live rule Material UI workflow", () => {
  it("shows textual row states and filters only authoritative active matches", async () => {
    const { grid, user } = await renderMessages();

    expect(within(grid).getByText("2 matches · Error")).toBeVisible();
    expect(within(grid).getByText("1 suppressed")).toBeVisible();
    expect(within(grid).getByText("Partial")).toBeVisible();
    expect(within(grid).getByText("Unavailable")).toBeVisible();
    expect(within(grid).getByText("—")).toBeVisible();
    expect(screen.getByRole("alert")).toHaveTextContent("2 applicable live rules were omitted");

    await user.click(screen.getByRole("button", { name: "Show message filters" }));
    const activeOnly = screen.getByRole("checkbox", { name: "Rule matches only" });
    await user.click(activeOnly);

    expect(within(grid).getByText("active-key")).toBeVisible();
    for (const hidden of ["suppressed-key", "partial-key", "unavailable-key", "plain-key"]) {
      expect(within(grid).queryByText(hidden)).not.toBeInTheDocument();
    }

    await user.click(activeOnly);
    expect(within(grid).getByText("suppressed-key")).toBeVisible();
  });

  it("hides and restores a retained selection while preserving a visible active selection", async () => {
    const { grid, user } = await renderMessages();
    await user.click(screen.getByRole("button", { name: "Show message filters" }));
    const activeOnly = screen.getByRole("checkbox", { name: "Rule matches only" });

    await user.click(within(grid).getByText("suppressed-key"));
    expect(await screen.findByRole("complementary", { name: "Message inspector" })).toBeVisible();
    await user.click(activeOnly);
    await waitFor(() => {
      expect(
        screen.queryByRole("complementary", { name: "Message inspector" }),
      ).not.toBeInTheDocument();
    });
    expect(screen.getByText("The selected message is hidden by the rule filter.")).toBeVisible();

    await user.click(activeOnly);
    let inspector = await screen.findByRole("complementary", { name: "Message inspector" });
    await user.click(within(inspector).getByRole("tab", { name: "Key" }));
    expect(inspector).toHaveTextContent("suppressed-key");
    await user.click(within(grid).getByText("active-key"));
    await user.click(activeOnly);
    inspector = await screen.findByRole("complementary", { name: "Message inspector" });
    await user.click(within(inspector).getByRole("tab", { name: "Key" }));
    expect(inspector).toHaveTextContent("active-key");
    expect(within(grid).getByText("active-key")).toBeVisible();
  });

  it("inspects active, suppressed, error, unavailable, and hostile evidence as inert text", async () => {
    const { grid, user } = await renderMessages({
      applicableRules: 4,
      omittedRules: 0,
      state: "ready",
    });

    await user.click(within(grid).getByText("active-key"));
    let inspector = await screen.findByRole("complementary", { name: "Message inspector" });
    await user.click(within(inspector).getByRole("tab", { name: "Value" }));
    expect(inspector).toHaveTextContent('"record": "active-key"');
    await user.click(within(inspector).getByRole("tab", { name: /^Rules$/u }));
    expect(inspector).toHaveTextContent("Rule evaluation");
    expect(inspector).toHaveTextContent("2 active matches");
    expect(inspector).toHaveTextContent("Slow order");
    expect(inspector).toHaveTextContent("<script>alert(1)</script>");
    expect(inspector.querySelector("script")).toBeNull();

    await user.click(within(inspector).getByRole("button", { name: "Close inspector" }));
    await user.click(within(grid).getByText("partial-key"));
    inspector = await screen.findByRole("complementary", { name: "Message inspector" });
    await user.click(within(inspector).getByRole("tab", { name: "Rules" }));
    expect(inspector).toHaveTextContent("Partial");
    expect(inspector).toHaveTextContent("Broken rule");
    expect(inspector).toHaveTextContent("Rule evaluation failed.");

    await user.click(within(inspector).getByRole("button", { name: "Close inspector" }));
    await user.click(within(grid).getByText("unavailable-key"));
    inspector = await screen.findByRole("complementary", { name: "Message inspector" });
    await user.click(within(inspector).getByRole("tab", { name: "Rules" }));
    expect(inspector).toHaveTextContent("Live evaluation limit exceeded");
    expect(inspector).toHaveTextContent("The complete payload remains available");
  });

  it("filters, selects, inspects raw data, and clears the filter with the keyboard", async () => {
    const { grid, user } = await renderMessages({
      applicableRules: 4,
      omittedRules: 0,
      state: "ready",
    });
    const filterDisclosure = screen.getByRole("button", { name: "Show message filters" });
    filterDisclosure.focus();
    await user.keyboard("{Enter}");
    expect(screen.getByRole("button", { name: "Hide message filters" })).toHaveFocus();
    const activeOnly = screen.getByRole("checkbox", { name: "Rule matches only" });

    activeOnly.focus();
    expect(activeOnly).toHaveFocus();
    await user.keyboard(" ");
    expect(activeOnly).toBeChecked();

    await user.tab();
    expect(document.activeElement).toHaveAttribute("role", "columnheader");
    await user.keyboard("{ArrowDown}");
    await user.keyboard("{ArrowRight}");
    await user.keyboard("{ArrowRight}");
    await user.keyboard("{ArrowRight}");
    await user.keyboard("{ArrowRight}");
    await user.keyboard("{ArrowRight}");
    expect(document.activeElement).toHaveAttribute("role", "gridcell");
    expect(document.activeElement).toHaveTextContent("2 matches · Error");
    await user.keyboard("{Shift>} {/Shift}");

    const inspector = await screen.findByRole("complementary", { name: "Message inspector" });
    const valueTab = within(inspector).getByRole("tab", { name: "Value" });
    valueTab.focus();
    await user.keyboard("{Enter}");
    const rawTab = within(inspector).getByRole("tab", { name: "Raw" });
    rawTab.focus();
    await user.keyboard("{Enter}");
    expect(inspector).toHaveTextContent('{"record":"active-key"}');

    activeOnly.focus();
    await user.keyboard(" ");
    expect(activeOnly).not.toBeChecked();
    expect(within(grid).getByText("suppressed-key")).toBeVisible();
  });
});
