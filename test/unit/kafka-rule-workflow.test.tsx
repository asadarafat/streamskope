// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";

import { pasteText } from "../support/paste-text";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type KafkaRuleDefinition,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { StreamSkopeApp as StreamSkopeWorkbench } from "../../src/features/kafka/ui/StreamSkopeApp";
import { streamSkopeGeometry } from "../../src/platform/ui/studioTokens";

const highPriority: KafkaRuleDefinition = {
  cooldownMs: 5_000,
  description: "Detect critical orders.",
  enabled: true,
  expression: '$.priority == "high"',
  level: "warn",
  name: "High priority",
  topic: "orders",
};

const slowPayment: KafkaRuleDefinition = {
  cooldownMs: 0,
  description: "Payment latency threshold.",
  enabled: true,
  expression: "$.latency > 500",
  level: "error",
  name: "Slow payment",
  topic: "payments",
};

class RuleHost implements StreamSkopeHost {
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

function ruleSnapshot(
  rules: readonly KafkaRuleDefinition[],
  sequence: number,
  durability: "durable" | "session" = "session",
): HostEvent {
  return {
    event: "rules.changed",
    payload: {
      rules,
      store: { durability, state: "ready" },
    },
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

function latestRuleCommand(host: RuleHost, command: HostCommand["command"]): HostCommand {
  const found = [...host.commands].reverse().find((candidate) => candidate.command === command);
  if (found === undefined) {
    throw new Error(`Expected ${command}.`);
  }
  return found;
}

function publishRuleContext(
  host: RuleHost,
  rules: readonly KafkaRuleDefinition[],
  durability: "durable" | "session" = "session",
): void {
  host.emit({
    event: "connection.state",
    payload: {
      connectionName: "Rule test cluster",
      state: "connected",
    },
    sequence: 1,
    version: HOST_PROTOCOL_VERSION,
  });
  host.emit({
    event: "topics.changed",
    payload: {
      refreshedAt: "2026-07-28T12:00:00.000Z",
      state: "ready",
      topics: ["orders"],
    },
    sequence: 2,
    version: HOST_PROTOCOL_VERSION,
  });
  host.emit(ruleSnapshot(rules, 3, durability));
}

async function openRuleWorkspace(user: ReturnType<typeof userEvent.setup>): Promise<void> {
  await user.click(await screen.findByRole("button", { name: "orders" }));
  await user.click(
    within(screen.getByRole("tablist", { name: "Topic sections" })).getByRole("tab", {
      name: "Rules",
    }),
  );
}

afterEach(() => {
  cleanup();
});

describe("Kafka rule Material UI workflow", () => {
  it.each([false, true])(
    "labels lost provenance honestly when returning to Rules (late reply: %s)",
    async (late) => {
      const host = new RuleHost();
      const user = userEvent.setup();
      render(<StreamSkopeWorkbench host={host} />);
      act(() => publishRuleContext(host, [highPriority]));
      await openRuleWorkspace(user);
      await user.click(screen.getByRole("tab", { name: "Test" }));
      await user.click(screen.getByRole("button", { name: "Evaluate selected rule" }));
      const request = latestRuleCommand(host, "rules.evaluate");
      const reply = (): void =>
        host.emit({
          event: "rules.evaluation",
          payload: {
            kind: "evaluation",
            requestId: request.id,
            results: [{ name: highPriority.name, outcome: "not-matched" }],
          },
          sequence: 4,
          version: HOST_PROTOCOL_VERSION,
        });
      if (!late) act(reply);
      const sections = screen.getByRole("tablist", { name: "Topic sections" });
      await user.click(within(sections).getByRole("tab", { name: "Messages" }));
      if (late) act(reply);
      await user.click(within(sections).getByRole("tab", { name: "Rules" }));
      await user.click(screen.getByRole("tab", { name: "Test" }));
      expect(screen.getByRole("status", { name: "Rule result" })).toHaveTextContent(
        "Previous inputs unavailable",
      );
      expect(screen.getByRole("status", { name: "Rule result" })).not.toHaveTextContent(
        "Inputs changed",
      );
      expect(host.commands.filter((command) => command.command === "rules.evaluate")).toHaveLength(
        1,
      );
    },
  );
  it("presents one authoritative loading state while the catalog is pending", async () => {
    const host = new RuleHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    act(() => {
      host.emit({
        event: "connection.state",
        payload: {
          connectionName: "Rule test cluster",
          state: "connected",
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "topics.changed",
        payload: {
          refreshedAt: "2026-07-28T12:00:00.000Z",
          state: "ready",
          topics: ["orders"],
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      });
    });

    await openRuleWorkspace(user);
    const workspace = screen.getByRole("region", { name: "Rule workspace" });
    expect(screen.getByRole("status", { name: "Rule storage status" })).toHaveTextContent(
      "Loading rules",
    );
    expect(within(workspace).queryByText("Loading the rule catalog…")).not.toBeInTheDocument();
    expect(within(workspace).queryByText("Loading rules…")).not.toBeInTheDocument();
  });

  it("creates, validates and evaluates a rule from the honest empty state", async () => {
    const host = new RuleHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    act(() => {
      publishRuleContext(host, []);
    });

    await openRuleWorkspace(user);
    expect(screen.getByRole("status", { name: "Rule storage status" })).toHaveTextContent(
      "Session-only rules",
    );
    expect(screen.getByText("No rules configured.")).toBeVisible();
    const createButton = screen.getByRole("button", { name: "Create rule" });
    expect(createButton).toBeEnabled();

    await user.click(createButton);
    expect(screen.getByRole("heading", { name: "Create rule" })).toBeVisible();
    const controls = screen.getByLabelText("Rule controls");
    expect(controls).toHaveClass("MuiToolbar-dense");
    expect(getComputedStyle(controls).minHeight).toBe(
      `${String(streamSkopeGeometry.localToolbarHeight)}px`,
    );
    expect(screen.getByRole("textbox", { name: "Rule name" })).toHaveFocus();
    await user.type(screen.getByRole("textbox", { name: "Rule name" }), "Created rule");
    await pasteText(
      user,
      screen.getByRole("textbox", { name: "JSONPath expression" }),
      '$.status == "ready"',
    );
    expect(
      getComputedStyle(screen.getByRole("textbox", { name: "JSONPath expression" })).fontFamily,
    ).toContain("system-ui");
    await user.type(screen.getByRole("textbox", { name: "Topic filter" }), "orders");
    await user.click(screen.getByRole("combobox", { name: "Severity" }));
    await user.click(screen.getByRole("option", { name: "warn" }));
    await user.clear(screen.getByRole("spinbutton", { name: "Cooldown (milliseconds)" }));
    await user.type(screen.getByRole("spinbutton", { name: "Cooldown (milliseconds)" }), "1000");
    await pasteText(user, screen.getByRole("textbox", { name: "Description" }), "Ready orders");

    await user.click(screen.getByRole("button", { name: "Validate rule" }));
    const validation = latestRuleCommand(host, "rules.validate");
    expect(validation).toMatchObject({
      payload: {
        rule: {
          cooldownMs: 1_000,
          description: "Ready orders",
          enabled: true,
          expression: '$.status == "ready"',
          level: "warn",
          name: "Created rule",
          topic: "orders",
        },
      },
    });
    act(() => {
      host.emit({
        event: "rules.evaluation",
        payload: {
          kind: "validation",
          requestId: validation.id,
          results: [{ name: "Created rule", outcome: "valid" }],
        },
        sequence: 4,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    expect(await screen.findByRole("status", { name: "Rule result" })).toHaveTextContent(
      "Rule is valid",
    );
    const expression = screen.getByRole("textbox", { name: "JSONPath expression" });
    await user.clear(expression);
    expect(screen.getByRole("status", { name: "Rule result" })).toHaveTextContent("Inputs changed");
    await user.click(expression);
    await user.paste('$.status == "ready"');

    await user.click(screen.getByRole("tab", { name: "Test" }));
    expect(screen.getByRole("heading", { level: 3, name: "Offline sample test" })).toBeVisible();
    const sample = screen.getByRole("textbox", { name: "Sample JSON" });
    await user.clear(sample);
    await user.click(sample);
    await user.paste('{"status":"ready"}');
    await user.click(screen.getByRole("button", { name: "Evaluate selected rule" }));
    const evaluation = latestRuleCommand(host, "rules.evaluate");
    expect(evaluation).toMatchObject({
      payload: {
        rule: {
          cooldownMs: 1_000,
          description: "Ready orders",
          enabled: true,
          expression: '$.status == "ready"',
          level: "warn",
          name: "Created rule",
          topic: "orders",
        },
        sample: '{"status":"ready"}',
        scope: "single",
        topic: "orders",
      },
    });
    await user.clear(sample);
    act(() => {
      host.emit({
        event: "rules.evaluation",
        payload: {
          kind: "evaluation",
          requestId: evaluation.id,
          results: [{ name: "Created rule", outcome: "matched" }],
        },
        sequence: 5,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    expect(await screen.findByRole("status", { name: "Rule result" })).toHaveTextContent(
      "Inputs changed",
    );
    await user.click(sample);
    await user.paste('{"status":"ready"}');
    expect(screen.getByRole("status", { name: "Rule result" })).toHaveTextContent("Matched");
    const commandCount = host.commands.length;
    await user.clear(screen.getByRole("textbox", { name: "Sample JSON" }));
    expect(screen.getByRole("status", { name: "Rule result" })).toHaveTextContent("Inputs changed");
    expect(screen.getByRole("status", { name: "Rule result" })).not.toHaveTextContent("Matched");
    expect(host.commands).toHaveLength(commandCount);

    await user.click(screen.getByRole("tab", { name: "Details" }));
    await user.click(screen.getByRole("button", { name: "Save rule" }));
    const creation = latestRuleCommand(host, "rules.create");
    act(() => {
      host.emit(
        ruleSnapshot(
          [
            {
              cooldownMs: 1_000,
              description: "Ready orders",
              enabled: true,
              expression: '$.status == "ready"',
              level: "warn",
              name: "Created rule",
              topic: "orders",
            },
          ],
          6,
        ),
      );
    });

    await waitFor(() => {
      expect(screen.queryByRole("heading", { name: "Create rule" })).not.toBeInTheDocument();
    });
    expect(screen.getByRole("heading", { name: "Created rule" })).toBeVisible();
    expect(createButton).toHaveFocus();
    expect(creation.payload).toMatchObject({ rule: { name: "Created rule" } });
  });

  it("searches, renames, disables and deletes the exact selected rule", async () => {
    const host = new RuleHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    act(() => {
      publishRuleContext(host, [highPriority, slowPayment], "durable");
    });
    await openRuleWorkspace(user);

    expect(screen.getByRole("status", { name: "Rule storage status" })).toHaveTextContent(
      "Durable rules",
    );
    expect(screen.getByText("2 rules")).toBeVisible();
    const search = screen.getByRole("searchbox", { name: "Search rules" });
    await user.type(search, "payments");
    expect(screen.getByRole("button", { name: /Slow payment/ })).toBeVisible();
    expect(screen.queryByRole("button", { name: /High priority/ })).not.toBeInTheDocument();
    await user.clear(search);
    await user.click(screen.getByRole("button", { name: /Slow payment/ }));
    expect(screen.getByRole("heading", { name: "Slow payment" })).toBeVisible();

    await user.click(screen.getByRole("button", { name: "Edit rule" }));
    const name = screen.getByRole("textbox", { name: "Rule name" });
    await user.clear(name);
    await user.type(name, "Payment latency");
    await user.click(screen.getByRole("button", { name: "Save rule" }));
    const update = latestRuleCommand(host, "rules.update");
    expect(update).toMatchObject({
      payload: {
        originalName: "Slow payment",
        rule: { name: "Payment latency" },
      },
    });
    act(() => {
      host.emit(ruleSnapshot([highPriority, { ...slowPayment, name: "Payment latency" }], 4));
    });
    expect(await screen.findByRole("heading", { name: "Payment latency" })).toBeVisible();

    await user.click(screen.getByRole("button", { name: "Disable rule" }));
    const disable = latestRuleCommand(host, "rules.update");
    expect(disable).toMatchObject({
      payload: {
        originalName: "Payment latency",
        rule: { enabled: false, name: "Payment latency" },
      },
    });
    act(() => {
      host.emit(
        ruleSnapshot(
          [highPriority, { ...slowPayment, enabled: false, name: "Payment latency" }],
          5,
        ),
      );
    });
    expect(await screen.findByRole("status", { name: "Rule state" })).toHaveTextContent("Disabled");

    const deleteButton = screen.getByRole("button", { name: "Delete rule" });
    await user.click(deleteButton);
    const dialog = screen.getByRole("dialog", { name: "Delete Payment latency?" });
    expect(dialog).toHaveTextContent("Delete rule Payment latency? This action cannot be undone.");
    await user.keyboard("{Escape}");
    expect(dialog).not.toBeInTheDocument();
    expect(deleteButton).toHaveFocus();

    await user.click(deleteButton);
    await user.click(
      within(screen.getByRole("dialog", { name: "Delete Payment latency?" })).getByRole("button", {
        name: "Delete rule",
      }),
    );
    const deletion = latestRuleCommand(host, "rules.delete");
    expect(deletion.payload).toEqual({ name: "Payment latency" });
    act(() => {
      host.emit(ruleSnapshot([highPriority], 6));
    });
    expect(await screen.findByRole("heading", { name: "High priority" })).toBeVisible();
    expect(screen.queryByText("Payment latency")).not.toBeInTheDocument();
  });

  it("keeps stale data explicit and blocks mutation when rule storage is unavailable", async () => {
    const host = new RuleHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    act(() => {
      publishRuleContext(host, [highPriority]);
      host.emit({
        event: "backend.availability",
        payload: { recovery: "Restart StreamSkope.", state: "unavailable" },
        sequence: 4,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    await openRuleWorkspace(user);

    expect(screen.getByRole("alert")).toHaveTextContent("Rule data is stale");
    expect(screen.getByRole("button", { name: "Create rule" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Edit rule" })).toBeDisabled();
    expect(screen.queryByRole("complementary", { name: "Activity log" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Expand Activity" })).toBeVisible();

    act(() => {
      host.emit({
        event: "rules.changed",
        payload: {
          rules: [],
          store: {
            durability: "session",
            recovery: "Restore the rule document and restart StreamSkope.",
            state: "unavailable",
          },
        },
        sequence: 5,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    expect(screen.getByRole("alert")).toHaveTextContent("Rule storage unavailable");
    expect(screen.getByRole("alert")).toHaveTextContent(
      "Restore the rule document and restart StreamSkope.",
    );
    expect(screen.queryByText("No rules configured.")).not.toBeInTheDocument();
  });

  it("cancels editing without a command and restores focus predictably", async () => {
    const host = new RuleHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    act(() => {
      publishRuleContext(host, [highPriority]);
    });
    await openRuleWorkspace(user);
    const edit = screen.getByRole("button", { name: "Edit rule" });
    await user.click(edit);
    const commandCount = host.commands.length;
    await user.clear(screen.getByRole("textbox", { name: "Rule name" }));
    await user.type(screen.getByRole("textbox", { name: "Rule name" }), "Discard me");

    await user.click(screen.getByRole("button", { name: "Cancel editing" }));

    expect(host.commands).toHaveLength(commandCount);
    expect(screen.getByRole("heading", { name: "High priority" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Edit rule" })).toHaveFocus();
    expect(screen.getByRole("region", { name: "Rule workspace" })).toHaveTextContent("Saved rules");
    const resources = screen.getByRole("navigation", { name: "StreamSkope resources" });
    expect(within(resources).getByRole("button", { name: "Connection Profiles" })).toBeVisible();
    expect(within(resources).getByRole("button", { name: "Topics" })).toHaveAttribute(
      "aria-current",
      "page",
    );
  });
});
