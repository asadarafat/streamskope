// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEventListener,
  type KafkaTopicConfigurationEntry,
  type KafkaTopicConfigurationHistorySnapshot,
  type KafkaTopicConfigurationSnapshot,
  type StreamSkopeHost,
} from "../../src/kafka/contracts";
import { TopicConfigurationWorkspace } from "../../src/kafka/ui/TopicConfigurationWorkspace";
import { streamSkopeGeometry } from "../../src/ui/studioTokens";

class RecordingHost implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];

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

  subscribe(_listener: HostEventListener): () => void {
    return (): void => undefined;
  }
}

function entry(
  name: string,
  value: string,
  overrides: Partial<KafkaTopicConfigurationEntry> = {},
): KafkaTopicConfigurationEntry {
  return {
    documentation: `${name} documentation`,
    isDefault: false,
    isSensitive: false,
    name,
    readOnly: false,
    source: "topic",
    synonyms: [],
    type: "string",
    value,
    ...overrides,
  };
}

const entries: readonly KafkaTopicConfigurationEntry[] = [
  entry("cleanup.policy", "delete", { readOnly: true }),
  entry("retention.ms", "86400000", { type: "long" }),
  entry("segment.ms", "1800000", { type: "long" }),
  entry("delete.retention.ms", "86400000", { type: "long" }),
  entry("min.cleanable.dirty.ratio", "0.5", { type: "double" }),
  entry("ssl.keystore.password", "", {
    isSensitive: true,
    type: "password",
    value: null,
  }),
];

function snapshot(
  state: KafkaTopicConfigurationSnapshot["state"] = "ready",
): KafkaTopicConfigurationSnapshot {
  return state === "ready"
    ? {
        connectionName: "Local aio",
        entries,
        refreshedAt: "2026-07-25T12:00:00.000Z",
        state,
        topic: "orders.events",
      }
    : state === "stale"
      ? {
          connectionName: "Local aio",
          entries,
          error: {
            activeStateChanged: false,
            code: "BROKER_UNREACHABLE",
            correlationId: "correlation-refresh",
            recovery: "Reconnect and refresh the selected topic.",
            retryable: true,
            stage: "kafka",
            summary: "Kafka applied the change, but refresh failed.",
          },
          refreshedAt: "2026-07-25T12:00:00.000Z",
          state,
          topic: "orders.events",
        }
      : {
          connectionName: "Local aio",
          entries: [],
          error: {
            activeStateChanged: false,
            code: state === "denied" ? "AUTHORIZATION_DENIED" : "TOPIC_NOT_FOUND",
            correlationId: "correlation-failure",
            recovery: "Refresh topics or request access.",
            retryable: false,
            stage: state === "denied" ? "authorization" : "kafka",
            summary: state === "denied" ? "Configuration access denied." : "Topic not found.",
          },
          refreshedAt: null,
          state,
          topic: "orders.events",
        };
}

const history: KafkaTopicConfigurationHistorySnapshot = {
  connectionName: "Local aio",
  entries: [
    {
      action: "apply",
      at: "2026-07-25T12:00:00.000Z",
      changes: [
        {
          from: "86400000",
          isSensitive: false,
          name: "retention.ms",
          to: "604800000",
          wasDefault: false,
        },
      ],
      connectionName: "Local aio",
      connectionTarget: "localhost:19093",
      id: "history-1",
      success: true,
      topic: "orders.events",
    },
  ],
  store: { durability: "durable", state: "ready" },
  topic: "orders.events",
};

afterEach(() => {
  cleanup();
});

describe("Kafka topic-configuration Material UI workflow", () => {
  it("compares inherited current values and masks sensitive proposals in both review surfaces", async () => {
    const host = new RecordingHost();
    const user = userEvent.setup();
    render(
      <TopicConfigurationWorkspace
        connectionName="Local aio"
        history={null}
        host={host}
        selectedTopic="orders.events"
        snapshot={{
          ...snapshot(),
          entries: [
            entry("retention.ms", "86400000", {
              isDefault: true,
              source: "dynamic-default-broker",
            }),
            entry("ssl.keystore.password", "", {
              isSensitive: true,
              type: "password",
              value: null,
            }),
          ],
        }}
      />,
    );
    const grid = screen.getByRole("grid", { name: "Topic configuration entries" });
    await user.click(within(grid).getByRole("gridcell", { name: "retention.ms" }));
    await user.click(screen.getByRole("button", { name: "Queue change" }));
    expect(screen.getByRole("list", { name: "Pending configuration changes" })).toHaveTextContent(
      "inherited/default",
    );
    expect(screen.getByRole("list", { name: "Pending configuration changes" })).toHaveTextContent(
      "Unchanged",
    );
    await user.click(within(grid).getByRole("gridcell", { name: "ssl.keystore.password" }));
    await user.type(screen.getByLabelText("Proposed value"), "synthetic-private-proposal");
    await user.click(screen.getByRole("button", { name: "Queue change" }));
    const pending = screen.getByRole("list", { name: "Pending configuration changes" });
    expect(pending).toHaveTextContent("Current: Sensitive value");
    expect(pending).not.toHaveTextContent("synthetic-private-proposal");
    await user.click(screen.getByRole("button", { name: "Apply changes" }));
    const dialog = screen.getByRole("dialog", { name: "Apply topic configuration?" });
    expect(dialog).toHaveTextContent(
      "Current: 86400000 (dynamic-default-broker, inherited/default) → Proposed: 86400000 · Unchanged",
    );
    expect(dialog).toHaveTextContent("Proposed: Sensitive value");
    expect(dialog).not.toHaveTextContent("synthetic-private-proposal");
    expect(host.commands.some((command) => command.command === "topicConfiguration.apply")).toBe(
      false,
    );
  });
  it("states the missing-topic condition without presenting inapplicable mutation actions", () => {
    render(
      <TopicConfigurationWorkspace
        connectionName="Local aio"
        history={null}
        host={new RecordingHost()}
        selectedTopic={null}
        snapshot={{
          connectionName: null,
          entries: [],
          refreshedAt: null,
          state: "unavailable",
          topic: null,
        }}
      />,
    );

    expect(screen.getByRole("main", { name: "Topic configuration workspace" })).toBeVisible();
    const controls = screen.getByLabelText("Configuration controls");
    expect(controls).toBeVisible();
    expect(controls).toHaveClass("MuiToolbar-dense");
    expect(getComputedStyle(controls).minHeight).toBe(
      `${String(streamSkopeGeometry.localToolbarHeight)}px`,
    );
    expect(
      screen.queryByRole("heading", { level: 1, name: "Configuration" }),
    ).not.toBeInTheDocument();
    expect(screen.getByText("Select a topic")).toBeVisible();
    expect(screen.getByRole("button", { name: "Refresh configuration" })).toBeDisabled();
    expect(screen.queryByRole("button", { name: "Dry-run changes" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Apply changes" })).not.toBeInTheDocument();
  });

  it("presents broker documentation semantics without creating active HTML", async () => {
    const user = userEvent.setup();
    const documentation = [
      "The compression level to use for <code>gzip</code>.",
      "<br>",
      "<p><i>Important</i></p>",
      '<p>Read the <a href="#topicconfigs">topic configuration section</a>.</p>',
      '<img src="https://example.invalid/track.png">',
      '<a href="https://example.invalid">external reference</a>',
      "<script>globalThis.configurationDocumentationExecuted = true</script>",
    ].join("");
    render(
      <TopicConfigurationWorkspace
        connectionName="Local aio"
        history={null}
        host={new RecordingHost()}
        selectedTopic="orders.events"
        snapshot={{
          connectionName: "Local aio",
          entries: [entry("compression.gzip.level", "9", { documentation })],
          refreshedAt: "2026-07-25T12:00:00.000Z",
          state: "ready",
          topic: "orders.events",
        }}
      />,
    );

    const grid = screen.getByRole("grid", { name: "Topic configuration entries" });
    await user.click(within(grid).getByRole("gridcell", { name: "compression.gzip.level" }));

    const selectedEntry = screen.getByRole("heading", { name: "Selected entry" }).parentElement;
    expect(selectedEntry).not.toBeNull();
    expect(within(selectedEntry!).getByText("gzip").tagName).toBe("CODE");
    const emphasis = within(selectedEntry!).getByText("Important");
    const reference = within(selectedEntry!).getByText("topic configuration section");
    expect(emphasis.tagName).toBe("EM");
    expect(reference.tagName).toBe("SPAN");
    expect(emphasis.closest("p")).not.toBeNull();
    expect(reference.closest("p")).not.toBeNull();
    expect(emphasis.closest("p")).not.toBe(reference.closest("p"));
    expect(selectedEntry!.querySelector("br")).not.toBeNull();
    expect(selectedEntry!.querySelector("a")).toBeNull();
    expect(selectedEntry!.querySelector("img")).toBeNull();
    expect(selectedEntry!.querySelector("script")).toBeNull();
    expect(selectedEntry).not.toHaveTextContent("<code>gzip</code>");
    expect(selectedEntry).toHaveTextContent('<img src="https://example.invalid/track.png">');
    expect(selectedEntry).toHaveTextContent(
      '<a href="https://example.invalid">external reference</a>',
    );
    expect(selectedEntry).toHaveTextContent(
      "<script>globalThis.configurationDocumentationExecuted = true</script>",
    );
    expect(
      (
        globalThis as typeof globalThis & {
          configurationDocumentationExecuted?: boolean;
        }
      ).configurationDocumentationExecuted,
    ).toBeUndefined();
  });

  it("queues one authoritative value, dry-runs, and applies only after exact confirmation", async () => {
    const host = new RecordingHost();
    const user = userEvent.setup();
    render(
      <TopicConfigurationWorkspace
        connectionName="Local aio"
        history={null}
        host={host}
        selectedTopic="orders.events"
        snapshot={snapshot()}
      />,
    );

    const grid = screen.getByRole("grid", { name: "Topic configuration entries" });
    await user.click(within(grid).getByRole("gridcell", { name: "retention.ms" }));
    const proposed = screen.getByRole("textbox", { name: "Proposed value" });
    expect(proposed.tagName).toBe("INPUT");
    expect(screen.queryByRole("button", { name: "Dry-run changes" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Apply changes" })).not.toBeInTheDocument();
    expect(screen.queryByRole("combobox", { name: "Guarded preset" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Advanced presets" })).toHaveAttribute(
      "aria-expanded",
      "false",
    );
    await user.clear(proposed);
    await user.type(proposed, "604800000");
    const queueChange = screen.getByRole("button", { name: "Queue change" });
    expect(queueChange).toHaveClass("MuiButton-contained");
    expect(screen.queryByRole("heading", { name: "No pending changes" })).not.toBeInTheDocument();
    await user.click(queueChange);
    expect(screen.getByText("1 pending change")).toBeVisible();

    await user.clear(proposed);
    await user.type(proposed, "604800001");
    await user.click(screen.getByRole("button", { name: "Queue change" }));
    expect(screen.getByText("1 pending change")).toBeVisible();

    await user.click(screen.getByRole("button", { name: "Dry-run changes" }));
    await waitFor(() => {
      expect(host.commands.at(-1)).toMatchObject({
        command: "topicConfiguration.validate",
        payload: {
          changes: [
            {
              isSensitive: false,
              name: "retention.ms",
              value: "604800001",
            },
          ],
          topic: "orders.events",
        },
      });
    });
    expect(screen.getByText("1 pending change")).toBeVisible();

    await user.click(screen.getByRole("button", { name: "Apply changes" }));
    const dialog = screen.getByRole("dialog", { name: "Apply topic configuration?" });
    expect(dialog).toHaveTextContent("orders.events");
    expect(dialog).toHaveTextContent("Local aio");
    expect(dialog).toHaveTextContent("retention.ms");
    expect(dialog).toHaveTextContent("Current: 86400000");
    expect(dialog).toHaveTextContent("Proposed: 604800001");
    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(
      host.commands.filter((command) => command.command === "topicConfiguration.apply"),
    ).toEqual([]);
    await waitFor(() => {
      expect(
        screen.queryByRole("dialog", { name: "Apply topic configuration?" }),
      ).not.toBeInTheDocument();
    });

    await user.click(screen.getByRole("button", { name: "Apply changes" }));
    await user.click(
      within(screen.getByRole("dialog")).getByRole("button", {
        name: "Apply named changes",
      }),
    );
    await waitFor(() => {
      expect(host.commands.at(-1)).toMatchObject({
        command: "topicConfiguration.apply",
        payload: {
          changes: [{ name: "retention.ms", value: "604800001" }],
          topic: "orders.events",
        },
      });
    });
    expect(screen.getByText("No pending changes")).toBeVisible();
  });

  it("applies the canonical preset, skips read-only entries, and keeps attribution", async () => {
    const host = new RecordingHost();
    const user = userEvent.setup();
    render(
      <TopicConfigurationWorkspace
        connectionName="Local aio"
        history={null}
        host={host}
        selectedTopic="orders.events"
        snapshot={snapshot()}
      />,
    );

    await user.click(screen.getByRole("button", { name: "Advanced presets" }));
    await user.click(screen.getByRole("combobox", { name: "Guarded preset" }));
    await user.click(screen.getByRole("option", { name: "Delete after 7 days" }));

    expect(screen.getByText("2 pending changes")).toBeVisible();
    expect(screen.getByRole("alert")).toHaveTextContent(
      "Skipped read-only configuration: cleanup.policy",
    );
    await user.click(screen.getByRole("button", { name: "Dry-run changes" }));
    await waitFor(() => {
      expect(host.commands.at(-1)).toMatchObject({
        command: "topicConfiguration.validate",
        payload: {
          changes: [
            { name: "retention.ms", value: "604800000" },
            { name: "segment.ms", value: "3600000" },
          ],
          presetId: "retention-7d",
        },
      });
    });
  });

  it("reports stale and denied states honestly and loads history only on request", async () => {
    const host = new RecordingHost();
    const user = userEvent.setup();
    const rendered = render(
      <TopicConfigurationWorkspace
        connectionName="Local aio"
        history={null}
        host={host}
        selectedTopic="orders.events"
        snapshot={snapshot("stale")}
      />,
    );

    expect(screen.getByRole("alert")).toHaveTextContent(
      "Kafka applied the change, but refresh failed.",
    );
    await user.click(screen.getByRole("button", { name: "Configuration history" }));
    await waitFor(() => {
      expect(host.commands.at(-1)).toMatchObject({
        command: "topicConfiguration.history",
        payload: { topic: "orders.events" },
      });
    });
    expect(
      host.commands.filter((command) => command.command === "topicConfiguration.history"),
    ).toHaveLength(1);

    rendered.rerender(
      <TopicConfigurationWorkspace
        connectionName="Local aio"
        history={history}
        host={host}
        selectedTopic="orders.events"
        snapshot={snapshot("stale")}
      />,
    );
    const historyDialog = screen.getByRole("dialog", { name: "Configuration history" });
    expect(historyDialog).toHaveTextContent("Durable history");
    expect(historyDialog).toHaveTextContent("retention.ms");
    expect(historyDialog).toHaveTextContent("604800000");

    await user.click(within(historyDialog).getByRole("button", { name: "Close" }));
    await waitFor(() => {
      expect(
        screen.queryByRole("dialog", { name: "Configuration history" }),
      ).not.toBeInTheDocument();
    });
    rendered.rerender(
      <TopicConfigurationWorkspace
        connectionName="Local aio"
        history={null}
        host={host}
        selectedTopic="orders.events"
        snapshot={snapshot("denied")}
      />,
    );
    expect(screen.getByRole("alert")).toHaveTextContent("Configuration access denied.");
    expect(screen.getByRole("alert")).toHaveTextContent("Refresh topics or request access.");
  });

  it("clears pending edits when topic or connection scope changes", async () => {
    const host = new RecordingHost();
    const user = userEvent.setup();
    const rendered = render(
      <TopicConfigurationWorkspace
        connectionName="Local aio"
        history={null}
        host={host}
        selectedTopic="orders.events"
        snapshot={snapshot()}
      />,
    );
    const grid = screen.getByRole("grid", { name: "Topic configuration entries" });
    expect(screen.getByRole("status", { name: "Configuration status" })).toHaveTextContent("Ready");
    await user.click(within(grid).getByRole("gridcell", { name: "retention.ms" }));
    await user.click(screen.getByRole("button", { name: "Queue change" }));
    expect(screen.getByText("1 pending change")).toBeVisible();

    rendered.rerender(
      <TopicConfigurationWorkspace
        connectionName="Replacement"
        history={null}
        host={host}
        selectedTopic="audit.events"
        snapshot={{
          connectionName: "Replacement",
          entries: [entry("retention.ms", "1000")],
          refreshedAt: "2026-07-25T13:00:00.000Z",
          state: "ready",
          topic: "audit.events",
        }}
      />,
    );

    expect(screen.getByText("No pending changes")).toBeVisible();
    expect(screen.queryByText("604800000")).not.toBeInTheDocument();
  });
});
