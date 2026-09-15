// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type KafkaLiveRuleCapability,
  type KafkaOperationalPreferenceSnapshot,
  type StreamSkopeHost,
} from "../../src/kafka/contracts";
import { StreamSkopeApp as StreamSkopeWorkbench } from "../../src/app/StreamSkopeApp";

type CommandHandler = (
  command: HostCommand,
) => HostCommandResponse | Promise<HostCommandResponse> | undefined;

class PreferenceHost implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  commandHandler: CommandHandler | undefined;
  private readonly listeners = new Set<HostEventListener>();

  emit(event: HostEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    const handled = this.commandHandler?.(command);
    return Promise.resolve(
      handled ?? {
        command: command.command,
        id: command.id,
        ok: true,
        result: { correlationId: `correlation-${command.id}` },
        version: HOST_PROTOCOL_VERSION,
      },
    );
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

const readyRuleCapability: KafkaLiveRuleCapability = {
  applicableRules: 0,
  omittedRules: 0,
  state: "ready",
};

const confirmedPreferences: KafkaOperationalPreferenceSnapshot = {
  preferences: {
    fetch: { maxMessages: 100, mode: "newest" },
    latency: {
      acknowledgements: 1,
      messageCount: 50,
      runbookUrl: "https://runbooks.example.test/kafka/latency",
      timeoutMs: 30_000,
    },
    rules: {
      logLevel: "warn",
      loggingEnabled: true,
      notificationsEnabled: false,
    },
    stream: {
      batchSize: 50,
      historySamples: 100,
      intervalMs: 50,
      queueDepth: 500,
    },
  },
  store: { durability: "durable", state: "ready" },
};

function emitPreferences(
  host: PreferenceHost,
  payload: KafkaOperationalPreferenceSnapshot,
  sequence: number,
): void {
  host.emit({
    event: "preferences.changed",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  });
}

function preferenceResponse(
  command: Extract<
    HostCommand,
    {
      readonly command: "preferences.get" | "preferences.reset" | "preferences.update";
    }
  >,
  snapshot: KafkaOperationalPreferenceSnapshot,
): HostCommandResponse {
  return {
    command: command.command,
    id: command.id,
    ok: true,
    result: {
      correlationId: `correlation-${command.id}`,
      snapshot,
    },
    version: HOST_PROTOCOL_VERSION,
  };
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("Kafka operational-preference Material UI workflow", () => {
  it("labels the pending factory fallback and accepts only newer confirmed session evidence", async () => {
    const host = new PreferenceHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);

    await user.click(screen.getByRole("button", { name: "Preferences" }));
    const dialog = await screen.findByRole("dialog", { name: "Workbench Preferences" });
    expect(
      within(dialog).getByText(
        "Defaults and limits for message reads, monitoring, latency probes, and rule output.",
      ),
    ).toBeVisible();
    expect(within(dialog).getByText("Preference storage is being loaded.")).toBeVisible();
    expect(
      within(dialog).getByText(
        "Awaiting host confirmation. Factory values are shown as a non-durable fallback and cannot be saved.",
      ),
    ).toBeVisible();
    expect(within(dialog).getByRole("button", { name: "Save preferences" })).toBeDisabled();

    const sessionSnapshot: KafkaOperationalPreferenceSnapshot = {
      ...confirmedPreferences,
      preferences: {
        ...confirmedPreferences.preferences,
        fetch: { maxMessages: 250, mode: "earliest" },
      },
      store: { durability: "session", state: "ready" },
    };
    act(() => {
      emitPreferences(host, sessionSnapshot, 3);
      emitPreferences(host, confirmedPreferences, 2);
    });

    await waitFor(() => {
      expect(within(dialog).getByText("Session-only storage")).toBeVisible();
      expect(within(dialog).getByRole("status")).toHaveTextContent(
        "Confirmed session-only workbench preferences.",
      );
      expect(within(dialog).getByLabelText("Default maximum results")).toHaveValue(250);
    });
  });

  it("loads and opens every grouped setting with exact confirmed durable values", async () => {
    const host = new PreferenceHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);

    await waitFor(() => {
      expect(host.commands.map((command) => command.command)).toContain("preferences.get");
    });
    act(() => emitPreferences(host, confirmedPreferences, 1));

    await user.click(screen.getByRole("button", { name: "Preferences" }));
    const dialog = await screen.findByRole("dialog", { name: "Workbench Preferences" });
    expect(
      within(dialog).getByRole("heading", { level: 3, name: "Fetch and stream" }),
    ).toBeVisible();
    expect(within(dialog).getByRole("heading", { level: 3, name: "Latency" })).toBeVisible();
    expect(within(dialog).getByRole("heading", { level: 3, name: "Rules" })).toBeVisible();
    expect(within(dialog).getByText(/Durable storage/u)).toBeVisible();
    expect(within(dialog).getByRole("combobox", { name: "Default fetch mode" })).toHaveTextContent(
      "Newest N",
    );
    expect(within(dialog).getByLabelText("Default maximum results")).toHaveValue(100);
    expect(
      within(dialog).getByText(
        "Topic-wide maximum for the next request. 1–1,000 messages · Confirmed 100 messages.",
      ),
    ).toBeVisible();
  });

  it("disables the rule Activity threshold when successful match logging is off", async () => {
    const host = new PreferenceHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    act(() => emitPreferences(host, confirmedPreferences, 1));

    await user.click(screen.getByRole("button", { name: "Preferences" }));
    const dialog = await screen.findByRole("dialog", { name: "Workbench Preferences" });
    const logging = within(dialog).getByRole("switch", {
      name: "Record successful rule matches in Activity",
    });
    const threshold = within(dialog).getByRole("combobox", {
      name: "Rule Activity threshold",
    });
    expect(logging).toBeChecked();
    expect(threshold).toBeEnabled();

    await user.click(logging);

    expect(logging).not.toBeChecked();
    expect(threshold).toHaveAttribute("aria-disabled", "true");
    expect(
      within(dialog).getByText(
        "Enable successful rule-match Activity recording to choose a threshold. Mandatory failures are always recorded.",
      ),
    ).toBeVisible();
  });

  it("applies confirmed fetch defaults only to an idle or later consumption request", async () => {
    const host = new PreferenceHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    const nonPresetPreferences: KafkaOperationalPreferenceSnapshot = {
      ...confirmedPreferences,
      preferences: {
        ...confirmedPreferences.preferences,
        fetch: { maxMessages: 25, mode: "newest" },
      },
    };
    act(() => {
      emitPreferences(host, nonPresetPreferences, 1);
      host.emit({
        event: "connection.state",
        payload: { connectionName: "Local validation", state: "connected" },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "topics.changed",
        payload: {
          refreshedAt: "2026-07-25T14:00:00.000Z",
          state: "ready",
          topics: ["orders.events"],
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      });
    });

    await user.click(await screen.findByRole("button", { name: "orders.events" }));
    expect(screen.getByRole("combobox", { name: "Read mode" })).toHaveTextContent("Newest N");
    expect(screen.getByRole("combobox", { name: "Record limit" })).toHaveTextContent("25");
    expect(screen.queryByText(/Confirmed durable defaults/u)).not.toBeInTheDocument();
    expect(host.commands.at(-1)).toMatchObject({
      command: "messages.start",
      payload: { maxMessages: 25, mode: "newest", topic: "orders.events" },
    });
    const activeRequest = {
      maxMessages: 25,
      mode: "newest",
      topic: "orders.events",
    } as const;
    act(() => {
      host.emit({
        event: "consumption.state",
        payload: {
          droppedMessages: 0,
          receivedMessages: 0,
          request: activeRequest,
          ruleEvaluation: readyRuleCapability,
          state: "loading",
        },
        sequence: 4,
        version: HOST_PROTOCOL_VERSION,
      });
      emitPreferences(
        host,
        {
          ...confirmedPreferences,
          preferences: {
            ...confirmedPreferences.preferences,
            fetch: { maxMessages: 500, mode: "earliest" },
          },
        },
        5,
      );
    });

    expect(screen.getByRole("combobox", { name: "Read mode" })).toHaveTextContent("Newest N");
    expect(screen.getByRole("combobox", { name: "Read mode" })).toHaveAttribute(
      "aria-disabled",
      "true",
    );
    expect(screen.getByRole("combobox", { name: "Record limit" })).toHaveTextContent("25");

    act(() => {
      host.emit({
        event: "consumption.state",
        payload: {
          droppedMessages: 0,
          receivedMessages: 0,
          request: activeRequest,
          ruleEvaluation: readyRuleCapability,
          state: "complete",
        },
        sequence: 6,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    await waitFor(() => {
      expect(screen.getByRole("combobox", { name: "Read mode" })).toHaveTextContent("First N");
      expect(screen.getByRole("combobox", { name: "Record limit" })).toHaveTextContent("500");
    });
  });

  it("keeps a failed save and keyboard focus distinguishable from confirmed values", async () => {
    const host = new PreferenceHost();
    let rejectUpdate: ((response: HostCommandResponse) => void) | undefined;
    const updateResponse = new Promise<HostCommandResponse>((resolve) => {
      rejectUpdate = resolve;
    });
    host.commandHandler = (command): Promise<HostCommandResponse> | undefined =>
      command.command === "preferences.update" ? updateResponse : undefined;
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    act(() => emitPreferences(host, confirmedPreferences, 1));

    await user.click(screen.getByRole("button", { name: "Preferences" }));
    const dialog = await screen.findByRole("dialog", { name: "Workbench Preferences" });
    const maximum = within(dialog).getByLabelText("Default maximum results");
    await user.clear(maximum);
    await user.type(maximum, "250");
    expect(within(dialog).getByText("Unsaved changes")).toBeVisible();
    expect(within(dialog).getByText(/Confirmed 100 messages/u)).toBeVisible();

    const save = within(dialog).getByRole("button", { name: "Save preferences" });
    save.focus();
    expect(save).toHaveFocus();
    await user.keyboard("{Enter}");
    expect(within(dialog).getByRole("status")).toHaveTextContent("Saving workbench preferences.");
    expect(within(dialog).queryByText(/Preferences saved/u)).not.toBeInTheDocument();

    const update = host.commands.find(
      (command): command is Extract<HostCommand, { readonly command: "preferences.update" }> =>
        command.command === "preferences.update",
    );
    if (update === undefined || rejectUpdate === undefined) {
      throw new Error("Expected one pending preference update.");
    }
    expect(update.payload.patch).toEqual({
      fetch: { maxMessages: 250, mode: "newest" },
    });
    rejectUpdate({
      command: "preferences.update",
      error: {
        activeStateChanged: false,
        code: "PREFERENCE_STORE_UNAVAILABLE",
        correlationId: "preference-save-failed",
        recovery: "Unlock application storage and retry.",
        retryable: true,
        stage: "preference",
        summary: "Kafka preferences were not saved.",
      },
      id: update.id,
      ok: false,
      version: HOST_PROTOCOL_VERSION,
    });

    await waitFor(() => {
      expect(within(dialog).getByRole("alert")).toHaveTextContent(
        "Kafka preferences were not saved. Unlock application storage and retry.",
      );
    });
    expect(maximum).toHaveValue(250);
    expect(within(dialog).getByText("Unsaved changes")).toBeVisible();
    expect(within(dialog).getByRole("button", { name: "Open activity" })).toBeVisible();
  });

  it("rebases untouched draft fields onto newer confirmed evidence before saving", async () => {
    const host = new PreferenceHost();
    const newerSnapshot: KafkaOperationalPreferenceSnapshot = {
      ...confirmedPreferences,
      preferences: {
        ...confirmedPreferences.preferences,
        latency: {
          ...confirmedPreferences.preferences.latency,
          timeoutMs: 45_000,
        },
        stream: {
          ...confirmedPreferences.preferences.stream,
          queueDepth: 750,
        },
      },
    };
    host.commandHandler = (command): HostCommandResponse | undefined => {
      if (command.command !== "preferences.update") {
        return undefined;
      }
      return preferenceResponse(command, {
        ...newerSnapshot,
        preferences: {
          ...newerSnapshot.preferences,
          fetch: { maxMessages: 250, mode: "newest" },
        },
      });
    };
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    act(() => emitPreferences(host, confirmedPreferences, 1));

    await user.click(screen.getByRole("button", { name: "Preferences" }));
    const dialog = await screen.findByRole("dialog", { name: "Workbench Preferences" });
    const maximum = within(dialog).getByLabelText("Default maximum results");
    await user.clear(maximum);
    await user.type(maximum, "250");
    act(() => emitPreferences(host, newerSnapshot, 2));

    await waitFor(() => {
      expect(maximum).toHaveValue(250);
      expect(within(dialog).getByLabelText("Default probe timeout")).toHaveValue(45_000);
      expect(within(dialog).getByLabelText("Stream queue depth")).toHaveValue(750);
    });
    await user.click(within(dialog).getByRole("button", { name: "Save preferences" }));

    const updates = host.commands.filter(
      (command): command is Extract<HostCommand, { readonly command: "preferences.update" }> =>
        command.command === "preferences.update",
    );
    const update = updates.at(-1);
    expect(update?.payload.patch).toEqual({
      fetch: { maxMessages: 250, mode: "newest" },
    });
  });

  it("saves one valid group and resets only after an exact confirmation", async () => {
    const host = new PreferenceHost();
    let sequence = 1;
    host.commandHandler = (command): HostCommandResponse | undefined => {
      if (command.command === "preferences.update") {
        const snapshot: KafkaOperationalPreferenceSnapshot = {
          ...confirmedPreferences,
          preferences: {
            ...confirmedPreferences.preferences,
            fetch: {
              ...confirmedPreferences.preferences.fetch,
              ...command.payload.patch.fetch,
            },
          },
        };
        emitPreferences(host, snapshot, ++sequence);
        return preferenceResponse(command, snapshot);
      }
      if (command.command === "preferences.reset") {
        const snapshot: KafkaOperationalPreferenceSnapshot = {
          preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
          store: confirmedPreferences.store,
        };
        emitPreferences(host, snapshot, ++sequence);
        return preferenceResponse(command, snapshot);
      }
      return undefined;
    };
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    act(() => emitPreferences(host, confirmedPreferences, sequence));

    await user.click(screen.getByRole("button", { name: "Preferences" }));
    let dialog = await screen.findByRole("dialog", { name: "Workbench Preferences" });
    const maximum = within(dialog).getByLabelText("Default maximum results");
    await user.clear(maximum);
    await user.type(maximum, "250");
    await user.click(within(dialog).getByRole("button", { name: "Save preferences" }));
    await waitFor(() => {
      expect(within(dialog).getByRole("status")).toHaveTextContent(
        "Saved durable workbench preferences.",
      );
    });
    expect(maximum).toHaveValue(250);
    expect(within(dialog).getByRole("button", { name: "Save preferences" })).toBeDisabled();

    await user.click(within(dialog).getByRole("button", { name: "Reset workbench preferences" }));
    const confirmation = screen.getByRole("dialog", {
      name: "Reset Workbench Preferences?",
    });
    expect(confirmation).toHaveTextContent(
      "Profiles, rules, templates, topic history, and Kafka data are unaffected.",
    );
    await user.click(within(confirmation).getByRole("button", { name: "Cancel" }));
    expect(host.commands.filter((command) => command.command === "preferences.reset")).toHaveLength(
      0,
    );
    await waitFor(() => {
      expect(
        screen.queryByRole("dialog", { name: "Reset Workbench Preferences?" }),
      ).not.toBeInTheDocument();
    });

    dialog = screen.getByRole("dialog", { name: "Workbench Preferences" });
    await user.click(within(dialog).getByRole("button", { name: "Reset workbench preferences" }));
    await user.click(
      within(screen.getByRole("dialog", { name: "Reset Workbench Preferences?" })).getByRole(
        "button",
        { name: "Reset workbench preferences" },
      ),
    );
    await waitFor(() => {
      expect(maximum).toHaveValue(1_000);
      expect(within(dialog).getByRole("status")).toHaveTextContent(
        "Factory workbench preferences confirmed.",
      );
    });
  });

  it("blocks invalid or unavailable drafts and restores focus without invoking storage", async () => {
    const host = new PreferenceHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    act(() => emitPreferences(host, confirmedPreferences, 1));

    const trigger = screen.getByRole("button", { name: "Preferences" });
    await user.click(trigger);
    let dialog = await screen.findByRole("dialog", { name: "Workbench Preferences" });
    const maximum = within(dialog).getByLabelText("Default maximum results");
    await user.clear(maximum);
    await user.type(maximum, "1001");
    expect(within(dialog).getByText("Enter a whole number from 1 through 1,000.")).toBeVisible();
    expect(within(dialog).getByRole("button", { name: "Save preferences" })).toBeDisabled();

    await user.clear(maximum);
    await user.type(maximum, "250");
    const runbook = within(dialog).getByLabelText("Latency runbook URL");
    await user.clear(runbook);
    await user.type(runbook, "http://runbooks.example.test/private");
    expect(within(dialog).getByText(/absolute credential-free HTTPS URL/u)).toBeVisible();
    expect(within(dialog).getByRole("button", { name: "Save preferences" })).toBeDisabled();
    expect(
      host.commands.filter((command) => command.command === "preferences.update"),
    ).toHaveLength(0);

    await user.click(within(dialog).getByRole("button", { name: "Close" }));
    await waitFor(() => expect(trigger).toHaveFocus());

    act(() => {
      emitPreferences(
        host,
        {
          preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
          store: {
            durability: "durable",
            recovery: "Reset Kafka operational preferences after restoring application storage.",
            state: "unavailable",
          },
        },
        2,
      );
    });
    await user.click(trigger);
    dialog = await screen.findByRole("dialog", { name: "Workbench Preferences" });
    expect(within(dialog).getByText("Preference storage unavailable")).toBeVisible();
    expect(
      within(dialog).getByText(
        "Reset Kafka operational preferences after restoring application storage.",
      ),
    ).toBeVisible();
    expect(within(dialog).getByRole("button", { name: "Save preferences" })).toBeDisabled();
    expect(
      within(dialog).getByRole("button", {
        name: "Reset workbench preferences",
      }),
    ).toBeEnabled();
  });

  it("accepts a complete reset response without waiting for a duplicate storage event", async () => {
    const host = new PreferenceHost();
    const unavailableSnapshot: KafkaOperationalPreferenceSnapshot = {
      preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
      store: {
        durability: "durable",
        recovery: "Reset Kafka operational preferences.",
        state: "unavailable",
      },
    };
    host.commandHandler = (command): HostCommandResponse | undefined =>
      command.command === "preferences.reset"
        ? preferenceResponse(command, {
            preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
            store: { durability: "durable", state: "ready" },
          })
        : undefined;
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    act(() => emitPreferences(host, unavailableSnapshot, 1));

    await user.click(screen.getByRole("button", { name: "Preferences" }));
    let dialog = await screen.findByRole("dialog", { name: "Workbench Preferences" });
    await user.click(within(dialog).getByRole("button", { name: "Reset workbench preferences" }));
    await user.click(
      within(screen.getByRole("dialog", { name: "Reset Workbench Preferences?" })).getByRole(
        "button",
        { name: "Reset workbench preferences" },
      ),
    );

    await waitFor(() => {
      expect(
        screen.queryByRole("dialog", { name: "Reset Workbench Preferences?" }),
      ).not.toBeInTheDocument();
    });
    dialog = await screen.findByRole("dialog", { name: "Workbench Preferences" });
    await waitFor(() => {
      expect(within(dialog).getByText("Durable storage")).toBeVisible();
    });
    const maximum = within(dialog).getByLabelText("Default maximum results");
    await user.clear(maximum);
    await user.type(maximum, "250");
    expect(within(dialog).getByRole("button", { name: "Save preferences" })).toBeEnabled();
  });

  it("reports load failure and links recovery to Activity", async () => {
    const host = new PreferenceHost();
    host.commandHandler = (command): HostCommandResponse | undefined =>
      command.command === "preferences.get"
        ? {
            command: command.command,
            error: {
              activeStateChanged: false,
              code: "PREFERENCE_STORE_UNAVAILABLE",
              correlationId: "preference-load-failed",
              recovery: "Restore application storage and retry.",
              retryable: true,
              stage: "preference",
              summary: "Kafka preferences could not be loaded.",
            },
            id: command.id,
            ok: false,
            version: HOST_PROTOCOL_VERSION,
          }
        : undefined;
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);

    await waitFor(() => {
      expect(host.commands.map((command) => command.command)).toContain("preferences.get");
    });
    await user.click(screen.getByRole("button", { name: "Preferences" }));
    const dialog = await screen.findByRole("dialog", { name: "Workbench Preferences" });
    expect(within(dialog).getByRole("alert")).toHaveTextContent(
      "Kafka preferences could not be loaded. Restore application storage and retry.",
    );
    await user.click(within(dialog).getByRole("button", { name: "Open activity" }));
    await waitFor(() => {
      expect(screen.getByRole("complementary", { name: "Activity log" })).toBeVisible();
    });
  });
});
