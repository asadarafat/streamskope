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
  type HostEventListener,
  type KafkaMessage,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import {
  createEmptyKafkaSavedRecordContext,
  type KafkaRecordLocator,
  type KafkaRecordLocatorOutcome,
} from "../../src/features/kafka/contracts/record-locator";
import { useInvestigationRecords } from "../../src/features/kafka/ui/use-investigation-records";
import { SavedRecordPositions } from "../../src/features/kafka/ui/SavedRecordPositions";
import { MessageWorkspace } from "../../src/features/kafka/ui/MessageWorkspace";
import { initialKafkaMessageFilters } from "../../src/features/kafka/ui/message-operations";
import { testHostExecute } from "../support/host-response";

const locator: KafkaRecordLocator = {
  schemaVersion: 1,
  clusterId: "saved-cluster",
  topicId: "4b914431-8917-44aa-ac51-982639d70b7e",
  topic: "orders",
  partition: 2,
  offset: "9007199254740993",
  leaderEpoch: 7,
};
function message(position = locator): KafkaMessage {
  return {
    id: `${position.topic}:${String(position.partition)}:${position.offset}`,
    topic: position.topic,
    partition: position.partition,
    offset: position.offset,
    provenance: {
      clusterId: position.clusterId,
      topicId: position.topicId,
      leaderEpoch: position.leaderEpoch,
    },
    timestamp: "2026-10-09T00:00:00.000Z",
    key: null,
    payload: "[MASKED]",
    preview: "[MASKED]",
    headers: {},
    originalByteSize: 8,
    truncated: false,
    original: { state: "unavailable", reason: "masked" },
    structured: {
      version: 1,
      key: { state: "null", codec: "auto", writerSchema: null },
      value: { state: "masked", codec: "json", writerSchema: null },
      headers: [],
      headersState: "complete",
      protection: "masked",
    },
  };
}
type LoadCommand = Extract<HostCommand, { command: "records.locator.load" }>;
class Host implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  readonly listeners = new Set<HostEventListener>();
  load: (command: LoadCommand) => Promise<KafkaRecordLocatorOutcome> = (command) =>
    Promise.resolve({
      ...command.payload,
      state: "loaded",
      message: message(command.payload.locator),
    });
  stopFails = false;
  readonly execute = testHostExecute(async (command) => {
    this.commands.push(command);
    const result =
      command.command === "records.locator.load"
        ? { outcome: await this.load(command) }
        : command.command === "records.locator.cancel" && !this.stopFails
          ? { requestId: command.payload.requestId, stopped: true }
          : ((): never => {
              throw new Error("private transport error must not appear");
            })();
    return {
      command: command.command,
      id: command.id,
      ok: true,
      version: HOST_PROTOCOL_VERSION,
      result: { correlationId: command.id, ...result },
    };
  });
  subscribe(listener: HostEventListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }
  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("Unused"));
  }
  emit(event: HostEvent): void {
    for (const listener of this.listeners) listener(event);
  }
}
type Props = Parameters<typeof useInvestigationRecords>[0];
function setup(host = new Host()): ReturnType<
  typeof renderHook<ReturnType<typeof useInvestigationRecords>, Props>
> & {
  host: Host;
  props: Props;
} {
  const props: Props = {
    host,
    connected: true,
    readBlocked: false,
    authorityKey: "profile-a",
    settingsKey: JSON.stringify([
      KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.protection,
      KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.codecs,
    ]),
  };
  return { ...renderHook(useInvestigationRecords, { initialProps: props }), host, props };
}
afterEach(cleanup);

it("restores unloaded positions without broker I/O and reloads only the current protected record", async () => {
  const { result, host } = setup();
  const records = {
    selected: locator,
    comparison: { ...locator, offset: "1" },
    bookmarks: [{ id: "one", name: "Incident", locator }],
  };
  act(() => result.current.restore(records));
  expect(host.commands).toEqual([]);
  expect(result.current.selected).toBeNull();
  expect(result.current.baseline).toBeNull();
  expect(result.current.capture(null)).toEqual(records);
  await act(() => result.current.reload("selected"));
  expect(result.current.selected?.payload).toBe("[MASKED]");
  expect(result.current.selected?.ruleEvaluation).toMatchObject({
    state: "unavailable",
    reason: "not-evaluated",
    evaluatedRules: 0,
  });
  expect(result.current.selected?.original).toEqual({ state: "unavailable", reason: "masked" });
  expect(result.current.capture(null)).toEqual(records);
  expect(JSON.stringify(result.current.capture(null))).not.toMatch(
    /MASKED|payload|original|structured|requestId/,
  );
  expect(host.commands.map((command) => command.command)).toEqual(["records.locator.load"]);
});

it("keeps baseline independent from selected and serializes competing reloads", async () => {
  const { result, host } = setup();
  let resolve!: (outcome: KafkaRecordLocatorOutcome) => void;
  host.load = (): Promise<KafkaRecordLocatorOutcome> =>
    new Promise((done) => {
      resolve = done;
    });
  act(() =>
    result.current.restore({
      ...createEmptyKafkaSavedRecordContext(),
      selected: locator,
      comparison: { ...locator, offset: "8" },
    }),
  );
  let loading!: Promise<void>;
  act(() => {
    loading = result.current.reload("selected");
    void result.current.reload("comparison");
  });
  expect(host.commands).toHaveLength(1);
  const command = host.commands[0] as LoadCommand;
  await act(async () => {
    resolve({ ...command.payload, state: "loaded", message: message() });
    await loading;
  });
  act(() => result.current.pin(result.current.selected));
  act(() => result.current.selectGrid(null));
  expect(result.current.selected).toBeNull();
  expect(result.current.baseline?.offset).toBe(locator.offset);
  expect(result.current.references.selected).toBeNull();
});

it("an unprovenanced selection cannot reuse a previously saved position", async () => {
  const { result } = setup();
  act(() => result.current.restore({ ...createEmptyKafkaSavedRecordContext(), selected: locator }));
  await act(() => result.current.reload("selected"));
  const previous = result.current.selected!;
  const withoutProvenance = { ...previous };
  delete withoutProvenance.provenance;
  act(() => result.current.selectGrid(withoutProvenance));
  expect(result.current.selected).toBeNull();
  expect(result.current.capture(withoutProvenance).selected).toBeNull();
  expect(result.current.capture(null).selected).toBeNull();
  act(() => result.current.pin(withoutProvenance));
  expect(result.current.baseline).toEqual(withoutProvenance);
  expect(result.current.capture(null).comparison).toBeNull();
});

it("retains failed cleanup with Retry stop, blocks new loads and refuses restoration", async () => {
  const { result, host } = setup();
  host.load = (): Promise<KafkaRecordLocatorOutcome> =>
    Promise.reject(new Error("private credential material"));
  host.stopFails = true;
  act(() => result.current.restore({ ...createEmptyKafkaSavedRecordContext(), selected: locator }));
  await act(() => result.current.reload("selected"));
  expect(result.current.cleanupPending).toBe(true);
  expect(result.current.error).not.toMatch(/private|credential/);
  expect(() => result.current.restore(createEmptyKafkaSavedRecordContext())).toThrow(/stop reload/);
  expect(() => result.current.clear()).toThrow(/stop reload/);
  render(
    <SavedRecordPositions
      controller={result.current}
      connected
      readBlocked={false}
      onChoose={vi.fn()}
      onOpenTopic={vi.fn()}
    />,
  );
  expect(screen.getByRole("button", { name: "Clear saved positions" })).toBeDisabled();
  await act(() => result.current.reload("selected"));
  expect(
    host.commands.filter((command) => command.command === "records.locator.load"),
  ).toHaveLength(1);
  await act(() => result.current.cancel());
  expect(result.current.cleanupPending).toBe(true);
  host.stopFails = false;
  await act(() => result.current.cancel());
  expect(result.current.busy).toBe(false);
  expect(result.current.selected).toBeNull();
  expect(result.current.outcomes.selected?.state).toBe("cancelled");
});

it("rejects a late result after changing authority and cancels through the original host", async () => {
  const { result, host, props, rerender } = setup();
  let resolve!: (outcome: KafkaRecordLocatorOutcome) => void;
  host.load = (): Promise<KafkaRecordLocatorOutcome> =>
    new Promise((done) => {
      resolve = done;
    });
  act(() => result.current.restore({ ...createEmptyKafkaSavedRecordContext(), selected: locator }));
  let loading!: Promise<void>;
  act(() => {
    loading = result.current.reload("selected");
  });
  const command = host.commands[0] as LoadCommand;
  const next = new Host();
  rerender({ ...props, host: next, authorityKey: "profile-b" });
  await act(async () => {
    resolve({ ...command.payload, state: "loaded", message: message() });
    await loading;
  });
  expect(result.current.selected).toBeNull();
  expect(host.commands.map((entry) => entry.command)).toEqual([
    "records.locator.load",
    "records.locator.cancel",
  ]);
  expect(next.commands).toEqual([]);
  expect(result.current.references.selected).toEqual(locator);
});

it.each(["loaded", "unavailable"] as const)(
  "reconciles a matching late %s outcome after Stop failed without restoring contents",
  async (state) => {
    const { result, host } = setup();
    let resolve!: (outcome: KafkaRecordLocatorOutcome) => void;
    host.load = (): Promise<KafkaRecordLocatorOutcome> =>
      new Promise((done) => {
        resolve = done;
      });
    host.stopFails = true;
    act(() =>
      result.current.restore({ ...createEmptyKafkaSavedRecordContext(), selected: locator }),
    );
    let loading!: Promise<void>;
    act(() => {
      loading = result.current.reload("selected");
    });
    const command = host.commands[0] as LoadCommand;
    await act(() => result.current.cancel());
    expect(result.current.cleanupPending).toBe(true);
    await act(async () => {
      resolve(
        state === "loaded"
          ? { ...command.payload, state, message: message() }
          : { ...command.payload, state, detail: "Another read owns the available capacity." },
      );
      await loading;
    });
    expect(result.current.busy).toBe(false);
    expect(result.current.error).toBeUndefined();
    expect(result.current.selected).toBeNull();
    expect(result.current.outcomes.selected?.state).toBe("cancelled");
    expect(result.current.references.selected).toEqual(locator);
  },
);

it.each(["failed", "mismatched"] as const)(
  "retains cleanup after a %s late load reply when Stop is unconfirmed",
  async (failure) => {
    const { result, host } = setup();
    let resolve!: (outcome: KafkaRecordLocatorOutcome) => void;
    let reject!: (error: Error) => void;
    host.load = (): Promise<KafkaRecordLocatorOutcome> =>
      new Promise((done, fail) => {
        resolve = done;
        reject = fail;
      });
    host.stopFails = true;
    act(() =>
      result.current.restore({ ...createEmptyKafkaSavedRecordContext(), selected: locator }),
    );
    let loading!: Promise<void>;
    act(() => {
      loading = result.current.reload("selected");
    });
    const command = host.commands[0] as LoadCommand;
    await act(() => result.current.cancel());
    await act(async () => {
      if (failure === "failed") reject(new Error("private transport details"));
      else
        resolve({
          ...command.payload,
          requestId: crypto.randomUUID(),
          state: "unavailable",
          detail: "No record loaded.",
        });
      await loading;
    });
    expect(result.current.cleanupPending).toBe(true);
    expect(result.current.selected).toBeNull();
    expect(result.current.error).toMatch(/Retry stop/);
    expect(result.current.error).not.toMatch(/private transport/);
  },
);

it("clears content immediately when settings change, retaining unloaded references", async () => {
  const { result, props, rerender } = setup();
  act(() => result.current.restore({ ...createEmptyKafkaSavedRecordContext(), selected: locator }));
  await act(() => result.current.reload("selected"));
  act(() => result.current.pin(result.current.selected));
  expect(result.current.baseline).not.toBeNull();
  rerender({ ...props, settingsKey: "changed-codecs-and-protection" });
  expect(result.current.selected).toBeNull();
  expect(result.current.baseline).toBeNull();
  expect(result.current.outcomes).toEqual({});
  expect(result.current.references.selected).toEqual(locator);
});

it("explicitly clears the workspace positions so another cluster can start a view without changing saved data", async () => {
  const { result, host, props, rerender } = setup();
  const saved = {
    selected: locator,
    comparison: locator,
    bookmarks: [{ id: "one", name: "Incident", locator }],
  };
  act(() => result.current.restore(saved));
  await act(() => result.current.reload("selected"));
  const fromB = {
    ...result.current.selected!,
    ...message({ ...locator, clusterId: "other-cluster" }),
  };
  rerender({ ...props, authorityKey: "profile-b" });
  act(() => result.current.selectGrid(fromB));
  expect(result.current.error).toMatch(/another Kafka cluster/);
  expect(() => result.current.capture(fromB)).toThrow(/same cluster/);
  const beforeClear = host.commands.length;
  render(
    <SavedRecordPositions
      controller={result.current}
      connected
      readBlocked={false}
      onChoose={vi.fn()}
      onOpenTopic={vi.fn()}
    />,
  );
  await userEvent.setup().click(screen.getByRole("button", { name: "Clear saved positions" }));
  expect(result.current.references).toEqual(createEmptyKafkaSavedRecordContext());
  expect(result.current.selected).toBeNull();
  expect(result.current.baseline).toBeNull();
  expect(host.commands).toHaveLength(beforeClear);
  expect(saved).toEqual({
    selected: locator,
    comparison: locator,
    bookmarks: [{ id: "one", name: "Incident", locator }],
  });
  act(() => result.current.selectGrid(fromB));
  expect(result.current.capture(fromB).selected?.clusterId).toBe("other-cluster");
  expect(result.current.error).toBeUndefined();
});

it("fences a loaded reply when a protection event arrives before React rerenders", async () => {
  const { result, host } = setup();
  let resolve!: (outcome: KafkaRecordLocatorOutcome) => void;
  host.load = (): Promise<KafkaRecordLocatorOutcome> =>
    new Promise((done) => {
      resolve = done;
    });
  act(() => result.current.restore({ ...createEmptyKafkaSavedRecordContext(), selected: locator }));
  let loading!: Promise<void>;
  act(() => {
    loading = result.current.reload("selected");
  });
  const command = host.commands[0] as LoadCommand;
  await act(async () => {
    host.emit({
      event: "preferences.changed",
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
      payload: {
        preferences: {
          ...KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
          protection: { ...KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.protection, maskKey: true },
        },
        store: { durability: "session", state: "ready" },
      },
    });
    resolve({ ...command.payload, state: "loaded", message: message() });
    await loading;
  });
  expect(result.current.selected).toBeNull();
});

it.each([
  "expired",
  "resource-replaced",
  "record-replaced",
  "topic-missing",
  "inaccessible",
  "record-missing",
  "unavailable",
] as const)("presents %s as an unloaded outcome without guessing another record", async (state) => {
  const { result, host } = setup();
  host.load = (command): Promise<KafkaRecordLocatorOutcome> =>
    Promise.resolve({ ...command.payload, state, detail: `Confirmed ${state}` });
  act(() => result.current.restore({ ...createEmptyKafkaSavedRecordContext(), selected: locator }));
  await act(() => result.current.reload("selected"));
  expect(result.current.selected).toBeNull();
  expect(result.current.busy).toBe(false);
  expect(result.current.outcomes.selected).toEqual({ state, detail: `Confirmed ${state}` });
});

it("does not read while disconnected or while the previous stream/probe is active", async () => {
  const { result, host, props, rerender } = setup();
  act(() => result.current.restore({ ...createEmptyKafkaSavedRecordContext(), selected: locator }));
  rerender({ ...props, connected: false });
  await act(() => result.current.reload("selected"));
  rerender({ ...props, readBlocked: true });
  await act(() => result.current.reload("selected"));
  expect(host.commands).toEqual([]);
});

it("keeps empty-grid positions accessible and choosing a bookmark separate from reload", async () => {
  const { result } = setup();
  act(() =>
    result.current.restore({
      ...createEmptyKafkaSavedRecordContext(),
      selected: locator,
      bookmarks: [{ id: "one", name: "Incident", locator }],
    }),
  );
  const user = userEvent.setup(),
    onChoose = vi.fn(),
    onOpenTopic = vi.fn();
  render(
    <SavedRecordPositions
      controller={result.current}
      connected
      readBlocked={false}
      onChoose={onChoose}
      onOpenTopic={onOpenTopic}
    />,
  );
  expect(screen.getByText("Not loaded")).toBeVisible();
  await user.click(screen.getByRole("combobox", { name: "Record bookmark" }));
  await user.click(screen.getByRole("option", { name: "Incident" }));
  await user.click(screen.getByRole("button", { name: "Use as baseline" }));
  expect(onChoose).toHaveBeenCalledExactlyOnceWith(locator, "comparison");
  expect(onOpenTopic).not.toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: "Reload selected" }));
  await waitFor(() => expect(onOpenTopic).toHaveBeenCalledExactlyOnceWith("orders"));
});

it("keeps a freshly reloaded inspector usable beside stale grid rows and blocks a competing read", async () => {
  const { result } = setup();
  act(() => result.current.restore({ ...createEmptyKafkaSavedRecordContext(), selected: locator }));
  await act(() => result.current.reload("selected"));
  const user = userEvent.setup(),
    pin = vi.fn(),
    bookmark = vi.fn();
  render(
    <MessageWorkspace
      connectionAvailable
      consumptionError={null}
      consumptionRequest={null}
      consumptionState="stopped"
      consumptionStopping={false}
      droppedMessages={0}
      fetchMaximum={100}
      fetchMode="earliest"
      filters={initialKafkaMessageFilters}
      liveRuleCapability={{ applicableRules: 0, omittedRules: 0, state: "ready" }}
      messages={[]}
      messagesStale
      selectedRecordCurrent
      readBusy
      onClearFilters={vi.fn()}
      onClearSelection={vi.fn()}
      onFetchMaximumChange={vi.fn()}
      onFetchModeChange={vi.fn()}
      onPartitionFilterChange={vi.fn()}
      onRuleFilterChange={vi.fn()}
      onSelectMessage={vi.fn()}
      onStart={vi.fn()}
      onStop={vi.fn()}
      onTextFilterChange={vi.fn()}
      retainedMessageCount={0}
      savedProfileCount={1}
      selectedMessage={result.current.selected}
      selectedMessageId={null}
      selectedTopic="orders"
      selectionNotice={undefined}
      comparison={{ baseline: null, onPin: pin }}
      onBookmark={bookmark}
    />,
  );
  expect(await screen.findByRole("button", { name: "Bookmark record" })).toBeEnabled();
  expect(screen.getByRole("button", { name: "Load messages orders" })).toBeDisabled();
  await user.click(screen.getByRole("tab", { name: "Compare" }));
  await user.click(screen.getByRole("button", { name: "Pin as baseline" }));
  expect(pin).toHaveBeenCalledWith(result.current.selected);
  await user.click(screen.getByRole("tab", { name: "Rules" }));
  expect(screen.getByText("Rules not evaluated", { exact: true })).toBeVisible();
});
