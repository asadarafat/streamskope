// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, render, renderHook, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import { KafkaQueryLibrary } from "../../src/features/kafka/application/query-library";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEvent,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import type { KafkaTopicAnnotation } from "../../src/features/kafka/contracts/topic-catalog";
import { LocalTopicNotesDialog } from "../../src/features/kafka/ui/LocalTopicNotesDialog";
import { useLocalTopicNotes } from "../../src/features/kafka/ui/use-local-topic-notes";
import { testHostExecute } from "../support/host-response";

const identity = {
  clusterId: "cluster-a",
  topicId: "4b914431-8917-44aa-ac51-982639d70b7e",
  topic: "orders",
};
const saved: KafkaTopicAnnotation = {
  identity,
  description: "Original description",
  owner: "Operations",
  labels: ["critical"],
  links: [{ title: "Recovery", url: "https://example.com/runbook" }],
};
afterEach(cleanup);
interface NotesFixture {
  readonly library: KafkaQueryLibrary;
  readonly host: StreamSkopeHost;
  readonly commands: HostCommand[];
  readonly control: { identity: typeof identity; beforeReply?: () => Promise<void> };
  readonly openExternalUrl: ReturnType<typeof vi.fn<StreamSkopeHost["openExternalUrl"]>>;
  revoke(): void;
}
function fixture(): NotesFixture {
  const library = new KafkaQueryLibrary();
  const commands: HostCommand[] = [];
  const listeners = new Set<(event: HostEvent) => void>();
  const control: { identity: typeof identity; beforeReply?: () => Promise<void> } = { identity };
  const openExternalUrl = vi
    .fn<StreamSkopeHost["openExternalUrl"]>()
    .mockResolvedValue({ state: "accepted", version: HOST_PROTOCOL_VERSION });
  const host: StreamSkopeHost = {
    execute: testHostExecute(async (command) => {
      commands.push(command);
      const snapshot =
        command.command === "catalog.list"
          ? await library.listTopics()
          : command.command === "catalog.load"
            ? await library.getTopic(control.identity)
            : command.command === "catalog.put"
              ? await library.putTopic(command.payload.annotation, command.payload.expected)
              : command.command === "catalog.delete"
                ? await library.deleteTopic(command.payload.identity, command.payload.expected)
                : ((): never => {
                    throw new Error("Unexpected non-catalog command");
                  })();
      await control.beforeReply?.();
      return {
        command: command.command,
        id: command.id,
        ok: true,
        version: HOST_PROTOCOL_VERSION,
        result: { correlationId: command.id, snapshot },
      };
    }),
    subscribe: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    openExternalUrl,
  };
  return {
    host,
    library,
    commands,
    control,
    openExternalUrl,
    revoke: (): void => {
      for (const listener of listeners)
        listener({
          event: "connection.state",
          version: HOST_PROTOCOL_VERSION,
          sequence: 1,
          payload: { connectionName: null, state: "disconnected" },
        });
    },
  };
}
const props = {
  connected: true,
  authorityKey: "connection-a",
  initialTopic: "orders",
  currentTopic: "orders",
  onClose: vi.fn(),
};

it("saves verified local notes and opens a validated runbook only on an explicit click", async () => {
  const f = fixture(),
    user = userEvent.setup();
  render(<LocalTopicNotesDialog {...props} host={f.host} />);
  await screen.findByText("No notes are saved for this topic identity.");
  await user.type(
    screen.getByRole("textbox", { name: "Description" }),
    "Investigate payment delays",
  );
  await user.type(screen.getByRole("textbox", { name: "Owner" }), "Support");
  await user.type(screen.getByRole("textbox", { name: "Labels" }), "payments, critical");
  await user.click(screen.getByRole("button", { name: "Add runbook link" }));
  await user.type(screen.getByRole("textbox", { name: "Runbook 1 title" }), "Recovery");
  await user.type(
    screen.getByRole("textbox", { name: "Runbook 1 HTTPS URL" }),
    "https://example.com/runbook",
  );
  await user.click(screen.getByRole("button", { name: "Save topic notes" }));
  await screen.findByText("Topic notes saved.");
  expect((await f.library.getTopic(identity)).annotation).toEqual({
    identity,
    description: "Investigate payment delays",
    owner: "Support",
    labels: ["payments", "critical"],
    links: saved.links,
  });
  expect(f.openExternalUrl).not.toHaveBeenCalled();
  expect(f.commands.map((command) => command.command)).toEqual([
    "catalog.list",
    "catalog.load",
    "catalog.put",
  ]);
  await user.click(screen.getByRole("button", { name: "Open runbook: Recovery" }));
  expect(f.openExternalUrl).toHaveBeenCalledExactlyOnceWith("https://example.com/runbook");
});

it("retains the draft and original CAS expectation after a concurrent change and after Refresh", async () => {
  const f = fixture(),
    user = userEvent.setup();
  await f.library.putTopic(saved, null);
  render(<LocalTopicNotesDialog {...props} host={f.host} />);
  await screen.findByText("Current topic identity verified.");
  await user.clear(screen.getByRole("textbox", { name: "Description" }));
  await user.type(screen.getByRole("textbox", { name: "Description" }), "My retained draft");
  const other = { ...saved, description: "Changed in another window" };
  await f.library.putTopic(other, saved);
  await user.click(screen.getByRole("button", { name: "Save topic notes" }));
  await screen.findByText(/Your draft is retained/);
  expect(screen.getByRole("textbox", { name: "Description" })).toHaveValue("My retained draft");
  expect((await f.library.getTopic(identity)).annotation).toEqual(other);
  expect(f.commands.find((command) => command.command === "catalog.put")?.payload).toMatchObject({
    expected: saved,
  });
  await user.click(screen.getByRole("button", { name: "Refresh saved notes" }));
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "Refresh saved notes" })).toBeEnabled(),
  );
  expect(screen.getByRole("textbox", { name: "Description" })).toHaveValue("My retained draft");
  await user.click(screen.getByRole("button", { name: "Verify current topic" }));
  expect(screen.getByText("Discard this unsaved draft before continuing?")).toBeVisible();
  await user.click(screen.getByRole("button", { name: "Keep draft" }));
  expect(f.commands.filter((command) => command.command === "catalog.load")).toHaveLength(1);
});

it("never inherits notes on a same-name replacement and permits orphan removal while disconnected", async () => {
  const f = fixture(),
    user = userEvent.setup();
  await f.library.putTopic(saved, null);
  f.control.identity = { ...identity, topicId: "f4758931-8917-44aa-ac51-982639d70b7e" };
  const view = render(<LocalTopicNotesDialog {...props} host={f.host} />);
  await screen.findByText("No notes are saved for this topic identity.");
  expect(screen.getByRole("textbox", { name: "Description" })).toHaveValue("");
  view.rerender(
    <LocalTopicNotesDialog
      {...props}
      host={f.host}
      connected={false}
      authorityKey="disconnected"
    />,
  );
  await user.click(screen.getByRole("combobox", { name: "Saved topic notes" }));
  await user.click(screen.getByRole("option", { name: /orders · cluster-a · 4b914431/ }));
  expect(screen.getByRole("textbox", { name: "Description" })).toHaveValue(saved.description);
  expect(screen.getByRole("button", { name: "Save topic notes" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Verify current topic" })).toBeDisabled();
  await user.click(screen.getByRole("button", { name: "Remove saved notes" }));
  await user.click(screen.getByRole("button", { name: "Confirm remove notes" }));
  await screen.findByText("Local topic notes removed. Kafka was not changed.");
  expect((await f.library.listTopics()).topics).toEqual([]);
  expect(f.commands.filter((command) => command.command === "catalog.delete")).toHaveLength(1);
});

it("fences a draft immediately on a connection event while retaining an admitted save's actual identity", async () => {
  const f = fixture();
  const { result } = renderHook(() =>
    useLocalTopicNotes({
      host: f.host,
      connected: true,
      authorityKey: "a",
      initialTopic: "orders",
    }),
  );
  await waitFor(() => expect(result.current.verified).toBe(true));
  act(() => result.current.update({ description: "Admitted old-topic draft" }));
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  f.control.beforeReply = (): Promise<void> => gate;
  let saving!: Promise<boolean>;
  act(() => {
    saving = result.current.save();
  });
  await waitFor(() =>
    expect(f.commands.some((command) => command.command === "catalog.put")).toBe(true),
  );
  act(() => f.revoke());
  expect(result.current.verified).toBe(false);
  expect(result.current.selection?.draft.description).toBe("Admitted old-topic draft");
  await act(async () => {
    release();
    await saving;
  });
  expect(result.current.status).toMatch(/saved for the original topic identity/);
  expect(result.current.selection?.expected?.identity).toEqual(identity);
  expect(result.current.verified).toBe(false);
  await act(async () => {
    await result.current.save();
  });
  expect(f.commands.filter((command) => command.command === "catalog.put")).toHaveLength(1);
});

it("discards a superseded host's late reply without exposing it in the new catalog", async () => {
  const old = fixture(),
    next = fixture();
  await old.library.putTopic(saved, null);
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  old.control.beforeReply = (): Promise<void> => gate;
  const { result, rerender } = renderHook(useLocalTopicNotes, {
    initialProps: {
      host: old.host,
      connected: false,
      authorityKey: "off",
      initialTopic: null as string | null,
    },
  });
  await waitFor(() => expect(old.commands).toHaveLength(1));
  rerender({ host: next.host, connected: false, authorityKey: "off", initialTopic: null });
  await waitFor(() => expect(result.current.catalog?.topics).toEqual([]));
  await act(async () => {
    release();
    await gate;
  });
  expect(result.current.catalog?.topics).toEqual([]);
  expect(result.current.selection).toBeUndefined();
  expect(next.commands.map((command) => command.command)).toEqual(["catalog.list"]);
});

it("rejects invalid link credentials without a host write or external request", async () => {
  const f = fixture();
  const { result } = renderHook(() =>
    useLocalTopicNotes({
      host: f.host,
      connected: true,
      authorityKey: "a",
      initialTopic: "orders",
    }),
  );
  await waitFor(() => expect(result.current.verified).toBe(true));
  act(() =>
    result.current.update({
      links: [{ title: "Runbook", url: "https://user:password@example.com/runbook" }],
    }),
  );
  await act(async () => {
    await result.current.save();
  });
  expect(result.current.error).toBeTruthy();
  expect(f.commands.some((command) => command.command === "catalog.put")).toBe(false);
  expect(f.openExternalUrl).not.toHaveBeenCalled();
  expect(result.current.selection?.draft.links).toHaveLength(1);
});

it("can retry a failed initial catalog read without closing the dialog", async () => {
  const f = fixture(),
    user = userEvent.setup();
  await f.library.putTopic(saved, null);
  f.control.beforeReply = (): Promise<void> => Promise.reject(new Error("Temporary host failure"));
  render(<LocalTopicNotesDialog {...props} host={f.host} connected={false} initialTopic={null} />);
  await screen.findByText(/The host did not confirm the notes operation/);
  expect(screen.getByRole("button", { name: "Refresh saved notes" })).toBeEnabled();
  delete f.control.beforeReply;
  await user.click(screen.getByRole("button", { name: "Refresh saved notes" }));
  await waitFor(() =>
    expect(screen.getByRole("combobox", { name: "Saved topic notes" })).toBeEnabled(),
  );
  await user.click(screen.getByRole("combobox", { name: "Saved topic notes" }));
  expect(screen.getByRole("option", { name: /orders · cluster-a · 4b914431/ })).toBeVisible();
  expect(f.commands.map((command) => command.command)).toEqual(["catalog.list", "catalog.list"]);
});
