// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import { KafkaQueryLibrary } from "../../src/features/kafka/application";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEvent,
  type HostEventListener,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { SavedQueriesDialog } from "../../src/features/kafka/ui/SavedQueriesDialog";
import { StreamSkopeApp } from "../../src/features/kafka/ui/StreamSkopeApp";
import { testHostAccepted, testHostExecute } from "../support/host-response";
import { pasteText } from "../support/paste-text";

class LibraryHost implements StreamSkopeHost {
  readonly library = new KafkaQueryLibrary();
  readonly commands: HostCommand[] = [];
  private readonly listeners = new Set<HostEventListener>();
  readonly execute = testHostExecute(async (command) => {
    this.commands.push(command);
    if (
      command.command === "queries.list" ||
      command.command === "queries.put" ||
      command.command === "queries.delete"
    ) {
      const snapshot =
        command.command === "queries.list"
          ? await this.library.list()
          : command.command === "queries.put"
            ? await this.library.put(command.payload.query)
            : await this.library.delete(command.payload.id);
      return {
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId: command.id, snapshot },
      };
    }
    return testHostAccepted(command, command.id);
  });
  emit(event: HostEvent): void {
    for (const listener of this.listeners) listener(event);
  }
  subscribe(listener: HostEventListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }
  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("Queries do not open external URLs."));
  }
}

afterEach(cleanup);

it("saves, reopens and deletes query settings without starting a read or keeping old coverage", async () => {
  const host = new LibraryHost();
  const user = userEvent.setup();
  render(<StreamSkopeApp host={host} />);
  act(() => {
    host.emit({
      event: "connection.state",
      version: HOST_PROTOCOL_VERSION,
      sequence: 1,
      payload: { connectionName: "Fixture", state: "connected" },
    });
    host.emit({
      event: "topics.changed",
      version: HOST_PROTOCOL_VERSION,
      sequence: 2,
      payload: { refreshedAt: "2026-07-25T14:00:00.000Z", state: "ready", topics: ["orders"] },
    });
  });
  await user.click(await screen.findByRole("button", { name: "orders" }));
  await user.click(screen.getByRole("combobox", { name: "Read mode" }));
  await user.click(screen.getByRole("option", { name: "First N" }));
  await user.click(screen.getByRole("button", { name: "Show message filters" }));
  await pasteText(
    user,
    screen.getByRole("textbox", { name: "JSON expression" }),
    '$.status == "failed"',
  );
  await user.click(screen.getByRole("button", { name: "Saved queries" }));
  await pasteText(user, screen.getByRole("textbox", { name: "Query name" }), "Failed orders");
  await user.click(screen.getByRole("button", { name: "Save current as new" }));
  expect(await screen.findByText("Query saved.")).toBeVisible();
  expect((await host.library.list()).queries[0]).toMatchObject({
    name: "Failed orders",
    configuration: {
      schemaVersion: 1,
      request: { mode: "earliest", topic: "orders", maxMessages: 1_000 },
      filters: { expression: '$.status == "failed"' },
    },
  });
  await user.click(screen.getByRole("button", { name: "Close", exact: true }));
  await user.clear(screen.getByRole("textbox", { name: "JSON expression" }));
  await user.click(screen.getByRole("combobox", { name: "Read mode" }));
  await user.click(screen.getByRole("option", { name: "Newest N" }));
  act(() =>
    host.emit({
      event: "consumption.state",
      version: HOST_PROTOCOL_VERSION,
      sequence: 3,
      payload: {
        state: "empty",
        request: { mode: "latest", topic: "orders", maxMessages: 1_000 },
        droppedMessages: 0,
        receivedMessages: 0,
        ruleEvaluation: { applicableRules: 0, omittedRules: 0, state: "ready" },
        coverage: {
          reason: "range-complete",
          scannedRecords: 0,
          scannedBytes: 0,
          matchedRecords: 0,
          unavailableRecords: 0,
          partitions: [],
        },
      },
    }),
  );
  expect(screen.getByRole("region", { name: "Read coverage" })).toBeVisible();
  await user.click(screen.getByRole("button", { name: "Saved queries" }));
  await user.click(screen.getByRole("combobox", { name: "Saved query" }));
  await user.click(await screen.findByRole("option", { name: "Failed orders" }));
  const beforeOpen = host.commands.length;
  await user.click(screen.getByRole("button", { name: "Open query" }));
  expect(screen.getByRole("combobox", { name: "Read mode" })).toHaveTextContent("First N");
  expect(screen.getByRole("textbox", { name: "JSON expression" })).toHaveValue(
    '$.status == "failed"',
  );
  expect(screen.queryByRole("region", { name: "Read coverage" })).not.toBeInTheDocument();
  expect(host.commands.slice(beforeOpen)).toEqual([]);
  expect(screen.getByRole("button", { name: "Load messages orders" })).toBeEnabled();

  await user.click(screen.getByRole("button", { name: "Saved queries" }));
  await user.click(screen.getByRole("combobox", { name: "Saved query" }));
  await user.click(await screen.findByRole("option", { name: "Failed orders" }));
  await user.click(screen.getByRole("button", { name: "Delete selected" }));
  expect((await host.library.list()).queries).toHaveLength(1);
  await user.click(screen.getByRole("button", { name: "Delete query", exact: true }));
  expect(await screen.findByText("Query deleted.")).toBeVisible();
  expect((await host.library.list()).queries).toEqual([]);
});

it("requires a missing profile reference to be resolved and blocks opening during a read", async () => {
  const host = new LibraryHost();
  const configuration = {
    schemaVersion: 1,
    request: { mode: "earliest", topic: "orders", maxMessages: 25 },
  } as const;
  await host.library.put({
    id: "incident",
    name: "Incident",
    profileId: "deleted-profile",
    configuration,
  });
  const user = userEvent.setup();
  const onRestore = vi.fn();
  const props = {
    host,
    profiles: [],
    currentTopic: null,
    captureCurrent: () => configuration,
    onRestore,
    onClose: vi.fn(),
  };
  const { rerender } = render(<SavedQueriesDialog {...props} readActive={false} />);
  await user.click(await screen.findByRole("combobox", { name: "Saved query" }));
  await user.click(await screen.findByRole("option", { name: "Incident" }));
  expect(screen.getByText(/The saved profile is unavailable/)).toBeVisible();
  const open = screen.getByRole("button", { name: "Open query" });
  expect(open).toBeDisabled();
  await user.click(screen.getByRole("combobox", { name: "Local connection profile" }));
  await user.click(screen.getByRole("option", { name: "Choose a connection when opening" }));
  rerender(<SavedQueriesDialog {...props} readActive />);
  expect(open).toBeDisabled();
  rerender(<SavedQueriesDialog {...props} readActive={false} />);
  await user.click(open);
  expect(onRestore).toHaveBeenCalledExactlyOnceWith(configuration, undefined);
  expect(host.commands.map((command) => command.command)).toEqual(["queries.list"]);
});
