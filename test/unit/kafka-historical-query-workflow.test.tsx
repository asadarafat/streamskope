// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { StreamSkopeApp } from "../../src/features/kafka/ui/StreamSkopeApp";
import { testHostAccepted } from "../support/host-response";
import { pasteText } from "../support/paste-text";

class QueryHost implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  private readonly listeners = new Set<HostEventListener>();

  emit(event: HostEvent): void {
    for (const listener of this.listeners) listener(event);
  }

  execute<Command extends HostCommand>(
    command: Command,
  ): Promise<HostCommandResponse<Command["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    return Promise.resolve(testHostAccepted(command, `query-${command.id}`));
  }

  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("Query editing does not open external URLs."));
  }

  subscribe(listener: HostEventListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }
}

afterEach(cleanup);

it("validates an explicit historical interval and converts its offsets before reading", async () => {
  const host = new QueryHost();
  const user = userEvent.setup();
  render(<StreamSkopeApp host={host} />);
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
        refreshedAt: "2026-07-25T14:00:00.000Z",
        state: "ready",
        topics: ["test"],
      },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    });
  });
  await user.click(await screen.findByRole("button", { name: "test" }));
  await user.click(screen.getByRole("combobox", { name: "Read mode" }));
  await user.click(screen.getByRole("option", { name: "Time window" }));
  await user.click(screen.getByRole("combobox", { name: "Time interval" }));
  await user.click(screen.getByRole("option", { name: "Custom interval" }));
  const start = screen.getByRole("textbox", { name: "Start time (inclusive)" });
  const end = screen.getByRole("textbox", { name: "End time (exclusive)" });
  await user.clear(start);
  await pasteText(user, start, "2026-07-24T16:03:00+02:00");
  await user.clear(end);
  await pasteText(user, end, "2026-07-24T14:04:00Z");
  const load = screen.getByRole("button", { name: "Load messages test" });
  await user.click(load);
  expect(host.commands.at(-1)).toMatchObject({
    command: "messages.start",
    payload: {
      topic: "test",
      mode: "time-window",
      maxMessages: 1_000,
      startTimeMs: Date.parse("2026-07-24T14:03:00Z"),
      endTimeMs: Date.parse("2026-07-24T14:04:00Z"),
    },
  });
  const request = {
    topic: "test",
    mode: "time-window" as const,
    maxMessages: 1_000,
    startTimeMs: Date.parse("2026-07-24T14:03:00Z"),
    endTimeMs: Date.parse("2026-07-24T14:04:00Z"),
  };
  const payload = {
    request,
    receivedMessages: 0,
    droppedMessages: 0,
    ruleEvaluation: { applicableRules: 0, omittedRules: 0, state: "ready" as const },
  };
  act(() =>
    host.emit({
      event: "consumption.state",
      version: HOST_PROTOCOL_VERSION,
      sequence: 3,
      payload: { ...payload, state: "fetching" },
    }),
  );
  await user.click(screen.getByRole("button", { name: "Cancel fetch test" }));
  expect(host.commands.at(-1)?.command).toBe("messages.stop");
  act(() =>
    host.emit({
      event: "consumption.state",
      version: HOST_PROTOCOL_VERSION,
      sequence: 4,
      payload: { ...payload, state: "stopped" },
    }),
  );
  expect(start).toHaveValue("2026-07-24T16:03:00+02:00");
  expect(load).toBeEnabled();
  const count = host.commands.length;
  await user.clear(end);
  await pasteText(user, end, "2026-07-24T14:02:00Z");
  expect(load).toBeDisabled();
  expect(screen.getByRole("alert")).toHaveTextContent("End time: must be after start time");
  await user.clear(end);
  await pasteText(user, end, "2026-07-24T14:04:00");
  expect(load).toBeDisabled();
  expect(screen.getByRole("alert")).toHaveTextContent("Z or an explicit UTC offset");
  expect(host.commands).toHaveLength(count);
});

it("keeps sample filtering explicit and sends a finite broker search with honest partial coverage", async () => {
  const host = new QueryHost();
  const user = userEvent.setup();
  render(<StreamSkopeApp host={host} />);
  act(() => {
    host.emit({
      event: "connection.state",
      payload: { connectionName: "Local search", state: "connected" },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });
    host.emit({
      event: "topics.changed",
      payload: { refreshedAt: "2026-07-25T14:00:00.000Z", state: "ready", topics: ["test"] },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    });
  });
  await user.click(await screen.findByRole("button", { name: "test" }));
  await user.click(screen.getByRole("button", { name: "Show message filters" }));
  const search = screen.getByRole("button", { name: "Search broker" });
  expect(search).toBeDisabled();
  expect(screen.getByText(/Filters below apply to the loaded sample/)).toBeVisible();
  await user.click(screen.getByRole("combobox", { name: "Read mode" }));
  await user.click(screen.getByRole("option", { name: "First N" }));
  await pasteText(
    user,
    screen.getByRole("textbox", { name: "Value or retained preview contains" }),
    "needle",
  );
  await user.click(search);
  const request = {
    topic: "test",
    mode: "earliest" as const,
    maxMessages: 1_000,
    search: { key: "", value: "needle", offset: "", timestamp: "", partition: null },
  };
  expect(host.commands.at(-1)).toMatchObject({ command: "messages.start", payload: request });
  act(() =>
    host.emit({
      event: "consumption.state",
      version: HOST_PROTOCOL_VERSION,
      sequence: 3,
      payload: {
        state: "empty",
        request,
        receivedMessages: 0,
        droppedMessages: 0,
        ruleEvaluation: { applicableRules: 0, omittedRules: 0, state: "ready" },
        coverage: {
          reason: "fetch-limit",
          scannedRecords: 15,
          scannedBytes: 45,
          matchedRecords: 0,
          unavailableRecords: 0,
          partitions: [{ partition: 0, startOffset: "0", endOffset: "100", nextOffset: "15" }],
        },
      },
    }),
  );
  expect(screen.getByRole("region", { name: "Read coverage" })).toHaveTextContent(
    "Partial read: fetch budget exhausted. 15 records scanned; 0 matches returned.",
  );
  expect(screen.queryByText("The snapshot contains no readable records.")).not.toBeInTheDocument();
  await user.click(screen.getByRole("checkbox", { name: "Rule matches only" }));
  expect(search).toBeDisabled();
});
