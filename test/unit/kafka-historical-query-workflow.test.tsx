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
