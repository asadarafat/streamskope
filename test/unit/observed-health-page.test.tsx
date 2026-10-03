// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import type { HostCommand, StreamSkopeHost } from "../../src/features/kafka/contracts";
import { ObservedHealthPage } from "../../src/features/kafka/ui/ObservedHealthPage";
import { StreamSkopeThemeProvider } from "../../src/platform/ui/StreamSkopeThemeProvider";
import { testHostExecute } from "../support/host-response";

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});
it("does not collect on entry, requires clear confirmation, and sends cancellation on navigation", async () => {
  const commands: HostCommand[] = [];
  const host: StreamSkopeHost = {
    subscribe: () => () => undefined,
    openExternalUrl: () => Promise.reject(new Error("Unexpected URL")),
    execute: testHostExecute((command) => {
      commands.push(command);
      const snapshot = { schemaVersion: 1, durability: "session", series: [] };
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "c",
          ...(command.command === "observations.history" || command.command === "observations.clear"
            ? { snapshot }
            : {}),
        },
      });
    }),
  };
  const view = render(
    <StreamSkopeThemeProvider>
      <ObservedHealthPage host={host} />
    </StreamSkopeThemeProvider>,
  );
  await waitFor(() => expect(commands).toHaveLength(1));
  expect(commands[0]?.command).toBe("observations.history");
  expect(screen.getByRole("button", { name: "Clear all observation history" })).toBeDisabled();
  fireEvent.change(screen.getByLabelText("Clear all history confirmation"), {
    target: { value: "CLEAR HISTORY" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Clear all observation history" }));
  await waitFor(() => expect(commands.some((c) => c.command === "observations.clear")).toBe(true));
  expect(commands.some((c) => c.command === "observations.capture")).toBe(false);
  view.unmount();
  expect(commands.at(-1)?.command).toBe("observations.cancel");
});

it("runs only the opted-in timer and cancels it immediately when stopped", async () => {
  vi.useFakeTimers();
  const commands: HostCommand[] = [];
  const host: StreamSkopeHost = {
    subscribe: () => () => undefined,
    openExternalUrl: () => Promise.reject(new Error("Unexpected URL")),
    execute: testHostExecute((command) => {
      commands.push(command);
      if (command.command === "observations.capture")
        return Promise.resolve({
          command: command.command,
          id: command.id,
          version: command.version,
          ok: false,
          error: {
            code: "VALIDATION",
            stage: "kafka",
            correlationId: "c",
            retryable: false,
            activeStateChanged: false,
            summary: "Observation unavailable",
            recovery: "Check access.",
          },
        });
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "c",
          ...(command.command === "observations.history"
            ? { snapshot: { schemaVersion: 1, durability: "session", series: [] } }
            : {}),
        },
      });
    }),
  };
  render(
    <StreamSkopeThemeProvider>
      <ObservedHealthPage host={host} />
    </StreamSkopeThemeProvider>,
  );
  await act(async () => {
    await vi.advanceTimersByTimeAsync(0);
  });
  fireEvent.change(screen.getByLabelText("Observed topic"), { target: { value: "events" } });
  fireEvent.click(screen.getByRole("button", { name: "Start observing" }));
  await act(async () => {
    await vi.advanceTimersByTimeAsync(0);
  });
  expect(commands.filter((c) => c.command === "observations.capture")).toHaveLength(1);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(10000);
  });
  expect(commands.filter((c) => c.command === "observations.capture")).toHaveLength(2);
  fireEvent.click(screen.getByRole("button", { name: "Stop observing" }));
  await act(async () => {
    await vi.advanceTimersByTimeAsync(60000);
  });
  expect(commands.filter((c) => c.command === "observations.capture")).toHaveLength(2);
  expect(commands.some((c) => c.command === "observations.cancel")).toBe(true);
});
