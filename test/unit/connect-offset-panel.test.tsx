// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEventListener,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { ConnectOffsetsPanel } from "../../src/features/kafka/ui/ConnectOffsetsPanel";
import { StreamSkopeThemeProvider } from "../../src/platform/ui/StreamSkopeThemeProvider";
import { testHostExecute } from "../support/host-response";

afterEach(cleanup);
function fixture(mode: "lost" | "unresolved" | "late"): {
  readonly commands: HostCommand[];
  reconnect(): void;
  finish(): void;
} {
  const commands: HostCommand[] = [];
  let listener: HostEventListener | undefined,
    finish = (): void => undefined;
  const snapshot = {
    name: "orders",
    connectionName: "Test",
    status: "available",
    snapshotId: "snapshot",
    expiresAt: "2026-10-10T17:00:00.000Z",
    clusterId: "cluster",
    workerVersion: "4.3.1",
    connectorState: "STOPPED",
    mapping: "kafka-sink",
    positions: [{ partitionRef: "partition", label: "orders · partition 0", position: 2 }],
    observedAt: "2026-10-10T16:58:00.000Z",
    detail: "Observed positions",
  };
  const host: StreamSkopeHost = {
    subscribe: (value): (() => void) => {
      listener = value;
      return (): void => undefined;
    },
    openExternalUrl: (): Promise<never> => Promise.reject(new Error("Unexpected external URL")),
    execute: testHostExecute(async (command) => {
      commands.push(command);
      const base = {
        id: command.id,
        command: command.command,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
      };
      if (command.command === "connect.offsets.inspect") {
        const response = { ...base, result: { correlationId: "corr", snapshot } };
        if (mode === "late")
          return new Promise<unknown>((resolve) => {
            finish = (): void => resolve(response);
          });
        return response;
      }
      if (command.command === "connect.offsets.review")
        return {
          ...base,
          result: {
            correlationId: "corr",
            review: {
              planId: "plan",
              expiresAt: snapshot.expiresAt,
              name: "orders",
              connectionName: "Test",
              clusterId: "cluster",
              mapping: "kafka-sink",
              input: command.payload,
              changes: [
                { label: "orders · partition 0", before: 2, after: command.payload.position },
              ],
              confirmation: "set OFFSETS orders",
            },
          },
        };
      if (command.command === "connect.offsets.apply") {
        if (mode === "lost") throw new Error("Reply lost");
        return {
          ...base,
          result: {
            correlationId: "corr",
            outcome: {
              planId: command.payload.planId,
              confirmation: command.payload.confirmation,
              state: "acknowledged",
              dispatch: "attempted",
              verification: "unavailable",
              cleanup: "unresolved",
              detail: "Original cleanup pending",
              observed: null,
            },
          },
        };
      }
      throw new Error("Unexpected command");
    }),
  };
  render(
    <StreamSkopeThemeProvider>
      <ConnectOffsetsPanel host={host} name="orders" connectionName="Test" canWrite />
    </StreamSkopeThemeProvider>,
  );
  return {
    commands,
    finish: (): void => finish(),
    reconnect: (): void => {
      listener?.({
        event: "connection.state",
        version: HOST_PROTOCOL_VERSION,
        sequence: 1,
        payload: { state: "connecting", connectionName: "Test" },
      });
      listener?.({
        event: "connection.state",
        version: HOST_PROTOCOL_VERSION,
        sequence: 2,
        payload: { state: "connected", connectionName: "Test" },
      });
    },
  };
}
async function apply(): Promise<void> {
  fireEvent.click(screen.getByRole("button", { name: "Inspect connector offsets" }));
  fireEvent.change(await screen.findByRole("textbox", { name: "New offset position" }), {
    target: { value: "1" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Review offset change" }));
  fireEvent.change(await screen.findByRole("textbox", { name: "Confirm exact offset change" }), {
    target: { value: "set OFFSETS orders" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Apply reviewed offset change" }));
}
it("retains an unavailable action result as uncertainty and never offers a resend", async () => {
  const f = fixture("lost");
  await apply();
  await screen.findByText(/it may have been sent/u);
  expect(screen.getByRole("button", { name: "Apply reviewed offset change" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Dismiss offset receipt" })).toBeDisabled();
  fireEvent.click(screen.getByRole("button", { name: "Apply reviewed offset change" }));
  expect(f.commands.filter((command) => command.command === "connect.offsets.apply")).toHaveLength(
    1,
  );
});
it("keeps actual ACK and unresolved cleanup visible without another review", async () => {
  const f = fixture("unresolved");
  await apply();
  await screen.findByText(
    /acknowledged · dispatch attempted · readback unavailable · cleanup unresolved/u,
  );
  expect(screen.getByRole("button", { name: "Dismiss offset receipt" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Review offset change" })).toBeDisabled();
  expect(f.commands.filter((command) => command.command === "connect.offsets.apply")).toHaveLength(
    1,
  );
});
it("does not project an old snapshot into a synchronously batched same-name reconnect", async () => {
  const f = fixture("late");
  fireEvent.click(screen.getByRole("button", { name: "Inspect connector offsets" }));
  await act(async () => {
    f.reconnect();
    f.finish();
    await Promise.resolve();
  });
  expect(
    screen.queryByRole("table", { name: "Observed connector offsets" }),
  ).not.toBeInTheDocument();
  expect(screen.queryByRole("textbox", { name: "New offset position" })).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Inspect connector offsets" })).toBeEnabled();
});
