// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import {
  type HostCommand,
  type HostCommandResponse,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { ReviewedWriteAction } from "../../src/features/kafka/ui/ReviewedWriteAction";

afterEach(cleanup);
class Host implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  loseResponse = false;
  execute<C extends HostCommand>(command: C): Promise<HostCommandResponse<C["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    if (command.command === "writes.review")
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "review",
          review: {
            planId: "reviewed-once",
            connectionName: "Selected cluster",
            expiresAt: "2026-10-03T03:00:00.000Z",
            input: command.payload,
          },
        },
      });
    if (command.command === "writes.apply") {
      if (this.loseResponse) {
        this.loseResponse = false;
        throw new Error("Connection lost");
      }
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "write",
          outcome: {
            state: "acknowledged",
            detail: "Kafka accepted the change.",
            receipt: { topic: "orders", partition: 0, offset: "42" },
            verification: "verified",
          },
        },
      });
    }
    throw new Error("Unexpected command");
  }
  subscribe(): () => void {
    return () => undefined;
  }
  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("Unexpected URL"));
  }
}

it("reviews topic properties before confirmation and refreshes only after acknowledgement", async () => {
  const host = new Host();
  const refresh = vi.fn();
  const user = userEvent.setup();
  render(<ReviewedWriteAction host={host} disabled={false} onCreated={refresh} />);
  await user.click(screen.getByRole("button", { name: "Create topic" }));
  await user.type(screen.getByRole("textbox", { name: "Topic name" }), "orders");
  fireEvent.change(screen.getByRole("textbox", { name: "Partitions" }), { target: { value: "3" } });
  await user.click(screen.getByRole("button", { name: "Review topic" }));
  expect(await screen.findByText(/Review destination: Selected cluster \/ orders/u)).toBeVisible();
  expect(host.commands.map(({ command }) => command)).toEqual(["writes.review"]);
  expect(refresh).not.toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: "Confirm create" }));
  expect(await screen.findByText("Acknowledged")).toBeVisible();
  expect(host.commands.at(-1)).toMatchObject({
    command: "writes.apply",
    payload: { planId: "reviewed-once" },
  });
  expect(refresh).toHaveBeenCalledTimes(1);
  expect(screen.queryByRole("button", { name: "Confirm create" })).not.toBeInTheDocument();
});

it("keeps nulls and ordered headers and checks the same attempt after response loss", async () => {
  const host = new Host();
  host.loseResponse = true;
  const user = userEvent.setup();
  render(<ReviewedWriteAction host={host} disabled={false} topic="orders" />);
  await user.click(screen.getByRole("button", { name: "Produce message" }));
  await user.click(screen.getByRole("switch", { name: "Tombstone (null value)" }));
  fireEvent.change(screen.getByRole("textbox", { name: "Ordered headers (JSON array)" }), {
    target: { value: '[{"key":"x","value":"one"},{"key":"x","value":null}]' },
  });
  await user.click(screen.getByRole("button", { name: "Review message" }));
  expect(host.commands[0]).toMatchObject({
    payload: {
      record: {
        key: null,
        value: null,
        headers: [
          { key: "eA==", value: "b25l" },
          { key: "eA==", value: null },
        ],
      },
    },
  });
  await user.click(await screen.findByRole("button", { name: "Confirm produce" }));
  expect(await screen.findByText(/The host response was lost/u)).toBeVisible();
  expect(screen.queryByRole("button", { name: "Edit" })).not.toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Check attempt result" }));
  expect(await screen.findByText(/offset 42/u)).toBeVisible();
  expect(
    host.commands.filter(({ command }) => command === "writes.apply").map(({ payload }) => payload),
  ).toEqual([{ planId: "reviewed-once" }, { planId: "reviewed-once" }]);
});

it("disables the write entry point when disconnected or read-only", () => {
  render(<ReviewedWriteAction host={new Host()} disabled topic="orders" />);
  expect(screen.getByRole("button", { name: "Produce message" })).toBeDisabled();
});
