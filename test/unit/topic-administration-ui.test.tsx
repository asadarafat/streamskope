// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import type {
  HostCommand,
  HostCommandResponse,
  StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { topicAdministrationConfirmation } from "../../src/features/kafka/contracts/topic-administration";
import { TopicAdministrationAction } from "../../src/features/kafka/ui/TopicAdministrationAction";

afterEach(cleanup);
class Host implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  loseResult = false;
  execute<C extends HostCommand>(command: C): Promise<HostCommandResponse<C["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    if (command.command === "topics.change.review")
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "review",
          review: {
            planId: "plan",
            connectionName: "Reviewed connection",
            expiresAt: new Date(Date.now() + 120_000).toISOString(),
            input: command.payload,
            confirmation: topicAdministrationConfirmation(command.payload),
            baseline: {
              identity: {
                clusterId: "cluster",
                topicId: "01234567-89ab-cdef-0123-456789abcdef",
                topic: "orders",
              },
              partitions: 2,
              replicasSha256: "a".repeat(64),
              internal: false,
              deleteSupported: true,
              deletePermission: "allowed",
              expandPermission: "allowed",
            },
          },
        },
      });
    if (command.command === "topics.change.apply") {
      if (this.loseResult) return Promise.reject(new Error("transport lost"));
      const input = this.commands.find((c) => c.command === "topics.change.review");
      if (input?.command !== "topics.change.review") throw new Error("No review");
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "apply",
          outcome: {
            input: input.payload,
            state: "acknowledged",
            verification: "verified",
            cleanup: "confirmed",
            detail: "Independent readback verified",
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
    return Promise.reject(new Error("No external URL"));
  }
}
it("reviews an increased total, requires exact confirmation, preserves the outcome and invalidates changed inputs", async () => {
  const host = new Host(),
    changed = vi.fn(),
    user = userEvent.setup();
  render(
    <TopicAdministrationAction
      host={host}
      topic="orders"
      canWrite
      onChanged={changed}
      onDeleted={vi.fn()}
    />,
  );
  await user.click(screen.getByRole("button", { name: "Manage topic…" }));
  await user.type(screen.getByRole("textbox", { name: "New total partitions" }), "4");
  await user.click(screen.getByRole("button", { name: "Review change" }));
  expect(await screen.findByText("Review destination: Reviewed connection / orders")).toBeVisible();
  expect(host.commands).toHaveLength(1);
  expect(changed).not.toHaveBeenCalled();
  const apply = screen.getByRole("button", { name: "Apply reviewed change" });
  expect(apply).toBeDisabled();
  await user.type(
    screen.getByRole("textbox", { name: "Confirm exact topic change" }),
    "EXPAND orders TO 4",
  );
  await user.click(apply);
  expect(await screen.findByText(/Independent readback verified/u)).toBeVisible();
  expect(apply).toBeDisabled();
  expect(changed).toHaveBeenCalledOnce();
  fireEvent.change(screen.getByRole("textbox", { name: "New total partitions" }), {
    target: { value: "5" },
  });
  expect(screen.queryByText(/Review destination/u)).not.toBeInTheDocument();
  expect(apply).toBeDisabled();
});
it("warns about deletion, refreshes only after acknowledgement, and leaves the topic after closing the receipt", async () => {
  const host = new Host(),
    changed = vi.fn(),
    deleted = vi.fn(),
    user = userEvent.setup();
  render(
    <TopicAdministrationAction
      host={host}
      topic="orders"
      canWrite
      onChanged={changed}
      onDeleted={deleted}
    />,
  );
  await user.click(screen.getByRole("button", { name: "Manage topic…" }));
  await user.click(screen.getByRole("combobox", { name: "Action" }));
  await user.click(screen.getByRole("option", { name: "Delete topic" }));
  expect(screen.getByText(/There is no undo/u)).toBeVisible();
  await user.click(screen.getByRole("button", { name: "Review change" }));
  await user.type(
    await screen.findByRole("textbox", { name: "Confirm exact topic change" }),
    "DELETE orders",
  );
  await user.click(screen.getByRole("button", { name: "Apply reviewed change" }));
  expect(await screen.findByText(/Independent readback verified/u)).toBeVisible();
  expect(changed).not.toHaveBeenCalled();
  expect(deleted).not.toHaveBeenCalled();
  await user.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Close" }));
  expect(deleted).toHaveBeenCalledOnce();
  expect(changed).toHaveBeenCalledOnce();
});
it("blocks read-only entry and keeps lost replies from being sent twice", async () => {
  const host = new Host(),
    changed = vi.fn(),
    user = userEvent.setup();
  const view = render(
    <TopicAdministrationAction
      host={host}
      topic="orders"
      canWrite={false}
      onChanged={changed}
      onDeleted={vi.fn()}
    />,
  );
  expect(screen.getByRole("button", { name: "Manage topic…" })).toBeDisabled();
  view.rerender(
    <TopicAdministrationAction
      host={host}
      topic="orders"
      canWrite
      onChanged={changed}
      onDeleted={vi.fn()}
    />,
  );
  host.loseResult = true;
  await user.click(screen.getByRole("button", { name: "Manage topic…" }));
  await user.type(screen.getByRole("textbox", { name: "New total partitions" }), "3");
  await user.click(screen.getByRole("button", { name: "Review change" }));
  await user.type(
    await screen.findByRole("textbox", { name: "Confirm exact topic change" }),
    "EXPAND orders TO 3",
  );
  const apply = screen.getByRole("button", { name: "Apply reviewed change" });
  await user.click(apply);
  expect(await screen.findByText(/Inspect Activity and the topic/u)).toBeVisible();
  expect(apply).toBeDisabled();
  fireEvent.click(apply);
  expect(host.commands.filter((c) => c.command === "topics.change.apply")).toHaveLength(1);
  expect(changed).not.toHaveBeenCalled();
});
