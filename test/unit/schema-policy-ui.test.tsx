// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import type {
  HostCommand,
  HostCommandResponse,
  SchemaVersionDetail,
  StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import type { SchemaCompatibilityPolicy } from "../../src/features/kafka/contracts/schema-changes";
import { SchemaPolicyDialog } from "../../src/features/kafka/ui/SchemaPolicyDialog";

const writer: SchemaVersionDetail = {
  id: 1,
  version: 1,
  subject: "events",
  schemaType: "AVRO",
  schema: '"string"',
  references: [],
};
class Host implements StreamSkopeHost {
  commands: HostCommand[] = [];
  policy: SchemaCompatibilityPolicy = {
    globalLevel: "BACKWARD",
    subjectLevel: null,
    effectiveLevel: "BACKWARD",
  };
  defer = false;
  settle?: () => void;
  execute<C extends HostCommand>(command: C): Promise<HostCommandResponse<C["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    const envelope = { id: command.id, version: command.version, ok: true as const };
    if (command.command === "schemas.policy.load")
      return Promise.resolve({
        ...envelope,
        command: command.command,
        result: {
          correlationId: "read",
          baseline: { writer: { id: 1, version: 1 }, policy: { ...this.policy } },
        },
      });
    if (command.command === "schemas.policy.review") {
      const next = command.payload.change.mode === "set" ? command.payload.change.level : null;
      const response: HostCommandResponse = {
        ...envelope,
        command: command.command,
        result: {
          correlationId: "review",
          review: {
            planId: "p",
            expiresAt: new Date(Date.now() + 120000).toISOString(),
            connectionName: "Local",
            input: command.payload,
            before: { writer: { id: 1, version: 1 }, policy: { ...this.policy } },
            after: {
              globalLevel: "BACKWARD",
              subjectLevel: next,
              effectiveLevel: next ?? "BACKWARD",
            },
          },
        },
      };
      if (this.defer)
        return new Promise((resolve) => {
          this.settle = (): void => resolve(response);
        });
      return Promise.resolve(response);
    }
    if (command.command === "schemas.policy.apply") {
      this.policy = { globalLevel: "BACKWARD", subjectLevel: "FULL", effectiveLevel: "FULL" };
      return Promise.resolve({
        ...envelope,
        command: command.command,
        result: {
          correlationId: "apply",
          outcome: {
            state: "acknowledged",
            verification: "unavailable",
            acknowledgedLevel: "FULL",
            observed: null,
            detail: "Acknowledged; read current policy without repeating the write.",
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
    return Promise.reject(new Error("unused"));
  }
}
afterEach(cleanup);
async function select(value: string): Promise<void> {
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Subject policy" }));
  await userEvent.click(await screen.findByRole("option", { name: value }));
}
it("shows policy provenance, compares desired override and requires exact subject confirmation", async () => {
  const host = new Host();
  render(<SchemaPolicyDialog host={host} writer={writer} enabled onClose={vi.fn()} />);
  const table = await screen.findByRole("table", { name: "Compatibility policy comparison" });
  expect(within(table).getByText("Global default")).toBeVisible();
  expect(within(table).getByText("Inherited")).toBeVisible();
  await select("FULL");
  await userEvent.click(screen.getByRole("button", { name: "Review policy change" }));
  await screen.findByLabelText("Type events to confirm policy change");
  expect(within(table).getByText("After")).toBeVisible();
  expect(host.commands.filter((x) => x.command === "schemas.policy.apply")).toHaveLength(0);
  const apply = screen.getByRole("button", { name: "Apply reviewed policy" });
  expect(apply).toBeDisabled();
  fireEvent.change(screen.getByLabelText("Type events to confirm policy change"), {
    target: { value: "events" },
  });
  expect(apply).toBeEnabled();
  await select("NONE");
  expect(screen.queryByLabelText("Type events to confirm policy change")).not.toBeInTheDocument();
  expect(apply).toBeDisabled();
  expect(screen.getByText(/NONE disables compatibility/)).toBeVisible();
});
it("retains acknowledgement when readback is unavailable; refresh only reads policy", async () => {
  const host = new Host();
  render(<SchemaPolicyDialog host={host} writer={writer} enabled onClose={vi.fn()} />);
  await screen.findByRole("table");
  await select("FULL");
  await userEvent.click(screen.getByRole("button", { name: "Review policy change" }));
  fireEvent.change(await screen.findByLabelText("Type events to confirm policy change"), {
    target: { value: "events" },
  });
  await userEvent.click(screen.getByRole("button", { name: "Apply reviewed policy" }));
  await screen.findByText("acknowledged · unavailable");
  await userEvent.click(screen.getByRole("button", { name: "Read current policy" }));
  await waitFor(() =>
    expect(host.commands.filter((x) => x.command === "schemas.policy.load")).toHaveLength(2),
  );
  expect(host.commands.filter((x) => x.command === "schemas.policy.apply")).toHaveLength(1);
  expect(host.commands.filter((x) => x.command === "schemas.policy.load")).toHaveLength(2);
  expect(screen.getByText("acknowledged · unavailable")).toBeVisible();
});
it("ignores late review after connection authority changes and permits close", async () => {
  const host = new Host(),
    onClose = vi.fn();
  const view = render(<SchemaPolicyDialog host={host} writer={writer} enabled onClose={onClose} />);
  await screen.findByRole("table");
  host.defer = true;
  await select("FULL");
  await userEvent.click(screen.getByRole("button", { name: "Review policy change" }));
  await waitFor(() => expect(host.settle).toBeDefined());
  view.rerender(
    <SchemaPolicyDialog host={host} writer={writer} enabled={false} onClose={onClose} />,
  );
  host.settle?.();
  await waitFor(() =>
    expect(screen.queryByLabelText("Type events to confirm policy change")).not.toBeInTheDocument(),
  );
  await userEvent.click(screen.getByRole("button", { name: "Close" }));
  expect(onClose).toHaveBeenCalledTimes(1);
  expect(host.commands.filter((x) => x.command === "schemas.policy.apply")).toHaveLength(0);
});
