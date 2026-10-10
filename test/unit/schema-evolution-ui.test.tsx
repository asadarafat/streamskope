// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import type {
  HostCommand,
  HostCommandResponse,
  SchemaVersionDetail,
  StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { SchemaEvolutionDialog } from "../../src/features/kafka/ui/SchemaEvolutionDialog";

const schema: SchemaVersionDetail = {
  id: 1,
  version: 1,
  subject: "events",
  schemaType: "AVRO",
  schema: '"string"',
  references: [],
};
class Host implements StreamSkopeHost {
  commands: HostCommand[] = [];
  defer = false;
  settle?: () => void;
  execute<C extends HostCommand>(command: C): Promise<HostCommandResponse<C["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    const envelope = {
      command: command.command,
      version: command.version,
      id: command.id,
      ok: true as const,
    };
    if (command.command === "schemas.change.review") {
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
            before: schema,
            policy: { globalLevel: "BACKWARD", subjectLevel: null, effectiveLevel: "BACKWARD" },
            compatible: true,
          },
        },
      };
      if (this.defer)
        return new Promise((resolve) => {
          this.settle = (): void => resolve(response);
        });
      return Promise.resolve(response);
    }
    if (command.command === "schemas.change.apply")
      return Promise.resolve({
        ...envelope,
        command: command.command,
        result: {
          correlationId: "apply",
          outcome: {
            state: "acknowledged",
            verification: "unavailable",
            id: 2,
            observed: null,
            detail: "Acknowledged; readback unavailable. Do not repeat the write to refresh it.",
          },
        },
      });
    throw new Error("Unexpected command");
  }
  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("Unused"));
  }
  subscribe(): () => void {
    return () => undefined;
  }
}
afterEach(cleanup);
it("pins the starting writer, shows a diff, clears review after edits and requires exact confirmation", async () => {
  const host = new Host(),
    user = userEvent.setup();
  render(
    <SchemaEvolutionDialog
      host={host}
      initial={schema}
      enabled
      onClose={() => undefined}
      onRegistered={() => undefined}
    />,
  );
  expect(screen.getByRole("textbox", { name: "Subject" })).toBeDisabled();
  fireEvent.change(screen.getByRole("textbox", { name: "Proposed schema" }), {
    target: { value: '"int"' },
  });
  await user.click(screen.getByRole("button", { name: "Review schema change" }));
  expect(await screen.findByRole("table", { name: "Differences" })).toBeVisible();
  expect(host.commands[0]).toMatchObject({
    command: "schemas.change.review",
    payload: { expectedWriter: { id: 1, version: 1 } },
  });
  const apply = screen.getByRole("button", { name: "Register reviewed schema" });
  expect(apply).toBeDisabled();
  await user.type(
    screen.getByRole("textbox", { name: "Type events to confirm registration" }),
    "events",
  );
  expect(apply).toBeEnabled();
  fireEvent.change(screen.getByRole("textbox", { name: "Pinned references" }), {
    target: { value: '[{"name":"dep","subject":"dependency","version":1}]' },
  });
  expect(apply).toBeDisabled();
  expect(screen.queryByRole("table", { name: "Differences" })).not.toBeInTheDocument();
  expect(host.commands.some((command) => command.command === "schemas.change.apply")).toBe(false);
});
it("retains acknowledged results and offers refresh without another registration", async () => {
  const host = new Host(),
    user = userEvent.setup(),
    refresh = vi.fn();
  render(
    <SchemaEvolutionDialog
      host={host}
      initial={schema}
      enabled
      onClose={() => undefined}
      onRegistered={refresh}
    />,
  );
  await user.click(screen.getByRole("button", { name: "Review schema change" }));
  await user.type(
    await screen.findByRole("textbox", { name: "Type events to confirm registration" }),
    "events",
  );
  await user.click(screen.getByRole("button", { name: "Register reviewed schema" }));
  expect(await screen.findByRole("status")).toHaveTextContent("Acknowledged; readback unavailable");
  expect(
    screen.queryByRole("button", { name: "Register reviewed schema" }),
  ).not.toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Refresh subject" }));
  expect(refresh).toHaveBeenCalledWith("events");
  expect(
    host.commands.filter((command) => command.command === "schemas.change.apply"),
  ).toHaveLength(1);
});
it("ignores a late review after connection authority changes and permits closing", async () => {
  const host = new Host(),
    user = userEvent.setup(),
    close = vi.fn();
  host.defer = true;
  const view = render(
    <SchemaEvolutionDialog
      host={host}
      initial={schema}
      enabled
      onClose={close}
      onRegistered={() => undefined}
    />,
  );
  await user.click(screen.getByRole("button", { name: "Review schema change" }));
  view.rerender(
    <SchemaEvolutionDialog
      host={host}
      initial={schema}
      enabled={false}
      onClose={close}
      onRegistered={() => undefined}
    />,
  );
  host.settle?.();
  await waitFor(() =>
    expect(
      screen.queryByRole("textbox", { name: "Type events to confirm registration" }),
    ).not.toBeInTheDocument(),
  );
  await user.click(screen.getByRole("button", { name: "Close" }));
  expect(close).toHaveBeenCalledOnce();
});
