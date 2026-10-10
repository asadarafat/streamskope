// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";

import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { ConnectPage } from "../../src/features/kafka/ui/ConnectPage";
import { StreamSkopeThemeProvider } from "../../src/platform/ui/StreamSkopeThemeProvider";
import { testHostExecute } from "../support/host-response";

afterEach(cleanup);
it("keeps edits non-mutating until exact reviewed confirmation and prevents a second apply", async () => {
  const commands: HostCommand[] = [];
  const host: StreamSkopeHost = {
    subscribe: () => () => undefined,
    openExternalUrl: () => Promise.reject(new Error("Unexpected external URL")),
    execute: testHostExecute((command) => {
      commands.push(command);
      const base = { command: command.command, id: command.id, version: command.version, ok: true };
      const result =
        command.command === "connect.list"
          ? { inventory: { names: [], plugins: ["Sink"] } }
          : command.command === "connect.validate"
            ? { validation: { issues: [] } }
            : command.command === "connect.review"
              ? {
                  review: {
                    planId: "plan",
                    expiresAt: "2026-10-03T16:00:00.000Z",
                    name: "orders",
                    action: "create",
                    fields: ["connector.class"],
                    removedFields: [],
                    connectionName: "Test",
                    confirmation: "create orders",
                    before: null,
                  },
                }
              : {
                  outcome: {
                    state: "acknowledged",
                    dispatch: "attempted",
                    verification: "unavailable",
                    cleanup: "confirmed",
                    detail: "Accepted; refresh task state",
                    observed: null,
                  },
                };
      return Promise.resolve({ ...base, result: { correlationId: "c", ...result } });
    }),
  };
  render(
    <StreamSkopeThemeProvider>
      <ConnectPage host={host} connectionName="Test" canWrite onOpenTopic={vi.fn()} />
    </StreamSkopeThemeProvider>,
  );
  await screen.findByText("Installed classes: Sink");
  fireEvent.change(screen.getByLabelText("Connector name"), { target: { value: "orders" } });
  fireEvent.change(screen.getByLabelText("Connector configuration (JSON string map)"), {
    target: { value: '{"connector.class":"Sink"}' },
  });
  fireEvent.click(screen.getByRole("button", { name: "Validate configuration" }));
  await screen.findByText("Validation passed. No connector change was made.");
  expect(commands.some((c) => c.command === "connect.apply")).toBe(false);
  fireEvent.click(screen.getByRole("button", { name: "Review action" }));
  const confirmation = await screen.findByLabelText("Type create orders to confirm");
  expect(screen.getByRole("button", { name: "Apply reviewed action" })).toBeDisabled();
  fireEvent.change(confirmation, { target: { value: "create orders" } });
  fireEvent.click(screen.getByRole("button", { name: "Apply reviewed action" }));
  await screen.findByText("acknowledged: Accepted; refresh task state");
  expect(commands.filter((c) => c.command === "connect.apply")).toHaveLength(1);
  expect(screen.getByRole("button", { name: "Apply reviewed action" })).toBeDisabled();
});

function editingHost(commands: HostCommand[]): StreamSkopeHost {
  return {
    subscribe: () => () => undefined,
    openExternalUrl: () => Promise.reject(new Error("Unexpected external URL")),
    execute: testHostExecute((command) => {
      commands.push(command);
      const result =
        command.command === "connect.list"
          ? { inventory: { names: ["orders"], plugins: ["Sink"] } }
          : command.command === "connect.load"
            ? {
                detail: {
                  name: "orders",
                  state: "RUNNING",
                  tasks: [],
                  config: { password: "[protected — retained unless replaced]" },
                  dlq: null,
                  observedAt: "2026-10-10T00:00:00.000Z",
                },
              }
            : command.command === "connect.review"
              ? {
                  review: {
                    planId: "review",
                    expiresAt: "2026-10-10T17:00:00.000Z",
                    name: "orders",
                    action: "update",
                    fields: Object.keys(command.payload.config).sort(),
                    removedFields: [...(command.payload.remove ?? [])].sort(),
                    connectionName: "Test",
                    confirmation: "update orders",
                    before: {
                      name: "orders",
                      state: "RUNNING",
                      tasks: [],
                      config: { password: "[protected — retained unless replaced]" },
                      dlq: null,
                      observedAt: "2026-10-10T00:00:00.000Z",
                    },
                  },
                }
              : { validation: { issues: [] } };
      return Promise.resolve({
        id: command.id,
        command: command.command,
        version: command.version,
        ok: true,
        result: { correlationId: "c", ...result },
      });
    }),
  };
}
it("reviews explicit removals without copying protected placeholders and keeps read-only writes blocked", async () => {
  const commands: HostCommand[] = [],
    host = editingHost(commands);
  render(
    <StreamSkopeThemeProvider>
      <ConnectPage host={host} connectionName="Test" canWrite={false} onOpenTopic={vi.fn()} />
    </StreamSkopeThemeProvider>,
  );
  await screen.findByText("Installed classes: Sink");
  fireEvent.mouseDown(screen.getByLabelText("Existing connector"));
  fireEvent.click(await screen.findByRole("option", { name: "orders" }));
  const config = await screen.findByLabelText("Configuration changes (JSON string map)"),
    remove = screen.getByLabelText("Fields to remove (JSON string array)");
  expect(config).toHaveValue("{}");
  expect(remove).toHaveValue("[]");
  fireEvent.change(config, {
    target: { value: '{"password":"[protected — retained unless replaced]"}' },
  });
  fireEvent.click(screen.getByRole("button", { name: "Review action" }));
  await screen.findByText(/Check the configured Connect endpoint/u);
  expect(commands.filter((c) => c.command === "connect.review")).toHaveLength(0);
  fireEvent.change(config, { target: { value: '{"tasks.max":"2"}' } });
  fireEvent.change(remove, { target: { value: '["password"]' } });
  fireEvent.click(screen.getByRole("button", { name: "Review action" }));
  const confirmation = await screen.findByLabelText("Type update orders to confirm");
  expect(screen.getByText(/Remove fields: password/u)).toBeVisible();
  expect(commands.find((c) => c.command === "connect.review")?.payload).toMatchObject({
    remove: ["password"],
    config: { "tasks.max": "2" },
  });
  fireEvent.change(confirmation, { target: { value: "update orders" } });
  expect(screen.getByRole("button", { name: "Apply reviewed action" })).toBeDisabled();
  expect(commands.filter((c) => c.command === "connect.apply")).toHaveLength(0);
});
it("does not project an old inventory or edited form into a replacement host connection", async () => {
  let resolveOld: ((value: unknown) => void) | undefined;
  let reads = 0;
  const old: StreamSkopeHost = {
    subscribe: () => () => undefined,
    openExternalUrl: () => Promise.reject(new Error()),
    execute: testHostExecute((command) => {
      if (reads++ === 0)
        return Promise.resolve({
          id: command.id,
          command: command.command,
          version: command.version,
          ok: true,
          result: { correlationId: "old", inventory: { names: [], plugins: ["OldOnly"] } },
        });
      return new Promise((resolve) => {
        resolveOld = (result: unknown): void =>
          resolve({
            id: command.id,
            command: command.command,
            version: command.version,
            ok: true,
            result,
          });
      });
    }),
  };
  const view = render(
    <StreamSkopeThemeProvider>
      <ConnectPage host={old} connectionName="Old" canWrite onOpenTopic={vi.fn()} />
    </StreamSkopeThemeProvider>,
  );
  await screen.findByText("Installed classes: OldOnly");
  fireEvent.change(screen.getByLabelText("Connector name"), { target: { value: "old-edit" } });
  fireEvent.click(screen.getByRole("button", { name: "Refresh connectors" }));
  const commands: HostCommand[] = [];
  view.rerender(
    <StreamSkopeThemeProvider>
      <ConnectPage
        host={editingHost(commands)}
        connectionName="Test"
        canWrite
        onOpenTopic={vi.fn()}
      />
    </StreamSkopeThemeProvider>,
  );
  await screen.findByText("Installed classes: Sink");
  expect(screen.getByLabelText("Connector name")).toHaveValue("");
  await act(async () => {
    resolveOld!({ correlationId: "old", inventory: { names: ["foreign"], plugins: ["OldOnly"] } });
    await Promise.resolve();
  });
  expect(screen.getByText("Installed classes: Sink")).toBeVisible();
  expect(screen.queryByText(/OldOnly/u)).toBeNull();
});

it("invalidates edited forms on synchronously batched reconnects to the same named profile", async () => {
  const commands: HostCommand[] = [];
  let receive!: (event: import("../../src/features/kafka/contracts").HostEvent) => void;
  const base = editingHost(commands),
    host = {
      ...base,
      subscribe: (listener: typeof receive): (() => void) => {
        receive = listener;
        return () => undefined;
      },
    };
  render(
    <StreamSkopeThemeProvider>
      <ConnectPage host={host} connectionName="Test" canWrite onOpenTopic={vi.fn()} />
    </StreamSkopeThemeProvider>,
  );
  await screen.findByText("Installed classes: Sink");
  fireEvent.change(screen.getByLabelText("Connector name"), {
    target: { value: "edited-before-reconnect" },
  });
  await act(async () => {
    receive({
      event: "connection.state",
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
      payload: { state: "connecting", connectionName: "Test" },
    });
    receive({
      event: "connection.state",
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
      payload: { state: "connected", connectionName: "Test" },
    });
    await Promise.resolve();
  });
  expect(screen.getByLabelText("Connector name")).toHaveValue("");
  expect(commands.filter((command) => command.command === "connect.list")).toHaveLength(2);
  expect(commands.filter((command) => command.command === "connect.apply")).toHaveLength(0);
});
it("keeps unresolved original cleanup visible and disables starting another review", async () => {
  const commands: HostCommand[] = [],
    base = editingHost(commands);
  const host: StreamSkopeHost = {
    ...base,
    execute: testHostExecute((command) =>
      command.command === "connect.apply"
        ? Promise.resolve({
            id: command.id,
            command: command.command,
            version: command.version,
            ok: true,
            result: {
              correlationId: "receipt",
              outcome: {
                state: "acknowledged",
                dispatch: "attempted",
                verification: "unavailable",
                cleanup: "unresolved",
                detail: "Accepted by original worker",
                observed: null,
              },
            },
          })
        : base.execute(command),
    ),
  };
  render(
    <StreamSkopeThemeProvider>
      <ConnectPage host={host} connectionName="Test" canWrite onOpenTopic={vi.fn()} />
    </StreamSkopeThemeProvider>,
  );
  await screen.findByText("Installed classes: Sink");
  fireEvent.mouseDown(screen.getByLabelText("Existing connector"));
  fireEvent.click(await screen.findByRole("option", { name: "orders" }));
  fireEvent.change(await screen.findByLabelText("Configuration changes (JSON string map)"), {
    target: { value: '{"tasks.max":"2"}' },
  });
  fireEvent.click(screen.getByRole("button", { name: "Review action" }));
  fireEvent.change(await screen.findByLabelText("Type update orders to confirm"), {
    target: { value: "update orders" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Apply reviewed action" }));
  await screen.findByText("Readback: unavailable. Original request cleanup: unresolved.");
  expect(
    screen.getByRole("button", { name: "Dismiss receipt and start another review" }),
  ).toBeDisabled();
  expect(screen.getByRole("button", { name: "Review action" })).toBeDisabled();
});
