// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import type { HostCommand, StreamSkopeHost } from "../../src/features/kafka/contracts";
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
                    confirmation: "create orders",
                    before: null,
                  },
                }
              : {
                  outcome: {
                    state: "acknowledged",
                    detail: "Accepted; refresh task state",
                    observed: null,
                  },
                };
      return Promise.resolve({ ...base, result: { correlationId: "c", ...result } });
    }),
  };
  render(
    <StreamSkopeThemeProvider>
      <ConnectPage host={host} canWrite onOpenTopic={vi.fn()} />
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
