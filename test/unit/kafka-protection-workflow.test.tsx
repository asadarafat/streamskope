// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";

import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it } from "vitest";

import {
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  type HostCommand,
  type HostCommandResponse,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { RecordProtectionPanel } from "../../src/features/kafka/ui/RecordProtectionPanel";
import { testHostAccepted } from "../support/host-response";

afterEach(cleanup);

class Host implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  execute<C extends HostCommand>(command: C): Promise<HostCommandResponse<C["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    if (command.command === "preferences.update")
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "protected",
          snapshot: {
            ...snapshot,
            preferences: {
              ...snapshot.preferences,
              protection: command.payload.patch.protection ?? snapshot.preferences.protection,
            },
          },
        },
      });
    return Promise.resolve(testHostAccepted(command, "protected"));
  }
  subscribe(): () => void {
    return () => undefined;
  }
  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("Unexpected URL"));
  }
}
const snapshot = {
  preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  store: { durability: "durable", state: "ready" },
} as const;

it("submits explicit protection settings and blocks invalid paths before sending", async () => {
  const host = new Host();
  const user = userEvent.setup();
  render(<RecordProtectionPanel host={host} snapshot={snapshot} disconnected />);
  await user.click(screen.getByRole("switch", { name: "Read-only mode" }));
  await user.type(
    screen.getByRole("textbox", { name: "Decoded JSON value paths to mask" }),
    "$.secret",
  );
  expect(screen.getByRole("button", { name: "Save protection" })).toBeDisabled();
  expect(host.commands).toHaveLength(0);
  await user.clear(screen.getByRole("textbox", { name: "Decoded JSON value paths to mask" }));
  await user.type(
    screen.getByRole("textbox", { name: "Decoded JSON value paths to mask" }),
    "/customer/email",
  );
  await user.click(screen.getByRole("button", { name: "Save protection" }));
  expect(host.commands).toEqual([
    expect.objectContaining({
      command: "preferences.update",
      payload: {
        patch: {
          protection: {
            readOnly: true,
            maskKey: false,
            maskHeaders: [],
            valuePaths: ["/customer/email"],
          },
        },
      },
    }),
  ]);
  expect(await screen.findByRole("status")).toHaveTextContent("Protection saved");
});

it("explains why a connected session cannot change protection", () => {
  render(<RecordProtectionPanel host={new Host()} snapshot={snapshot} disconnected={false} />);
  expect(screen.getByRole("switch", { name: "Read-only mode" })).toBeDisabled();
  expect(screen.getByText(/Disconnect Kafka before changing protection/u)).toBeVisible();
});
