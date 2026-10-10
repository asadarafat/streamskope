// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it } from "vitest";

import { RepairJobHistory } from "../../src/features/kafka/ui/RepairJobHistory";
import {
  parseHostCommand,
  parseHostCommandResponse,
  HOST_PROTOCOL_VERSION,
  type StreamSkopeHost,
  type HostCommand,
  type HostCommandResponse,
} from "../../src/features/kafka/contracts";
afterEach(cleanup);
const job = {
  id: "interrupted-job",
  createdAt: "2026-10-10T00:00:00Z",
  updatedAt: "2026-10-10T00:00:00Z",
  targetName: "Target",
  topic: "events",
  partition: 0,
  status: "interrupted" as const,
  cleanup: "pending" as const,
  total: 3,
  unsent: 1,
  uncertainIndex: 1,
  outcomes: [
    {
      state: "acknowledged" as const,
      detail: "Accepted",
      receipt: { topic: "events", partition: 0, offset: "19" },
      verification: "verified" as const,
    },
  ],
};
class Host implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  execute<C extends HostCommand>(command: C): Promise<HostCommandResponse<C["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(parseHostCommand(command));
    if (command.command !== "records.repair.list") throw new Error("Unexpected mutation.");
    return Promise.resolve(
      parseHostCommandResponse({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: { correlationId: "history", durability: "durable", jobs: [job] },
      }),
    );
  }
  subscribe(): () => void {
    return () => undefined;
  }
  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("Unexpected external URL."));
  }
}
it("loads interrupted metadata and actual receipts without requiring selected records or sending writes", async () => {
  const host = new Host();
  render(<RepairJobHistory host={host} />);
  await userEvent.click(screen.getByRole("button", { name: "Repair history" }));
  expect(await screen.findByText(/1 definitely unsent · record 2 uncertain/u)).toBeVisible();
  expect(screen.getByText("events/0@19")).toBeVisible();
  expect(screen.getByText(/Protected, durable host storage/u)).toBeVisible();
  expect(host.commands.map((c) => c.command)).toEqual(["records.repair.list"]);
  expect(screen.queryByRole("button", { name: /retry/iu })).not.toBeInTheDocument();
});
it("rejects payload disclosure and forged inconsistent counts at the host boundary", () => {
  const command = {
    command: "records.repair.list",
    id: "history",
    version: HOST_PROTOCOL_VERSION,
    payload: {},
  };
  expect(() => parseHostCommand({ ...command, payload: { includeOriginals: true } })).toThrow();
  const result = { correlationId: "history", durability: "durable", jobs: [job] };
  const response = { ...command, ok: true, result };
  delete (response as { payload?: unknown }).payload;
  expect(() =>
    parseHostCommandResponse({
      ...response,
      result: { ...result, jobs: [{ ...job, original: "private" }] },
    }),
  ).toThrow();
  expect(() =>
    parseHostCommandResponse({ ...response, result: { ...result, jobs: [{ ...job, unsent: 3 }] } }),
  ).toThrow();
});
