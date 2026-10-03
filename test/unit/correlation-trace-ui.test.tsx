// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it } from "vitest";

import type {
  HostCommand,
  HostCommandResponse,
  StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { CorrelationTracePanel } from "../../src/features/kafka/ui/CorrelationTracePanel";

class Host implements StreamSkopeHost {
  commands: HostCommand[] = [];
  execute<C extends HostCommand>(command: C): Promise<HostCommandResponse<C["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    if (command.command !== "records.trace") throw new Error("Unexpected command");
    return Promise.resolve({
      command: command.command,
      id: command.id,
      version: command.version,
      ok: true,
      result: {
        correlationId: "trace",
        trace: {
          input: command.payload,
          connectionName: "Fixture",
          matches: [],
          topics: [
            {
              topic: "events",
              state: "denied",
              reason: "permission-denied",
              evaluated: 0,
              unavailable: 0,
              matches: 0,
              coverage: null,
            },
          ],
        },
      },
    });
  }
  subscribe(): () => void {
    return () => undefined;
  }
  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("Unused"));
  }
}
afterEach(cleanup);
it("requires an explicit value, shows denied coverage for zero matches and clears stale evidence on edits", async () => {
  const user = userEvent.setup();
  const host = new Host();
  render(<CorrelationTracePanel host={host} topic="events" enabled />);
  await user.click(screen.getByRole("button", { name: "Trace correlation" }));
  expect(screen.getByRole("button", { name: "Start trace" })).toBeDisabled();
  await user.type(screen.getByLabelText("Exact correlation value"), "request-42");
  await user.click(screen.getByRole("button", { name: "Start trace" }));
  expect(await screen.findByText(/Partial evidence: review each topic/u)).toBeVisible();
  expect(screen.getByText("events: denied (permission-denied)")).toBeVisible();
  expect(host.commands[0]).toMatchObject({
    command: "records.trace",
    payload: {
      topics: ["events"],
      value: "request-42",
      selector: { source: "header", path: "correlation-id", format: "json" },
    },
  });
  await user.type(screen.getByLabelText("Exact correlation value"), "-different");
  expect(screen.queryByRole("table", { name: "Correlation matches" })).not.toBeInTheDocument();
});
