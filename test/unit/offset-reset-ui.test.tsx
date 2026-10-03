// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it } from "vitest";

import { ResetOffsetsAction } from "../../src/features/kafka/ui/ResetOffsetsAction";
import {
  type StreamSkopeHost,
  type HostCommand,
  type HostCommandResponse,
  type KafkaConsumerGroupDetails,
} from "../../src/features/kafka/contracts";
afterEach(cleanup);
const group: KafkaConsumerGroupDetails = {
  id: "payments",
  state: "empty",
  members: [],
  protocol: "",
  protocolType: "consumer",
  omittedAssignments: 0,
  omittedMembers: 0,
  omittedOffsets: 0,
  offsets: [{ topic: "orders", partition: 0, committedOffset: "3", endOffset: "5", lag: "2" }],
};
class Host implements StreamSkopeHost {
  commands: HostCommand[] = [];
  execute<C extends HostCommand>(command: C): Promise<HostCommandResponse<C["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    if (command.command === "consumerGroups.reset.review")
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "r",
          review: {
            planId: "r",
            connectionName: "lab",
            expiresAt: new Date(Date.now() + 120_000).toISOString(),
            input: command.payload,
            baseline: {
              inactive: true,
              state: "Empty",
              groupRead: "allowed",
              partitions: command.payload.targets.map((t) => ({
                ...t,
                before: "3",
                low: "0",
                high: "5",
                replayUpperBound: "0",
              })),
            },
            examples: [],
            exampleStatus: "empty",
          },
        },
      });
    if (command.command === "consumerGroups.reset.apply")
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "a",
          outcome: {
            groupId: group.id,
            detail: "Acknowledgement unavailable. Inspect offsets before retrying.",
            partitions: [
              {
                topic: "orders",
                partition: 0,
                offset: "3",
                observed: null,
                verified: false,
                state: "unknown",
              },
            ],
          },
        },
      });
    throw new Error("Unexpected command");
  }
  subscribe(): () => void {
    return () => undefined;
  }
  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("Unexpected URL"));
  }
}
it("requires partition selection, review and exact group confirmation, then disables retry of an uncertain result", async () => {
  const host = new Host(),
    user = userEvent.setup();
  render(<ResetOffsetsAction host={host} group={group} enabled canWrite />);
  await user.click(screen.getByRole("button", { name: "Reset offsets…" }));
  expect(screen.getByRole("button", { name: "Preview reset" })).toBeDisabled();
  await user.click(screen.getByRole("checkbox", { name: "Reset orders:0" }));
  await user.click(screen.getByRole("button", { name: "Preview reset" }));
  expect(await screen.findByRole("table", { name: "Offset reset preview" })).toBeVisible();
  expect(host.commands.map((c) => c.command)).toEqual(["consumerGroups.reset.review"]);
  expect(screen.getByRole("button", { name: "Apply reviewed reset" })).toBeDisabled();
  await user.type(
    screen.getByRole("textbox", { name: "Type the exact group ID to confirm" }),
    "payments",
  );
  await user.click(screen.getByRole("button", { name: "Apply reviewed reset" }));
  expect(
    await screen.findByText("Acknowledgement unavailable. Inspect offsets before retrying."),
  ).toBeVisible();
  expect(screen.getByRole("button", { name: "Apply reviewed reset" })).toBeDisabled();
  expect(host.commands.at(-1)?.payload).toEqual({ planId: "r", confirmation: "payments" });
});
