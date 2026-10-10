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
    if (command.command === "consumerGroups.reset.review") {
      const request = command.payload;
      const resolved =
        "targets" in request
          ? request
          : {
              groupId: request.groupId,
              targets: request.partitions.map((p) => ({
                ...p,
                offset: request.position.kind === "latest" ? "5" : "0",
              })),
            };
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
            input: resolved,
            ...("position" in command.payload ? { selection: command.payload } : {}),
            baseline: {
              inactive: true,
              state: "Empty",
              groupRead: "allowed",
              clusterId: "fixture-cluster",
              topics: [{ topic: "orders", topicId: "11111111-1111-1111-1111-111111111111" }],
              partitions: resolved.targets.map((t) => ({
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
    }
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
                cleanup: "confirmed",
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

it("previews selected end offsets, warns about skipped records and requires a new preview on selector change", async () => {
  const host = new Host(),
    user = userEvent.setup();
  render(<ResetOffsetsAction host={host} group={group} enabled canWrite />);
  await user.click(screen.getByRole("button", { name: "Reset offsets…" }));
  await user.click(screen.getByRole("checkbox", { name: "Reset orders:0" }));
  await user.click(screen.getByRole("combobox", { name: "Reset position" }));
  await user.click(screen.getByRole("option", { name: "Current end" }));
  expect(screen.getByText(/skips all retained records/u)).toBeVisible();
  expect(screen.getByRole("textbox", { name: "Next offset orders:0" })).toBeDisabled();
  expect(screen.getByRole("textbox", { name: "Next offset orders:0" })).toHaveValue("");
  await user.click(screen.getByRole("button", { name: "Preview reset" }));
  await screen.findByRole("table", { name: "Offset reset preview" });
  expect(screen.getByRole("textbox", { name: "Next offset orders:0" })).toHaveValue("5");
  expect(host.commands[0]?.payload).toEqual({
    groupId: "payments",
    partitions: [{ topic: "orders", partition: 0 }],
    position: { kind: "latest" },
  });
  await user.click(screen.getByRole("combobox", { name: "Reset position" }));
  await user.click(screen.getByRole("option", { name: "At or after UTC time" }));
  expect(screen.queryByRole("table", { name: "Offset reset preview" })).toBeNull();
  expect(screen.getByRole("textbox", { name: "Next offset orders:0" })).toHaveValue("");
  await user.type(
    screen.getByRole("textbox", { name: "UTC time (ISO 8601)" }),
    "2026-10-10T14:00:00",
  );
  await user.click(screen.getByRole("button", { name: "Preview reset" }));
  expect(host.commands).toHaveLength(1);
});
