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
import {
  replayBatch,
  UNCHANGED_REPLAY_TRANSFORM,
} from "../../src/features/kafka/contracts/record-replay";
afterEach(cleanup);
const job = {
  id: "interrupted-job",
  revision: 1,
  parentJobId: null,
  continuationId: null,
  findings: [],
  targetProfile: null,
  canContinue: true,
  canArchive: false,
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
it("requires a fresh preview and exact destination confirmation, sends only the reviewed child and leaves uncertainty visible", async () => {
  const input = {
    targetProfile: null,
    topic: job.topic,
    partition: job.partition,
    ratePerSecond: 1,
    records: [
      {
        topic: "source",
        partition: 0,
        offset: "2",
        timestampMs: null,
        original: {
          state: "complete" as const,
          encoding: "base64" as const,
          key: null,
          value: "c2VjcmV0",
          headers: [],
        },
      },
    ],
    transform: UNCHANGED_REPLAY_TRANSFORM,
  };
  const review = {
    planId: "child",
    sourceName: "Source",
    targetName: job.targetName,
    expiresAt: "2026-10-10T12:00:00Z",
    input,
    batch: replayBatch(input),
    destination: { clusterId: "cluster", topicId: "topic", partitions: 1 },
  };
  const commands: HostCommand[] = [];
  const host: StreamSkopeHost = new Host();
  host.execute = ((command: HostCommand): Promise<HostCommandResponse> => {
    commands.push(parseHostCommand(command));
    const result =
      command.command === "records.repair.list"
        ? { correlationId: "history", durability: "durable", jobs: [job] }
        : command.command === "records.repair.review"
          ? {
              correlationId: "review",
              continuation: {
                parentJobId: job.id,
                skipped: { acknowledged: 1, rejected: 0, uncertain: 1 },
                review,
              },
            }
          : command.command === "records.replay.apply"
            ? {
                correlationId: "apply",
                outcome: {
                  total: 1,
                  unsent: 0,
                  outcomes: [
                    {
                      ...job.outcomes[0]!,
                      receipt: { topic: "events", partition: 0, offset: "20" },
                    },
                  ],
                  stopReason: "complete",
                  cleanup: "complete",
                },
              }
            : { correlationId: "cancel" };
    return Promise.resolve(
      parseHostCommandResponse({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result,
      }),
    );
  }) as StreamSkopeHost["execute"];
  render(<RepairJobHistory host={host} />);
  await userEvent.click(screen.getByRole("button", { name: "Repair history" }));
  await userEvent.click(await screen.findByRole("button", { name: "Recovery controls" }));
  expect(screen.queryByRole("button", { name: "Archive confirmed chain" })).not.toBeInTheDocument();
  await userEvent.click(screen.getByRole("button", { name: "Review definitely unsent records" }));
  expect(
    await screen.findByText(/Skipped: 1 acknowledged, 0 rejected, 1 uncertain/u),
  ).toBeVisible();
  const apply = screen.getByRole("button", { name: "Apply reviewed continuation" });
  expect(apply).toBeDisabled();
  await userEvent.type(
    screen.getByRole("textbox", { name: "Type Target / events / 0" }),
    "Target / events / 0",
  );
  await userEvent.click(apply);
  expect(await screen.findByText(/Continuation stopped: complete/u)).toBeVisible();
  expect(
    commands.filter((c) => c.command === "records.replay.apply").map((c) => c.payload),
  ).toEqual([{ planId: "child", confirmation: "Target / events / 0" }]);
  expect(screen.getByText(/record 2 uncertain after interruption/u)).toBeVisible();
});
