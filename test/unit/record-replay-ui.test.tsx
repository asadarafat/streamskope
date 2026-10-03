// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it } from "vitest";

import { ReplayRecordsAction } from "../../src/features/kafka/ui/ReplayRecordsAction";
import { replayBatch } from "../../src/features/kafka/contracts/record-replay";
import type {
  KafkaExploredMessage,
  StreamSkopeHost,
  HostCommand,
  HostCommandResponse,
} from "../../src/features/kafka/contracts";
afterEach(cleanup);
const message: KafkaExploredMessage = {
  id: "source:0:2",
  topic: "source",
  partition: 0,
  offset: "2",
  timestamp: "2026-10-03T00:00:00Z",
  key: null,
  payload: null,
  preview: "Tombstone",
  headers: {},
  originalByteSize: 0,
  truncated: false,
  original: { state: "complete", encoding: "base64", key: null, value: null, headers: [] },
  ruleEvaluation: {
    state: "unavailable",
    reason: "catalog-unavailable",
    activeMatchCount: 0,
    activeMatches: [],
    suppressedMatchCount: 0,
    suppressedMatches: [],
    durationMicros: 0,
    errorCount: 0,
    errors: [],
    evaluatedRules: 0,
    omittedEvidence: 0,
    omittedRules: 0,
  },
};
class Host implements StreamSkopeHost {
  commands: HostCommand[] = [];
  execute<C extends HostCommand>(command: C): Promise<HostCommandResponse<C["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    if (command.command === "records.replay.review")
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "r",
          review: {
            planId: "r",
            sourceName: "Source",
            targetName: "Target",
            expiresAt: new Date(Date.now() + 120_000).toISOString(),
            input: command.payload,
            batch: replayBatch(command.payload),
            destination: { clusterId: "target-cluster", topicId: "target-topic", partitions: 1 },
          },
        },
      });
    if (command.command === "records.replay.apply")
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "a",
          outcome: {
            total: 1,
            unsent: 0,
            stopReason: "write-failed",
            cleanup: "complete",
            outcomes: [
              {
                state: "unknown",
                receipt: null,
                verification: "unavailable",
                detail: "May be accepted",
              },
            ],
          },
        },
      });
    if (command.command === "records.replay.cancel")
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: { correlationId: "c" },
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
it("previews a frozen tombstone, requires exact destination text and accounts for an uncertain write without a retry button", async () => {
  const host = new Host(),
    user = userEvent.setup();
  render(
    <ReplayRecordsAction
      host={host}
      messages={[message]}
      selected={message}
      profiles={[]}
      enabled
      canWrite
    />,
  );
  await user.click(screen.getByRole("button", { name: "Replay…" }));
  await user.click(screen.getByRole("button", { name: "Preview replay" }));
  const confirm = await screen.findByRole("textbox", {
    name: "Type Target / source / 0 to confirm",
  });
  expect(host.commands.map((c) => c.command)).toEqual(["records.replay.review"]);
  expect(screen.getByRole("button", { name: "Apply reviewed replay" })).toBeDisabled();
  await user.type(confirm, "Target / source / 0");
  await user.click(screen.getByRole("button", { name: "Apply reviewed replay" }));
  expect(await screen.findByText(/0 acknowledged; 1 unknown; 0 rejected; 0 unsent/u)).toBeVisible();
  expect(screen.getByRole("button", { name: "Apply reviewed replay" })).toBeDisabled();
  expect(host.commands.at(-1)?.payload).toEqual({
    planId: "r",
    confirmation: "Target / source / 0",
  });
});
it("disables incomplete records rather than replaying a displayed preview", async () => {
  const host = new Host(),
    user = userEvent.setup();
  const truncated: KafkaExploredMessage = {
    ...message,
    original: { state: "unavailable", reason: "size-limit" },
  };
  render(
    <ReplayRecordsAction
      host={host}
      messages={[truncated]}
      selected={truncated}
      profiles={[]}
      enabled
      canWrite
    />,
  );
  await user.click(screen.getByRole("button", { name: "Replay…" }));
  expect(screen.getByRole("checkbox", { name: "Replay source/0@2" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Preview replay" })).toBeDisabled();
  expect(host.commands).toEqual([]);
});
