// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it } from "vitest";

import type {
  HostCommand,
  HostCommandResponse,
  SchemaVersionDetail,
  StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import type { RecordBatchOutcome } from "../../src/features/kafka/contracts/schema-samples";
import { SchemaSamplesPanel } from "../../src/features/kafka/ui/SchemaSamplesPanel";

const schema: SchemaVersionDetail = {
  subject: "events",
  version: 1,
  id: 7,
  schemaType: "AVRO",
  schema: '"string"',
  references: [],
};
class Host implements StreamSkopeHost {
  commands: HostCommand[] = [];
  rejectPublish = false;
  settle?: (outcome: RecordBatchOutcome) => void;
  execute<C extends HostCommand>(command: C): Promise<HostCommandResponse<C["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    const base = { id: command.id, version: command.version, ok: true as const };
    if (command.command === "schemas.samples")
      return Promise.resolve({
        ...base,
        command: command.command,
        result: {
          correlationId: "c",
          samples: {
            schema: { subject: "events", version: 1 },
            schemaId: 7,
            seed: 1,
            encoding: "Avro",
            samples: [
              {
                json: '"sample"',
                record: {
                  state: "complete",
                  encoding: "base64",
                  value: "c2FtcGxl",
                  key: null,
                  headers: [],
                },
              },
            ],
          },
        },
      });
    if (command.command === "records.batch.review")
      return Promise.resolve({
        ...base,
        command: command.command,
        result: {
          correlationId: "c",
          review: {
            planId: "plan",
            connectionName: "Fixture",
            expiresAt: "2026-10-03T10:00:00Z",
            input: command.payload,
          },
        },
      });
    if (command.command === "records.batch.apply" && this.rejectPublish)
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: false,
        error: {
          code: "AUTHORIZATION_DENIED",
          stage: "authorization",
          correlationId: "c",
          activeStateChanged: false,
          retryable: false,
          summary: "Read-only mode blocks this operation.",
          recovery: "Review Protection before publishing.",
        },
      });
    if (command.command === "records.batch.apply")
      return new Promise((resolve) => {
        this.settle = (outcome): void =>
          resolve({
            ...base,
            command: "records.batch.apply",
            result: { correlationId: "c", outcome },
          });
      });
    if (command.command === "records.batch.cancel")
      return Promise.resolve({ ...base, command: command.command, result: { correlationId: "c" } });
    throw new Error("Unexpected command");
  }
  subscribe(): () => void {
    return () => undefined;
  }
  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("Unused"));
  }
}
afterEach(cleanup);
async function preview(host: Host): Promise<ReturnType<typeof userEvent.setup>> {
  const user = userEvent.setup();
  render(<SchemaSamplesPanel schema={schema} host={host} enabled />);
  await user.click(screen.getByRole("button", { name: "Generate samples" }));
  await user.click(screen.getByRole("button", { name: "Generate preview" }));
  expect(await screen.findByLabelText("Generated sample")).toHaveTextContent('"sample"');
  return user;
}
it("keeps preview free of writes and invalidates confirmation when the destination changes", async () => {
  const host = new Host();
  const user = await preview(host);
  expect(host.commands.map((command) => command.command)).toEqual(["schemas.samples"]);
  await user.type(screen.getByLabelText("Destination topic"), "test.events");
  await user.click(screen.getByRole("button", { name: "Review batch destination" }));
  expect(await screen.findByRole("button", { name: "Publish reviewed batch" })).toBeDisabled();
  await user.type(screen.getByLabelText("Type destination topic to confirm"), "test.events");
  expect(screen.getByRole("button", { name: "Publish reviewed batch" })).toBeEnabled();
  await user.type(screen.getByLabelText("Destination topic"), ".changed");
  expect(screen.queryByRole("button", { name: "Publish reviewed batch" })).not.toBeInTheDocument();
  expect(host.commands.map((command) => command.command)).toEqual([
    "schemas.samples",
    "records.batch.review",
  ]);
});
it("waits for in-flight accounting after cancellation and does not offer to resend the review", async () => {
  const host = new Host();
  const user = await preview(host);
  await user.type(screen.getByLabelText("Destination topic"), "test.events");
  await user.click(screen.getByRole("button", { name: "Review batch destination" }));
  await user.type(await screen.findByLabelText("Type destination topic to confirm"), "test.events");
  await user.click(screen.getByRole("button", { name: "Publish reviewed batch" }));
  expect(screen.getByRole("button", { name: "Close" })).toBeDisabled();
  await user.click(screen.getByRole("button", { name: "Cancel remaining records" }));
  expect(screen.getByRole("status")).toHaveTextContent("in-flight");
  host.settle?.({
    total: 1,
    unsent: 0,
    stopReason: "write-failed",
    outcomes: [
      {
        state: "unknown",
        detail: "Uncertain dispatch",
        receipt: null,
        verification: "unavailable",
      },
    ],
  });
  await waitFor(() => expect(screen.getByText(/1 uncertain, 0 unsent/u)).toBeVisible());
  expect(screen.queryByRole("button", { name: "Publish reviewed batch" })).not.toBeInTheDocument();
  expect(host.commands.map((command) => command.command)).toEqual([
    "schemas.samples",
    "records.batch.review",
    "records.batch.apply",
    "records.batch.cancel",
  ]);
});

it("shows a known host rejection without claiming an uncertain dispatch", async () => {
  const host = new Host();
  host.rejectPublish = true;
  const user = await preview(host);
  await user.type(screen.getByLabelText("Destination topic"), "test.events");
  await user.click(screen.getByRole("button", { name: "Review batch destination" }));
  await user.type(await screen.findByLabelText("Type destination topic to confirm"), "test.events");
  await user.click(screen.getByRole("button", { name: "Publish reviewed batch" }));
  expect(await screen.findByText(/Read-only mode blocks this operation/u)).toBeVisible();
  expect(screen.queryByText(/batch result is unavailable/u)).not.toBeInTheDocument();
});
