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
import type { SchemaAuthoringResult } from "../../src/features/kafka/contracts/schema-authoring";
import { SchemaAuthorPanel } from "../../src/features/kafka/ui/SchemaAuthorPanel";

const schema: SchemaVersionDetail = {
  subject: "events",
  version: 1,
  id: 7,
  schemaType: "AVRO",
  schema: '"string"',
  references: [],
};
const valid: SchemaAuthoringResult = {
  state: "valid",
  writer: { subject: "events", version: 1, id: 7, schemaType: "AVRO" },
  messageType: null,
  json: '"edited"',
  encoding: "Confluent Avro",
  record: {
    state: "complete",
    encoding: "base64",
    key: null,
    value: "AAAAAAcMZWRpdGVk",
    headers: [],
  },
};
class Host implements StreamSkopeHost {
  commands: HostCommand[] = [];
  result: SchemaAuthoringResult = valid;
  defer = false;
  settle?: () => void;
  execute<C extends HostCommand>(command: C): Promise<HostCommandResponse<C["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    const base = { id: command.id, version: command.version, ok: true as const };
    if (command.command === "schemas.author") {
      const response: HostCommandResponse = {
        ...base,
        command: command.command,
        result: { correlationId: "c", authoring: this.result },
      };
      if (this.defer)
        return new Promise((resolve) => {
          this.settle = (): void => resolve(response);
        });
      return Promise.resolve(response);
    }
    if (command.command === "records.batch.review")
      return Promise.resolve({
        ...base,
        command: command.command,
        result: {
          correlationId: "c",
          review: {
            planId: "plan",
            connectionName: "Fixture",
            expiresAt: "2026-10-10T10:00:00Z",
            input: command.payload,
          },
        },
      });
    if (command.command === "records.batch.apply")
      return Promise.resolve({
        ...base,
        command: command.command,
        result: {
          correlationId: "c",
          outcome: { total: 1, unsent: 1, outcomes: [], stopReason: "write-failed" },
        },
      });
    throw new Error("Unexpected implicit command");
  }
  subscribe(): () => void {
    return () => undefined;
  }
  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("Unused"));
  }
}
afterEach(cleanup);
it("keeps validation free of writes and removes publication after any payload edit", async () => {
  const user = userEvent.setup(),
    host = new Host();
  render(<SchemaAuthorPanel schema={schema} host={host} enabled />);
  await user.click(screen.getByRole("button", { name: "Author record" }));
  const payload = screen.getByRole("textbox", { name: "Record payload JSON" });
  await user.clear(payload);
  await user.type(payload, '"edited"');
  await user.click(screen.getByRole("button", { name: "Validate payload" }));
  expect(await screen.findByLabelText("Validated record projection")).toHaveTextContent('"edited"');
  expect(host.commands).toHaveLength(1);
  expect(host.commands[0]).toMatchObject({
    command: "schemas.author",
    payload: { subject: "events", version: 1, schemaId: 7, payload: '"edited"' },
  });
  await user.type(screen.getByRole("textbox", { name: "Destination topic" }), "target");
  await user.click(screen.getByRole("button", { name: "Review batch destination" }));
  await user.type(
    await screen.findByRole("textbox", { name: "Type destination topic to confirm" }),
    "target",
  );
  expect(screen.getByRole("button", { name: "Publish reviewed batch" })).toBeEnabled();
  await user.type(payload, " ");
  expect(screen.queryByRole("button", { name: "Publish reviewed batch" })).not.toBeInTheDocument();
  expect(screen.queryByLabelText("Validated record projection")).not.toBeInTheDocument();
  expect(host.commands.map((command) => command.command)).toEqual([
    "schemas.author",
    "records.batch.review",
  ]);
});
it("does not offer publication for invalid payloads", async () => {
  const user = userEvent.setup(),
    host = new Host();
  host.result = {
    state: "invalid",
    issues: [
      {
        path: "/id",
        code: "precision",
        detail: "64-bit integers require an exact decimal string.",
      },
    ],
  };
  render(<SchemaAuthorPanel schema={schema} host={host} enabled />);
  await user.click(screen.getByRole("button", { name: "Author record" }));
  await user.click(screen.getByRole("button", { name: "Validate payload" }));
  expect(
    await screen.findByText(/64-bit integers require an exact decimal string/u),
  ).toBeInTheDocument();
  expect(
    screen.queryByRole("button", { name: "Review batch destination" }),
  ).not.toBeInTheDocument();
  expect(host.commands.map((command) => command.command)).toEqual(["schemas.author"]);
});
it("discards validation from the previous writer after a subject or connection change", async () => {
  const user = userEvent.setup(),
    host = new Host();
  host.defer = true;
  const view = render(<SchemaAuthorPanel schema={schema} host={host} enabled />);
  await user.click(screen.getByRole("button", { name: "Author record" }));
  await user.click(screen.getByRole("button", { name: "Validate payload" }));
  await waitFor(() => expect(host.settle).toBeDefined());
  view.rerender(
    <SchemaAuthorPanel schema={{ ...schema, id: 8, version: 2 }} host={host} enabled={false} />,
  );
  host.settle!();
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "Validate payload" })).toBeDisabled(),
  );
  expect(screen.queryByLabelText("Validated record projection")).not.toBeInTheDocument();
  expect(
    screen.queryByRole("button", { name: "Review batch destination" }),
  ).not.toBeInTheDocument();
});
