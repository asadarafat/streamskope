// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { useState } from "react";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import type {
  HostCommand,
  HostCommandResponse,
  KafkaExploredMessage,
  SchemaVersionDetail,
  StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { RecordComparisonPanel } from "../../src/features/kafka/ui/RecordComparisonPanel";
import { SchemaHistoryPanel } from "../../src/features/kafka/ui/SchemaHistoryPanel";

const record = (offset: string, value: string | null): KafkaExploredMessage => ({
  id: `t:0:${offset}`,
  topic: "t",
  partition: 0,
  offset,
  timestamp: "2026-10-03T00:00:00Z",
  key: null,
  payload: value,
  preview: value ?? "",
  headers: {},
  originalByteSize: value?.length ?? 0,
  truncated: false,
  original: {
    state: "complete",
    encoding: "base64",
    key: null,
    value: value === null ? null : btoa(value),
    headers: [],
  },
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
});
const schema: SchemaVersionDetail = {
  subject: "events",
  version: 2,
  id: 20,
  schemaType: "AVRO",
  schema: '"string"',
  references: [],
};
class Host implements StreamSkopeHost {
  commands: HostCommand[] = [];
  execute<C extends HostCommand>(command: C): Promise<HostCommandResponse<C["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    if (command.command === "schemas.inspect")
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "c",
          inspection: {
            root: {
              ...schema,
              version: command.payload.version,
              schema: command.payload.version === 1 ? '"int"' : schema.schema,
            },
            edges: [
              {
                from: { subject: "events", version: 2 },
                to: { subject: "deleted", version: 1 },
                name: "data",
                depth: 1,
                state: "unavailable",
              },
            ],
            limited: false,
          },
        },
      });
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
it("pins a stable snapshot and compares a different offset without confusing null and empty bytes", async () => {
  function Harness({
    current,
    enabled = true,
  }: {
    readonly current: KafkaExploredMessage;
    readonly enabled?: boolean;
  }): React.JSX.Element {
    const [baseline, setBaseline] = useState<KafkaExploredMessage | null>(null);
    return (
      <RecordComparisonPanel
        current={current}
        baseline={baseline}
        onPin={setBaseline}
        enabled={enabled}
      />
    );
  }
  const user = userEvent.setup();
  const view = render(<Harness current={record("10", null)} />);
  await user.click(screen.getByRole("button", { name: "Pin as baseline" }));
  view.rerender(<Harness current={record("11", "")} />);
  expect(screen.getByText(/Before: t \/ partition 0 \/ offset 10/u)).toBeVisible();
  expect(screen.getByText(/After: t \/ partition 0 \/ offset 11/u)).toBeVisible();
  await user.click(screen.getByRole("combobox", { name: "Comparison representation" }));
  await user.click(screen.getByRole("option", { name: "Original bytes (Base64)" }));
  await user.click(screen.getByRole("button", { name: "Compare records" }));
  expect(await screen.findByRole("table", { name: "Differences" })).toHaveTextContent("/bytes");
  expect(screen.getByRole("table")).toHaveTextContent("null");
  view.rerender(
    <Harness
      current={{
        ...record("12", "preview"),
        original: { state: "unavailable", reason: "size-limit" },
      }}
    />,
  );
  expect(screen.getByRole("button", { name: "Compare records" })).toBeDisabled();
  expect(screen.getByText(/cannot establish byte equality/u)).toBeVisible();
  expect(screen.queryByRole("table")).not.toBeInTheDocument();
});
it("requests exact versions for comparison and labels missing reference evidence without claiming data lineage", async () => {
  const user = userEvent.setup();
  const host = new Host();
  render(
    <SchemaHistoryPanel schema={schema} versions={[1, 2]} host={host} enabled onSelect={vi.fn()} />,
  );
  expect(host.commands).toHaveLength(0);
  await user.type(screen.getByLabelText("Compare from version"), "1");
  await user.click(screen.getByRole("button", { name: "Compare schema versions" }));
  expect(await screen.findByRole("table", { name: "Differences" })).toHaveTextContent("int");
  expect(host.commands[0]).toMatchObject({
    command: "schemas.inspect",
    payload: { subject: "events", version: 1 },
  });
  await user.click(screen.getByRole("button", { name: "Show reference tree" }));
  expect(await screen.findByText(/deleted@1 · unavailable/u)).toBeVisible();
  expect(screen.getByText(/does not establish producer/u)).toBeVisible();
  expect(host.commands[1]).toMatchObject({
    command: "schemas.inspect",
    payload: { subject: "events", version: 2 },
  });
});
