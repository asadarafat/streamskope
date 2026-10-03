// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it } from "vitest";

import type {
  HostCommand,
  HostCommandResponse,
  StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { RecordDecodePanel } from "../../src/features/kafka/ui/RecordDecodePanel";

afterEach(cleanup);
class Host implements StreamSkopeHost {
  commands: HostCommand[] = [];
  pending: ((response: HostCommandResponse<"records.decode">) => void) | undefined;
  execute<C extends HostCommand>(command: C): Promise<HostCommandResponse<C["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    return new Promise<HostCommandResponse<"records.decode">>((resolve) => {
      this.pending = resolve;
    });
  }
  subscribe(): () => void {
    return () => undefined;
  }
  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("Unused"));
  }
  complete(): void {
    const command = this.commands.at(-1)!;
    this.pending?.({
      command: "records.decode",
      id: command.id,
      version: command.version,
      ok: true,
      result: {
        correlationId: "c",
        decoded: {
          state: "decoded",
          format: "json",
          json: '{"decoded":"first-record"}',
          schemaId: null,
          messageType: null,
          notes: "JSON projection",
        },
      },
    });
  }
}
const original = {
  state: "complete" as const,
  encoding: "base64" as const,
  key: null,
  value: "e30=",
  headers: [],
};

it("requires explicit decoding and shows a projection without changing bytes", async () => {
  const host = new Host();
  const user = userEvent.setup();
  render(<RecordDecodePanel original={original} host={host} enabled />);
  expect(host.commands).toHaveLength(0);
  await user.click(screen.getByRole("button", { name: "Decode record" }));
  expect(host.commands.at(-1)).toMatchObject({
    command: "records.decode",
    payload: { format: "json", bytes: "e30=" },
  });
  await act(async () => {
    host.complete();
    await Promise.resolve();
  });
  expect(screen.getByLabelText("Decoded JSON")).toHaveTextContent("first-record");
  expect(original.value).toBe("e30=");
});

it("does not disclose stale results after the record or connection changes", async () => {
  const host = new Host();
  const user = userEvent.setup();
  const view = render(<RecordDecodePanel original={original} host={host} enabled />);
  await user.click(screen.getByRole("button", { name: "Decode record" }));
  view.rerender(
    <RecordDecodePanel original={{ ...original, value: "bnVsbA==" }} host={host} enabled={false} />,
  );
  await act(async () => {
    host.complete();
    await Promise.resolve();
  });
  expect(screen.queryByLabelText("Decoded JSON")).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Decode record" })).toBeDisabled();
});

it("cannot submit an unavailable or masked original", () => {
  const host = new Host();
  render(
    <RecordDecodePanel original={{ state: "unavailable", reason: "masked" }} host={host} enabled />,
  );
  expect(screen.getByRole("button", { name: "Decode record" })).toBeDisabled();
  expect(screen.getByText(/cannot use a truncated preview or bypass masking/u)).toBeVisible();
  expect(host.commands).toHaveLength(0);
});
