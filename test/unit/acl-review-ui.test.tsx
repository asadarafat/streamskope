// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import type {
  HostCommand,
  HostCommandResponse,
  StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { aclChangeConfirmation } from "../../src/features/kafka/contracts/acl-review";
import {
  AclReviewDialog,
  type AclReviewSelection,
} from "../../src/features/kafka/ui/AclReviewDialog";

afterEach(cleanup);
const selection: AclReviewSelection = {
  action: "create",
  acl: {
    resourceType: "TOPIC",
    resourceName: "events",
    patternType: "LITERAL",
    principal: "User:reader",
    host: "127.0.0.1",
    operation: "READ",
    permission: "ALLOW",
  },
};
class Host implements StreamSkopeHost {
  commands: HostCommand[] = [];
  execute<C extends HostCommand>(command: C): Promise<HostCommandResponse<C["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    if (command.command === "acls.change.review")
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "r",
          review: {
            planId: "r",
            connectionName: "Fixture",
            expiresAt: new Date(Date.now() + 120_000).toISOString(),
            input: command.payload,
            beforePresent: false,
            afterPresent: true,
            beforeAccess: null,
            afterAccess: null,
          },
        },
      });
    return Promise.reject(new Error("Host response lost"));
  }
  subscribe(): () => void {
    return () => undefined;
  }
  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("Unexpected URL"));
  }
}
it("invalidates edited reviews and prevents repeating a change when the host response is lost", async () => {
  const host = new Host(),
    user = userEvent.setup();
  render(
    <AclReviewDialog
      host={host}
      selection={selection}
      canWrite
      onClose={vi.fn()}
      onApplied={vi.fn()}
    />,
  );
  await user.click(screen.getByRole("button", { name: "Preview ACL change" }));
  expect(await screen.findByRole("textbox", { name: "Exact change confirmation" })).toBeVisible();
  fireEvent.change(screen.getByRole("textbox", { name: "Kafka principal" }), {
    target: { value: "User:other" },
  });
  expect(screen.queryByRole("textbox", { name: "Exact change confirmation" })).toBeNull();
  expect(screen.getByRole("button", { name: "Apply reviewed ACL change" })).toBeDisabled();
  await user.click(screen.getByRole("button", { name: "Preview ACL change" }));
  await user.type(
    await screen.findByRole("textbox", { name: "Exact change confirmation" }),
    aclChangeConfirmation({ ...selection, access: null }),
  );
  await user.click(screen.getByRole("button", { name: "Apply reviewed ACL change" }));
  expect(await screen.findByText(/the ACL change may have been applied/u)).toBeVisible();
  expect(screen.getByRole("button", { name: "Apply reviewed ACL change" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Preview ACL change" })).toBeDisabled();
  expect(host.commands.filter((c) => c.command === "acls.change.apply")).toHaveLength(1);
});
it("permits preview but blocks apply and confirmation in read-only mode", async () => {
  const host = new Host(),
    user = userEvent.setup();
  render(
    <AclReviewDialog
      host={host}
      selection={selection}
      canWrite={false}
      onClose={vi.fn()}
      onApplied={vi.fn()}
    />,
  );
  await user.click(screen.getByRole("button", { name: "Preview ACL change" }));
  expect(await screen.findByRole("textbox", { name: "Exact change confirmation" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Apply reviewed ACL change" })).toBeDisabled();
  expect(host.commands.map((c) => c.command)).toEqual(["acls.change.review"]);
});
