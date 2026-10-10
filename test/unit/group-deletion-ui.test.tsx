// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import { GroupDeletionAction } from "../../src/features/kafka/ui/GroupDeletionAction";
import { KafkaBackendFacade } from "../../src/features/kafka/facade";
import {
  HOST_PROTOCOL_VERSION,
  type StreamSkopeHost,
  type HostCommand,
} from "../../src/features/kafka/contracts";
import {
  createFacade,
  RecordingConnectionPort,
  RecordingActiveConnection,
} from "../support/kafka-backend-facade-fixture";

const group = {
  groupId: "payments",
  clusterId: "owned",
  protocolType: "",
  state: "Empty",
  members: 0,
  offsetsSha256: "a".repeat(64),
  deletePermission: "allowed" as const,
  deleteSupported: true,
};
afterEach(cleanup);
it("holds the actual deletion receipt until Close, then refreshes exactly once", async () => {
  const port = new RecordingConnectionPort(),
    connection = new RecordingActiveConnection();
  Object.assign(connection, {
    groupAdministrationSnapshot: () => Promise.resolve(group),
    deleteConsumerGroup: () =>
      Promise.resolve({
        groupId: "payments",
        state: "acknowledged",
        verification: "verified",
        cleanup: "confirmed",
        detail: "Actual fixture group receipt",
      }),
  });
  const facade: KafkaBackendFacade = createFacade(port);
  port.openOperations.push(() => Promise.resolve(connection));
  await facade.execute({
    command: "connection.connect",
    id: "connect",
    version: HOST_PROTOCOL_VERSION,
    payload: { name: "Fixture", brokers: ["localhost:9092"], tls: { enabled: false } },
  });
  const execute = vi.fn(facade.execute.bind(facade)),
    deleted = vi.fn(),
    user = userEvent.setup();
  const host: StreamSkopeHost = {
    execute: execute as StreamSkopeHost["execute"],
    subscribe: facade.subscribe.bind(facade),
    openExternalUrl: (): Promise<never> => Promise.reject(new Error("No external URL expected")),
  };
  render(
    <GroupDeletionAction host={host} groupId="payments" enabled canWrite onDeleted={deleted} />,
  );
  await user.click(screen.getByRole("button", { name: "Delete group…" }));
  await user.click(screen.getByRole("button", { name: "Review group deletion" }));
  expect(
    await screen.findByRole("textbox", { name: "Confirm exact group deletion" }),
  ).toBeVisible();
  expect(screen.getByRole("button", { name: "Delete reviewed group" })).toBeDisabled();
  await user.type(
    screen.getByRole("textbox", { name: "Confirm exact group deletion" }),
    "DELETE GROUP payments",
  );
  await user.click(screen.getByRole("button", { name: "Delete reviewed group" }));
  expect(await screen.findByText(/Actual fixture group receipt/u)).toBeVisible();
  expect(deleted).not.toHaveBeenCalled();
  expect(
    execute.mock.calls.filter(([c]: [HostCommand]) => c.command === "consumerGroups.delete.apply"),
  ).toHaveLength(1);
  await user.click(screen.getByRole("button", { name: "Close" }));
  expect(deleted).toHaveBeenCalledOnce();
  await facade.shutdown();
});
