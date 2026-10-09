// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, render, renderHook, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { ConsumerGroupsPage } from "../../src/features/kafka/ui/ConsumerGroupsPage";
import { useConsumerGroupWorkbench } from "../../src/features/kafka/ui/use-consumer-group-workbench";
import { initialKafkaUiState } from "../../src/features/kafka/ui/state";
import type {
  KafkaConsumerGroupDetailSnapshot,
  HostCommand,
  HostCommandResponse,
  StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { testHostAccepted, testHostExecute } from "../support/host-response";

function hostFixture(): {
  host: StreamSkopeHost;
  execute: ReturnType<typeof vi.fn<(command: HostCommand) => Promise<HostCommandResponse>>>;
} {
  const execute = vi.fn((command: HostCommand) =>
    Promise.resolve(testHostAccepted(command, command.id)),
  );
  const host: StreamSkopeHost = {
    execute: testHostExecute(execute),
    subscribe: () => () => undefined,
    openExternalUrl: () => Promise.reject(new Error("Not used")),
  };
  return { host, execute };
}
afterEach(cleanup);

it("keeps a passively restored group outside the bounded inventory without fetching detail", () => {
  const { host, execute } = hostFixture();
  const props = {
    host,
    connected: true,
    connectionName: "Fixture",
    inventory: {
      ...initialKafkaUiState.consumerGroupInventory,
      state: "ready" as const,
      omittedGroups: 100,
    },
    onNavigationChange: vi.fn(),
  };
  const { result, rerender } = renderHook(useConsumerGroupWorkbench, { initialProps: props });
  act(() => result.current.restoreSelection("not-in-first-page"));
  rerender({
    ...props,
    inventory: { ...props.inventory, refreshedAt: "2026-10-09T00:00:00.000Z" },
  });
  expect(result.current.selectedGroupId).toBe("not-in-first-page");
  expect(result.current.detailRequested).toBe(false);
  expect(execute).not.toHaveBeenCalled();
  act(() => result.current.onSelect("not-in-first-page"));
  expect(result.current.detailRequested).toBe(true);
  expect(execute).toHaveBeenCalledWith(
    expect.objectContaining({
      command: "consumerGroups.load",
      payload: { groupId: "not-in-first-page" },
    }),
  );
});

it.each([
  ["previous", true],
  ["restored", false],
] as const)(
  "does not expose cached %s detail or offset reset before an explicit matching request",
  (cached, requested) => {
    const { host } = hostFixture();
    const snapshot: KafkaConsumerGroupDetailSnapshot = {
      connectionName: "Fixture",
      groupId: cached,
      state: "ready",
      refreshedAt: "2026-10-09T00:00:00.000Z",
      group: {
        id: cached,
        state: "empty",
        protocol: "range",
        protocolType: "consumer",
        members: [],
        offsets: [],
        omittedAssignments: 0,
        omittedMembers: 0,
        omittedOffsets: 0,
      },
    };
    render(
      <ConsumerGroupsPage
        host={host}
        canWrite
        connected
        detail={snapshot}
        detailRequested={requested}
        inventory={initialKafkaUiState.consumerGroupInventory}
        filter=""
        onFilterChange={vi.fn()}
        onRefresh={vi.fn()}
        onSelect={vi.fn()}
        selectedGroupId="restored"
      />,
    );
    expect(screen.getByText("Consumer group not loaded")).toBeVisible();
    expect(screen.getByRole("button", { name: "Reset offsets…" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Refresh consumer group restored" })).toBeEnabled();
    expect(screen.queryByRole("grid")).not.toBeInTheDocument();
  },
);
