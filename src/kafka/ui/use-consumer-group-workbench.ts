import { useCallback, useEffect, useState } from "react";

import {
  HOST_PROTOCOL_VERSION,
  type KafkaConsumerGroupInventorySnapshot,
  type StreamSkopeHost,
} from "../contracts";

import type { NavigationView } from "./workbench-navigation";

interface ConsumerGroupWorkbenchOptions {
  readonly connected: boolean;
  readonly connectionName: string | null;
  readonly host: StreamSkopeHost;
  readonly inventory: KafkaConsumerGroupInventorySnapshot;
  readonly onNavigationChange: (navigation: NavigationView) => void;
}

interface ConsumerGroupWorkbenchController {
  readonly filter: string;
  readonly onFilterChange: (value: string) => void;
  readonly onClearSelection: () => void;
  readonly onNavigationChange: (navigation: NavigationView) => void;
  readonly onRefresh: () => void;
  readonly onSelect: (groupId: string) => void;
  readonly requestError: string | undefined;
  readonly selectedGroupId: string | null;
}

export function useConsumerGroupWorkbench({
  connected,
  connectionName,
  host,
  inventory,
  onNavigationChange,
}: ConsumerGroupWorkbenchOptions): ConsumerGroupWorkbenchController {
  const [filter, setFilter] = useState("");
  const [requestError, setRequestError] = useState<string>();
  const [selectedGroupId, setSelectedGroupId] = useState<string | null>(null);

  const requestInventory = useCallback(async (): Promise<void> => {
    setRequestError(undefined);
    try {
      await host.execute({
        command: "consumerGroups.list",
        id: globalThis.crypto.randomUUID(),
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
    } catch {
      setRequestError(
        "The application host did not accept the consumer-group request. Open Activity for diagnostics.",
      );
    }
  }, [host]);

  const changeNavigation = useCallback(
    (nextNavigation: NavigationView): void => {
      onNavigationChange(nextNavigation);
      if (nextNavigation === "consumer-groups" && connected && inventory.state === "unavailable") {
        void requestInventory();
      }
    },
    [connected, inventory.state, onNavigationChange, requestInventory],
  );

  const select = useCallback(
    async (groupId: string): Promise<void> => {
      setSelectedGroupId(groupId);
      setRequestError(undefined);
      try {
        await host.execute({
          command: "consumerGroups.load",
          id: globalThis.crypto.randomUUID(),
          payload: { groupId },
          version: HOST_PROTOCOL_VERSION,
        });
      } catch {
        setRequestError(
          "The application host did not accept the consumer-group detail request. Open Activity for diagnostics.",
        );
      }
    },
    [host],
  );

  useEffect(() => {
    setFilter("");
    setSelectedGroupId(null);
  }, [connectionName]);

  useEffect(() => {
    if (
      (inventory.state === "ready" || inventory.state === "empty") &&
      selectedGroupId !== null &&
      !inventory.groups.some((group) => group.id === selectedGroupId)
    ) {
      setSelectedGroupId(null);
    }
  }, [inventory, selectedGroupId]);

  return {
    filter,
    onClearSelection: (): void => setSelectedGroupId(null),
    onFilterChange: setFilter,
    onNavigationChange: changeNavigation,
    onRefresh: (): void => {
      void requestInventory();
    },
    onSelect: (groupId): void => {
      void select(groupId);
    },
    requestError,
    selectedGroupId,
  };
}
