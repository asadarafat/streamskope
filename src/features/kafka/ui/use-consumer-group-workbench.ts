import { useCallback, useEffect, useRef, useState } from "react";

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
  readonly onSelect: (groupId: string | null) => void;
  readonly restoreSelection: (groupId: string) => void;
  readonly detailRequested: boolean;
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
  const [detailRequested, setDetailRequested] = useState(false);
  const generation = useRef(0);

  const requestInventory = useCallback(async (): Promise<void> => {
    const current = generation.current;
    setRequestError(undefined);
    try {
      await host.execute({
        command: "consumerGroups.list",
        id: globalThis.crypto.randomUUID(),
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
    } catch {
      if (current !== generation.current) return;
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
      const current = ++generation.current;
      setSelectedGroupId(groupId);
      setDetailRequested(true);
      setRequestError(undefined);
      try {
        const response = await host.execute({
          command: "consumerGroups.load",
          id: globalThis.crypto.randomUUID(),
          payload: { groupId },
          version: HOST_PROTOCOL_VERSION,
        });
        if (current === generation.current && !response.ok)
          setRequestError(`${response.error.summary} ${response.error.recovery}`);
      } catch {
        if (current !== generation.current) return;
        setRequestError(
          "The application host did not accept the consumer-group detail request. Open Activity for diagnostics.",
        );
      }
    },
    [host],
  );

  useEffect(() => {
    generation.current++;
    setFilter("");
    setSelectedGroupId(null);
    setDetailRequested(false);
    return (): void => {
      generation.current++;
    };
  }, [connectionName, host]);

  const restoreSelection = useCallback((groupId: string): void => {
    generation.current++;
    setSelectedGroupId(groupId);
    setDetailRequested(false);
    setRequestError(undefined);
  }, []);

  return {
    filter,
    onClearSelection: (): void => {
      generation.current++;
      setSelectedGroupId(null);
      setDetailRequested(false);
    },
    restoreSelection,
    detailRequested,
    onFilterChange: setFilter,
    onNavigationChange: changeNavigation,
    onRefresh: (): void => {
      void requestInventory();
    },
    onSelect: (groupId): void => {
      if (groupId === null) {
        generation.current++;
        setSelectedGroupId(null);
        setDetailRequested(false);
        setRequestError(undefined);
        return;
      }
      void select(groupId);
    },
    requestError,
    selectedGroupId,
  };
}
