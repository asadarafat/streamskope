import {
  validateKafkaRuleExpression,
  type KafkaFetchMode,
  type KafkaSearchFilter,
  type ProfileSummary,
} from "../contracts";

import type { KafkaMessageFilters } from "./message-operations";
import type { WorkbenchCommandAction } from "./WorkbenchCommandPalette";

export function investigationCommands({
  connected,
  selectedTopic,
  selectedProfile,
  profileBusy,
  readActive,
  stopping,
  mode,
  timeError,
  filters,
  openQueries,
  toggleProfile,
  startRead,
  stopRead,
}: {
  readonly connected: boolean;
  readonly selectedTopic: string | null;
  readonly selectedProfile: ProfileSummary | null;
  readonly profileBusy: boolean;
  readonly readActive: boolean;
  readonly stopping: boolean;
  readonly mode: KafkaFetchMode;
  readonly timeError: string | undefined;
  readonly filters: KafkaMessageFilters;
  readonly openQueries: () => void;
  readonly toggleProfile: (profile: ProfileSummary) => Promise<void>;
  readonly startRead: (topic: string, search?: KafkaSearchFilter) => Promise<void>;
  readonly stopRead: () => Promise<void>;
}): readonly WorkbenchCommandAction[] {
  const canRead =
    connected &&
    selectedTopic !== null &&
    !readActive &&
    !stopping &&
    (mode !== "time-window" || timeError === undefined);
  const expression = filters.expression ?? "";
  const canSearch =
    canRead &&
    mode !== "tail" &&
    !filters.activeRuleMatchesOnly &&
    (expression.trim() === "" || validateKafkaRuleExpression(expression).valid);
  const search: KafkaSearchFilter = {
    key: filters.key,
    value: filters.value,
    offset: filters.offset,
    ...(filters.offsetExact === undefined ? {} : { offsetExact: filters.offsetExact }),
    timestamp: filters.timestamp,
    partition: filters.partition,
    ...(expression.length === 0 ? {} : { expression }),
  };
  return [
    { id: "queries", label: "Saved queries", disabled: false, run: openQueries },
    {
      id: "profile",
      label: `${connected && selectedProfile?.active ? "Disconnect" : "Connect"} profile ${selectedProfile?.name ?? "(select a profile)"}`,
      disabled: selectedProfile === null || profileBusy,
      run: (): void => {
        if (selectedProfile !== null && !profileBusy) void toggleProfile(selectedProfile);
      },
    },
    {
      id: "read",
      label: `${mode === "tail" ? "Start tail" : "Load messages"} ${selectedTopic ?? "(select a topic)"}`,
      disabled: !canRead,
      run: (): void => {
        if (canRead && selectedTopic !== null)
          void startRead(
            selectedTopic,
            mode !== "tail" && filters.offsetExact !== undefined ? search : undefined,
          );
      },
    },
    {
      id: "search",
      label: `Search broker ${selectedTopic ?? "(select a topic)"}`,
      disabled: !canSearch,
      run: (): void => {
        if (canSearch && selectedTopic !== null) void startRead(selectedTopic, search);
      },
    },
    {
      id: "stop",
      label: "Stop current read",
      disabled: !readActive || stopping,
      run: (): void => {
        if (readActive && !stopping) void stopRead();
      },
    },
  ];
}
