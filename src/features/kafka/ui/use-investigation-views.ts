import { useCallback, useEffect, useRef, useState, type Dispatch } from "react";

import { HostContractValidationError, type KafkaInvestigationQuery } from "../contracts";
import {
  createEmptyKafkaSavedRecordContext,
  type KafkaSavedRecordContext,
} from "../contracts/record-locator";
import { createDefaultKafkaInvestigationView } from "../contracts/investigation-view";

import { initialKafkaMessageFilters } from "./message-operations";
import { parseViewSettings, type KafkaViewSettings } from "./investigation-view-settings";
import { useMessageViewPresentation } from "./use-message-view-presentation";
import type { KafkaUiAction, KafkaUiState } from "./state";
import type { NavigationView } from "./workbench-navigation";
import type { TopicWorkspaceView } from "./WorkbenchContextBar";
import { isKafkaConsumptionActive, kafkaConsumptionStopLabel } from "./workbench-status";

export function useInvestigationViews({
  state,
  connected,
  navigation,
  selectedTopic,
  topicWorkspace,
  selectedGroupId,
  captureQuery,
  captureRecords = createEmptyKafkaSavedRecordContext,
  restoreRecords,
  restoreQuery,
  clearQuery,
  restoreGroup,
  setTopicWorkspace,
  setNavigation,
  setSelectedProfileId,
  dispatch,
  closeEditors,
  stopping,
  initialQueryImport,
  initialRestoredView,
  onPendingViewConnection,
}: {
  readonly state: Pick<
    KafkaUiState,
    "profiles" | "consumptionState" | "consumptionRequest" | "consumptionError" | "latency"
  >;
  readonly connected: boolean;
  readonly navigation: NavigationView;
  readonly selectedTopic: string | null;
  readonly topicWorkspace: TopicWorkspaceView;
  readonly selectedGroupId: string | null;
  readonly captureQuery: () => KafkaInvestigationQuery;
  readonly captureRecords?: () => KafkaSavedRecordContext;
  readonly restoreRecords?: (records: KafkaSavedRecordContext) => void;
  readonly restoreQuery: (query: KafkaInvestigationQuery, awaitConnection: boolean) => void;
  readonly clearQuery: () => void;
  readonly restoreGroup: (groupId: string) => void;
  readonly setTopicWorkspace: (task: TopicWorkspaceView) => void;
  readonly setNavigation: (page: NavigationView) => void;
  readonly setSelectedProfileId: (id: string | null) => void;
  readonly dispatch: Dispatch<KafkaUiAction>;
  readonly closeEditors: () => void;
  readonly stopping: boolean;
  readonly initialQueryImport?: string | undefined;
  readonly initialRestoredView?: KafkaViewSettings | undefined;
  readonly onPendingViewConnection?:
    ((settings: KafkaViewSettings, profileId: string | undefined) => void) | undefined;
}): {
  readonly open: boolean;
  readonly queryImport: string | undefined;
  readonly error: string | undefined;
  readonly revision: number;
  readonly readActive: boolean;
  readonly currentResource: string | null;
  readonly presentation: ReturnType<typeof useMessageViewPresentation>;
  capture(this: void): KafkaViewSettings;
  restore(this: void, settings: KafkaViewSettings, profileId?: string): void;
  show(this: void): void;
  close(this: void): void;
} {
  const [open, setOpen] = useState(initialQueryImport !== undefined);
  const [queryImport, setQueryImport] = useState(initialQueryImport);
  const [error, setError] = useState<string>();
  const [revision, setRevision] = useState(0);
  const presentation = useMessageViewPresentation();
  const pending = useRef<
    | { settings: KafkaViewSettings; profileId: string | undefined; disconnected: boolean }
    | undefined
  >(undefined);
  const initialApplied = useRef(false);
  const readActive =
    stopping ||
    isKafkaConsumptionActive(state.consumptionState, state.consumptionRequest) ||
    kafkaConsumptionStopLabel(
      state.consumptionState,
      state.consumptionRequest,
      state.consumptionError,
    ) !== null ||
    state.latency.state === "running";
  const currentResource =
    navigation === "consumer-groups" && selectedGroupId !== null
      ? `Consumer group ${selectedGroupId}`
      : navigation === "topics" && selectedTopic !== null
        ? `${selectedTopic} · ${topicWorkspace}`
        : null;
  const capture = useCallback((): KafkaViewSettings => {
    if (currentResource === null)
      throw new HostContractValidationError("View", "choose a topic task or consumer group first");
    return parseViewSettings({
      configuration: selectedTopic === null ? null : captureQuery(),
      records: captureRecords(),
      view: {
        ...createDefaultKafkaInvestigationView(
          navigation === "consumer-groups" && selectedGroupId !== null
            ? { kind: "consumer-group", groupId: selectedGroupId }
            : { kind: "topic", workspace: topicWorkspace },
        ),
        messages: presentation.value,
      },
    });
  }, [
    currentResource,
    selectedTopic,
    captureQuery,
    captureRecords,
    navigation,
    selectedGroupId,
    topicWorkspace,
    presentation.value,
  ]);
  const apply = useCallback(
    (settings: KafkaViewSettings, awaitConnection: boolean): void => {
      restoreRecords?.(settings.records);
      if (settings.configuration !== null) restoreQuery(settings.configuration, awaitConnection);
      else clearQuery();
      presentation.restore(settings.view.messages);
      dispatch({
        type: "query.restored",
        filters:
          settings.configuration?.filters ??
          settings.configuration?.request.search ??
          initialKafkaMessageFilters,
      });
      closeEditors();
      setRevision((value) => value + 1);
      if (awaitConnection) setNavigation("profiles");
      else if (settings.view.destination.kind === "consumer-group") {
        restoreGroup(settings.view.destination.groupId);
        setNavigation("consumer-groups");
      } else {
        setTopicWorkspace(settings.view.destination.workspace);
        setNavigation("topics");
      }
    },
    [
      restoreRecords,
      restoreQuery,
      clearQuery,
      presentation.restore,
      dispatch,
      closeEditors,
      setNavigation,
      restoreGroup,
      setTopicWorkspace,
    ],
  );
  const restore = useCallback(
    (input: KafkaViewSettings, profileId?: string): void => {
      setError(undefined);
      if (readActive) {
        setError(
          "Stop the current read or latency probe and wait for confirmation before opening another view.",
        );
        return;
      }
      try {
        const settings = parseViewSettings(input);
        const profile = state.profiles.find((entry) => entry.id === profileId);
        if (profileId !== undefined && profile === undefined) {
          setError(
            "The saved profile is unavailable. Choose an existing profile or clear the reference before opening.",
          );
          return;
        }
        const needsConnection = !connected || (profile !== undefined && !profile.active);
        pending.current = needsConnection
          ? { settings, profileId, disconnected: !connected }
          : undefined;
        if (needsConnection) onPendingViewConnection?.(settings, profileId);
        apply(settings, needsConnection);
        if (profile !== undefined) setSelectedProfileId(profile.id);
        setOpen(false);
        setQueryImport(undefined);
      } catch (failure) {
        setError(
          failure instanceof HostContractValidationError
            ? failure.message
            : "This view could not be restored. Refresh the library and retry.",
        );
      }
    },
    [readActive, state.profiles, connected, onPendingViewConnection, apply, setSelectedProfileId],
  );
  useEffect(() => {
    // The provider activation owner already checked the exact selected profile identity.
    if (initialRestoredView === undefined || !connected || initialApplied.current) return;
    initialApplied.current = true;
    apply(parseViewSettings(initialRestoredView), false);
  }, [initialRestoredView, connected, apply]);
  useEffect(() => {
    // Direct Kafka embeddings do not remount through provider activation.
    const intent = pending.current;
    if (onPendingViewConnection !== undefined || intent === undefined) return;
    if (!connected) {
      intent.disconnected = true;
      return;
    }
    const active = state.profiles.find((profile) => profile.active);
    if (intent.profileId !== undefined && active?.id !== intent.profileId) {
      if (intent.disconnected && active !== undefined) pending.current = undefined;
      return;
    }
    if (!intent.disconnected && intent.profileId === undefined) return;
    pending.current = undefined;
    apply(intent.settings, false);
  }, [connected, state.profiles, onPendingViewConnection, apply]);
  return {
    open,
    queryImport,
    error,
    revision,
    readActive,
    currentResource,
    presentation,
    capture,
    restore,
    show: (): void => {
      setError(undefined);
      setOpen(true);
    },
    close: (): void => {
      setOpen(false);
      setQueryImport(undefined);
      setError(undefined);
    },
  };
}
