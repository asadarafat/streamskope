import {
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  type Dispatch,
  type SetStateAction,
} from "react";

import type { ActivityEntry, StreamSkopeHost } from "../contracts";
import type { StreamSkopeDesktop } from "../../../platform/desktop";

import { selectKafkaQueryMessages } from "./message-operations";
import { type KafkaUiAction, type KafkaUiState } from "./state";
import type { RendererStreamMonitorObserver } from "./stream-monitor-observer";

interface RendererStreamMonitorLifecycleOptions {
  readonly dispatch: Dispatch<KafkaUiAction>;
  readonly filterDurationMs: number | null;
  readonly host: StreamSkopeHost;
  readonly lastSequence: number;
  readonly observer: RendererStreamMonitorObserver;
  readonly presentationActive: boolean;
  readonly messagesMounted: boolean;
  readonly operationId: string | null;
  readonly rendererDroppedMessages: number;
  readonly rendererWindowEvictions: number;
  readonly retainedMessages: number;
  readonly visibleMessages: number;
}

export function useWorkbenchMessageSelection(
  presentationActive: boolean,
  messageFilters: KafkaUiState["messageFilters"],
  retainedMessages: KafkaUiState["messages"],
): {
  readonly unavailable: number;
  readonly durationMs: number | null;
  readonly messages: KafkaUiState["messages"];
} {
  return useMemo(() => {
    if (!presentationActive) {
      return { durationMs: null, messages: retainedMessages, unavailable: 0 };
    }
    const startedAt = globalThis.performance.now();
    const selection = selectKafkaQueryMessages(retainedMessages, messageFilters);
    return {
      durationMs: Math.max(0, globalThis.performance.now() - startedAt),
      ...selection,
    };
  }, [messageFilters, presentationActive, retainedMessages]);
}

export function useDesktopActions(
  desktop: StreamSkopeDesktop | undefined,
  openActivity: () => void,
  setPreferenceDialogMounted: Dispatch<SetStateAction<boolean>>,
  setPreferenceDialogOpen: Dispatch<SetStateAction<boolean>>,
): void {
  useEffect(() => {
    if (desktop === undefined) {
      return;
    }
    return desktop.subscribeActions((action) => {
      if (action.action === "activity.open") {
        setPreferenceDialogOpen(false);
        openActivity();
      } else {
        setPreferenceDialogMounted(true);
        setPreferenceDialogOpen(true);
      }
    });
  }, [desktop, openActivity, setPreferenceDialogMounted, setPreferenceDialogOpen]);
}

export function useAutoOpenActivityOnError(
  entries: readonly ActivityEntry[],
  activityOpen: boolean,
  openActivity: () => void,
): void {
  const lastAutoOpenedErrorId = useRef<string | null>(null);

  useEffect(() => {
    let latestErrorId: string | null = null;
    for (let index = entries.length - 1; index >= 0; index -= 1) {
      const entry = entries[index];
      if (entry?.severity === "error") {
        latestErrorId = entry.id;
        break;
      }
    }
    if (latestErrorId === null || latestErrorId === lastAutoOpenedErrorId.current) {
      return;
    }
    lastAutoOpenedErrorId.current = latestErrorId;
    if (!activityOpen) {
      openActivity();
    }
  }, [activityOpen, entries, openActivity]);
}

export function useRendererStreamMonitorLifecycle({
  dispatch,
  filterDurationMs,
  host,
  lastSequence,
  observer,
  presentationActive,
  messagesMounted,
  operationId,
  rendererDroppedMessages,
  rendererWindowEvictions,
  retainedMessages,
  visibleMessages,
}: RendererStreamMonitorLifecycleOptions): void {
  const presentationActiveRef = useRef(presentationActive);

  useLayoutEffect(() => {
    presentationActiveRef.current = presentationActive;
    observer.setPresentationActive(presentationActive);
  }, [observer, presentationActive]);

  useLayoutEffect(() => {
    observer.setOperation(operationId);
    observer.setMessagesMounted(messagesMounted);
  }, [messagesMounted, observer, operationId]);

  useLayoutEffect(() => {
    if (filterDurationMs !== null) {
      observer.recordFilterDuration(filterDurationMs);
    }
  }, [filterDurationMs, observer]);

  useLayoutEffect(() => {
    if (!presentationActive || lastSequence < 0) {
      return;
    }
    observer.commit({
      lastSequence,
      rendererDroppedMessages,
      rendererWindowEvictions,
      retainedMessages,
      visibleMessages,
    });
  }, [
    lastSequence,
    observer,
    presentationActive,
    rendererDroppedMessages,
    rendererWindowEvictions,
    retainedMessages,
    visibleMessages,
  ]);

  useEffect(
    () =>
      host.subscribe((event) => {
        if (presentationActiveRef.current || event.event === "streamMetrics.changed") {
          observer.eventReceived(event);
        }
        dispatch({ event, type: "host.event" });
      }),
    [dispatch, host, observer],
  );

  useEffect(
    () => (): void => {
      observer.dispose();
    },
    [observer],
  );
}
