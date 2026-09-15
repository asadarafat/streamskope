import {
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  type Dispatch,
  type SetStateAction,
} from "react";

import type { ActivityEntry, StreamSkopeHost } from "../contracts";
import type { StreamSkopeDesktop } from "../../platform/desktop";

import { selectVisibleKafkaMessages, type KafkaUiAction, type KafkaUiState } from "./state";
import type { RendererStreamMonitorObserver } from "./stream-monitor-observer";

interface RendererStreamMonitorLifecycleOptions {
  readonly dispatch: Dispatch<KafkaUiAction>;
  readonly filterDurationMs: number | null;
  readonly host: StreamSkopeHost;
  readonly lastSequence: number;
  readonly observer: RendererStreamMonitorObserver;
  readonly presentationActive: boolean;
  readonly rendererDroppedMessages: number;
  readonly retainedMessages: number;
  readonly visibleMessages: number;
}

export function useWorkbenchMessageSelection(
  presentationActive: boolean,
  messageFilters: KafkaUiState["messageFilters"],
  retainedMessages: KafkaUiState["messages"],
): {
  readonly durationMs: number | null;
  readonly messages: KafkaUiState["messages"];
} {
  return useMemo(() => {
    if (!presentationActive) {
      return { durationMs: null, messages: retainedMessages };
    }
    const startedAt = globalThis.performance.now();
    const messages = selectVisibleKafkaMessages({
      messageFilters,
      messages: retainedMessages,
    });
    return {
      durationMs: Math.max(0, globalThis.performance.now() - startedAt),
      messages,
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
  rendererDroppedMessages,
  retainedMessages,
  visibleMessages,
}: RendererStreamMonitorLifecycleOptions): void {
  const presentationActiveRef = useRef(presentationActive);

  useLayoutEffect(() => {
    presentationActiveRef.current = presentationActive;
    observer.setPresentationActive(presentationActive);
  }, [observer, presentationActive]);

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
      retainedMessages,
      visibleMessages,
    });
  }, [
    lastSequence,
    observer,
    presentationActive,
    rendererDroppedMessages,
    retainedMessages,
    visibleMessages,
  ]);

  useEffect(
    () =>
      host.subscribe((event) => {
        if (presentationActiveRef.current) {
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
