import { KAFKA_STREAM_MONITOR_HISTORY_LIMIT, type HostEvent } from "../contracts";

export const RENDERER_STREAM_MONITOR_PENDING_EVENT_LIMIT = 512 as const;
export const RENDERER_STREAM_MONITOR_PRESSURE_LIMITS = Object.freeze({
  eventToCommitMs: 120,
  filterDurationMs: 24,
  renderDurationMs: 24,
});

export const RENDERER_STREAM_MONITOR_SAMPLING_STATES = [
  "unavailable",
  "sampling",
  "ready",
  "hidden",
] as const;

export type RendererStreamMonitorSamplingState =
  (typeof RENDERER_STREAM_MONITOR_SAMPLING_STATES)[number];

export interface RendererStreamMonitorSample {
  readonly eventBacklog: number;
  readonly eventToCommitMs: number | null;
  readonly filterDurationMs: number | null;
  readonly fps: number | null;
  readonly rendererDroppedMessages: number;
  readonly renderDurationMs: number | null;
  readonly retainedMessages: number;
  readonly sampledAt: string | null;
  readonly samplingState: RendererStreamMonitorSamplingState;
  readonly visibleMessages: number;
}

export interface RendererStreamMonitorSnapshot extends RendererStreamMonitorSample {
  readonly history: readonly RendererStreamMonitorSample[];
}

export interface RendererStreamMonitorCommit {
  readonly lastSequence: number;
  readonly rendererDroppedMessages: number;
  readonly retainedMessages: number;
  readonly visibleMessages: number;
}

export interface RendererStreamMonitorObserver {
  commit(input: RendererStreamMonitorCommit): void;
  dispose(): void;
  eventReceived(event: HostEvent): void;
  getSnapshot(): RendererStreamMonitorSnapshot;
  recordFilterDuration(durationMs: number): void;
  recordRenderDuration(durationMs: number): void;
  setPresentationActive(active: boolean): void;
  subscribe(listener: () => void): () => void;
}

export interface RendererStreamMonitorObserverOptions {
  readonly cancelFrame?: (frameId: number) => void;
  readonly isDocumentVisible?: () => boolean;
  readonly monotonicNow?: () => number;
  readonly requestFrame?: (callback: (timestamp: number) => void) => number;
  readonly subscribeVisibility?: (listener: () => void) => () => void;
  readonly wallNow?: () => Date;
}

interface PendingRendererEvent {
  readonly messageWork: boolean;
  readonly receivedAtMs: number;
}

const initialSample: RendererStreamMonitorSample = Object.freeze({
  eventBacklog: 0,
  eventToCommitMs: null,
  filterDurationMs: null,
  fps: null,
  rendererDroppedMessages: 0,
  renderDurationMs: null,
  retainedMessages: 0,
  sampledAt: null,
  samplingState: "unavailable",
  visibleMessages: 0,
});

function freezeSample(sample: RendererStreamMonitorSample): RendererStreamMonitorSample {
  return Object.freeze(sample);
}

function freezeSnapshot(
  sample: RendererStreamMonitorSample,
  history: readonly RendererStreamMonitorSample[],
): RendererStreamMonitorSnapshot {
  return Object.freeze({
    ...sample,
    history: Object.freeze([...history]),
  });
}

function currentSample(snapshot: RendererStreamMonitorSnapshot): RendererStreamMonitorSample {
  return {
    eventBacklog: snapshot.eventBacklog,
    eventToCommitMs: snapshot.eventToCommitMs,
    filterDurationMs: snapshot.filterDurationMs,
    fps: snapshot.fps,
    rendererDroppedMessages: snapshot.rendererDroppedMessages,
    renderDurationMs: snapshot.renderDurationMs,
    retainedMessages: snapshot.retainedMessages,
    sampledAt: snapshot.sampledAt,
    samplingState: snapshot.samplingState,
    visibleMessages: snapshot.visibleMessages,
  };
}

function measuredDuration(value: number): number | null {
  return Number.isFinite(value) && value >= 0 ? value : null;
}

function boundedCount(value: number, field: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${field} must be a non-negative safe integer.`);
  }
  return value;
}

export function createRendererStreamMonitorObserver(
  options: RendererStreamMonitorObserverOptions = {},
): RendererStreamMonitorObserver {
  const monotonicNow = options.monotonicNow ?? ((): number => globalThis.performance.now());
  const wallNow = options.wallNow ?? ((): Date => new Date());
  const requestFrame =
    options.requestFrame ??
    (typeof globalThis.requestAnimationFrame === "function"
      ? (callback: (timestamp: number) => void): number =>
          globalThis.requestAnimationFrame(callback)
      : undefined);
  const cancelFrame =
    options.cancelFrame ??
    (typeof globalThis.cancelAnimationFrame === "function"
      ? (frameId: number): void => {
          globalThis.cancelAnimationFrame(frameId);
        }
      : (): void => undefined);
  const isDocumentVisible =
    options.isDocumentVisible ??
    ((): boolean =>
      typeof globalThis.document !== "undefined" &&
      globalThis.document.visibilityState !== "hidden");
  const subscribeVisibility =
    options.subscribeVisibility ??
    (typeof globalThis.document !== "undefined"
      ? (listener: () => void): (() => void) => {
          globalThis.document.addEventListener("visibilitychange", listener);
          return (): void => {
            globalThis.document.removeEventListener("visibilitychange", listener);
          };
        }
      : undefined);
  const listeners = new Set<() => void>();
  const pendingEvents = new Map<number, PendingRendererEvent>();
  let activeFrameId: number | null = null;
  let disposed = false;
  let frameCount = 0;
  let frameWindowStartedAt = 0;
  let history: readonly RendererStreamMonitorSample[] = [];
  let lastCommittedSequence = -1;
  let pendingFilterDurationMs: number | null = null;
  let pendingRenderDurationMs: number | null = null;
  let presentationActive = true;
  let snapshot = freezeSnapshot(initialSample, history);

  function publish(next: RendererStreamMonitorSnapshot): void {
    if (disposed) {
      return;
    }
    snapshot = next;
    for (const listener of listeners) {
      listener();
    }
  }

  function publishBacklog(): void {
    publish(
      freezeSnapshot(
        freezeSample({
          ...currentSample(snapshot),
          eventBacklog: pendingEvents.size,
        }),
        history,
      ),
    );
  }

  function publishSamplingState(
    samplingState: RendererStreamMonitorSamplingState,
    fps: number | null,
  ): void {
    publish(
      freezeSnapshot(
        freezeSample({
          ...currentSample(snapshot),
          fps,
          samplingState,
        }),
        history,
      ),
    );
  }

  function stopFrameLoop(samplingState: "hidden" | "unavailable"): void {
    if (activeFrameId !== null) {
      cancelFrame(activeFrameId);
      activeFrameId = null;
    }
    frameCount = 0;
    publishSamplingState(samplingState, null);
  }

  function onFrame(): void {
    activeFrameId = null;
    if (disposed || listeners.size === 0 || requestFrame === undefined) {
      return;
    }
    if (!isDocumentVisible()) {
      stopFrameLoop("hidden");
      return;
    }
    frameCount += 1;
    const sampledAtMs = monotonicNow();
    const elapsedMs = sampledAtMs - frameWindowStartedAt;
    if (elapsedMs >= 1_000) {
      const fps = Math.max(0, Math.round((frameCount * 1_000) / elapsedMs));
      const sample = freezeSample({
        ...currentSample(snapshot),
        fps,
        sampledAt: wallNow().toISOString(),
        samplingState: "ready",
      });
      history = [...history, sample].slice(-KAFKA_STREAM_MONITOR_HISTORY_LIMIT);
      frameCount = 0;
      frameWindowStartedAt = sampledAtMs;
      publish(freezeSnapshot(sample, history));
    }
    activeFrameId = requestFrame(onFrame);
  }

  function startFrameLoop(): void {
    if (
      disposed ||
      !presentationActive ||
      listeners.size === 0 ||
      activeFrameId !== null ||
      requestFrame === undefined
    ) {
      return;
    }
    if (!isDocumentVisible()) {
      publishSamplingState("hidden", null);
      return;
    }
    frameCount = 0;
    frameWindowStartedAt = monotonicNow();
    publishSamplingState("sampling", null);
    activeFrameId = requestFrame(onFrame);
  }

  function onVisibilityChanged(): void {
    if (disposed || listeners.size === 0 || requestFrame === undefined) {
      return;
    }
    if (isDocumentVisible()) {
      startFrameLoop();
    } else {
      stopFrameLoop("hidden");
    }
  }

  const removeVisibilityListener = subscribeVisibility?.(onVisibilityChanged);

  return {
    commit(input): void {
      if (disposed || !presentationActive) {
        return;
      }
      const committedAt = monotonicNow();
      lastCommittedSequence = Math.max(lastCommittedSequence, input.lastSequence);
      let committedMessageWork = false;
      let latestCommittedSequence = -1;
      let latestReceivedAt: number | undefined;
      for (const [sequence, pendingEvent] of pendingEvents) {
        if (sequence <= lastCommittedSequence) {
          pendingEvents.delete(sequence);
          committedMessageWork ||= pendingEvent.messageWork;
          if (sequence > latestCommittedSequence) {
            latestCommittedSequence = sequence;
            latestReceivedAt = pendingEvent.receivedAtMs;
          }
        }
      }
      const eventToCommitMs =
        latestReceivedAt === undefined ? null : measuredDuration(committedAt - latestReceivedAt);
      const sample = freezeSample({
        eventBacklog: pendingEvents.size,
        eventToCommitMs,
        filterDurationMs: committedMessageWork
          ? pendingFilterDurationMs
          : snapshot.filterDurationMs,
        fps: snapshot.fps,
        rendererDroppedMessages: boundedCount(
          input.rendererDroppedMessages,
          "rendererDroppedMessages",
        ),
        renderDurationMs: committedMessageWork
          ? pendingRenderDurationMs
          : snapshot.renderDurationMs,
        retainedMessages: boundedCount(input.retainedMessages, "retainedMessages"),
        sampledAt: wallNow().toISOString(),
        samplingState: snapshot.samplingState,
        visibleMessages: boundedCount(input.visibleMessages, "visibleMessages"),
      });
      history = [...history, sample].slice(-KAFKA_STREAM_MONITOR_HISTORY_LIMIT);
      pendingFilterDurationMs = null;
      pendingRenderDurationMs = null;
      publish(freezeSnapshot(sample, history));
    },
    dispose(): void {
      if (activeFrameId !== null) {
        cancelFrame(activeFrameId);
        activeFrameId = null;
      }
      removeVisibilityListener?.();
      disposed = true;
      listeners.clear();
      pendingEvents.clear();
      history = [];
      pendingFilterDurationMs = null;
      pendingRenderDurationMs = null;
    },
    eventReceived(event): void {
      if (disposed || !presentationActive || event.sequence <= lastCommittedSequence) {
        return;
      }
      pendingEvents.set(event.sequence, {
        messageWork: event.event === "messages.batch",
        receivedAtMs: monotonicNow(),
      });
      while (pendingEvents.size > RENDERER_STREAM_MONITOR_PENDING_EVENT_LIMIT) {
        const oldestSequence = pendingEvents.keys().next().value;
        if (oldestSequence === undefined) {
          break;
        }
        pendingEvents.delete(oldestSequence);
      }
      publishBacklog();
    },
    getSnapshot(): RendererStreamMonitorSnapshot {
      return snapshot;
    },
    recordFilterDuration(durationMs): void {
      if (!presentationActive) {
        return;
      }
      pendingFilterDurationMs = measuredDuration(durationMs);
    },
    recordRenderDuration(durationMs): void {
      if (!presentationActive) {
        return;
      }
      pendingRenderDurationMs = measuredDuration(durationMs);
    },
    setPresentationActive(active): void {
      if (disposed || presentationActive === active) {
        return;
      }
      presentationActive = active;
      if (!active) {
        if (activeFrameId !== null) {
          cancelFrame(activeFrameId);
          activeFrameId = null;
        }
        frameCount = 0;
        pendingEvents.clear();
        pendingFilterDurationMs = null;
        pendingRenderDurationMs = null;
        snapshot = freezeSnapshot(
          freezeSample({
            ...currentSample(snapshot),
            eventBacklog: 0,
            fps: null,
            samplingState: "unavailable",
          }),
          history,
        );
        return;
      }
      startFrameLoop();
    },
    subscribe(listener): () => void {
      if (disposed) {
        return (): void => undefined;
      }
      listeners.add(listener);
      if (listeners.size === 1 && presentationActive) {
        startFrameLoop();
      }
      return (): void => {
        listeners.delete(listener);
        if (listeners.size === 0 && !disposed) {
          stopFrameLoop("unavailable");
        }
      };
    },
  };
}
