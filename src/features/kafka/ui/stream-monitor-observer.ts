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
  readonly operationId: string | null;
  readonly messagesMounted: boolean;
  readonly eventSampledAt: string | null;
  readonly filterSampledAt: string | null;
  readonly renderSampledAt: string | null;
  readonly fpsSampledAt: string | null;
  readonly fpsWindowMs: number | null;
  readonly eventBacklog: number;
  readonly eventToCommitMs: number | null;
  readonly filterDurationMs: number | null;
  readonly fps: number | null;
  readonly rendererDroppedMessages: number;
  readonly rendererWindowEvictions: number;
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
  readonly rendererWindowEvictions: number;
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
  setMessagesMounted(mounted: boolean): void;
  setOperation(operationId: string | null): void;
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

export const initialRendererStreamMonitorSample: RendererStreamMonitorSample = Object.freeze({
  operationId: null,
  messagesMounted: true,
  eventSampledAt: null,
  filterSampledAt: null,
  renderSampledAt: null,
  fpsSampledAt: null,
  fpsWindowMs: null,
  eventBacklog: 0,
  eventToCommitMs: null,
  filterDurationMs: null,
  fps: null,
  rendererDroppedMessages: 0,
  rendererWindowEvictions: 0,
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
    operationId: snapshot.operationId,
    messagesMounted: snapshot.messagesMounted,
    eventSampledAt: snapshot.eventSampledAt,
    filterSampledAt: snapshot.filterSampledAt,
    renderSampledAt: snapshot.renderSampledAt,
    fpsSampledAt: snapshot.fpsSampledAt,
    fpsWindowMs: snapshot.fpsWindowMs,
    eventBacklog: snapshot.eventBacklog,
    eventToCommitMs: snapshot.eventToCommitMs,
    filterDurationMs: snapshot.filterDurationMs,
    fps: snapshot.fps,
    rendererDroppedMessages: snapshot.rendererDroppedMessages,
    rendererWindowEvictions: snapshot.rendererWindowEvictions,
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
  let lastReceivedSequence = -1;
  let pendingFilterDurationMs: number | null = null;
  let pendingRenderDurationMs: number | null = null;
  let presentationActive = true;
  let messagesMounted = true;
  let snapshot = freezeSnapshot(initialRendererStreamMonitorSample, history);

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
          fpsSampledAt: null,
          fpsWindowMs: null,
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
      const measuredAt = wallNow().toISOString();
      const sample = freezeSample({
        ...currentSample(snapshot),
        fps,
        fpsSampledAt: measuredAt,
        fpsWindowMs: elapsedMs,
        sampledAt: measuredAt,
        samplingState: "ready",
      });
      // A frame tick is new frame evidence only, never another message-work measurement.
      history = [
        ...history,
        freezeSample({
          ...sample,
          eventToCommitMs: null,
          eventSampledAt: null,
          filterDurationMs: null,
          filterSampledAt: null,
          renderDurationMs: null,
          renderSampledAt: null,
        }),
      ].slice(-KAFKA_STREAM_MONITOR_HISTORY_LIMIT);
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

  function setOperation(operationId: string | null): void {
    if (disposed || snapshot.operationId === operationId) return;
    pendingEvents.clear();
    pendingFilterDurationMs = null;
    pendingRenderDurationMs = null;
    history = [];
    frameCount = 0;
    frameWindowStartedAt = monotonicNow();
    publish(
      freezeSnapshot(
        {
          ...initialRendererStreamMonitorSample,
          operationId,
          messagesMounted,
          samplingState: snapshot.samplingState === "ready" ? "sampling" : snapshot.samplingState,
        },
        history,
      ),
    );
  }

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
      const measuredAt = wallNow().toISOString();
      const filterDurationMs =
        committedMessageWork && messagesMounted ? pendingFilterDurationMs : null;
      const renderDurationMs =
        committedMessageWork && messagesMounted ? pendingRenderDurationMs : null;
      const sample = freezeSample({
        ...currentSample(snapshot),
        eventBacklog: pendingEvents.size,
        eventToCommitMs: eventToCommitMs ?? snapshot.eventToCommitMs,
        eventSampledAt: eventToCommitMs === null ? snapshot.eventSampledAt : measuredAt,
        filterDurationMs: filterDurationMs ?? snapshot.filterDurationMs,
        filterSampledAt: filterDurationMs === null ? snapshot.filterSampledAt : measuredAt,
        renderDurationMs: renderDurationMs ?? snapshot.renderDurationMs,
        renderSampledAt: renderDurationMs === null ? snapshot.renderSampledAt : measuredAt,
        rendererDroppedMessages: boundedCount(
          input.rendererDroppedMessages,
          "rendererDroppedMessages",
        ),
        rendererWindowEvictions: boundedCount(
          input.rendererWindowEvictions,
          "rendererWindowEvictions",
        ),
        retainedMessages: boundedCount(input.retainedMessages, "retainedMessages"),
        sampledAt: measuredAt,
        visibleMessages: boundedCount(input.visibleMessages, "visibleMessages"),
      });
      history = [
        ...history,
        freezeSample({
          ...sample,
          eventToCommitMs,
          eventSampledAt: eventToCommitMs === null ? null : measuredAt,
          filterDurationMs,
          filterSampledAt: filterDurationMs === null ? null : measuredAt,
          renderDurationMs,
          renderSampledAt: renderDurationMs === null ? null : measuredAt,
          fps: null,
          fpsSampledAt: null,
          fpsWindowMs: null,
        }),
      ].slice(-KAFKA_STREAM_MONITOR_HISTORY_LIMIT);
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
      if (disposed || event.sequence <= Math.max(lastCommittedSequence, lastReceivedSequence))
        return;
      lastReceivedSequence = event.sequence;
      if (event.event === "streamMetrics.changed") {
        if (event.payload.state === "loading" || snapshot.operationId === null)
          setOperation(event.payload.operationId);
        else if (snapshot.operationId !== event.payload.operationId) return;
      }
      if (!presentationActive) {
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
      if (!presentationActive || !messagesMounted) {
        return;
      }
      pendingFilterDurationMs = measuredDuration(durationMs);
    },
    recordRenderDuration(durationMs): void {
      if (!presentationActive || !messagesMounted) {
        return;
      }
      pendingRenderDurationMs = measuredDuration(durationMs);
    },
    setOperation,
    setMessagesMounted(mounted): void {
      if (disposed || messagesMounted === mounted) return;
      messagesMounted = mounted;
      pendingFilterDurationMs = null;
      pendingRenderDurationMs = null;
      publish(freezeSnapshot({ ...currentSample(snapshot), messagesMounted: mounted }, history));
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
