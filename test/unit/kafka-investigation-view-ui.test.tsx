// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import {
  createDefaultKafkaInvestigationView,
  createEmptyKafkaSavedRecordContext,
  KAFKA_TOPIC_VIEW_WORKSPACES,
  type KafkaInvestigationQuery,
} from "../../src/features/kafka/contracts";
import { initialKafkaUiState } from "../../src/features/kafka/ui/state";
import { useInvestigationViews } from "../../src/features/kafka/ui/use-investigation-views";
import { useMessageViewPresentation } from "../../src/features/kafka/ui/use-message-view-presentation";
import {
  changedMessageGridColumns,
  messageGridVisibility,
} from "../../src/features/kafka/ui/message-grid-presentation";

type Props = Parameters<typeof useInvestigationViews>[0];
const query: KafkaInvestigationQuery = {
  schemaVersion: 1,
  request: { topic: "orders", mode: "earliest", maxMessages: 25 },
};
const settings = {
  records: createEmptyKafkaSavedRecordContext(),
  configuration: query,
  view: {
    ...createDefaultKafkaInvestigationView(),
    messages: {
      ...createDefaultKafkaInvestigationView().messages,
      visibleColumns: ["key", "preview"] as const,
      columnWidths: [{ column: "key" as const, pixels: 240 }],
      inspectorWidth: 416,
      filtersOpen: true,
    },
  },
};
function setup(overrides: Partial<Props> = {}): ReturnType<
  typeof renderHook<ReturnType<typeof useInvestigationViews>, Props>
> & {
  props: Props;
} {
  const props: Props = {
    state: initialKafkaUiState,
    connected: true,
    navigation: "topics",
    selectedTopic: "orders",
    topicWorkspace: "messages",
    selectedGroupId: null,
    captureQuery: vi.fn(() => query),
    restoreQuery: vi.fn(),
    clearQuery: vi.fn(),
    restoreGroup: vi.fn(),
    setTopicWorkspace: vi.fn(),
    setNavigation: vi.fn(),
    setSelectedProfileId: vi.fn(),
    dispatch: vi.fn(),
    closeEditors: vi.fn(),
    stopping: false,
    ...overrides,
  };
  return { ...renderHook(useInvestigationViews, { initialProps: props }), props };
}
afterEach(() => {
  cleanup();
  localStorage.clear();
});

it.each(KAFKA_TOPIC_VIEW_WORKSPACES)(
  "restores the %s task and whole presentation without acquiring read or connection authority",
  (workspace) => {
    const { result, props } = setup();
    act(() =>
      result.current.restore({
        ...settings,
        view: { ...settings.view, destination: { kind: "topic", workspace } },
      }),
    );
    expect(props.restoreQuery).toHaveBeenCalledExactlyOnceWith(query, false);
    expect(props.setTopicWorkspace).toHaveBeenLastCalledWith(workspace);
    expect(props.setNavigation).toHaveBeenLastCalledWith("topics");
    expect(props.restoreGroup).not.toHaveBeenCalled();
    expect(result.current.presentation.value).toEqual(settings.view.messages);
    expect(props.closeEditors).toHaveBeenCalledOnce();
    expect(props.dispatch).toHaveBeenCalledWith(
      expect.objectContaining({ type: "query.restored" }),
    );
    expect(result.current.revision).toBe(1);
  },
);

it("captures and restores a group-only view without inventing a topic", () => {
  const { result, props } = setup({
    navigation: "consumer-groups",
    selectedGroupId: "payments",
    selectedTopic: null,
  });
  const view = result.current.capture();
  expect(view.configuration).toBeNull();
  expect(view.view.destination).toEqual({ kind: "consumer-group", groupId: "payments" });
  expect(props.captureQuery).not.toHaveBeenCalled();
  act(() => result.current.restore(view));
  expect(props.clearQuery).toHaveBeenCalledOnce();
  expect(props.restoreQuery).not.toHaveBeenCalled();
  expect(props.restoreGroup).toHaveBeenCalledExactlyOnceWith("payments");
  expect(props.setNavigation).toHaveBeenLastCalledWith("consumer-groups");
});

it("waits for a pending stop acknowledgment before restoring", () => {
  const { result, props, rerender } = setup({ stopping: true });
  act(() => {
    result.current.show();
    result.current.restore(settings);
  });
  expect(result.current.error).toMatch(/wait for confirmation/);
  expect(result.current.open).toBe(true);
  expect(props.restoreQuery).not.toHaveBeenCalled();
  rerender({ ...props, stopping: false });
  act(() => result.current.restore(settings));
  expect(props.restoreQuery).toHaveBeenCalledOnce();
});

it("binds pending restoration to the exact profile ID even when names match", () => {
  const profile = {
    id: "active",
    name: "Same name",
    revision: 1,
    brokers: ["localhost:9092"],
    transport: "plaintext" as const,
    active: true,
    createdAt: "2026-10-09T00:00:00.000Z",
    updatedAt: "2026-10-09T00:00:00.000Z",
  };
  const onPendingViewConnection = vi.fn();
  const { result, props } = setup({
    state: {
      ...initialKafkaUiState,
      profiles: [profile, { ...profile, id: "saved", active: false }],
    },
    onPendingViewConnection,
  });
  act(() => result.current.restore(settings, "saved"));
  expect(onPendingViewConnection).toHaveBeenCalledExactlyOnceWith(settings, "saved");
  expect(props.setNavigation).toHaveBeenLastCalledWith("profiles");
  expect(props.restoreQuery).toHaveBeenCalledExactlyOnceWith(query, true);
});

it("restoring an inspector width preserves the global manual-resize default", () => {
  localStorage.setItem("streamskope-inspector-pane-width", "352");
  const { result } = renderHook(useMessageViewPresentation);
  expect(result.current.value.inspectorWidth).toBe(352);
  act(() => result.current.restore(settings.view.messages));
  expect(result.current.value.inspectorWidth).toBe(416);
  expect(localStorage.getItem("streamskope-inspector-pane-width")).toBe("352");
  act(() => result.current.change({ inspectorWidth: 432 }));
  expect(localStorage.getItem("streamskope-inspector-pane-width")).toBe("432");
  act(() => result.current.change({ inspectorWidth: 600 }));
  expect(result.current.value.inspectorWidth).toBe(560);
  expect(localStorage.getItem("streamskope-inspector-pane-width")).toBe("560");
  act(() => result.current.change({ inspectorWidth: 260 }));
  expect(result.current.value.inspectorWidth).toBe(292);
});

it("keeps compact layout hides out of saved column intent, including one-column views", () => {
  const presentation = settings.view.messages;
  const compact = messageGridVisibility(presentation, true);
  expect(compact.preview).toBe(false);
  expect(changedMessageGridColumns(presentation, true, compact)).toEqual(["key", "preview"]);
  expect(changedMessageGridColumns(presentation, true, { ...compact, key: false })).toEqual([
    "preview",
  ]);
  const single = { ...presentation, visibleColumns: ["preview"] as const };
  expect(messageGridVisibility(single, true).preview).toBe(true);
  expect(
    changedMessageGridColumns(single, true, {
      ...messageGridVisibility(single, true),
      preview: false,
    }),
  ).toEqual(["preview"]);
  expect(messageGridVisibility(presentation, false).preview).toBe(true);
});

it("preserves unresolved stream-cleanup authority instead of clearing Retry stop on restore", () => {
  const { result, props } = setup({
    state: {
      ...initialKafkaUiState,
      consumptionState: "failed",
      consumptionRequest: query.request,
      consumptionError: {
        activeStateChanged: false,
        code: "TIMEOUT",
        retryable: true,
        target: "kafka-consumption-cleanup",
        stage: "backend",
        summary: "Stop not confirmed",
        recovery: "Retry stop",
        correlationId: "cleanup",
      },
    },
  });
  act(() => result.current.restore(settings));
  expect(result.current.error).toMatch(/wait for confirmation/);
  expect(props.restoreQuery).not.toHaveBeenCalled();
  expect(props.dispatch).not.toHaveBeenCalled();
});
it("does not replace a running latency probe with restored setup", () => {
  const { result, props } = setup({
    state: {
      ...initialKafkaUiState,
      latency: {
        state: "running",
        evidence: null,
        request: { topic: "orders", acknowledgements: -1, messageCount: 1, timeoutMs: 5000 },
      },
    },
  });
  act(() => result.current.restore(settings));
  expect(result.current.readActive).toBe(true);
  expect(props.restoreQuery).not.toHaveBeenCalled();
});
