// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { useState } from "react";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { NatsRecord } from "../../src/features/nats/contracts";
import { SubscriptionWorkspace } from "../../src/features/nats/ui/SubscriptionWorkspace";
import type { NatsWorkspaceController } from "../../src/features/nats/ui/use-nats-workspace";
import { initialNatsWorkspaceSnapshot } from "../../src/features/nats/ui/workspace-state";
import { uiNatsRecord } from "../support/nats-ui-host-fixture";

vi.mock("../../src/features/nats/ui/RecordDataGrid", () => ({
  RecordDataGrid: ({
    records,
    onSelect,
  }: {
    readonly records: readonly NatsRecord[];
    readonly onSelect: (id: string) => void;
  }): React.JSX.Element => (
    <div role="grid">
      {records.map((record): React.JSX.Element => (
        <button key={record.id} role="gridcell" onClick={(): void => onSelect(record.id)}>
          Select {record.id}
        </button>
      ))}
    </div>
  ),
}));
vi.mock("../../src/features/nats/ui/RecordInspector", () => ({
  RecordInspector: ({ onClose }: { readonly onClose: () => void }): React.JSX.Element => (
    <aside aria-label="Record inspector">
      <button onClick={onClose}>Close record inspector</button>
    </aside>
  ),
}));

const records = [uiNatsRecord("first"), uiNatsRecord("second")];
const alwaysInteractive = (): boolean => true;
const foreignNodes: HTMLElement[] = [];
afterEach((): void => {
  cleanup();
  for (const node of foreignNodes.splice(0)) node.remove();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function Harness({
  selection,
  retained = records,
  isInteractive = alwaysInteractive,
}: {
  readonly selection?: NatsRecord | null;
  readonly retained?: readonly NatsRecord[];
  readonly isInteractive?: () => boolean;
}): React.JSX.Element {
  const [selected, setSelected] = useState<NatsRecord | null>(null);
  const controller: NatsWorkspaceController = {
    ...initialNatsWorkspaceSnapshot(),
    loading: false,
    records: retained,
    selectedRecord: selection === undefined ? selected : selection,
    createProfile: () => Promise.resolve(false),
    updateProfile: () => Promise.resolve(false),
    deleteProfile: () => Promise.resolve(false),
    connectProfile: () => Promise.resolve(false),
    disconnect: () => Promise.resolve(false),
    startSubscription: () => Promise.resolve(false),
    stopSubscription: () => Promise.resolve(false),
    refresh: () => Promise.resolve(false),
    selectRecord: (id): void => setSelected(retained.find((record) => record.id === id) ?? null),
    clearFailure: (): void => undefined,
  };
  return <SubscriptionWorkspace controller={controller} isInteractive={isInteractive} />;
}

function frames(): {
  readonly cancel: (id: number) => void;
  readonly pendingIds: () => readonly number[];
  readonly deliver: (id: number) => void;
} {
  let nextId = 0;
  const pending = new Set<number>();
  const callbacks = new Map<number, FrameRequestCallback>();
  vi.stubGlobal(
    "requestAnimationFrame",
    vi.fn((callback: FrameRequestCallback): number => {
      const id = ++nextId;
      pending.add(id);
      callbacks.set(id, callback);
      return id;
    }),
  );
  const cancel = vi.fn((id: number): void => {
    pending.delete(id);
  });
  vi.stubGlobal("cancelAnimationFrame", cancel);
  return {
    cancel,
    pendingIds: (): readonly number[] => [...pending],
    deliver: (id): void => {
      const callback = callbacks.get(id);
      if (callback === undefined) throw new Error("Expected an admitted focus frame.");
      pending.delete(id);
      // A cancelled callback may still be delivered; the production owner must reject it.
      act((): void => callback(0));
    },
  };
}

function closeFrom(id: string): HTMLElement {
  const opener = screen.getByRole("gridcell", { name: "Select " + id });
  opener.focus();
  fireEvent.click(opener);
  const close = screen.getByRole("button", { name: "Close record inspector" });
  close.focus();
  fireEvent.click(close);
  return opener;
}

describe("NATS inspector focus ownership", () => {
  it("restores the invoking cell after the inspector closes", (): void => {
    const scheduled = frames();
    render(<Harness />);
    const opener = closeFrom("second");
    expect(scheduled.pendingIds()).toEqual([1]);
    expect(opener).not.toHaveFocus();
    scheduled.deliver(1);
    expect(opener).toHaveFocus();
    expect(scheduled.pendingIds()).toEqual([]);
  });

  it("cancels old restoration when a newer record is selected and rejects a delivered stale frame", (): void => {
    const scheduled = frames();
    const view = render(<Harness />);
    const opener = closeFrom("first");
    view.rerender(<Harness selection={records[1]!} />);
    const close = screen.getByRole("button", { name: "Close record inspector" });
    close.focus();
    expect(scheduled.cancel).toHaveBeenCalledWith(1);
    expect(scheduled.pendingIds()).toEqual([]);
    scheduled.deliver(1);
    expect(close).toHaveFocus();
    expect(opener).not.toHaveFocus();
  });

  it("keeps only the latest close's restoration when an older cancelled frame is delivered", (): void => {
    const scheduled = frames();
    render(<Harness />);
    closeFrom("first");
    const opener = closeFrom("second");
    expect(scheduled.cancel).toHaveBeenCalledWith(1);
    expect(scheduled.pendingIds()).toEqual([2]);
    scheduled.deliver(1);
    expect(opener).not.toHaveFocus();
    expect(scheduled.pendingIds()).toEqual([2]);
    scheduled.deliver(2);
    expect(opener).toHaveFocus();
  });

  it("cancels restoration on unmount even when its old opener remains connected elsewhere", (): void => {
    const scheduled = frames();
    const view = render(<Harness />);
    const opener = closeFrom("second");
    const restoreFocus = vi.spyOn(opener, "focus");
    view.unmount();
    document.body.append(opener);
    foreignNodes.push(opener);
    expect(opener.isConnected).toBe(true);
    expect(scheduled.cancel).toHaveBeenCalledWith(1);
    expect(scheduled.pendingIds()).toEqual([]);
    scheduled.deliver(1);
    expect(restoreFocus).not.toHaveBeenCalled();
  });

  it("refuses to restore focus after the provider activation retires", (): void => {
    const scheduled = frames();
    let interactive = true;
    render(<Harness isInteractive={(): boolean => interactive} />);
    const opener = closeFrom("first");
    const restoreFocus = vi.spyOn(opener, "focus");
    interactive = false;
    scheduled.deliver(1);
    expect(restoreFocus).not.toHaveBeenCalled();
  });

  it("uses a retained cell when the invoking cell leaves the live window", (): void => {
    const scheduled = frames();
    const view = render(<Harness />);
    const opener = closeFrom("second");
    view.rerender(<Harness retained={[records[0]!]} />);
    expect(opener.isConnected).toBe(false);
    document.body.append(opener);
    foreignNodes.push(opener);
    scheduled.deliver(1);
    expect(screen.getByRole("gridcell", { name: "Select first" })).toHaveFocus();
    expect(opener).not.toHaveFocus();
  });
});
