// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { PluginTransitionStatus } from "../../src/features/kafka/ui/PluginTransitionStatus";
import { pluginTransition } from "../support/plugin-management-fixture";

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

it.each(["visible", "hidden"] as const)(
  "suspends elapsed updates while hidden, resynchronizes on return and disposes all work when initially %s",
  (initialVisibility) => {
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    vi.setSystemTime(new Date("2026-10-08T12:00:12.000Z"));
    const visibility = vi
      .spyOn(document, "visibilityState", "get")
      .mockReturnValue(initialVisibility);
    const added = vi.spyOn(document, "addEventListener");
    const removed = vi.spyOn(document, "removeEventListener");
    const transition = pluginTransition({
      startedAt: "2026-10-08T12:00:00.000Z",
      stageStartedAt: "2026-10-08T12:00:05.000Z",
    });
    const view = render(<PluginTransitionStatus transition={transition} />);
    const progress = screen.getByRole("group", { name: "Plugin change progress" });
    expect(progress).toHaveTextContent("12s elapsed · 7s in this step");
    expect(vi.getTimerCount()).toBe(initialVisibility === "visible" ? 1 : 0);
    const listener = added.mock.calls.find(([name]) => name === "visibilitychange")?.[1];
    expect(listener).toBeTypeOf("function");

    act(() => {
      visibility.mockReturnValue("visible");
      document.dispatchEvent(new Event("visibilitychange"));
      document.dispatchEvent(new Event("visibilitychange"));
      vi.advanceTimersByTime(1000);
    });
    expect(progress).toHaveTextContent("13s elapsed · 8s in this step");
    expect(vi.getTimerCount()).toBe(1);

    act(() => {
      visibility.mockReturnValue("hidden");
      document.dispatchEvent(new Event("visibilitychange"));
    });
    expect(vi.getTimerCount()).toBe(0);
    act(() => {
      vi.advanceTimersByTime(5000);
    });
    expect(progress).toHaveTextContent("13s elapsed · 8s in this step");

    act(() => {
      visibility.mockReturnValue("visible");
      document.dispatchEvent(new Event("visibilitychange"));
    });
    expect(progress).toHaveTextContent("18s elapsed · 13s in this step");
    expect(vi.getTimerCount()).toBe(1);
    view.unmount();
    expect(vi.getTimerCount()).toBe(0);
    expect(removed).toHaveBeenCalledWith("visibilitychange", listener);
    act(() => {
      document.dispatchEvent(new Event("visibilitychange"));
      vi.advanceTimersByTime(5000);
    });
    expect(vi.getTimerCount()).toBe(0);
  },
);
