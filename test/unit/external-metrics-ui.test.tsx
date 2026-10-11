// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, renderHook, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEvent,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import {
  EXTERNAL_METRICS,
  type ExternalMetricsSnapshot,
} from "../../src/features/kafka/contracts/external-metrics";
import { ExternalMetricsPanel } from "../../src/features/kafka/ui/ExternalMetricsPanel";
import { useExternalMetrics } from "../../src/features/kafka/ui/use-external-metrics";
import { StreamSkopeThemeProvider } from "../../src/platform/ui/StreamSkopeThemeProvider";
import { testHostExecute } from "../support/host-response";

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});
it("allows explicit cancellation after the configured endpoint was removed", async () => {
  const f = fixture();
  const hook = renderHook(() => useExternalMetrics(f.host, true, undefined));
  await act(async () => {
    await hook.result.current.read({ endpoint, job: "kafka", instance: "broker:9404" });
    await hook.result.current.cancel();
  });
  expect(f.commands.map((command) => command.command)).toEqual(["observations.metrics.cancel"]);
});
const endpoint = "https://metrics.test";
function snapshot(): ExternalMetricsSnapshot {
  const at = Date.now();
  return {
    source: "prometheus",
    identity: "not-established",
    endpoint,
    job: "kafka",
    instance: "broker:9404",
    evaluatedAt: at,
    collectedAt: at,
    metrics: EXTERNAL_METRICS.map(({ name }) => ({
      name,
      state: "ready",
      value: 0,
      scrapedAt: at,
    })),
  };
}
function fixture(deferred = false): {
  host: StreamSkopeHost;
  commands: HostCommand[];
  settle: () => void;
  publish: (event: HostEvent) => void;
} {
  const commands: HostCommand[] = [];
  let listener: ((event: HostEvent) => void) | undefined;
  let resolve!: () => void;
  const waiting = new Promise<void>((done) => {
    resolve = done;
  });
  const host: StreamSkopeHost = {
    openExternalUrl: () => Promise.reject(new Error("No external URL is used by metrics")),
    subscribe: (handler) => {
      listener = handler;
      return () => {
        listener = undefined;
      };
    },
    execute: testHostExecute(async (command) => {
      commands.push(command);
      if (deferred) await waiting;
      const base = {
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true as const,
      };
      if (command.command === "observations.metrics.capture")
        return { ...base, result: { correlationId: "fixture", snapshot: snapshot() } };
      if (command.command === "observations.metrics.cancel")
        return { ...base, result: { correlationId: "fixture" } };
      throw new Error("Unexpected metrics command");
    }),
  };
  return { host, commands, settle: resolve, publish: (event) => listener?.(event) };
}
it("never starts remote work on attachment, reads explicitly and expires the displayed zeros without scraping again", async () => {
  vi.useFakeTimers();
  const f = fixture();
  render(
    <StreamSkopeThemeProvider>
      <ExternalMetricsPanel host={f.host} available endpoint={endpoint} />
    </StreamSkopeThemeProvider>,
  );
  fireEvent.click(screen.getByRole("button", { name: "Prometheus metrics (optional)" }));
  expect(f.commands).toEqual([]);
  fireEvent.change(screen.getByLabelText("Exporter job"), { target: { value: "kafka" } });
  fireEvent.change(screen.getByLabelText("Exporter instance"), {
    target: { value: "broker:9404" },
  });
  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: "Read metrics" }));
    await Promise.resolve();
  });
  expect(screen.getAllByText("0", { exact: true })).toHaveLength(2);
  expect(screen.getByText(/identity is not bound/)).toBeVisible();
  await act(async () => {
    vi.advanceTimersByTime(45_001);
    await Promise.resolve();
  });
  expect(screen.queryByText("0", { exact: true })).toBeNull();
  expect(screen.getAllByText("Unknown", { exact: true })).toHaveLength(2);
  expect(screen.getAllByText("Expired capture", { exact: true })).toHaveLength(2);
  expect(f.commands.map((c) => c.command)).toEqual(["observations.metrics.capture"]);
});
it("fences a reply after same-named reconnect or unavailable host and never auto-retries", async () => {
  const f = fixture(true);
  const hook = renderHook(({ available }) => useExternalMetrics(f.host, available, endpoint), {
    initialProps: { available: true },
  });
  let operation!: Promise<void>;
  act(() => {
    operation = hook.result.current.read({ endpoint, job: "kafka", instance: "broker:9404" });
  });
  act(() => {
    f.publish({
      event: "connection.state",
      version: HOST_PROTOCOL_VERSION,
      sequence: 1,
      payload: { state: "connected", connectionName: "Same name" },
    });
  });
  await act(async () => {
    f.settle();
    await operation;
  });
  expect(hook.result.current.snapshot).toBeNull();
  hook.rerender({ available: false });
  hook.rerender({ available: true });
  expect(f.commands).toHaveLength(1);
});
it("keeps cancellation pending until its original acknowledgement and prevents a late read from republishing", async () => {
  const f = fixture(true);
  const hook = renderHook(() => useExternalMetrics(f.host, true, endpoint));
  let read!: Promise<void>, cancel!: Promise<void>;
  act(() => {
    read = hook.result.current.read({ endpoint, job: "kafka", instance: "broker:9404" });
  });
  act(() => {
    cancel = hook.result.current.cancel();
  });
  expect(hook.result.current.pending).toBe(true);
  await act(async () => {
    f.settle();
    await Promise.all([read, cancel]);
  });
  expect(hook.result.current.pending).toBe(false);
  expect(hook.result.current.snapshot).toBeNull();
  expect(f.commands.map((c) => c.command)).toEqual([
    "observations.metrics.capture",
    "observations.metrics.cancel",
  ]);
});
