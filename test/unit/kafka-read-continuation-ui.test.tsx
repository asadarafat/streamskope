// @vitest-environment jsdom

import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import type { HostCommand, StreamSkopeHost } from "../../src/features/kafka/contracts";
import { useReadContinuation } from "../../src/features/kafka/ui/use-read-continuation";
import { testHostAccepted, testHostExecute } from "../support/host-response";

type Props = Parameters<typeof useReadContinuation>[0];

function setup(overrides: Partial<Props> = {}): {
  props: Props;
  dispatch: ReturnType<typeof vi.fn<(command: HostCommand) => Promise<unknown>>>;
  result: ReturnType<typeof renderHook<ReturnType<typeof useReadContinuation>, Props>>["result"];
  rerender: (props: Props) => void;
} {
  const dispatch = vi.fn((command: HostCommand): Promise<unknown> =>
    Promise.resolve(testHostAccepted(command, "continue-test")),
  );
  const host: StreamSkopeHost = {
    execute: testHostExecute(dispatch),
    subscribe: () => () => undefined,
    openExternalUrl: () => Promise.reject(new Error("Not used in read continuation")),
  };
  const props: Props = {
    host,
    context: "submitted read controls",
    progress: {
      pass: 1,
      scannedRecords: 10,
      scannedBytes: 100,
      matchedRecords: 1,
      unavailableRecords: 0,
      continuation: {
        id: "host-issued-only",
        expiresAt: new Date(Date.now() + 60_000).toISOString(),
      },
    },
    connected: true,
    active: false,
    stopping: false,
    topicMatches: true,
    onError: vi.fn(),
    onContinued: vi.fn(),
    ...overrides,
  };
  const initialProps: Props = { ...props, progress: null };
  const { result, rerender } = renderHook(useReadContinuation, { initialProps });
  act(() => result.current.bindContinuation(props.context));
  rerender(props);
  return { props, dispatch, result, rerender };
}

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

it("sends only the authoritative token and cannot resubmit an accepted checkpoint", async () => {
  const { props, dispatch, result, rerender } = setup();
  expect(result.current.continuationAvailable).toBe(true);
  await act(async () => result.current.continueConsumption());
  expect(dispatch).toHaveBeenCalledTimes(1);
  expect(dispatch.mock.calls[0]?.[0]).toMatchObject({
    command: "messages.continue",
    payload: { continuationId: "host-issued-only" },
  });
  expect(Object.keys(dispatch.mock.calls[0]?.[0].payload ?? {})).toEqual(["continuationId"]);
  expect(props.onContinued).toHaveBeenCalledTimes(1);
  expect(result.current.continuationAvailable).toBe(false);
  await act(async () => result.current.continueConsumption());
  expect(dispatch).toHaveBeenCalledTimes(1);
  rerender({
    ...props,
    progress: {
      pass: 2,
      scannedRecords: 20,
      scannedBytes: 200,
      matchedRecords: 2,
      unavailableRecords: 0,
      continuation: {
        id: "next-host-checkpoint",
        expiresAt: new Date(Date.now() + 60_000).toISOString(),
      },
    },
  });
  expect(result.current.continuationAvailable).toBe(true);
});

it.each(["active", "stopping"] as const)(
  "waits for %s to finish before continuing",
  async (field) => {
    const { props, dispatch, result, rerender } = setup({ [field]: true });
    expect(result.current.continuationAvailable).toBe(false);
    await act(async () => result.current.continueConsumption());
    expect(dispatch).not.toHaveBeenCalled();
    rerender({ ...props, [field]: false });
    expect(result.current.continuationAvailable).toBe(true);
  },
);

it("invalidates the checkpoint after an edit even if the original settings are restored", async () => {
  const { props, dispatch, result, rerender } = setup();
  rerender({ ...props, context: "edited read controls" });
  expect(result.current.continuationAvailable).toBe(false);
  expect(result.current.continuationNotice).toContain("Start a new read");
  rerender(props);
  expect(result.current.continuationAvailable).toBe(false);
  await act(async () => result.current.continueConsumption());
  expect(dispatch).not.toHaveBeenCalled();
});

it("does not revive a checkpoint when the connection returns", () => {
  const { props, result, rerender } = setup();
  rerender({ ...props, connected: false });
  rerender(props);
  expect(result.current.continuationAvailable).toBe(false);
});

it("never offers a checkpoint without a terminal token or for a different topic", () => {
  const { props, result, rerender } = setup();
  rerender({ ...props, topicMatches: false });
  expect(result.current.continuationAvailable).toBe(false);
  rerender({ ...props, progress: null });
  expect(result.current.continuationAvailable).toBe(false);
});

it("expires an idle checkpoint without a user gesture and sends no stale command", async () => {
  vi.useFakeTimers();
  const { dispatch, result } = setup();
  expect(result.current.continuationAvailable).toBe(true);
  act(() => {
    vi.advanceTimersByTime(60_000);
  });
  expect(result.current.continuationAvailable).toBe(false);
  expect(result.current.continuationNotice).toBe("This continuation expired. Start a new read.");
  await act(async () => result.current.continueConsumption());
  expect(dispatch).not.toHaveBeenCalled();
});

it("preserves selection when the host rejects a token and gives the host recovery instruction", async () => {
  const { props, dispatch, result } = setup();
  dispatch.mockImplementation((command) =>
    Promise.resolve({
      command: command.command,
      id: command.id,
      version: command.version,
      ok: false,
      error: {
        code: "VALIDATION",
        activeStateChanged: false,
        stage: "validation",
        correlationId: "expired-token",
        retryable: false,
        summary: "The read checkpoint is no longer valid.",
        recovery: "Start a new read.",
      },
    }),
  );
  await act(async () => result.current.continueConsumption());
  expect(props.onContinued).not.toHaveBeenCalled();
  expect(props.onError).toHaveBeenLastCalledWith(
    "The read checkpoint is no longer valid. Start a new read.",
  );
  expect(result.current.continuationAvailable).toBe(false);
});

it("does not claim that a transport failure proves the next pass never started", async () => {
  const { props, dispatch, result } = setup();
  dispatch.mockRejectedValueOnce(new Error("Lost host transport"));
  await act(async () => result.current.continueConsumption());
  expect(props.onContinued).not.toHaveBeenCalled();
  expect(props.onError).toHaveBeenLastCalledWith(
    "The host did not acknowledge continuation. Check the read status before starting again.",
  );
  expect(result.current.continuationAvailable).toBe(false);
});

it("does not rebind an old displayed checkpoint while a new read is being validated", async () => {
  const { dispatch, props, result } = setup();
  expect(result.current.continuationAvailable).toBe(true);
  act(() => result.current.bindContinuation(props.context));
  expect(result.current.continuationAvailable).toBe(false);
  await act(async () => result.current.continueConsumption());
  expect(dispatch).not.toHaveBeenCalled();
});
