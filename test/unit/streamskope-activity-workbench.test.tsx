// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";

import { StreamSkopeApp } from "../../src/features/kafka/ui/StreamSkopeApp";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";

class ActivityHost implements StreamSkopeHost {
  private readonly listeners = new Set<HostEventListener>();

  emit(event: HostEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  execute(command: HostCommand): Promise<HostCommandResponse> {
    return Promise.resolve({
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: `activity-${command.id}` },
      version: HOST_PROTOCOL_VERSION,
    });
  }

  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("External URL action was not expected."));
  }

  subscribe(listener: HostEventListener): () => void {
    this.listeners.add(listener);
    return (): void => {
      this.listeners.delete(listener);
    };
  }
}

afterEach(cleanup);

describe("StreamSkope Activity attention", () => {
  it("opens once for each new failure and stays quiet for informational work", async () => {
    const host = new ActivityHost();
    const user = userEvent.setup();
    render(<StreamSkopeApp host={host} />);

    const session = screen.getByRole("region", { name: "Activity dock" });
    expect(within(session).getByRole("button", { name: "Expand Activity" })).toBeVisible();
    expect(screen.queryByRole("complementary", { name: "Activity log" })).not.toBeInTheDocument();

    act(() => {
      host.emit({
        event: "activity.recorded",
        payload: {
          correlationId: "activity-info-correlation",
          detail: "The safe profile inventory was refreshed.",
          id: "activity-info",
          object: "Kafka profiles",
          operation: "Load profiles",
          outcome: "succeeded",
          severity: "info",
          timestamp: "2026-08-10T09:00:00.000Z",
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    expect(screen.queryByRole("complementary", { name: "Activity log" })).not.toBeInTheDocument();

    const failure = {
      event: "activity.recorded" as const,
      payload: {
        correlationId: "activity-error-correlation",
        detail: "Kafka refused the connection.",
        id: "activity-error",
        object: "local-aio · clab.orb.local:19093",
        operation: "Test profile connection",
        outcome: "failed" as const,
        severity: "error" as const,
        timestamp: "2026-08-10T09:01:00.000Z",
      },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    };
    act(() => host.emit(failure));
    expect(await screen.findByRole("complementary", { name: "Activity log" })).toHaveTextContent(
      "Kafka refused the connection.",
    );

    await user.click(within(session).getByRole("button", { name: "Collapse Activity" }));
    act(() => host.emit({ ...failure, sequence: 3 }));
    expect(screen.queryByRole("complementary", { name: "Activity log" })).not.toBeInTheDocument();

    act(() => {
      host.emit({
        ...failure,
        payload: {
          ...failure.payload,
          correlationId: "activity-error-correlation-2",
          id: "activity-error-2",
        },
        sequence: 4,
      });
    });
    expect(await screen.findByRole("complementary", { name: "Activity log" })).toBeVisible();
  });
});
