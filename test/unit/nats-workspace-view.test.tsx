// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, render, renderHook, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { NatsWorkspace } from "../../src/features/nats/ui/NatsWorkspace";
import { useNatsWorkspace } from "../../src/features/nats/ui/use-nats-workspace";
import {
  natsUiHostFixture,
  uiNatsProfile,
  uiNatsProfiles,
  uiNatsSubscription,
} from "../support/nats-ui-host-fixture";

afterEach(cleanup);

describe("NATS workspace effect and failure presentation", () => {
  it("owns one live listener after StrictMode replay and refuses updates from the discarded bootstrap", async () => {
    const f = natsUiHostFixture();
    const view = renderHook(() => useNatsWorkspace({ host: f.host }), {
      reactStrictMode: true,
    });
    expect(f.requests).toHaveLength(2);
    expect(f.listenerCount()).toBe(1);
    await act(() => {
      const subscription = { ...uiNatsSubscription(), revision: 2 };
      f.requests[1]!.answer({
        profiles: uiNatsProfiles([{ ...uiNatsProfile, name: "Active mount" }], 2),
        connection: {
          revision: 2,
          state: "connected",
          profile: {
            id: uiNatsProfile.id,
            revision: uiNatsProfile.revision,
            name: uiNatsProfile.name,
          },
        },
        subscription,
      });
      return Promise.resolve();
    });
    await waitFor(() => expect(view.result.current.loading).toBe(false));
    await act(() => {
      f.bootstrap(f.requests[0]);
      return Promise.resolve();
    });
    expect(view.result.current.profiles?.profiles[0]?.name).toBe("Active mount");
    expect(view.result.current.connection.state).toBe("connected");
    view.unmount();
    expect(f.listenerCount()).toBe(0);
    expect(f.calls.filter((call) => call === "unsubscribe")).toHaveLength(2);
    expect(f.calls).not.toContain("subscription.stop");
  });

  it("displays safe asynchronous failure guidance until the host actually recovers", async () => {
    const f = natsUiHostFixture();
    render(<NatsWorkspace source={{ state: "ready", host: f.host }} />);
    await act(() => {
      f.bootstrap();
      return Promise.resolve();
    });
    await act(() => {
      f.emit({
        event: "connection.state",
        operation: "profiles.connect",
        correlationId: "server-loss",
        payload: {
          revision: 2,
          state: "failed",
          profile: null,
          failure: {
            code: "connection",
            summary: "The NATS server disconnected.",
            recovery: "Reconnect the profile before subscribing.",
          },
        },
      });
      return Promise.resolve();
    });
    expect(screen.getByRole("alert")).toHaveTextContent("The NATS server disconnected.");
    expect(screen.getByRole("alert")).toHaveTextContent(
      "Reconnect the profile before subscribing.",
    );
    expect(screen.queryByRole("button", { name: "Dismiss", exact: true })).not.toBeInTheDocument();
    await act(() => {
      f.emit({
        event: "backend.availability",
        payload: { state: "unavailable", recovery: "Reload the host." },
      });
      return Promise.resolve();
    });
    expect(screen.getByRole("alert")).toHaveTextContent("The NATS host is unavailable.");
    await act(() => {
      f.emit({ event: "backend.availability", payload: { state: "ready" } });
      f.bootstrap(f.requests[1]);
      f.emit({
        event: "connection.state",
        operation: "profiles.connect",
        correlationId: "new-connection",
        payload: {
          revision: 3,
          state: "connected",
          profile: {
            id: uiNatsProfile.id,
            name: uiNatsProfile.name,
            revision: uiNatsProfile.revision,
          },
        },
      });
      return Promise.resolve();
    });
    await waitFor(() => expect(screen.queryByRole("alert")).not.toBeInTheDocument());
  });
});
