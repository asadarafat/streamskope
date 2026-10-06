// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import type {
  ProviderProfileManagementControls,
  ProviderProfilesFacet,
} from "../../src/platform/ui/provider-workspaces";
import { createNatsProfilesFacet } from "../../src/features/nats/ui/profiles-facet";
import {
  natsUiHostFixture,
  uiNatsProfile,
  uiNatsProfiles,
  uiNatsRecord,
  uiNatsSubscription,
} from "../support/nats-ui-host-fixture";

const releaseObservers: (() => void)[] = [];
afterEach(() => {
  cleanup();
  for (const release of releaseObservers.splice(0)) release();
});

function fixture(): {
  readonly host: ReturnType<typeof natsUiHostFixture>;
  readonly facet: ProviderProfilesFacet;
  readonly changed: ReturnType<typeof vi.fn>;
} {
  const host = natsUiHostFixture();
  const facet = createNatsProfilesFacet(() => ({ state: "ready", host: host.host }));
  const changed = vi.fn();
  releaseObservers.push(facet.subscribe(changed));
  return { host, facet, changed };
}
async function ready(f: ReturnType<typeof fixture>): Promise<void> {
  f.host.bootstrap();
  await waitFor(() => expect(f.facet.getSnapshot().loading).toBe(false));
}
function controls(
  action: ProviderProfileManagementControls["action"],
): ProviderProfileManagementControls {
  return {
    action,
    isInteractive: () => true,
    onClose: vi.fn(),
    onProfileReady: vi.fn(),
    onConnect: vi.fn().mockResolvedValue({ ok: true }),
  };
}

describe("NATS shared-catalog profile authority", () => {
  it("projects safe native summaries and direct creation metadata without buffering message traffic", async () => {
    const f = fixture();
    await ready(f);
    expect(f.facet.getSnapshot()).toMatchObject({
      profiles: [
        {
          id: uiNatsProfile.id,
          revision: 1,
          name: uiNatsProfile.name,
          source: "Direct",
          active: false,
        },
      ],
      creationActions: [{ id: "new", label: "NATS server", kind: "direct", available: true }],
      storageReady: true,
    });
    const before = f.facet.getSnapshot();
    f.changed.mockClear();
    f.host.emit({
      event: "subscription.changed",
      operation: "subscription.start",
      correlationId: "start",
      payload: uiNatsSubscription(),
    });
    f.host.emit({
      event: "records.batch",
      operation: "subscription.start",
      correlationId: "start",
      payload: {
        generation: "generation-1",
        records: [uiNatsRecord("secret-payload", "generation-1", "private fixture record")],
        counters: uiNatsSubscription("streaming", "generation-1", 10).counters,
      },
    });
    f.host.emit({
      event: "subscription.changed",
      operation: "subscription.start",
      correlationId: "counters",
      payload: uiNatsSubscription("streaming", "generation-1", 100),
    });
    expect(f.facet.getSnapshot()).toBe(before);
    expect(f.changed).not.toHaveBeenCalled();
    expect(JSON.stringify(before)).not.toContain("private fixture record");
    expect(f.host.calls).toEqual(["subscribe", "profiles.list"]);
  });

  it("fences stale profile/connection snapshots and connects with the captured expected revision", async () => {
    const f = fixture();
    await ready(f);
    f.host.emit({
      event: "profiles.changed",
      operation: "profiles.update",
      correlationId: "update",
      payload: uiNatsProfiles([{ ...uiNatsProfile, revision: 2, name: "Updated" }], 2),
    });
    f.host.emit({
      event: "profiles.changed",
      operation: "profiles.list",
      correlationId: "old-list",
      payload: uiNatsProfiles(),
    });
    expect(f.facet.getSnapshot().profiles[0]?.name).toBe("Updated");
    await expect(f.facet.connect({ id: uiNatsProfile.id, revision: 1 })).resolves.toMatchObject({
      ok: false,
    });
    expect(f.host.requests).toHaveLength(1);
    const connecting = f.facet.connect({ id: uiNatsProfile.id, revision: 2 });
    expect(f.host.requests[1]!.command).toMatchObject({
      command: "profiles.connect",
      payload: { profileId: uiNatsProfile.id, expectedRevision: 2 },
    });
    f.host.requests[1]!.answer({
      connection: {
        revision: 2,
        state: "connected",
        profile: { id: uiNatsProfile.id, revision: 2, name: "Updated" },
      },
    });
    await expect(connecting).resolves.toEqual({ ok: true });
    expect(f.facet.getSnapshot().profiles[0]?.active).toBe(true);
    f.host.emit({
      event: "connection.state",
      operation: "connection.disconnect",
      correlationId: "late",
      payload: { revision: 1, state: "disconnected", profile: null },
    });
    expect(f.facet.getSnapshot().profiles[0]?.active).toBe(true);
  });

  it("owns one management observer with no open action and releases its host listener", async () => {
    const host = natsUiHostFixture();
    const facet = createNatsProfilesFacet(() => ({ state: "ready", host: host.host }));
    const view = render(facet.renderManagement(controls(null)), { reactStrictMode: true });
    expect(host.listenerCount()).toBe(1);
    expect(host.requests).toHaveLength(2);
    await act(() => {
      host.bootstrap(host.requests[1]);
      return Promise.resolve();
    });
    await waitFor(() => expect(facet.getSnapshot().loading).toBe(false));
    view.unmount();
    expect(host.listenerCount()).toBe(0);
    expect(host.calls).not.toContain("subscription.stop");
    expect(host.calls).not.toContain("connection.disconnect");
  });

  it("renders inline details and routes connection exclusively through the application coordinator", async () => {
    const f = fixture();
    await ready(f);
    const management = controls({ kind: "inspect", profileId: uiNatsProfile.id });
    render(f.facet.renderManagement(management));
    expect(screen.getByRole("region", { name: "NATS profile details" })).toBeVisible();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    await userEvent.setup().click(screen.getByRole("button", { name: /^Connect$/u }));
    expect(management.onConnect).toHaveBeenCalledWith({ id: uiNatsProfile.id, revision: 1 });
    expect(f.host.requests).toHaveLength(1);
    expect(management.onClose).toHaveBeenCalledOnce();
  });

  it("pins an edit revision while retaining host-held secrets after concurrent summary changes", async () => {
    const f = fixture();
    await ready(f);
    const secretSummary = {
      ...uiNatsProfile,
      authentication: { mode: "token" as const, tokenPresent: true },
      tls: { mode: "tls" as const, caPresent: true },
    };
    f.host.emit({
      event: "profiles.changed",
      operation: "profiles.update",
      correlationId: "secrets",
      payload: uiNatsProfiles([secretSummary], 1),
    });
    const management = controls({ kind: "edit", profileId: uiNatsProfile.id });
    render(f.facet.renderManagement(management));
    expect(screen.getByLabelText("Token", { exact: true })).toHaveValue("");
    expect(screen.getByRole("textbox", { name: "CA certificate PEM" })).toHaveValue("");
    await act(() => {
      f.host.emit({
        event: "profiles.changed",
        operation: "profiles.update",
        correlationId: "concurrent",
        payload: uiNatsProfiles([{ ...secretSummary, revision: 2, name: "Concurrent change" }], 2),
      });
      return Promise.resolve();
    });
    await userEvent.setup().click(screen.getByRole("button", { name: "Save profile" }));
    expect(f.host.requests[1]!.command).toMatchObject({
      command: "profiles.update",
      payload: {
        profileId: uiNatsProfile.id,
        expectedRevision: 1,
        profile: {
          name: uiNatsProfile.name,
          authentication: { mode: "token", token: { mode: "retain" } },
          tls: { mode: "tls", caPem: { mode: "retain" } },
        },
      },
    });
    await act(() => {
      f.host.requests[1]!.reject(new Error("private rejection detail"));
      return Promise.resolve();
    });
    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent(
        "The NATS request could not be completed.",
      ),
    );
    expect(management.onProfileReady).not.toHaveBeenCalled();
  });

  it("explains active-profile editing without misreporting an unavailable host", async () => {
    const f = fixture();
    await ready(f);
    f.host.emit({
      event: "connection.state",
      operation: "profiles.connect",
      correlationId: "active",
      payload: {
        revision: 1,
        state: "connected",
        profile: { id: uiNatsProfile.id, revision: 1, name: uiNatsProfile.name },
      },
    });
    render(f.facet.renderManagement(controls({ kind: "edit", profileId: uiNatsProfile.id })));
    expect(screen.getByRole("alert")).toHaveTextContent(
      "Disconnect this NATS profile before editing",
    );
    expect(screen.getByRole("alert")).not.toHaveTextContent("host unavailable");
    expect(screen.queryByRole("textbox", { name: "Profile name" })).not.toBeInTheDocument();
    expect(f.host.requests).toHaveLength(1);
  });

  it("creates a profile through the reused editor without obtaining stream or connection authority", async () => {
    const f = fixture();
    await ready(f);
    const management = controls({ kind: "create", actionId: "new" });
    render(f.facet.renderManagement(management));
    fireEvent.change(screen.getByRole("textbox", { name: "Profile name" }), {
      target: { value: "Remote NATS" },
    });
    fireEvent.change(screen.getByRole("textbox", { name: "NATS servers" }), {
      target: { value: "tls://nats.example:4222" },
    });
    await userEvent.setup().click(screen.getByRole("button", { name: "Save profile" }));
    expect(f.host.requests[1]!.command).toMatchObject({
      command: "profiles.create",
      payload: {
        profile: {
          name: "Remote NATS",
          servers: ["tls://nats.example:4222"],
          authentication: { mode: "none" },
          tls: { mode: "tls", caPem: { mode: "clear" } },
        },
      },
    });
    await act(() => {
      f.host.requests[1]!.answer({
        profiles: uiNatsProfiles(
          [uiNatsProfile, { ...uiNatsProfile, id: "created-nats", name: "Remote NATS" }],
          1,
        ),
      });
      return Promise.resolve();
    });
    await waitFor(() => expect(management.onProfileReady).toHaveBeenCalledWith("created-nats"));
    expect(management.onConnect).not.toHaveBeenCalled();
    expect(f.host.calls).not.toContain("profiles.connect");
    expect(f.host.calls).not.toContain("subscription.start");
  });

  it("pins deletion to the originally reviewed revision and retains the dialog after failure", async () => {
    const f = fixture();
    await ready(f);
    const management = controls({ kind: "delete", profileId: uiNatsProfile.id });
    render(f.facet.renderManagement(management));
    await act(() => {
      f.host.emit({
        event: "profiles.changed",
        operation: "profiles.update",
        correlationId: "concurrent",
        payload: uiNatsProfiles([{ ...uiNatsProfile, revision: 2 }], 2),
      });
      return Promise.resolve();
    });
    await userEvent.setup().click(screen.getByRole("button", { name: /^Delete profile$/u }));
    expect(f.host.requests[1]!.command).toMatchObject({
      command: "profiles.delete",
      payload: { profileId: uiNatsProfile.id, expectedRevision: 1 },
    });
    await act(() => {
      f.host.requests[1]!.reject(new Error("private rejection"));
      return Promise.resolve();
    });
    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent(
        "The NATS request could not be completed.",
      ),
    );
    expect(screen.getByRole("dialog")).toBeVisible();
    expect(management.onClose).not.toHaveBeenCalled();
  });
});
