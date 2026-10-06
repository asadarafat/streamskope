// @vitest-environment jsdom

import { act, cleanup, render, waitFor } from "@testing-library/react";
import type { ComponentProps, MouseEvent, ReactElement } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type {
  NatsProfileCreateInput,
  NatsProfileUpdateInput,
} from "../../src/features/nats/contracts";
import type { ProfileEditorProperties } from "../../src/features/nats/ui/ProfileEditor";
import { createNatsProfilesFacet } from "../../src/features/nats/ui/profiles-facet";
import type { StudioButton } from "../../src/platform/ui/controls";
import type {
  ProviderProfileManagementControls,
  ProviderProfilesFacet,
} from "../../src/platform/ui/provider-workspaces";
import { natsUiHostFixture, uiNatsProfile, uiNatsProfiles } from "../support/nats-ui-host-fixture";

const retained = vi.hoisted(() => ({
  editor: null as ProfileEditorProperties | null,
  buttons: new Map<string, () => void>(),
}));
vi.mock("../../src/features/nats/ui/ProfileEditor", () => ({
  ProfileEditor: (properties: ProfileEditorProperties): null => {
    retained.editor = properties;
    return null;
  },
}));
vi.mock("../../src/platform/ui/controls", async (original) => {
  const controls = await original<typeof import("../../src/platform/ui/controls")>();
  const { createElement } = await import("react");
  return {
    ...controls,
    StudioButton: (properties: ComponentProps<typeof StudioButton>): ReactElement => {
      if (typeof properties.children === "string" && properties.onClick !== undefined) {
        retained.buttons.set(properties.children, () =>
          properties.onClick?.({} as MouseEvent<HTMLButtonElement>),
        );
      }
      return createElement(controls.StudioButton, properties);
    },
  };
});

const releaseObservers: (() => void)[] = [];
afterEach(() => {
  cleanup();
  for (const release of releaseObservers.splice(0)) release();
  retained.editor = null;
  retained.buttons.clear();
});

const createInput: NatsProfileCreateInput = {
  name: "Remote NATS",
  servers: ["nats://nats.example:4222"],
  authentication: { mode: "none" },
  tls: { mode: "plaintext" },
};
const updateInput: NatsProfileUpdateInput = {
  ...createInput,
  name: "Updated NATS",
};

interface LifetimeFixture {
  readonly host: ReturnType<typeof natsUiHostFixture>;
  readonly facet: ProviderProfilesFacet;
  readonly controls: ProviderProfileManagementControls;
  readonly view: ReturnType<typeof render>;
}

async function fixture(
  action: ProviderProfileManagementControls["action"],
): Promise<LifetimeFixture> {
  const host = natsUiHostFixture();
  const facet = createNatsProfilesFacet(() => ({ state: "ready", host: host.host }));
  // The catalog's observer and manager owner outlive the selected management view.
  releaseObservers.push(facet.subscribe(vi.fn()));
  host.bootstrap();
  await waitFor(() => expect(facet.getSnapshot().loading).toBe(false));
  const controls: ProviderProfileManagementControls = {
    action,
    isInteractive: () => true,
    onClose: vi.fn(),
    onProfileReady: vi.fn(),
    onConnect: vi.fn().mockResolvedValue({ ok: true }),
  };
  const view = render(facet.renderManagement(controls));
  return { host, facet, controls, view };
}

function retire(f: Awaited<ReturnType<typeof fixture>>): void {
  f.view.unmount();
  expect(f.controls.isInteractive()).toBe(true);
  expect(f.host.listenerCount()).toBe(1);
}

describe("NATS management view admission lifetime", () => {
  it("rejects a retained create callback after its view unmounts while the catalog remains active", async () => {
    const f = await fixture({ kind: "create", actionId: "new" });
    const editor = retained.editor!;
    retire(f);
    await expect(editor.onCreate(createInput)).resolves.toBe(false);
    editor.onClose();
    expect(f.host.requests).toHaveLength(1);
    expect(f.controls.onClose).not.toHaveBeenCalled();
    expect(f.controls.onProfileReady).not.toHaveBeenCalled();
  });

  it("rejects a retained update callback after its view unmounts", async () => {
    const f = await fixture({ kind: "edit", profileId: uiNatsProfile.id });
    const editor = retained.editor!;
    retire(f);
    await expect(editor.onUpdate(editor.profile!, updateInput)).resolves.toBe(false);
    expect(f.host.requests).toHaveLength(1);
    expect(f.controls.onProfileReady).not.toHaveBeenCalled();
  });

  it("rejects retained deletion and close actions after the reviewed dialog unmounts", async () => {
    const f = await fixture({ kind: "delete", profileId: uiNatsProfile.id });
    const remove = retained.buttons.get("Delete profile")!;
    const cancel = retained.buttons.get("Cancel")!;
    retire(f);
    await act(() => {
      remove();
      cancel();
      return Promise.resolve();
    });
    expect(f.host.requests).toHaveLength(1);
    expect(f.controls.onClose).not.toHaveBeenCalled();
  });

  it("rejects a retained connect action after inline details unmount", async () => {
    const f = await fixture({ kind: "inspect", profileId: uiNatsProfile.id });
    const connect = retained.buttons.get("Connect")!;
    retire(f);
    await act(() => {
      connect();
      return Promise.resolve();
    });
    expect(f.controls.onConnect).not.toHaveBeenCalled();
    expect(f.host.requests).toHaveLength(1);
    expect(f.controls.onClose).not.toHaveBeenCalled();
  });

  it("revokes an earlier action even when the new editor uses the same action kind and ID", async () => {
    const f = await fixture({ kind: "create", actionId: "new" });
    const oldEditor = retained.editor!;
    f.view.rerender(
      f.facet.renderManagement({
        ...f.controls,
        action: { kind: "create", actionId: "new" },
      }),
    );
    const currentEditor = retained.editor!;
    await expect(oldEditor.onCreate(createInput)).resolves.toBe(false);
    expect(f.host.requests).toHaveLength(1);
    let saved!: Promise<boolean>;
    await act(() => {
      saved = currentEditor.onCreate(createInput);
      return Promise.resolve();
    });
    expect(f.host.requests[1]!.command.command).toBe("profiles.create");
    await act(async () => {
      f.host.requests[1]!.answer({
        profiles: uiNatsProfiles(
          [uiNatsProfile, { ...uiNatsProfile, id: "created-nats", name: createInput.name }],
          1,
        ),
      });
      await expect(saved).resolves.toBe(true);
    });
    expect(f.controls.onProfileReady).toHaveBeenCalledWith("created-nats");
  });

  it("preserves an admitted create receipt after unmount without running its retired handoff", async () => {
    const f = await fixture({ kind: "create", actionId: "new" });
    const saved = retained.editor!.onCreate(createInput);
    expect(f.host.requests[1]!.command.command).toBe("profiles.create");
    retire(f);
    await act(async () => {
      f.host.requests[1]!.answer({
        profiles: uiNatsProfiles(
          [uiNatsProfile, { ...uiNatsProfile, id: "created-nats", name: createInput.name }],
          1,
        ),
      });
      await expect(saved).resolves.toBe(true);
    });
    expect(f.facet.getSnapshot().profiles.some((profile) => profile.id === "created-nats")).toBe(
      true,
    );
    expect(f.controls.onProfileReady).not.toHaveBeenCalled();
    expect(f.controls.onClose).not.toHaveBeenCalled();
  });

  it("preserves an admitted update receipt after unmount without running its retired handoff", async () => {
    const f = await fixture({ kind: "edit", profileId: uiNatsProfile.id });
    const editor = retained.editor!;
    const saved = editor.onUpdate(editor.profile!, updateInput);
    expect(f.host.requests[1]!.command).toMatchObject({
      command: "profiles.update",
      payload: { expectedRevision: 1 },
    });
    retire(f);
    await act(async () => {
      f.host.requests[1]!.answer({
        profiles: uiNatsProfiles([{ ...uiNatsProfile, revision: 2, name: updateInput.name }], 1),
      });
      await expect(saved).resolves.toBe(true);
    });
    expect(f.facet.getSnapshot().profiles[0]?.name).toBe(updateInput.name);
    expect(f.controls.onProfileReady).not.toHaveBeenCalled();
    expect(f.controls.onClose).not.toHaveBeenCalled();
  });
});
