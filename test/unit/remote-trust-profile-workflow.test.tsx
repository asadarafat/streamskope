// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { pasteText } from "../support/paste-text";
import {
  HOST_PROTOCOL_VERSION,
  type ConnectionTemplateSnapshot,
  type HostCommand,
  type HostCommandResponse,
  type HostEventListener,
  type ProfileSummary,
  type RemoteTrustAcquisitionSummary,
  type StreamSkopeHost,
  type TrustAcquisitionRecipe,
} from "../../src/kafka/contracts";
import { ProfileDialog } from "../../src/kafka/ui/ProfileDialog";

const recipe: TrustAcquisitionRecipe = {
  id: "generic-jks",
  revision: 1,
  name: "Remote trust",
  kind: "jks",
  syntax: "named-v1",
  method: "ssh",
  ssh: {
    source: "stdout",
    value: "read-certificate",
    password: { source: "command", command: "read-password" },
  },
  parameters: [],
  timeoutSeconds: 30,
};

const templates: ConnectionTemplateSnapshot = {
  catalogs: [
    {
      catalog: "truststore-fetch",
      entries: [{ name: "Remote trust", template: "copy {truststorePath} {storepass}" }],
      selectedName: "Remote trust",
    },
    {
      catalog: "truststore-password",
      entries: [{ name: "Remote password", template: "fetch password" }],
      selectedName: "Remote password",
    },
    {
      catalog: "oauth-endpoint",
      entries: [],
      selectedName: null,
    },
  ],
  store: { durability: "session", state: "ready" },
};

const passwordSummary: RemoteTrustAcquisitionSummary = {
  expiresAt: "2099-07-26T13:10:00.000Z",
  id: "acquisition-1",
  material: null,
  password: { present: true, templateName: "Remote password" },
  target: {
    host: "kafka-lab.example.test",
    hostKeyFingerprint: `SHA256:${"A".repeat(43)}`,
    port: 22,
  },
};

const materialSummary: RemoteTrustAcquisitionSummary = {
  ...passwordSummary,
  editor: { id: "editor", generation: 2 },
  material: {
    byteCount: 4_096,
    kind: "jks",
    label: "remote.truststore.jks",
    templateName: "Remote trust",
  },
};

const existingProfile: ProfileSummary = {
  active: false,
  brokers: ["kafka-lab.example.test:9093"],
  createdAt: "2026-07-25T10:00:00.000Z",
  id: "profile-1",
  name: "Remote Kafka lab",
  trust: {
    kind: "jks",
    label: "saved.truststore.jks",
    materialPresent: true,
    passwordPresent: true,
  },
  updatedAt: "2026-07-25T10:00:00.000Z",
};

class FakeHost implements StreamSkopeHost {
  private readonly listeners = new Set<HostEventListener>();
  readonly commands: HostCommand[] = [];
  failProfileSave = false;

  execute(command: HostCommand): Promise<HostCommandResponse> {
    if (command.command === "recipes.list") {
      for (const listener of this.listeners)
        listener({
          event: "recipes.changed",
          sequence: 1,
          version: HOST_PROTOCOL_VERSION,
          payload: { recipes: [recipe], store: { durability: "session", state: "ready" } },
        });
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId: "recipes" },
      });
    }
    if (command.command === "profiles.binding.get")
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: {
          correlationId: "binding",
          bindingDetail: { profileId: "profile-1", revision: 1, binding: null },
        },
      });
    if (
      command.command.startsWith("trustAcquisition.editor.") ||
      command.command === "trustAcquisition.apply"
    ) {
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result:
          command.command === "trustAcquisition.editor.open"
            ? { correlationId: "editor", editor: { id: "editor", generation: 1 } }
            : { correlationId: "editor" },
      });
    }
    if (command.command === "trustAcquisition.capabilities")
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId: "cap", sshAgent: "unavailable" },
      });
    this.commands.push(command);
    if (command.command === "trustAcquisition.hostKey.discover") {
      return Promise.resolve({
        command: command.command,
        id: command.id,
        ok: true,
        result: {
          correlationId: "discovery",
          hostKey: {
            review: {
              id: "identity-review",
              expiresAt: "2099-01-01T00:00:00Z",
              confirmationRequired: false,
            },
            fingerprint: `SHA256:${"A".repeat(43)}`,
            target: command.payload.target,
          },
        },
        version: HOST_PROTOCOL_VERSION,
      });
    }
    if (command.command === "trustAcquisition.password.fetch") {
      return Promise.resolve({
        command: command.command,
        id: command.id,
        ok: true,
        result: { acquisition: passwordSummary, correlationId: "password" },
        version: HOST_PROTOCOL_VERSION,
      });
    }
    if (command.command === "trustAcquisition.material.fetch") {
      return Promise.resolve({
        command: command.command,
        id: command.id,
        ok: true,
        result: { acquisition: materialSummary, correlationId: "material" },
        version: HOST_PROTOCOL_VERSION,
      });
    }
    if (
      this.failProfileSave &&
      (command.command === "profiles.create" || command.command === "profiles.update")
    ) {
      return Promise.resolve({
        command: command.command,
        error: {
          activeStateChanged: false,
          code: "PROFILE_STORE_UNAVAILABLE",
          correlationId: "profile-failed",
          recovery: "Unlock protected storage and retry.",
          retryable: true,
          stage: "storage",
          summary: "The profile could not be saved.",
        },
        id: command.id,
        ok: false,
        version: HOST_PROTOCOL_VERSION,
      });
    }
    return Promise.resolve({
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: "accepted" },
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

async function acquireRemoteTrust(
  dialog: HTMLElement,
  user: ReturnType<typeof userEvent.setup>,
): Promise<void> {
  await user.click(within(dialog).getByRole("button", { name: "Secret Retrieval Profile" }));
  await user.click(await within(dialog).findByRole("combobox", { name: "Use profile" }));
  await user.click(await screen.findByRole("option", { name: "Remote trust" }));
  await pasteText(
    user,
    within(dialog).getByRole("textbox", { name: "SSH host" }),
    "kafka-lab.example.test",
  );
  await pasteText(user, within(dialog).getByRole("textbox", { name: "SSH username" }), "operator");
  await pasteText(user, within(dialog).getByLabelText("SSH password"), "ssh-password");
  await user.click(within(dialog).getByRole("button", { name: "Retrieve" }));
  await waitFor(() => {
    expect(
      within(dialog).getByRole("status", { name: "Remote trust acquisition status" }),
    ).toHaveTextContent("JKS trust material acquired");
  });
  await user.click(within(dialog).getByRole("button", { name: "Apply to connection" }));
}

afterEach(() => {
  cleanup();
});

describe("Material UI acquired-trust profile workflow", () => {
  it("preserves manual material and password when another file selection is cancelled", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(
      <ProfileDialog
        host={host}
        onClose={() => undefined}
        onOpenActivity={() => undefined}
        open
        profile={existingProfile}
        templateLoading={false}
        templateSnapshot={templates}
      />,
    );
    const file = new File(["manual-jks"], "manual.jks", { type: "application/octet-stream" });
    const input = screen.getByLabelText("Trust material file");
    fireEvent.change(input, { target: { files: [file] } });
    await screen.findByText("manual.jks");
    await user.type(screen.getByLabelText("Truststore password"), "manual-password");
    fireEvent.change(input, { target: { files: [] } });
    expect(screen.getByText("manual.jks")).toBeVisible();
    expect(screen.getByLabelText("Truststore password")).toHaveValue("manual-password");
    await user.click(screen.getByRole("button", { name: "Update profile" }));
    expect(host.commands.at(-1)).toMatchObject({
      command: "profiles.update",
      payload: {
        profile: {
          trust: {
            material: { mode: "replace", value: "bWFudWFsLWprcw==" },
            password: { mode: "replace", value: "manual-password" },
          },
        },
      },
    });
  });

  it("keeps applied material through collapse while a manual password overrides retrieval", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(
      <ProfileDialog
        host={host}
        onClose={() => undefined}
        onOpenActivity={() => undefined}
        open
        profile={existingProfile}
        templateLoading={false}
        templateSnapshot={templates}
      />,
    );
    const dialog = screen.getByRole("dialog");
    await acquireRemoteTrust(dialog, user);
    expect(screen.getByLabelText("Truststore password")).toHaveValue("");
    expect(
      screen.getByText("Retrieved successfully. Stored securely in the host; type to replace it."),
    ).toBeVisible();
    expect(screen.getByLabelText("Truststore password")).toHaveAttribute("placeholder", "••••••••");
    expect(
      screen.queryByRole("button", { name: "Show truststore password" }),
    ).not.toBeInTheDocument();
    expect(host.commands.some((command) => command.command.startsWith("profiles."))).toBe(false);
    await user.type(screen.getByLabelText("Truststore password"), "my-password");
    expect(screen.getByLabelText("Truststore password")).not.toHaveAttribute("placeholder");
    expect(screen.getByRole("button", { name: "Show truststore password" })).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Secret Retrieval Profile" }));
    await user.click(screen.getByRole("button", { name: "Update profile" }));
    expect(host.commands.at(-1)).toMatchObject({
      command: "profiles.update",
      payload: {
        profile: {
          trust: {
            material: { mode: "acquired", acquisitionId: "acquisition-1", editorId: "editor" },
            password: { mode: "replace", value: "my-password" },
          },
        },
      },
    });
  });

  it("keeps manual fields available and unchanged while optional retrieval is opened and collapsed", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(
      <ProfileDialog
        host={host}
        onClose={() => undefined}
        onOpenActivity={() => undefined}
        open
        profile={existingProfile}
        templateLoading={false}
        templateSnapshot={templates}
      />,
    );
    const format = screen.getByRole("combobox", { name: "Trust material format" });
    const selectFile = screen.getByRole("button", { name: "Select trust material" });
    const disclosure = screen.getByRole("button", { name: "Secret Retrieval Profile" });
    expect(format.compareDocumentPosition(selectFile) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(
      0,
    );
    expect(
      selectFile.compareDocumentPosition(disclosure) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).not.toBe(0);
    expect(disclosure).toHaveAttribute("aria-expanded", "false");
    expect(disclosure).toHaveAccessibleDescription(
      "Optional: use saved retrieval instructions to fill certificate and secret fields. You can also configure these fields manually.",
    );
    expect(screen.getByText(/Optional: use saved retrieval instructions/)).toBeVisible();
    expect(
      screen.queryByRole("button", { name: "Manage retrieval profiles" }),
    ).not.toBeInTheDocument();
    await user.type(screen.getByLabelText("Truststore password"), "manual-password");
    await user.click(screen.getByRole("button", { name: "Secret Retrieval Profile" }));
    expect(screen.getByRole("button", { name: "Select trust material" })).toBeVisible();
    expect(screen.getByLabelText("Truststore password")).toHaveValue("manual-password");
    expect(await screen.findByRole("combobox", { name: "Use profile" })).toBeVisible();
    expect(
      within(screen.getByRole("region", { name: "Secret Retrieval Profile" })).getByRole("button", {
        name: "Manage retrieval profiles",
      }),
    ).toBeVisible();
    expect(screen.queryByRole("textbox", { name: "SSH username" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Secret Retrieval Profile" }));
    expect(screen.getByLabelText("Truststore password")).toHaveValue("manual-password");
    await user.click(screen.getByRole("button", { name: "Update profile" }));
    expect(host.commands.at(-1)).toMatchObject({
      command: "profiles.update",
      payload: {
        profile: {
          trust: {
            material: { mode: "retain" },
            password: { mode: "replace", value: "manual-password" },
          },
        },
      },
    });
  });

  it("starts a new profile with generic manual PEM and explicit recipe selection", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(
      <ProfileDialog
        host={host}
        onClose={() => undefined}
        onOpenActivity={() => undefined}
        open
        templateLoading={false}
        templateSnapshot={templates}
      />,
    );
    const dialog = screen.getByRole("dialog", { name: "Add Kafka profile" });

    expect(
      within(dialog).getByRole("combobox", { name: "Trust material format" }),
    ).toHaveTextContent("PEM");
    await user.click(within(dialog).getByRole("button", { name: "Secret Retrieval Profile" }));
    expect(
      within(dialog).queryByRole("button", { name: "Acquire password" }),
    ).not.toBeInTheDocument();
    expect(within(dialog).queryByRole("button", { name: "Retrieve" })).not.toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "Select trust material" })).toBeVisible();
  });

  it("creates a profile with opaque acquired values and clears selection after save", async () => {
    const host = new FakeHost();
    const onClose = vi.fn();
    const user = userEvent.setup();
    render(
      <ProfileDialog
        host={host}
        onClose={onClose}
        onOpenActivity={() => undefined}
        open
        templateLoading={false}
        templateSnapshot={templates}
      />,
    );
    const dialog = screen.getByRole("dialog", { name: "Add Kafka profile" });
    await user.type(within(dialog).getByRole("textbox", { name: "Profile name" }), "Remote lab");
    await pasteText(
      user,
      within(dialog).getByRole("textbox", { name: "Bootstrap brokers" }),
      "kafka-lab.example.test:9093",
    );
    await user.click(within(dialog).getByRole("combobox", { name: "Trust material format" }));
    await user.click(screen.getByRole("option", { name: "JKS truststore" }));
    expect(
      within(dialog).queryByRole("textbox", { name: "Trust material label" }),
    ).not.toBeInTheDocument();
    await acquireRemoteTrust(dialog, user);

    expect(
      host.commands.find((command) => command.command === "trustAcquisition.material.fetch"),
    ).toMatchObject({
      command: "trustAcquisition.material.fetch",
      payload: {
        kind: "jks",
        label: "truststore.jks",
      },
      version: HOST_PROTOCOL_VERSION,
    });

    await user.click(within(dialog).getByRole("button", { name: "Save profile" }));

    expect(host.commands.at(-1)).toMatchObject({
      command: "profiles.create",
      payload: {
        profile: {
          brokers: ["kafka-lab.example.test:9093"],
          name: "Remote lab",
          trust: {
            kind: "jks",
            label: "remote.truststore.jks",
            material: { acquisitionId: "acquisition-1", mode: "acquired" },
            password: { acquisitionId: "acquisition-1", mode: "acquired" },
          },
        },
      },
    });
    expect(onClose).toHaveBeenCalledOnce();
    expect(within(dialog).getByRole("button", { name: "Select trust material" })).toBeVisible();
  });

  it("updates with retained material and an acquired password from one complete acquisition", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(
      <ProfileDialog
        host={host}
        onClose={() => undefined}
        onOpenActivity={() => undefined}
        open
        profile={existingProfile}
        templateLoading={false}
        templateSnapshot={templates}
      />,
    );
    const dialog = screen.getByRole("dialog", {
      name: "Edit Kafka profile Remote Kafka lab",
    });
    await acquireRemoteTrust(dialog, user);
    await user.click(within(dialog).getByRole("button", { name: "Retain saved trust material" }));

    await user.click(within(dialog).getByRole("button", { name: "Update profile" }));

    expect(host.commands.at(-1)).toMatchObject({
      command: "profiles.update",
      payload: {
        profile: {
          trust: {
            kind: "jks",
            material: { mode: "retain" },
            password: { acquisitionId: "acquisition-1", mode: "acquired" },
          },
        },
        profileId: "profile-1",
      },
    });
  });

  it("retains acquisition and reports exact storage recovery after save failure", async () => {
    const host = new FakeHost();
    host.failProfileSave = true;
    const onOpenActivity = vi.fn();
    const user = userEvent.setup();
    render(
      <ProfileDialog
        host={host}
        onClose={() => undefined}
        onOpenActivity={onOpenActivity}
        open
        profile={existingProfile}
        templateLoading={false}
        templateSnapshot={templates}
      />,
    );
    const dialog = screen.getByRole("dialog", {
      name: "Edit Kafka profile Remote Kafka lab",
    });
    await acquireRemoteTrust(dialog, user);

    await user.click(within(dialog).getByRole("button", { name: "Update profile" }));

    expect(await within(dialog).findByRole("alert")).toHaveTextContent(
      "The profile could not be saved.",
    );
    expect(within(dialog).getByRole("alert")).toHaveTextContent(
      "Unlock protected storage and retry.",
    );
    expect(
      within(dialog).getByRole("status", { name: "Remote trust acquisition status" }),
    ).toHaveTextContent("JKS trust material acquired");
    await user.click(within(dialog).getByRole("button", { name: "Open activity log" }));
    expect(onOpenActivity).toHaveBeenCalledOnce();
    expect(within(dialog).getByRole("textbox", { name: "Profile name" })).toHaveValue(
      "Remote Kafka lab",
    );
  });
});
