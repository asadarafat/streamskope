// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";

import { readFile } from "node:fs/promises";
import { join } from "node:path";

import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import { createKafkaBackend } from "../../src/platform/electron/main";
import { createBrowserKafkaProfileStore } from "../../src/platform/electron/main/kafka-backend";
import {
  HOST_PROTOCOL_VERSION,
  type StreamSkopeHost,
  type ProfileSummary,
  type HostEventListener,
  type TrustAcquisitionRecipe,
} from "../../src/features/kafka/contracts";
import {
  ProfileTrustRecipeSelector,
  type ProfileTrustRecipeSelection,
} from "../../src/features/kafka/ui/ProfileTrustRecipeSelector";
import { ProfileDialog } from "../../src/features/kafka/ui/ProfileDialog";

afterEach(cleanup);

it("reviews a pinned update without silently adopting it and resets incompatible choices", async () => {
  const pinned: TrustAcquisitionRecipe = {
    id: "recipe",
    revision: 1,
    name: "Pinned CA",
    kind: "pem",
    method: "ssh",
    syntax: "named-v1",
    ssh: { source: "file", value: "/ca.pem", password: { source: "none" } },
    parameters: [
      {
        key: "environment",
        label: "Environment",
        type: "choice",
        choices: ["prod", "dev"],
        required: true,
      },
    ],
    timeoutSeconds: 30,
  };
  const latest: TrustAcquisitionRecipe = {
    ...pinned,
    revision: 2,
    name: "Updated CA",
    parameters: [{ ...pinned.parameters[0]!, choices: ["staging"] }],
  };
  const listeners = new Set<HostEventListener>();
  const host: StreamSkopeHost = {
    execute: (command) => {
      const base = {
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true as const,
      };
      if (command.command === "recipes.list") {
        for (const listener of listeners)
          listener({
            event: "recipes.changed",
            sequence: 1,
            version: HOST_PROTOCOL_VERSION,
            payload: { recipes: [latest], store: { durability: "session", state: "ready" } },
          });
        return Promise.resolve({ ...base, result: { correlationId: "list" } });
      }
      if (command.command === "profiles.binding.get")
        return Promise.resolve({
          ...base,
          result: {
            correlationId: "detail",
            bindingDetail: {
              profileId: "profile",
              revision: 1,
              binding: { recipe: pinned, overrides: { environment: "prod" } },
            },
          },
        });
      return Promise.reject(new Error("Rendering must not execute or save"));
    },
    subscribe: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    openExternalUrl: () => Promise.reject(new Error("Not expected")),
  };
  const changed = vi.fn<(selection: ProfileTrustRecipeSelection | null) => void>();
  render(
    <ProfileTrustRecipeSelector
      host={host}
      profile={{ id: "profile", revision: 1 }}
      onChange={changed}
    />,
  );
  const user = userEvent.setup();
  await screen.findByRole("button", { name: "Review template update" });
  expect(changed.mock.lastCall?.[0]?.recipe.name).toBe("Pinned CA");
  await user.click(screen.getByRole("button", { name: "Review template update" }));
  expect(screen.getByRole("region", { name: "Template update review" })).toHaveTextContent(
    "Updated CA",
  );
  await user.click(screen.getByRole("button", { name: "Cancel update" }));
  expect(changed.mock.lastCall?.[0]?.recipe.revision).toBe(1);
  await user.click(screen.getByRole("button", { name: "Review template update" }));
  await user.click(screen.getByRole("button", { name: "Adopt template update" }));
  expect(changed.mock.lastCall?.[0]?.recipe.revision).toBe(2);
  expect(changed.mock.lastCall?.[0]?.reference.overrides).toEqual({});
});

it("acquires a generic JKS recipe, explicitly applies it and saves its protected profile binding", async () => {
  const bytes = await readFile(
    join(process.cwd(), "node_modules/jks-js/examples/assets/truststore.jks"),
  );
  const material = vi.fn(() => Promise.resolve(bytes));
  let fingerprint = `SHA256:${"A".repeat(43)}`;
  const store = createBrowserKafkaProfileStore();
  const backend = createKafkaBackend(store, undefined, undefined, undefined, {
    discoverHostKey: () => Promise.resolve(fingerprint),
    fetchMaterial: material,
    fetchPassword: () => Promise.reject(new Error("Supplied password must not execute a command")),
  });
  let saved: readonly ProfileSummary[] = [];
  backend.subscribe((event) => {
    if (event.event === "profiles.changed") saved = event.payload.profiles;
  });
  const host: StreamSkopeHost = {
    execute: backend.execute.bind(backend),
    subscribe: backend.subscribe.bind(backend),
    openExternalUrl: () => Promise.reject(new Error("Not expected")),
  };
  const close = vi.fn();
  try {
    render(
      <ProfileDialog
        host={host}
        open
        onClose={close}
        onOpenActivity={() => undefined}
        templateLoading={false}
        templateSnapshot={null}
      />,
    );
    const user = userEvent.setup();
    await user.type(screen.getByLabelText(/Profile name/u), "Generic SSH profile");
    await user.type(screen.getByLabelText(/Bootstrap brokers/u), "localhost:19093");
    await user.click(screen.getByRole("button", { name: "Secret Retrieval Profile" }));
    await user.click(await screen.findByRole("combobox", { name: "Use profile" }));
    await user.click(await screen.findByRole("option", { name: "SSH truststore" }));
    await user.type(
      screen.getByRole("textbox", { name: /Certificate path/u }),
      "/etc/tls/trust.jks",
    );
    await user.type(screen.getByLabelText(/Acquisition truststore password/u), "password");
    await user.type(screen.getByLabelText("SSH host"), "lab.example.test");
    await user.type(screen.getByLabelText("SSH username"), "operator");
    await user.type(screen.getByLabelText("SSH password"), "ephemeral-ssh-password");
    expect(screen.getByLabelText(/Acquisition truststore password/u)).toHaveValue("password");
    await user.click(screen.getByRole("button", { name: "Retrieve" }));
    await user.click(await screen.findByRole("button", { name: "Accept identity and acquire" }));
    await user.click(await screen.findByRole("button", { name: "Apply to connection" }));
    expect(saved).toEqual([]);
    await user.click(screen.getByRole("button", { name: "Save profile" }));
    await waitFor(() => expect(close).toHaveBeenCalledOnce());
    expect(saved).toHaveLength(1);
    expect(material).toHaveBeenCalledOnce();
    const response = await host.execute({
      command: "profiles.binding.get",
      id: "inspect-saved",
      version: HOST_PROTOCOL_VERSION,
      payload: { profileId: saved[0]!.id },
    });
    expect(response).toMatchObject({
      ok: true,
      result: {
        bindingDetail: {
          binding: {
            recipe: { name: "SSH truststore" },
            overrides: { certificate_path: "/etc/tls/trust.jks" },
            access: {
              host: "lab.example.test",
              port: 22,
              username: "operator",
              authentication: "password",
            },
            identity: {
              host: "lab.example.test",
              port: 22,
              fingerprint: `SHA256:${"A".repeat(43)}`,
            },
          },
        },
      },
    });
    expect(JSON.stringify(response)).not.toContain("ephemeral-ssh-password");
    const reopened = await host.execute({
      command: "trustAcquisition.editor.open",
      id: "reopen",
      version: HOST_PROTOCOL_VERSION,
      payload: { profile: { id: saved[0]!.id, revision: saved[0]!.revision! } },
    });
    if (!reopened.ok || !("editor" in reopened.result)) throw new Error("Expected profile editor");
    const discovery = {
      command: "trustAcquisition.hostKey.discover" as const,
      id: "known",
      version: HOST_PROTOCOL_VERSION,
      payload: { editor: reopened.result.editor, target: { host: "lab.example.test", port: 22 } },
    };
    expect(await host.execute(discovery)).toMatchObject({
      ok: true,
      result: { hostKey: { review: { confirmationRequired: false } } },
    });
    fingerprint = `SHA256:${"B".repeat(43)}`;
    expect(await host.execute({ ...discovery, id: "changed" })).toMatchObject({
      ok: false,
      error: { code: "SSH_IDENTITY" },
    });
    expect(material).toHaveBeenCalledOnce();
    const beforeReset = store.records()[0]!;
    const pinned = beforeReset.binding!;
    const reset = await host.execute({
      command: "profiles.update",
      id: "reset-identity",
      version: HOST_PROTOCOL_VERSION,
      payload: {
        profileId: beforeReset.id,
        profile: {
          name: beforeReset.name,
          brokers: beforeReset.brokers,
          expectedRevision: beforeReset.revision!,
          trust: {
            kind: beforeReset.trust.kind,
            label: beforeReset.trust.label,
            material: { mode: "retain" },
            password: { mode: "retain" },
          },
          binding: {
            mode: "replace",
            recipeId: pinned.recipe.id,
            recipeRevision: pinned.recipe.revision,
            overrides: pinned.overrides,
            identity: { mode: "reset" },
          },
        },
      },
    });
    expect(reset.ok).toBe(true);
    expect(store.records()[0]?.binding?.identity).toBeUndefined();
    expect(store.records()[0]?.trust).toEqual(beforeReset.trust);
    expect(material).toHaveBeenCalledOnce();
  } finally {
    cleanup();
    await backend.shutdown();
  }
});

it("starts manual profiles with PEM and exposes the generic selector only for remote acquisition", async () => {
  const backend = createKafkaBackend();
  const host: StreamSkopeHost = {
    execute: backend.execute.bind(backend),
    subscribe: backend.subscribe.bind(backend),
    openExternalUrl: () => Promise.reject(new Error("Not expected")),
  };
  try {
    render(
      <ProfileDialog
        host={host}
        open
        onClose={() => undefined}
        onOpenActivity={() => undefined}
        templateLoading={false}
        templateSnapshot={null}
      />,
    );
    expect(screen.getByRole("combobox", { name: "Trust material format" })).toHaveTextContent(
      "PEM certificate",
    );
    expect(screen.queryByRole("combobox", { name: "Use profile" })).not.toBeInTheDocument();
    await userEvent.setup().click(screen.getByRole("button", { name: "Secret Retrieval Profile" }));
    expect(await screen.findByRole("combobox", { name: "Use profile" })).toBeVisible();
  } finally {
    cleanup();
    await backend.shutdown();
  }
});

it("selects a generic template, edits profile-only parameters and resets to template defaults", async () => {
  const backend = createKafkaBackend();
  const execute = vi.fn(backend.execute.bind(backend));
  const host: StreamSkopeHost = {
    execute,
    subscribe: backend.subscribe.bind(backend),
    openExternalUrl: () => Promise.reject(new Error("Not expected")),
  };
  const onChange = vi.fn<(selection: ProfileTrustRecipeSelection | null) => void>();
  try {
    render(<ProfileTrustRecipeSelector host={host} onChange={onChange} />);
    const user = userEvent.setup();
    await user.click(await screen.findByRole("combobox", { name: "Use profile" }));
    await user.click(await screen.findByRole("option", { name: /SSH certificate file/u }));
    const path = screen.getByRole("textbox", { name: /Certificate path/u });
    await user.clear(path);
    await user.type(path, "/etc/tls/ca.pem");
    expect(onChange.mock.lastCall?.[0]?.reference.overrides).toEqual({
      certificate_path: "/etc/tls/ca.pem",
    });
    await user.click(screen.getByRole("button", { name: "Reset Certificate path" }));
    expect(onChange.mock.lastCall?.[0]?.reference.overrides).toEqual({});
    expect(
      execute.mock.calls.some(
        ([command]) =>
          command.command.startsWith("trustAcquisition.") &&
          command.command !== "trustAcquisition.capabilities",
      ),
    ).toBe(false);
    expect(execute.mock.calls.some(([command]) => command.command === "recipes.update")).toBe(
      false,
    );
  } finally {
    cleanup();
    await backend.shutdown();
  }
});
