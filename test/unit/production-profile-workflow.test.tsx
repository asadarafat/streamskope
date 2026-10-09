// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";

import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type ProfileSummary,
} from "../../src/features/kafka/contracts";
import { ProfileDialog } from "../../src/features/kafka/ui/ProfileDialog";
import { testHostExecute, type TestHostDispatch } from "../support/host-response";
import { pasteText } from "../support/paste-text";

const profile: ProfileSummary = {
  active: false,
  brokers: ["broker.example:9093"],
  createdAt: "2026-10-09T10:00:00.000Z",
  id: "production",
  name: "Production",
  revision: 4,
  transport: "tls",
  trust: { kind: "pem", label: "ca.pem", materialPresent: true, passwordPresent: false },
  updatedAt: "2026-10-09T10:00:00.000Z",
};

function setup(saved: ProfileSummary = profile): {
  user: ReturnType<typeof userEvent.setup>;
  execute: ReturnType<typeof vi.fn<TestHostDispatch>>;
  dialog: HTMLElement;
} {
  const execute = vi.fn<TestHostDispatch>((command) =>
    Promise.resolve({
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: "production-profile-test" },
      version: HOST_PROTOCOL_VERSION,
    }),
  );
  render(
    <ProfileDialog
      host={{
        execute: testHostExecute(execute),
        subscribe: () => (): void => undefined,
        openExternalUrl: () => Promise.reject(new Error("Unexpected external URL")),
      }}
      onClose={() => undefined}
      onOpenActivity={() => undefined}
      profile={saved}
      open
    />,
  );
  return {
    user: userEvent.setup(),
    execute,
    dialog: screen.getByRole("dialog", { name: "Edit Kafka profile Production" }),
  };
}

async function select(
  user: ReturnType<typeof userEvent.setup>,
  label: string,
  option: string,
): Promise<void> {
  await user.click(screen.getByRole("combobox", { name: label }));
  await user.click(screen.getByRole("option", { name: option }));
}

function submitted(
  execute: ReturnType<typeof vi.fn<TestHostDispatch>>,
): HostCommand & { command: "profiles.test" } {
  const command = execute.mock.calls.at(-1)?.[0];
  if (command?.command !== "profiles.test") throw new Error("Profile test was not submitted");
  return command;
}

afterEach(cleanup);

describe("production connection profile editor", () => {
  it.each(["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] as const)(
    "submits %s credentials through the protected host input",
    async (mechanism) => {
      const { user, execute, dialog } = setup();
      await select(user, "Broker authentication", `SASL ${mechanism}`);
      await pasteText(user, within(dialog).getByLabelText("SASL username"), "engineer");
      await pasteText(user, within(dialog).getByLabelText("SASL password"), "broker-secret");
      expect(within(dialog).getByLabelText("SASL password")).toHaveAttribute("type", "password");
      await user.click(within(dialog).getByRole("button", { name: "Test connection" }));
      await waitFor(() => expect(execute).toHaveBeenCalledOnce());
      expect(submitted(execute).payload.profile).toMatchObject({
        sasl: {
          mechanism,
          username: "engineer",
          password: { mode: "replace", value: "broker-secret" },
        },
        trust: { material: { mode: "retain" } },
      });
      expect(submitted(execute).payload.profile).not.toHaveProperty("oauth");
    },
  );

  it("retains broker and service secrets and identities without putting stored material in the editor", async () => {
    const identity = { certificatePresent: true, privateKeyPresent: true, passphrasePresent: true };
    const { user, execute, dialog } = setup({
      ...profile,
      sasl: { mechanism: "SCRAM-SHA-512", username: "engineer", passwordPresent: true },
      clientIdentity: identity,
      services: {
        schemaRegistry: {
          baseUrl: "https://registry.example",
          authentication: "basic",
          basic: { username: "registry-user", passwordPresent: true },
          trust: {
            mode: "custom",
            kind: "pkcs12",
            label: "registry.p12",
            materialPresent: true,
            passwordPresent: true,
          },
          clientIdentity: identity,
        },
      },
    });
    expect(within(dialog).getByLabelText("SASL password")).toHaveValue("");
    expect(within(dialog).getByLabelText("Schema Registry HTTP Basic password")).toHaveValue("");
    expect(within(dialog).getByLabelText("Broker private key passphrase")).toHaveValue("");
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));
    await waitFor(() => expect(execute).toHaveBeenCalledOnce());
    const retainedIdentity = {
      certificatePem: { mode: "retain" },
      privateKeyPem: { mode: "retain" },
      passphrase: { mode: "retain" },
    };
    expect(submitted(execute).payload.profile).toMatchObject({
      sasl: { password: { mode: "retain" } },
      clientIdentity: retainedIdentity,
      services: {
        schemaRegistry: {
          basic: { password: { mode: "retain" } },
          clientIdentity: retainedIdentity,
          trust: { mode: "custom", material: { mode: "retain" }, password: { mode: "retain" } },
        },
      },
    });
    expect(JSON.stringify(submitted(execute))).not.toContain('"value":');
  });

  it("uploads a broker identity and clears a saved optional key passphrase explicitly", async () => {
    const { user, execute, dialog } = setup({
      ...profile,
      clientIdentity: {
        certificatePresent: true,
        privateKeyPresent: true,
        passphrasePresent: true,
      },
    });
    await user.upload(
      within(dialog).getByLabelText("Broker client certificate file"),
      new File(["certificate-fixture"], "client.pem"),
    );
    await user.upload(
      within(dialog).getByLabelText("Broker client private key file"),
      new File(["private-key-fixture"], "client.key"),
    );
    await user.click(
      within(dialog).getByRole("button", { name: "Clear broker private key passphrase" }),
    );
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));
    await waitFor(() => expect(execute).toHaveBeenCalledOnce());
    expect(submitted(execute).payload.profile).toMatchObject({
      clientIdentity: {
        certificatePem: { mode: "replace", value: "certificate-fixture" },
        privateKeyPem: { mode: "replace", value: "private-key-fixture" },
        passphrase: { mode: "clear" },
      },
    });
    expect(dialog).not.toHaveTextContent("private-key-fixture");
  });

  it("configures independent Connect OAuth and Registry Basic auth, trust and identity", async () => {
    const { user, execute, dialog } = setup();
    await pasteText(
      user,
      within(dialog).getByLabelText("Kafka Connect URL"),
      "https://connect.example",
    );
    await select(user, "Connect authentication", "Separate OAuth client");
    await pasteText(
      user,
      within(dialog).getByLabelText("Connect OAuth token endpoint"),
      "https://connect-identity.example/token",
    );
    await pasteText(
      user,
      within(dialog).getByLabelText("Connect OAuth client ID"),
      "connect-client",
    );
    await pasteText(
      user,
      within(dialog).getByLabelText("Connect OAuth client secret"),
      "connect-secret",
    );
    await pasteText(
      user,
      within(dialog).getByLabelText("Schema Registry URL"),
      "https://registry.example",
    );
    await select(user, "Schema Registry authentication", "HTTP Basic");
    await pasteText(
      user,
      within(dialog).getByLabelText("Schema Registry HTTP Basic username"),
      "registry-user",
    );
    await pasteText(
      user,
      within(dialog).getByLabelText("Schema Registry HTTP Basic password"),
      "registry-secret",
    );
    await select(user, "Schema Registry certificate trust", "Separate CA or truststore");
    await user.upload(
      within(dialog).getByLabelText("Schema Registry trust material file"),
      new File(["registry-ca"], "registry.pem"),
    );
    await user.click(within(dialog).getByRole("switch", { name: "Schema Registry mutual TLS" }));
    await user.upload(
      within(dialog).getByLabelText("Schema Registry client certificate file"),
      new File(["registry-cert"], "client.pem"),
    );
    await user.upload(
      within(dialog).getByLabelText("Schema Registry client private key file"),
      new File(["registry-key"], "client.key"),
    );
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));
    await waitFor(() => expect(execute).toHaveBeenCalledOnce());
    expect(submitted(execute).payload.profile).toMatchObject({
      services: {
        connect: {
          authentication: "oauth-client",
          oauth: {
            clientId: "connect-client",
            clientSecret: { mode: "replace", value: "connect-secret" },
          },
          trust: { mode: "system" },
        },
        schemaRegistry: {
          authentication: "basic",
          basic: {
            username: "registry-user",
            password: { mode: "replace", value: "registry-secret" },
          },
          trust: { mode: "custom", material: { mode: "replace", value: "registry-ca" } },
          clientIdentity: { privateKeyPem: { mode: "replace", value: "registry-key" } },
        },
      },
    });
    expect(submitted(execute).payload.profile).not.toHaveProperty("oauth");
    expect(dialog).not.toHaveTextContent("registry-key");
  });

  it("blocks incomplete SASL and mutual TLS drafts before host submission", async () => {
    const { user, execute, dialog } = setup();
    await select(user, "Broker authentication", "SASL SCRAM-SHA-256");
    await user.click(within(dialog).getByRole("switch", { name: "Broker mutual TLS" }));
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));
    expect(execute).not.toHaveBeenCalled();
    expect(within(dialog).getByText("SASL username is required.")).toBeVisible();
    expect(within(dialog).getByText("SASL password is required.")).toBeVisible();
    expect(within(dialog).getByText("Client certificate is required.")).toBeVisible();
  });

  it("can remove identities and independent credentials without sending retained secrets", async () => {
    const { user, execute, dialog } = setup({
      ...profile,
      clientIdentity: {
        certificatePresent: true,
        privateKeyPresent: true,
        passphrasePresent: false,
      },
      services: {
        connect: {
          baseUrl: "https://connect.example",
          authentication: "bearer",
          bearerPresent: true,
        },
      },
    });
    await user.click(within(dialog).getByRole("switch", { name: "Broker mutual TLS" }));
    await select(user, "Connect authentication", "No HTTP authorization");
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));
    await waitFor(() => expect(execute).toHaveBeenCalledOnce());
    expect(submitted(execute).payload.profile).not.toHaveProperty("clientIdentity");
    expect(submitted(execute).payload.profile.services?.connect).not.toHaveProperty("bearer");
  });
});
