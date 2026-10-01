// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type ProfileSummary,
} from "../../src/features/kafka/contracts";
import { ProfileDialog } from "../../src/features/kafka/ui/ProfileDialog";
import { ProfilePanel } from "../../src/features/kafka/ui/ProfilePanel";
import { ProfileWorkspace } from "../../src/features/kafka/ui/ProfileWorkspace";
import { pasteText } from "../support/paste-text";
import { testHostExecute, type TestHostDispatch } from "../support/host-response";

const tlsProfile: ProfileSummary = {
  active: false,
  brokers: ["broker.example.test:9093"],
  createdAt: "2026-09-17T18:00:00.000Z",
  id: "tls-profile",
  name: "TLS profile",
  revision: 1,
  transport: "tls",
  trust: {
    kind: "pem",
    label: "ca.pem",
    materialPresent: true,
    passwordPresent: false,
  },
  updatedAt: "2026-09-17T18:00:00.000Z",
};

const plaintextProfile: ProfileSummary = {
  active: false,
  brokers: ["broker.example.test:9092"],
  createdAt: "2026-09-17T18:00:00.000Z",
  id: "plaintext-profile",
  name: "Plaintext profile",
  revision: 1,
  transport: "plaintext",
  updatedAt: "2026-09-17T18:00:00.000Z",
};

class DeferredFileReader extends EventTarget {
  static current: DeferredFileReader | undefined;
  result: string | ArrayBuffer | null = null;

  abort(): void {
    this.dispatchEvent(new ProgressEvent("abort"));
    this.dispatchEvent(new ProgressEvent("loadend"));
  }

  complete(value: string): void {
    this.result = value;
    this.dispatchEvent(new ProgressEvent("load"));
    this.dispatchEvent(new ProgressEvent("loadend"));
  }

  readAsArrayBuffer(): void {
    DeferredFileReader.current = this;
  }

  readAsText(): void {
    DeferredFileReader.current = this;
  }
}

function setup(
  profile?: ProfileSummary,
  implementation?: TestHostDispatch,
): {
  readonly execute: ReturnType<typeof vi.fn<TestHostDispatch>>;
  readonly user: ReturnType<typeof userEvent.setup>;
} {
  const execute = vi.fn<TestHostDispatch>(
    implementation ??
      ((command: HostCommand): Promise<unknown> =>
        Promise.resolve({
          command: command.command,
          id: command.id,
          ok: true,
          result: { correlationId: "plaintext-correlation" },
          version: HOST_PROTOCOL_VERSION,
        })),
  );
  render(
    <ProfileDialog
      host={{
        execute: testHostExecute(execute),
        openExternalUrl: (): Promise<never> =>
          Promise.reject(new Error("External URL was not expected.")),
        subscribe: (): (() => void) => (): void => undefined,
      }}
      onClose={(): void => undefined}
      onOpenActivity={(): void => undefined}
      open
      {...(profile === undefined ? {} : { profile })}
    />,
  );
  return { execute, user: userEvent.setup() };
}

function latestProfileTest(
  execute: ReturnType<typeof vi.fn<TestHostDispatch>>,
): Extract<HostCommand, { readonly command: "profiles.test" }> {
  const command = execute.mock.calls.at(-1)?.[0];
  if (command?.command !== "profiles.test") {
    throw new Error("Expected a profiles.test command.");
  }
  return command;
}

function latestProfileUpdate(
  execute: ReturnType<typeof vi.fn<TestHostDispatch>>,
): Extract<HostCommand, { readonly command: "profiles.update" }> {
  const command = execute.mock.calls.at(-1)?.[0];
  if (command?.command !== "profiles.update") {
    throw new Error("Expected a profiles.update command.");
  }
  return command;
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  DeferredFileReader.current = undefined;
});

describe("plaintext Kafka profile workflow", () => {
  it("defaults to TLS and submits deliberate plaintext without trust or source fields", async () => {
    const { execute, user } = setup();
    const dialog = screen.getByRole("dialog", { name: "Add Kafka profile" });
    expect(within(dialog).getByRole("radio", { name: "TLS" })).toBeChecked();
    expect(within(dialog).queryByText(/Plaintext is insecure/u)).not.toBeInTheDocument();

    await user.click(within(dialog).getByRole("radio", { name: "Plaintext (insecure)" }));

    expect(within(dialog).getByText(/Plaintext is insecure/u)).toBeVisible();
    expect(
      within(dialog).getByText(/metadata, messages, and Kafka OAuth credentials/u),
    ).toBeVisible();
    expect(within(dialog).queryByRole("heading", { name: "TLS trust" })).not.toBeInTheDocument();
    expect(within(dialog).getByRole("heading", { name: "Authentication" })).toBeVisible();
    expect(within(dialog).getByRole("heading", { name: "Cluster services" })).toBeVisible();

    await user.type(within(dialog).getByRole("textbox", { name: "Profile name" }), "Plaintext lab");
    await pasteText(
      user,
      within(dialog).getByRole("textbox", { name: "Bootstrap brokers" }),
      "127.0.0.1:19092",
    );
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));

    await waitFor(() => expect(execute).toHaveBeenCalledOnce());
    const command = latestProfileTest(execute);
    expect(command.id.length).toBeGreaterThan(0);
    expect(command.payload).toEqual({
      mode: "create",
      profile: {
        brokers: ["127.0.0.1:19092"],
        name: "Plaintext lab",
        transport: "plaintext",
      },
    });
    const serialized = JSON.stringify(command);
    expect(serialized).not.toMatch(/trust|source|capture/i);
  });

  it("keeps OAuth and services available for plaintext without fabricating trust", async () => {
    const { execute, user } = setup();
    const dialog = screen.getByRole("dialog", { name: "Add Kafka profile" });
    await user.click(within(dialog).getByRole("radio", { name: "Plaintext (insecure)" }));
    await user.type(within(dialog).getByRole("textbox", { name: "Profile name" }), "Plain OAuth");
    await pasteText(
      user,
      within(dialog).getByRole("textbox", { name: "Bootstrap brokers" }),
      "127.0.0.1:19092",
    );
    await user.click(within(dialog).getByRole("switch", { name: "Use OAuth OAUTHBEARER" }));
    await user.type(
      within(dialog).getByRole("textbox", { name: "OAuth token endpoint" }),
      "https://identity.example.test/token",
    );
    await user.type(within(dialog).getByRole("textbox", { name: "OAuth client ID" }), "client");
    await user.type(within(dialog).getByRole("textbox", { name: "OAuth scope" }), "kafka");
    await user.type(within(dialog).getByLabelText("OAuth client secret"), "secret");
    await user.type(
      within(dialog).getByRole("textbox", { name: "Schema Registry URL" }),
      "https://schema.example.test:8081",
    );
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));

    await waitFor(() => expect(execute).toHaveBeenCalledOnce());
    const command = latestProfileTest(execute);
    expect(command.payload).toMatchObject({
      mode: "create",
      profile: {
        oauth: { clientId: "client" },
        services: {
          schemaRegistry: {
            authentication: "none",
            baseUrl: "https://schema.example.test:8081",
          },
        },
        transport: "plaintext",
      },
    });
    expect(JSON.stringify(command)).not.toMatch(/trust|source|capture/i);
  });

  it("requires destructive confirmation before saving TLS as plaintext", async () => {
    const { execute, user } = setup(tlsProfile);
    const dialog = screen.getByRole("dialog", { name: "Edit Kafka profile TLS profile" });
    await user.click(within(dialog).getByRole("radio", { name: "Plaintext (insecure)" }));
    await user.click(within(dialog).getByRole("button", { name: "Update profile" }));

    expect(execute).not.toHaveBeenCalled();
    const confirmation = screen.getByRole("dialog", { name: "Switch profile to plaintext?" });
    expect(confirmation).toHaveTextContent(/removes saved broker trust/i);
    await user.click(within(confirmation).getByRole("button", { name: "Switch to plaintext" }));

    await waitFor(() => expect(execute).toHaveBeenCalledOnce());
    const command = latestProfileUpdate(execute);
    expect(command.payload).toMatchObject({
      profile: {
        expectedRevision: 1,
        transport: "plaintext",
      },
      profileId: "tls-profile",
    });
    expect(JSON.stringify(command)).not.toMatch(/trust|binding|apiCa/i);
  });

  it("requires fresh trust when a saved plaintext profile selects TLS", async () => {
    const { execute, user } = setup(plaintextProfile);
    const dialog = screen.getByRole("dialog", { name: "Edit Kafka profile Plaintext profile" });
    await user.click(within(dialog).getByRole("radio", { name: "TLS" }));
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));

    expect(within(dialog).getByText("Select certificate or truststore material.")).toBeVisible();
    expect(execute).not.toHaveBeenCalled();
  });

  it("does not clear saved TLS binding intent when transport selection is reversed", async () => {
    const { execute, user } = setup(tlsProfile);
    const dialog = screen.getByRole("dialog", { name: "Edit Kafka profile TLS profile" });
    await user.click(within(dialog).getByRole("radio", { name: "Plaintext (insecure)" }));
    await user.click(within(dialog).getByRole("radio", { name: "TLS" }));
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));

    await waitFor(() => expect(execute).toHaveBeenCalledOnce());
    const command = latestProfileTest(execute);
    expect(command.payload).toMatchObject({
      mode: "update",
      profile: {
        transport: "tls",
        trust: {
          material: { mode: "retain" },
        },
      },
      profileId: "tls-profile",
    });
    expect(command.payload.profile).not.toHaveProperty("binding");
  });

  it("freezes the complete plaintext draft while a test is pending", async () => {
    let complete: ((response: HostCommandResponse) => void) | undefined;
    const pending = new Promise<HostCommandResponse>((resolve) => {
      complete = resolve;
    });
    const { user } = setup(undefined, () => pending);
    const dialog = screen.getByRole("dialog", { name: "Add Kafka profile" });
    await user.click(within(dialog).getByRole("radio", { name: "Plaintext (insecure)" }));
    await user.type(within(dialog).getByRole("textbox", { name: "Profile name" }), "Pending");
    await user.type(
      within(dialog).getByRole("textbox", { name: "Bootstrap brokers" }),
      "127.0.0.1:19092",
    );
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));

    await waitFor(() => expect(dialog).toHaveAttribute("aria-busy", "true"));
    expect(within(dialog).getByRole("textbox", { name: "Profile name" })).toBeDisabled();
    expect(within(dialog).getByRole("textbox", { name: "Bootstrap brokers" })).toBeDisabled();
    expect(within(dialog).getByRole("radio", { name: "TLS" })).toBeDisabled();
    expect(within(dialog).getByRole("switch", { name: "Use OAuth OAUTHBEARER" })).toBeDisabled();
    expect(within(dialog).getByRole("textbox", { name: "Schema Registry URL" })).toBeDisabled();

    complete?.({
      command: "profiles.test",
      id: "pending",
      ok: true,
      result: { correlationId: "pending" },
      version: HOST_PROTOCOL_VERSION,
    });
    await waitFor(() => expect(dialog).toHaveAttribute("aria-busy", "false"));
  });

  it("disables retrieval management while a TLS draft test is pending", async () => {
    let complete: ((response: HostCommandResponse) => void) | undefined;
    const pending = new Promise<HostCommandResponse>((resolve) => {
      complete = resolve;
    });
    const { execute, user } = setup(undefined, (command) =>
      command.command === "profiles.test"
        ? pending
        : Promise.resolve({
            command: command.command,
            id: command.id,
            ok: true,
            result: { correlationId: "setup" },
            version: HOST_PROTOCOL_VERSION,
          }),
    );
    const dialog = screen.getByRole("dialog", { name: "Add Kafka profile" });
    await user.type(within(dialog).getByRole("textbox", { name: "Profile name" }), "Pending TLS");
    await user.type(
      within(dialog).getByRole("textbox", { name: "Bootstrap brokers" }),
      "127.0.0.1:19093",
    );
    await user.upload(
      within(dialog).getByLabelText("Trust material file"),
      new File(["-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----"], "ca.pem"),
    );
    await user.click(
      within(dialog).getByRole("button", { name: "Retrieve certificates and credentials" }),
    );
    const manage = within(dialog).getByRole("button", { name: "Manage retrieval presets" });
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));

    await waitFor(() => expect(dialog).toHaveAttribute("aria-busy", "true"));
    expect(manage).toBeDisabled();

    const command = latestProfileTest(execute);
    complete?.({
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: "pending-tls" },
      version: HOST_PROTOCOL_VERSION,
    });
    await waitFor(() => expect(dialog).toHaveAttribute("aria-busy", "false"));
  });

  it("ignores a trust-file read that completes after plaintext is selected", async () => {
    vi.stubGlobal("FileReader", DeferredFileReader);
    const { execute, user } = setup();
    const dialog = screen.getByRole("dialog", { name: "Add Kafka profile" });
    await user.type(within(dialog).getByRole("textbox", { name: "Profile name" }), "Read race");
    await user.type(
      within(dialog).getByRole("textbox", { name: "Bootstrap brokers" }),
      "127.0.0.1:19092",
    );
    await user.upload(
      within(dialog).getByLabelText("Trust material file"),
      new File(["pending"], "pending.pem"),
    );
    const reader = DeferredFileReader.current;
    if (reader === undefined) throw new Error("Expected a pending trust-file read.");

    await user.click(within(dialog).getByRole("radio", { name: "Plaintext (insecure)" }));
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));
    await waitFor(() => expect(execute).toHaveBeenCalledOnce());
    reader.complete("-----BEGIN CERTIFICATE-----\nlate\n-----END CERTIFICATE-----");
    await Promise.resolve();
    await user.click(within(dialog).getByRole("radio", { name: "TLS" }));

    expect(within(dialog).getByText("No trust material selected.")).toBeVisible();
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));
    expect(within(dialog).getByText("Select certificate or truststore material.")).toBeVisible();
    expect(execute).toHaveBeenCalledOnce();
  });

  it("labels saved plaintext detail as insecure without TLS evidence", () => {
    render(
      <ProfileWorkspace
        action={null}
        activityOpen={false}
        component="section"
        clusterDiagnostics={{
          cluster: null,
          endpoint: null,
          fetchedAt: null,
          profile: null,
          state: "unavailable",
        }}
        host={{
          execute: testHostExecute((): Promise<never> =>
            Promise.reject(new Error("Host execution was not expected.")),
          ),
          openExternalUrl: (): Promise<never> =>
            Promise.reject(new Error("External URL was not expected.")),
          subscribe: (): (() => void) => (): void => undefined,
        }}
        onActionClose={(): void => undefined}
        onOpenActivity={(): void => undefined}
        profile={plaintextProfile}
        transfer={{
          copy: (): Promise<void> => Promise.resolve(),
          download: (): Promise<void> => Promise.resolve(),
        }}
      />,
    );

    const workspace = screen.getByRole("region", { name: "Connection profile workspace" });
    expect(within(workspace).getByText("Plaintext — insecure")).toBeVisible();
    expect(
      within(workspace).getByText(/metadata and messages are not protected by TLS/u),
    ).toBeVisible();
    expect(within(workspace).queryByText(/trust material present/u)).not.toBeInTheDocument();
    expect(within(workspace).queryByText(/Hostname verification/u)).not.toBeInTheDocument();
  });

  it("labels saved plaintext inventory and its connection affordance as insecure", () => {
    render(
      <ProfilePanel
        activityOpen={false}
        connected={false}
        connectionOperation={null}
        filter=""
        host={{
          execute: testHostExecute((): Promise<never> =>
            Promise.reject(new Error("Host execution was not expected.")),
          ),
          openExternalUrl: (): Promise<never> =>
            Promise.reject(new Error("External URL was not expected.")),
          subscribe: (): (() => void) => (): void => undefined,
        }}
        loading={false}
        onFilterChange={(): void => undefined}
        onOpenActivity={(): void => undefined}
        onProfileAction={(): void => undefined}
        onSelectProfile={(): void => undefined}
        onToggleConnection={(): void => undefined}
        profiles={[plaintextProfile]}
        selectedProfileId="plaintext-profile"
        store={{ durability: "session", protection: "memory", state: "ready" }}
      />,
    );

    const list = screen.getByRole("list", { name: "Kafka connection profiles" });
    expect(within(list).getByText(/Plaintext · insecure/u)).toBeVisible();
    expect(
      within(list).getByRole("button", {
        name: "Connect insecure plaintext profile Plaintext profile",
      }),
    ).toBeVisible();
  });
});
