// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";

import { pasteText } from "../support/paste-text";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostError,
  type HostEvent,
  type HostEventListener,
  type ProfileStoreCapability,
  type ProfileSummary,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { StreamSkopeApp as StreamSkopeWorkbench } from "../../src/features/kafka/ui/StreamSkopeApp";

const sessionStore: ProfileStoreCapability = {
  durability: "session",
  protection: "memory",
  state: "ready",
};

function profile(overrides: Partial<ProfileSummary> = {}): ProfileSummary {
  return {
    active: false,
    brokers: ["127.0.0.1:9093"],
    createdAt: "2026-07-25T10:00:00.000Z",
    id: "profile-local",
    name: "Local aio",
    oauth: {
      clientId: "admin",
      clientSecretPresent: true,
      scope: "kafka",
      tokenEndpoint: "http://127.0.0.1:5000/rest-gateway/rest/api/v1/auth/token",
    },
    trust: {
      kind: "pkcs12",
      label: "kafka.truststore.jks",
      materialPresent: true,
      passwordPresent: true,
    },
    updatedAt: "2026-07-25T10:00:00.000Z",
    ...overrides,
  };
}

function failure(command: HostCommand, error: Partial<HostError> = {}): HostCommandResponse {
  return {
    command: command.command,
    error: {
      activeStateChanged: false,
      code: "PROFILE_STORE_UNAVAILABLE",
      correlationId: `correlation-${command.id}`,
      recovery: "Unlock protected storage and restart StreamSkope.",
      retryable: false,
      stage: "storage",
      summary: "The profile could not be saved.",
      ...error,
    },
    id: command.id,
    ok: false,
    version: HOST_PROTOCOL_VERSION,
  };
}

class FakeHost implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  private readonly listeners = new Set<HostEventListener>();
  private nextResponse: ((command: HostCommand) => HostCommandResponse) | undefined;

  emit(event: HostEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  failNext(error: Partial<HostError> = {}): void {
    this.nextResponse = (command): HostCommandResponse => failure(command, error);
  }

  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    const response = this.nextResponse?.(command) ?? {
      command: command.command,
      id: command.id,
      ok: true as const,
      result: { correlationId: `correlation-${command.id}` },
      version: HOST_PROTOCOL_VERSION,
    };
    this.nextResponse = undefined;
    return Promise.resolve(response);
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

function publishProfiles(
  host: FakeHost,
  profiles: readonly ProfileSummary[],
  store: ProfileStoreCapability = sessionStore,
  sequence = 1,
): void {
  act(() => {
    host.emit({
      event: "profiles.changed",
      payload: { profiles, store },
      sequence,
      version: HOST_PROTOCOL_VERSION,
    });
  });
}

async function openProfileAction(
  user: ReturnType<typeof userEvent.setup>,
  action: "Cluster detail" | "Delete" | "Edit",
): Promise<void> {
  const resources = screen.getByRole("main", { name: "Connection profiles page" });
  await user.click(
    within(resources).getByRole("button", { name: "More actions for profile Local aio" }),
  );
  await user.click(
    within(screen.getByRole("menu", { name: "Profile actions for Local aio" })).getByRole(
      "menuitem",
      { name: action },
    ),
  );
}

afterEach(() => {
  cleanup();
});

describe("Material UI Kafka profile workflow", () => {
  it("keeps service authentication independent and blocks service OAuth without profile OAuth", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    publishProfiles(host, [profile()]);
    await openProfileAction(user, "Edit");
    const dialog = screen.getByRole("dialog", { name: "Edit Kafka profile Local aio" });
    await user.type(
      within(dialog).getByLabelText("Schema Registry URL"),
      "https://schema.example:8081",
    );
    await user.type(
      within(dialog).getByLabelText("Redpanda Admin URL"),
      "https://admin.example:9644",
    );
    await user.click(
      within(dialog).getByRole("combobox", { name: "Schema Registry authentication" }),
    );
    await user.click(screen.getByRole("option", { name: "Profile OAuth bearer token" }));
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));
    expect(host.commands.at(-1)).toMatchObject({
      command: "profiles.test",
      payload: {
        profile: {
          services: {
            schemaRegistry: { authentication: "oauth", baseUrl: "https://schema.example:8081" },
            redpandaAdmin: { authentication: "none", baseUrl: "https://admin.example:9644" },
          },
        },
      },
    });
    await user.click(within(dialog).getByRole("switch", { name: "Use OAuth OAUTHBEARER" }));
    const count = host.commands.length;
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));
    expect(
      within(dialog).getByText("Schema Registry OAuth requires the profile OAuth configuration."),
    ).toBeVisible();
    expect(host.commands).toHaveLength(count);
    expect(within(dialog).getByLabelText("Redpanda Admin URL")).toHaveValue(
      "https://admin.example:9644",
    );
  });

  it("remasks a replacement OAuth secret on test and can return to saved-secret retention", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    publishProfiles(host, [profile()]);
    await openProfileAction(user, "Edit");
    const dialog = screen.getByRole("dialog", { name: "Edit Kafka profile Local aio" });
    const secret = within(dialog).getByLabelText("OAuth client secret");
    await user.type(secret, "replacement-test-value");
    await user.click(within(dialog).getByRole("button", { name: "Show OAuth client secret" }));
    expect(secret).toHaveAttribute("type", "text");
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));
    expect(secret).toHaveAttribute("type", "password");
    expect(secret).toHaveValue("replacement-test-value");
    await user.click(within(dialog).getByRole("button", { name: "Retain saved client secret" }));
    expect(secret).toHaveValue("");
    expect(within(dialog).getByText("Saved client secret will be retained.")).toBeVisible();
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));
    expect(host.commands.at(-1)).toMatchObject({
      command: "profiles.test",
      payload: { profile: { oauth: { clientSecret: { mode: "retain" } } } },
    });
  });

  it("keeps the opened revision when profile inventory changes behind an editor", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    publishProfiles(host, [profile({ revision: 3 })]);
    await openProfileAction(user, "Edit");
    const dialog = screen.getByRole("dialog", { name: "Edit Kafka profile Local aio" });
    publishProfiles(host, [profile({ revision: 4 })]);
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));
    await waitFor(() =>
      expect(host.commands.at(-1)).toMatchObject({
        command: "profiles.test",
        payload: { profile: { expectedRevision: 3 } },
      }),
    );
    await user.click(within(dialog).getByRole("button", { name: "Update profile" }));
    expect(host.commands.at(-1)).toMatchObject({
      command: "profiles.update",
      payload: { profile: { expectedRevision: 3 } },
    });
  });

  it("uses the vscode-nsp profile row as the sole owner of profile actions", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    publishProfiles(host, [profile()]);

    const resources = screen.getByRole("main", { name: "Connection profiles page" });
    await user.click(
      within(resources).getByRole("button", { name: "More actions for profile Local aio" }),
    );

    const menu = screen.getByRole("menu", { name: "Profile actions for Local aio" });
    expect(within(menu).getByRole("menuitem", { name: "Edit" })).toBeEnabled();
    expect(within(menu).getByRole("menuitem", { name: "Cluster detail" })).toHaveAttribute(
      "aria-disabled",
      "true",
    );
    expect(within(menu).getByRole("menuitem", { name: "Delete" })).toBeEnabled();
    await user.keyboard("{Escape}");
    const workspace = screen.getByRole("region", { name: "Connection profile workspace" });
    expect(
      within(workspace).queryByRole("button", { name: /Edit profile/ }),
    ).not.toBeInTheDocument();
    expect(
      within(workspace).queryByRole("button", { name: /Cluster details for profile/ }),
    ).not.toBeInTheDocument();
    expect(
      within(workspace).queryByRole("button", { name: /Delete profile/ }),
    ).not.toBeInTheDocument();
  });

  it("exposes one test-before-save action in the profile editor", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    publishProfiles(host, []);

    await user.click(screen.getByRole("button", { name: "Add profile" }));
    const dialog = screen.getByRole("dialog", { name: "Add Kafka profile" });

    expect(within(dialog).getByRole("button", { name: "Test connection" })).toBeEnabled();
    expect(within(dialog).getByRole("heading", { level: 3, name: "Authentication" })).toBeVisible();
    expect(within(dialog).getByRole("group", { name: "Profile actions" })).toBeVisible();
    expect(
      within(dialog).queryByRole("textbox", { name: "Trust material label" }),
    ).not.toBeInTheDocument();
    expect(within(dialog).getByText("No trust material selected.")).toBeVisible();
    expect(
      screen.queryByRole("dialog", { name: "Configure Kafka connection" }),
    ).not.toBeInTheDocument();
  });

  it("returns from Activity to an unchanged failed profile draft", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    publishProfiles(host, [profile()]);

    await openProfileAction(user, "Edit");
    let dialog = screen.getByRole("dialog", { name: "Edit Kafka profile Local aio" });
    await user.clear(within(dialog).getByRole("textbox", { name: "Profile name" }));
    await user.type(within(dialog).getByRole("textbox", { name: "Profile name" }), "Draft aio");
    host.failNext();
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));
    await waitFor(() => {
      expect(within(dialog).getByRole("alert")).toHaveTextContent(
        "The profile could not be saved.",
      );
    });

    await user.click(within(dialog).getByRole("button", { name: "Open activity log" }));
    await waitFor(() => {
      expect(
        screen.queryByRole("dialog", { name: "Edit Kafka profile Local aio" }),
      ).not.toBeInTheDocument();
    });
    await user.click(screen.getByRole("button", { name: "Collapse Activity" }));

    dialog = await screen.findByRole("dialog", { name: "Edit Kafka profile Local aio" });
    expect(within(dialog).getByRole("textbox", { name: "Profile name" })).toHaveValue("Draft aio");
  });

  it("loads profiles, distinguishes empty and unavailable stores, and searches safe summaries", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    const view = render(<StreamSkopeWorkbench host={host} />);

    expect(screen.getByRole("status", { name: "Profile list status" })).toHaveTextContent(
      "Loading profiles",
    );
    expect(screen.getByRole("button", { name: "Connection Profiles" })).toBeVisible();
    await waitFor(() => {
      expect(host.commands[0]).toMatchObject({
        command: "profiles.list",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
    });

    publishProfiles(host, []);
    expect(screen.getByText("Add a connection profile to connect to Kafka.")).toBeVisible();
    expect(screen.getByRole("button", { name: "Add profile" })).toBeEnabled();
    expect(screen.getByRole("status", { name: "Profile storage status" })).toHaveTextContent(
      "Session-only",
    );

    publishProfiles(
      host,
      [
        profile(),
        {
          active: false,
          brokers: ["broker.example.test:9094"],
          createdAt: "2026-07-25T10:00:00.000Z",
          id: "profile-remote",
          name: "Remote staging",
          trust: {
            kind: "pem",
            label: "staging-ca.pem",
            materialPresent: true,
            passwordPresent: false,
          },
          updatedAt: "2026-07-25T10:00:00.000Z",
        },
      ],
      sessionStore,
      2,
    );
    const list = screen.getByRole("list", { name: "Kafka connection profiles" });
    expect(within(list).getByText("Local aio")).toBeVisible();
    expect(within(list).getByText("Remote staging")).toBeVisible();
    expect(
      within(screen.getByRole("button", { name: "Connection Profiles" })).queryByText("2"),
    ).not.toBeInTheDocument();
    expect(list).not.toHaveTextContent("kafka.truststore.jks");
    expect(list).not.toHaveTextContent("staging-ca.pem");

    const resourceFilter = screen.getByRole("searchbox", { name: "Search profiles" });
    await user.type(resourceFilter, "9094");
    expect(within(list).queryByText("Local aio")).not.toBeInTheDocument();
    expect(within(list).getByText("Remote staging")).toBeVisible();
    expect(
      within(screen.getByRole("button", { name: "Connection Profiles" })).queryByText("2"),
    ).not.toBeInTheDocument();
    await user.clear(resourceFilter);
    await user.type(resourceFilter, "missing");
    expect(screen.getByText('No profiles match "missing".')).toBeVisible();
    await user.clear(resourceFilter);
    expect(
      within(screen.getByRole("list", { name: "Kafka connection profiles" })).getByText(
        "Local aio",
      ),
    ).toBeVisible();
    publishProfiles(
      host,
      [profile()],
      {
        durability: "durable",
        protection: "os-protected",
        state: "ready",
      },
      3,
    );
    expect(screen.getByRole("status", { name: "Profile storage status" })).toHaveTextContent(
      "OS-protected",
    );

    view.unmount();
    const unavailableHost = new FakeHost();
    render(<StreamSkopeWorkbench host={unavailableHost} />);
    publishProfiles(unavailableHost, [], {
      durability: "durable",
      protection: "unavailable",
      recovery: "Unlock the operating-system credential store, then restart StreamSkope.",
      state: "unavailable",
    });
    expect(screen.getByRole("alert")).toHaveTextContent("Profile storage unavailable");
    expect(screen.getByRole("alert")).toHaveTextContent(
      "Unlock the operating-system credential store, then restart StreamSkope.",
    );
    expect(screen.getByRole("button", { name: "Add profile" })).toBeDisabled();
    expect(
      screen.queryByRole("button", { name: "Configure ad hoc connection" }),
    ).not.toBeInTheDocument();
  });

  it("creates a PEM/OAuth profile without retrieving or retaining renderer secrets", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    publishProfiles(host, []);

    await user.click(screen.getByRole("button", { name: "Add profile" }));
    const dialog = screen.getByRole("dialog", { name: "Add Kafka profile" });
    await user.type(within(dialog).getByRole("textbox", { name: "Profile name" }), "Local aio");
    await pasteText(
      user,
      within(dialog).getByRole("textbox", { name: "Bootstrap brokers" }),
      "127.0.0.1:9093",
    );
    await user.click(within(dialog).getByRole("combobox", { name: "Trust material format" }));
    await user.click(screen.getByRole("option", { name: "PEM certificate" }));
    await user.upload(
      within(dialog).getByLabelText("Trust material file"),
      new File(["-----BEGIN CERTIFICATE-----\nfixture-ca\n-----END CERTIFICATE-----"], "ca.pem", {
        type: "application/x-pem-file",
      }),
    );
    await user.click(within(dialog).getByRole("switch", { name: "Use OAuth OAUTHBEARER" }));
    await pasteText(
      user,
      within(dialog).getByRole("textbox", { name: "OAuth token endpoint" }),
      "http://127.0.0.1:5000/rest-gateway/rest/api/v1/auth/token",
    );
    await user.type(within(dialog).getByRole("textbox", { name: "OAuth client ID" }), "admin");
    await user.type(within(dialog).getByRole("textbox", { name: "OAuth scope" }), "kafka");
    await user.type(within(dialog).getByLabelText("OAuth client secret"), "NokiaNsp@");

    const secret = within(dialog).getByLabelText("OAuth client secret");
    expect(secret).toHaveAttribute("type", "password");
    await user.click(within(dialog).getByRole("button", { name: "Show OAuth client secret" }));
    expect(secret).toHaveAttribute("type", "text");
    expect(secret).toHaveValue("NokiaNsp@");
    await user.click(within(dialog).getByRole("button", { name: "Hide OAuth client secret" }));

    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));
    await waitFor(() => {
      expect(host.commands.at(-1)).toMatchObject({
        command: "profiles.test",
        payload: {
          mode: "create",
          profile: {
            brokers: ["127.0.0.1:9093"],
            name: "Local aio",
            oauth: {
              clientId: "admin",
              clientSecret: { mode: "replace", value: "NokiaNsp@" },
              scope: "kafka",
              tokenEndpoint: "http://127.0.0.1:5000/rest-gateway/rest/api/v1/auth/token",
            },
            trust: {
              kind: "pem",
              label: "ca.pem",
              material: {
                mode: "replace",
                value: "-----BEGIN CERTIFICATE-----\nfixture-ca\n-----END CERTIFICATE-----",
              },
              password: { mode: "clear" },
            },
          },
        },
        version: HOST_PROTOCOL_VERSION,
      });
    });
    expect(within(dialog).getByRole("status")).toHaveTextContent(
      "Connection test passed. No profile was saved and the active connection was unchanged.",
    );
    expect(screen.getByRole("dialog", { name: "Add Kafka profile" })).toBeVisible();
    expect(host.commands.filter((command) => command.command === "profiles.create")).toHaveLength(
      0,
    );

    await user.click(within(dialog).getByRole("button", { name: "Save profile" }));
    await waitFor(() => {
      expect(host.commands.at(-1)).toMatchObject({
        command: "profiles.create",
        payload: {
          profile: {
            brokers: ["127.0.0.1:9093"],
            name: "Local aio",
            oauth: {
              clientId: "admin",
              clientSecret: { mode: "replace", value: "NokiaNsp@" },
              scope: "kafka",
              tokenEndpoint: "http://127.0.0.1:5000/rest-gateway/rest/api/v1/auth/token",
            },
            trust: {
              kind: "pem",
              label: "ca.pem",
              material: {
                mode: "replace",
                value: "-----BEGIN CERTIFICATE-----\nfixture-ca\n-----END CERTIFICATE-----",
              },
              password: { mode: "clear" },
            },
          },
        },
        version: HOST_PROTOCOL_VERSION,
      });
    });
    expect(screen.queryByRole("dialog", { name: "Add Kafka profile" })).not.toBeInTheDocument();
  });

  it("edits safe metadata with explicit protected-value retention and connects or disconnects", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    publishProfiles(host, [
      profile({
        services: {
          redpandaAdmin: { authentication: "oauth", baseUrl: "https://admin.local:9644" },
          schemaRegistry: { authentication: "none", baseUrl: "https://schema.local:8081" },
        },
      }),
    ]);

    await openProfileAction(user, "Edit");
    const dialog = screen.getByRole("dialog", { name: "Edit Kafka profile Local aio" });
    expect(within(dialog).queryByDisplayValue("NokiaNsp@")).not.toBeInTheDocument();
    expect(within(dialog).getByText("Saved client secret will be retained.")).toBeVisible();
    expect(within(dialog).getByText("Saved trust material will be retained.")).toBeVisible();
    expect(within(dialog).getByRole("textbox", { name: "Schema Registry URL" })).toHaveValue(
      "https://schema.local:8081",
    );
    expect(within(dialog).getByRole("textbox", { name: "Redpanda Admin URL" })).toHaveValue(
      "https://admin.local:9644",
    );
    await user.clear(within(dialog).getByRole("textbox", { name: "Bootstrap brokers" }));
    await pasteText(
      user,
      within(dialog).getByRole("textbox", { name: "Bootstrap brokers" }),
      "127.0.0.1:19093",
    );
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));
    await waitFor(() => {
      expect(host.commands.at(-1)).toMatchObject({
        command: "profiles.test",
        payload: {
          mode: "update",
          profile: {
            brokers: ["127.0.0.1:19093"],
            name: "Local aio",
            oauth: {
              clientId: "admin",
              clientSecret: { mode: "retain" },
              scope: "kafka",
              tokenEndpoint: "http://127.0.0.1:5000/rest-gateway/rest/api/v1/auth/token",
            },
            services: {
              redpandaAdmin: {
                authentication: "oauth",
                baseUrl: "https://admin.local:9644",
              },
              schemaRegistry: {
                authentication: "none",
                baseUrl: "https://schema.local:8081",
              },
            },
            trust: {
              kind: "pkcs12",
              label: "kafka.truststore.jks",
              material: { mode: "retain" },
              password: { mode: "retain" },
            },
          },
          profileId: "profile-local",
        },
      });
    });
    expect(within(dialog).getByRole("status")).toHaveTextContent("Connection test passed");

    await user.click(within(dialog).getByRole("button", { name: "Update profile" }));

    expect(host.commands.at(-1)).toMatchObject({
      command: "profiles.update",
      payload: {
        profile: {
          brokers: ["127.0.0.1:19093"],
          name: "Local aio",
          oauth: {
            clientId: "admin",
            clientSecret: { mode: "retain" },
            scope: "kafka",
            tokenEndpoint: "http://127.0.0.1:5000/rest-gateway/rest/api/v1/auth/token",
          },
          services: {
            redpandaAdmin: {
              authentication: "oauth",
              baseUrl: "https://admin.local:9644",
            },
            schemaRegistry: {
              authentication: "none",
              baseUrl: "https://schema.local:8081",
            },
          },
          trust: {
            kind: "pkcs12",
            label: "kafka.truststore.jks",
            material: { mode: "retain" },
            password: { mode: "retain" },
          },
        },
        profileId: "profile-local",
      },
      version: HOST_PROTOCOL_VERSION,
    });

    const resources = screen.getByRole("main", { name: "Connection profiles page" });
    const profileWorkspace = screen.getByRole("region", {
      name: "Connection profile workspace",
    });
    expect(
      within(profileWorkspace).queryByRole("button", { name: "Connect profile Local aio" }),
    ).not.toBeInTheDocument();
    const connectAction = within(resources).getByRole("button", {
      name: "Connect profile Local aio",
    });
    expect(connectAction).toHaveTextContent("Connect");
    expect(connectAction).toHaveClass("MuiButton-contained", "MuiButton-colorPrimary");
    const actions = within(resources).getByRole("group", { name: "Actions for profile Local aio" });
    expect(within(actions).getByRole("button", { name: "Connect profile Local aio" })).toBe(
      connectAction,
    );
    expect(
      within(actions).getByRole("button", { name: "More actions for profile Local aio" }),
    ).toBeEnabled();
    await user.click(connectAction);
    await waitFor(() => {
      expect(host.commands.at(-1)).toMatchObject({
        command: "profiles.connect",
        payload: { profileId: "profile-local" },
        version: HOST_PROTOCOL_VERSION,
      });
    });

    act(() => {
      host.emit({
        event: "connection.state",
        payload: {
          connectionName: "Local aio",
          state: "connected",
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    publishProfiles(host, [profile({ active: true })], sessionStore, 3);
    await waitFor(() => {
      expect(screen.getByRole("main", { name: "Topics page" })).toBeVisible();
    });
    await user.click(screen.getByRole("button", { name: "Connection Profiles" }));
    const connectedResources = screen.getByRole("main", { name: "Connection profiles page" });
    const connectedProfileWorkspace = screen.getByRole("region", {
      name: "Connection profile workspace",
    });
    const disconnectAction = within(connectedResources).getByRole("button", {
      name: "Disconnect profile Local aio",
    });
    expect(disconnectAction).toBeEnabled();
    expect(disconnectAction).toHaveTextContent("Disconnect");
    expect(disconnectAction).toHaveClass("MuiButton-contained", "MuiButton-colorInherit");
    expect(
      within(connectedResources).getByRole("group", { name: "Actions for profile Local aio" }),
    ).toContainElement(disconnectAction);
    expect(
      within(connectedProfileWorkspace).queryByRole("button", {
        name: "Disconnect profile Local aio",
      }),
    ).not.toBeInTheDocument();
    await user.click(
      within(connectedResources).getByRole("button", {
        name: "More actions for profile Local aio",
      }),
    );
    const menu = screen.getByRole("menu", { name: "Profile actions for Local aio" });
    expect(within(menu).getByRole("menuitem", { name: "Cluster detail" })).toBeEnabled();
    expect(within(menu).getByRole("menuitem", { name: "Edit" })).toHaveAttribute(
      "aria-disabled",
      "true",
    );
    expect(within(menu).getByRole("menuitem", { name: "Delete" })).toHaveAttribute(
      "aria-disabled",
      "true",
    );

    await user.click(within(menu).getByRole("menuitem", { name: "Cluster detail" }));
    await waitFor(
      () => {
        expect(host.commands.at(-1)).toMatchObject({
          command: "clusterDetails.load",
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        });
      },
      { timeout: 5_000 },
    );
    act(() => {
      host.emit({
        event: "clusterDetails.changed",
        payload: {
          cluster: null,
          endpoint: "127.0.0.1:9093",
          fetchedAt: null,
          profile: {
            brokers: ["127.0.0.1:9093"],
            id: "profile-local",
            name: "Local aio",
          },
          state: "loading",
        },
        sequence: 4,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    expect(screen.getByRole("dialog", { name: "Cluster details — Local aio" })).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Close" }));

    await user.click(
      within(connectedResources).getByRole("button", { name: "Disconnect profile Local aio" }),
    );
    await waitFor(() => {
      expect(host.commands.at(-1)).toMatchObject({
        command: "connection.disconnect",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
    });
  });

  it("imports a bounded binary truststore and independently reveals its password", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    publishProfiles(host, []);

    await user.click(screen.getByRole("button", { name: "Add profile" }));
    const dialog = screen.getByRole("dialog", { name: "Add Kafka profile" });
    await user.type(within(dialog).getByRole("textbox", { name: "Profile name" }), "Binary trust");
    await pasteText(
      user,
      within(dialog).getByRole("textbox", { name: "Bootstrap brokers" }),
      "broker.example.test:9093",
    );
    await user.click(within(dialog).getByRole("combobox", { name: "Trust material format" }));
    await user.click(screen.getByRole("option", { name: "JKS truststore" }));
    expect(
      within(dialog).queryByRole("textbox", { name: "Trust material label" }),
    ).not.toBeInTheDocument();
    await user.upload(
      within(dialog).getByLabelText("Trust material file"),
      new File([new Uint8Array([0xfe, 0xed, 0xfe, 0xed])], "truststore.jks", {
        type: "application/octet-stream",
      }),
    );
    await user.type(within(dialog).getByLabelText("Truststore password"), "changeit");

    const password = within(dialog).getByLabelText("Truststore password");
    expect(password).toHaveAttribute("type", "password");
    await user.click(within(dialog).getByRole("button", { name: "Show truststore password" }));
    expect(password).toHaveAttribute("type", "text");
    expect(password).toHaveValue("changeit");

    await user.click(within(dialog).getByRole("button", { name: "Save profile" }));
    await waitFor(() => {
      expect(host.commands.at(-1)).toMatchObject({
        command: "profiles.create",
        payload: {
          profile: {
            trust: {
              kind: "jks",
              label: "truststore.jks",
              material: { mode: "replace", value: "/u3+7Q==" },
              password: { mode: "replace", value: "changeit" },
            },
          },
        },
      });
    });
  });

  it("requires object-specific deletion confirmation and keeps failed mutations actionable", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    publishProfiles(host, [profile()]);

    await openProfileAction(user, "Delete");
    let dialog = screen.getByRole("dialog", { name: "Delete Kafka profile Local aio" });
    expect(dialog).toHaveTextContent("Local aio");
    expect(dialog).toHaveTextContent("127.0.0.1:9093");
    expect(dialog).toHaveTextContent("stored credentials");
    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(host.commands.filter((command) => command.command === "profiles.delete")).toHaveLength(
      0,
    );

    await openProfileAction(user, "Delete");
    dialog = screen.getByRole("dialog", { name: "Delete Kafka profile Local aio" });
    host.failNext({
      recovery: "Check profile storage permissions, then retry.",
      summary: "The profile remains stored because the delete commit failed.",
    });
    await user.click(within(dialog).getByRole("button", { name: "Delete profile" }));
    expect(host.commands.at(-1)).toMatchObject({
      command: "profiles.delete",
      payload: { profileId: "profile-local" },
      version: HOST_PROTOCOL_VERSION,
    });
    expect(await within(dialog).findByRole("alert")).toHaveTextContent(
      "The profile remains stored because the delete commit failed.",
    );
    expect(within(dialog).getByRole("alert")).toHaveTextContent(
      "Check profile storage permissions, then retry.",
    );
  });
});
