// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";

import { HOST_PROTOCOL_VERSION, type ProfileCreateInput } from "../../src/features/kafka/contracts";
import { StreamSkopeApp } from "../../src/features/kafka/ui/StreamSkopeApp";
import { testHostResponse, edaDesktopHost } from "../support/eda-ui-host";
import type {
  EdaUiCommand as HostCommand,
  EdaUiResponse as HostCommandResponse,
  EdaUiEvent as HostEvent,
  EdaUiEventListener as HostEventListener,
  EdaUiHost as StreamSkopeHost,
} from "../../plugins/eda/ui/host";
import edaRenderer from "../../plugins/eda/ui/renderer";
const pluginImporter = (): Promise<{ default: typeof edaRenderer }> =>
  Promise.resolve({ default: edaRenderer });

class CaptureHost implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  readonly createdProfiles: ProfileCreateInput[] = [];
  private readonly listeners = new Set<HostEventListener>();

  constructor(private readonly sources = ["existing-export"]) {}

  execute<Command extends HostCommand>(
    command: Command,
  ): Promise<HostCommandResponse<Command["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    if (command.command === "edaCapture.application.status")
      return Promise.resolve(
        testHostResponse(command, {
          command: command.command,
          id: command.id,
          ok: true,
          version: HOST_PROTOCOL_VERSION,
          result: {
            correlationId: command.id,
            application: {
              appId: "capture.streamskope.io",
              publisher: "StreamSkope",
              state: "installed",
              version: "v26.8.2",
            },
          },
        }),
      );
    if (command.command === "edaCapture.preflight")
      return Promise.resolve(
        testHostResponse(command, {
          command: command.command,
          id: command.id,
          ok: true,
          version: HOST_PROTOCOL_VERSION,
          result: {
            correlationId: command.id,
            captureHost: {
              state: "configured",
              context: "kind-eda",
              detail: "Explicit Kubernetes capture host configured.",
            },
          },
        }),
      );
    if (command.command === "edaCapture.status")
      return Promise.resolve(
        testHostResponse(command, {
          command: command.command,
          id: command.id,
          ok: true,
          version: HOST_PROTOCOL_VERSION,
          result: {
            correlationId: command.id,
            captureSession: {
              state: "idle",
              tunnel: "closed",
              detail: "No active fixture tunnel.",
            },
          },
        }),
      );
    if (command.command === "edaCapture.inspect") {
      const sourcePrefix = command.payload.edaApi.baseUrl.includes("eda-two") ? "changed-" : "";
      return Promise.resolve(
        testHostResponse(command, {
          command: command.command,
          id: command.id,
          ok: true,
          result: {
            correlationId: "capture-inspection",
            inspection: {
              context: "kind-eda",
              contexts: ["kind-eda"],
              edaApiUrl: command.payload.edaApi?.baseUrl ?? "https://eda.example.test:9443",
              imageSetup: {
                image: "docker.redpanda.example/redpanda:v1",
                imageDelivery: "embedded",
                registryHost: "bridge.example.test",
                registryPort: 5_443,
                state: "configured",
              },
              namespace: "eda-system",
              sources: this.sources.map((name) => ({
                apiVersion: "kafka.eda.nokia.com/v1",
                kind: "ClusterProducer",
                name: `${sourcePrefix}${name}`,
                namespace: "eda-system",
                topics: [name === "audit-export" ? "audit" : "interfaces"],
              })),
            },
          },
          version: HOST_PROTOCOL_VERSION,
        }),
      );
    }
    if (command.command === "edaCapture.deploy") {
      this.emit({
        event: "edaCapture.progress",
        payload: {
          detail:
            "EDA capture broker, exporter, and local tunnel are ready. Configured topics appear as EDA emits matching data.",
          phase: "ready",
          requestId: command.id,
        },
        sequence: 50,
        version: HOST_PROTOCOL_VERSION,
      });
      const sourceName = command.payload.source.name;
      return Promise.resolve(
        testHostResponse(command, {
          command: command.command,
          id: command.id,
          ok: true,
          result: {
            correlationId: "capture-deployment",
            deployment: {
              sessionId: "fixture-session",
              broker: "127.0.0.1:19092",
              clusterBroker: "streamskope-redpanda.eda-system.svc:9092",
              context: "kind-eda",
              exporterName: "streamskope-capture",
              namespace: "eda-system",
              profileName: `EDA capture · ${sourceName}`,
              topics: [sourceName === "audit-export" ? "audit" : "interfaces"],
              workloadName: "streamskope-redpanda",
            },
          },
          version: HOST_PROTOCOL_VERSION,
        }),
      );
    }
    if (command.command === "profiles.create") {
      this.createdProfiles.push(command.payload.profile);
      this.emit({
        event: "profiles.changed",
        payload: {
          profiles: [
            {
              active: false,
              brokers: [...command.payload.profile.brokers],
              createdAt: "2026-09-19T10:00:00.000Z",
              id: "captured-profile",
              name: command.payload.profile.name,
              ...(command.payload.profile.source === undefined
                ? {}
                : { source: command.payload.profile.source }),
              transport: "plaintext",
              updatedAt: "2026-09-19T10:00:00.000Z",
            },
          ],
          store: { durability: "session", protection: "memory", state: "ready" },
        },
        sequence: 51,
        version: HOST_PROTOCOL_VERSION,
      });
    }
    return Promise.resolve(
      testHostResponse(command, {
        command: command.command,
        id: command.id,
        ok: true,
        result: {
          correlationId: `correlation-${command.id}`,
          ...(command.command === "profiles.create" ? { profileId: "captured-profile" } : {}),
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    );
  }

  emit(event: HostEvent): void {
    for (const listener of this.listeners) listener(event);
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

describe("EDA capture workflow", () => {
  it("discloses the configured Kubernetes host and reviews remote changes before capture", async () => {
    const host = new CaptureHost();
    const user = userEvent.setup();
    render(<StreamSkopeApp host={edaDesktopHost(host)} pluginImporter={pluginImporter} />);
    act(() => {
      host.emit({
        event: "profiles.changed",
        payload: {
          profiles: [],
          store: { durability: "session", protection: "memory", state: "ready" },
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
    });

    await user.click(screen.getByRole("button", { name: "Add connection" }));
    await user.click(await screen.findByRole("menuitem", { name: "Connect via EDA" }));
    const dialog = screen.getByRole("dialog", { name: "Connect via EDA" });
    expect(within(dialog).getByLabelText("EDA API URL")).toHaveValue("");
    expect(within(dialog).getByLabelText("EDA username")).toBeVisible();
    expect(within(dialog).getByLabelText("EDA password")).toBeVisible();
    expect(within(dialog).getByLabelText("Verify EDA API certificate")).toBeChecked();
    expect(within(dialog).queryByLabelText("Local Kafka port")).not.toBeInTheDocument();
    expect(within(dialog).queryByLabelText("Kube context")).not.toBeInTheDocument();
    expect(within(dialog).queryByLabelText("EDA namespace")).not.toBeInTheDocument();
    expect(within(dialog).queryByLabelText("Source Redpanda image")).not.toBeInTheDocument();
    expect(
      within(dialog).queryByLabelText("Registry host advertised to nodes"),
    ).not.toBeInTheDocument();
    expect(
      within(dialog).queryByText(/already be trusted by every cluster node/u),
    ).not.toBeInTheDocument();
    expect(dialog.querySelectorAll('input[type="file"]')).toHaveLength(0);

    await user.type(within(dialog).getByLabelText("EDA API URL"), "https://eda.example.test:9443");
    await user.type(within(dialog).getByLabelText("EDA username"), "admin");
    await user.type(within(dialog).getByLabelText("EDA password"), "password");
    await user.click(within(dialog).getByLabelText("Verify EDA API certificate"));
    expect(
      within(dialog).getByText(
        /Certificate verification is disabled for this capture request only/u,
      ),
    ).toBeVisible();
    await user.click(within(dialog).getByRole("button", { name: "Discover sources" }));
    expect(host.commands.some((command) => command.command === "edaCapture.deploy")).toBe(false);
    await user.click(within(dialog).getByRole("button", { name: "Set up temporary capture" }));
    expect(within(dialog).getByLabelText("Local Kafka port")).toHaveValue(19_092);
    expect(within(dialog).getByText(/Starting capture creates a temporary broker/u)).toBeVisible();
    await waitFor(() =>
      expect(within(dialog).getByRole("button", { name: "Start capture" })).toBeEnabled(),
    );
    await user.click(within(dialog).getByRole("button", { name: "Start capture" }));

    await waitFor(() =>
      expect(screen.queryByRole("dialog", { name: "Connect via EDA" })).toBeNull(),
    );
    expect(
      screen.getByRole("button", {
        name: "Select profile EDA capture · existing-export",
      }),
    ).toBeVisible();
    expect(screen.getByText(/EDA API capture · existing-export/u)).toBeVisible();
    await user.click(
      screen.getByRole("button", {
        name: "More actions for profile EDA capture · existing-export",
      }),
    );
    await user.click(screen.getByRole("menuitem", { name: "Edit" }));
    const editor = screen.getByRole("dialog", {
      name: "Edit Kafka profile EDA capture · existing-export",
    });
    expect(within(editor).getByText(/managed by a plugin/u)).toBeVisible();
    expect(
      within(editor).getByRole("textbox", {
        name: "Plugin-managed endpoint",
      }),
    ).toHaveAttribute("readonly");
    await user.click(within(editor).getByRole("button", { name: "Cancel" }));
    const workflow = host.commands.filter(
      (command) =>
        command.command === "edaCapture.inspect" ||
        command.command === "edaCapture.deploy" ||
        command.command === "profiles.test" ||
        command.command === "profiles.create",
    );
    expect(workflow.map((command) => command.command)).toEqual([
      "edaCapture.inspect",
      "edaCapture.deploy",
      "profiles.test",
      "profiles.create",
    ]);
    expect(workflow[0]).toMatchObject({
      payload: {
        edaApi: {
          baseUrl: "https://eda.example.test:9443",
          password: "password",
          username: "admin",
          verifyTls: false,
        },
      },
    });
    expect(workflow[1]).toMatchObject({
      payload: {
        context: "kind-eda",
        edaApi: {
          baseUrl: "https://eda.example.test:9443",
          password: "password",
          username: "admin",
          verifyTls: false,
        },
        imageDelivery: "configured",
        localPort: 19_092,
        source: {
          kind: "ClusterProducer",
          name: "existing-export",
          namespace: "eda-system",
        },
      },
    });
    expect(workflow[2]).toMatchObject({
      payload: {
        profile: {
          brokers: ["127.0.0.1:19092"],
          name: "EDA capture · existing-export",
          source: {
            kind: "plugin",
            pluginId: "streamskope.eda",
            version: 1,
            data: {
              kind: "eda-capture",
              source: {
                kind: "ClusterProducer",
                name: "existing-export",
                namespace: "eda-system",
              },
              state: "ready",
            },
          },
          transport: "plaintext",
        },
      },
    });
  });

  it("lets the user choose between multiple EDA API sources", async () => {
    const host = new CaptureHost(["existing-export", "audit-export"]);
    const user = userEvent.setup();
    render(<StreamSkopeApp host={edaDesktopHost(host)} pluginImporter={pluginImporter} />);
    act(() => {
      host.emit({
        event: "profiles.changed",
        payload: {
          profiles: [],
          store: { durability: "session", protection: "memory", state: "ready" },
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
    });

    await user.click(screen.getByRole("button", { name: "Add connection" }));
    await user.click(await screen.findByRole("menuitem", { name: "Connect via EDA" }));
    const dialog = screen.getByRole("dialog", { name: "Connect via EDA" });
    await user.type(within(dialog).getByLabelText("EDA API URL"), "https://eda.example.test:9443");
    await user.type(within(dialog).getByLabelText("EDA username"), "admin");
    await user.type(within(dialog).getByLabelText("EDA password"), "password");
    await user.click(within(dialog).getByRole("button", { name: "Discover sources" }));
    await waitFor(() => expect(within(dialog).getByLabelText("Exporter source")).toBeVisible());
    await user.click(within(dialog).getByLabelText("Exporter source"));
    await user.click(
      screen.getByRole("option", { name: "ClusterProducer · audit-export · audit" }),
    );
    await user.click(within(dialog).getByRole("button", { name: "Set up temporary capture" }));
    await waitFor(() =>
      expect(within(dialog).getByRole("button", { name: "Start capture" })).toBeEnabled(),
    );
    await user.click(within(dialog).getByRole("button", { name: "Start capture" }));

    await waitFor(() =>
      expect(screen.queryByRole("dialog", { name: "Connect via EDA" })).toBeNull(),
    );
    expect(
      host.commands.filter((command) => command.command === "edaCapture.deploy").at(-1),
    ).toMatchObject({
      payload: {
        edaApi: { username: "admin" },
        imageDelivery: "configured",
        source: { name: "audit-export" },
      },
    });
  });

  it("invalidates discovered EDA sources when the API target changes", async () => {
    const host = new CaptureHost(["existing-export"]);
    const user = userEvent.setup();
    render(<StreamSkopeApp host={edaDesktopHost(host)} pluginImporter={pluginImporter} />);
    act(() => {
      host.emit({
        event: "profiles.changed",
        payload: {
          profiles: [],
          store: { durability: "session", protection: "memory", state: "ready" },
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
    });

    await user.click(screen.getByRole("button", { name: "Add connection" }));
    await user.click(await screen.findByRole("menuitem", { name: "Connect via EDA" }));
    const dialog = screen.getByRole("dialog", { name: "Connect via EDA" });
    const apiUrl = within(dialog).getByLabelText("EDA API URL");
    await user.type(apiUrl, "https://eda-one.example.test:9443");
    await user.type(within(dialog).getByLabelText("EDA username"), "admin");
    await user.type(within(dialog).getByLabelText("EDA password"), "password");
    await user.click(within(dialog).getByRole("button", { name: "Discover sources" }));
    await waitFor(() =>
      expect(within(dialog).getByText("Source: ClusterProducer · existing-export · interfaces")),
    );

    await user.clear(apiUrl);
    await user.type(apiUrl, "https://eda-two.example.test:9443");
    await user.click(within(dialog).getByRole("button", { name: "Discover sources" }));
    expect(host.commands.some((command) => command.command === "edaCapture.deploy")).toBe(false);
    await user.click(within(dialog).getByRole("button", { name: "Set up temporary capture" }));
    await waitFor(() =>
      expect(within(dialog).getByRole("button", { name: "Start capture" })).toBeEnabled(),
    );
    await user.click(within(dialog).getByRole("button", { name: "Start capture" }));

    await waitFor(() =>
      expect(
        host.commands.filter((command) => command.command === "edaCapture.inspect"),
      ).toHaveLength(2),
    );
    await waitFor(() =>
      expect(screen.queryByRole("dialog", { name: "Connect via EDA" })).toBeNull(),
    );
    expect(
      host.commands.filter((command) => command.command === "edaCapture.deploy").at(-1),
    ).toMatchObject({
      payload: {
        edaApi: { baseUrl: "https://eda-two.example.test:9443" },
        source: { name: "changed-existing-export" },
      },
    });
    expect(host.createdProfiles.at(-1)).toMatchObject({
      name: "EDA capture · changed-existing-export",
      source: {
        kind: "plugin",
        pluginId: "streamskope.eda",
        version: 1,
        data: { kind: "eda-capture", source: { name: "changed-existing-export" } },
      },
    });
  });
});
