// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import { HOST_PROTOCOL_VERSION, type ProfileEdaCaptureSource } from "../../plugins/eda/contracts";
import { EdaCaptureDialog } from "../../plugins/eda/ui/EdaCaptureDialog";
import { testHostExecute } from "../support/eda-ui-host";
import type {
  EdaUiCommand as HostCommand,
  EdaUiHost as StreamSkopeHost,
} from "../../plugins/eda/ui/host";

afterEach(cleanup);
const source: ProfileEdaCaptureSource = {
  kind: "eda-capture",
  state: "ready",
  broker: "127.0.0.1:19092",
  clusterBroker: "capture:9092",
  context: "explicit-host",
  edaApiUrl: "https://eda.example.test",
  sessionId: "capture-session",
  source: {
    apiVersion: "kafka.eda.nokia.com/v1",
    kind: "Producer",
    namespace: "eda-system",
    name: "interfaces",
  },
  topics: ["interfaces"],
  exporterName: "streamskope-capture",
  workloadName: "streamskope-redpanda",
};

function fixture(mode: "missing" | "deploy-failure" | "test-failure" | "save-failure"): {
  host: StreamSkopeHost;
  commands: HostCommand[];
} {
  const commands: HostCommand[] = [];
  return {
    commands,
    host: {
      subscribe: () => () => undefined,
      openExternalUrl: () =>
        Promise.reject(new Error("External navigation is not part of this fixture.")),
      execute: testHostExecute((command): Promise<unknown> => {
        commands.push(command);
        const base = { command: command.command, id: command.id, version: HOST_PROTOCOL_VERSION };
        const failedCommand =
          mode === "deploy-failure"
            ? "edaCapture.deploy"
            : mode === "test-failure"
              ? "profiles.test"
              : "profiles.create";
        if (command.command === failedCommand)
          return Promise.resolve({
            ...base,
            ok: false,
            error: {
              code: "BACKEND_UNAVAILABLE",
              stage: "backend",
              summary: "Fixture operation failed.",
              recovery: "Retry the failed operation.",
              retryable: true,
              activeStateChanged: false,
              correlationId: command.id,
            },
          });
        if (command.command === "edaCapture.preflight")
          return Promise.resolve({
            ...base,
            ok: true,
            result: {
              correlationId: command.id,
              captureHost: {
                state: "configured",
                context: source.context,
                detail: "Explicit Kubernetes host configured.",
              },
            },
          });
        if (command.command === "edaCapture.application.status")
          return Promise.resolve({
            ...base,
            ok: true,
            result: {
              correlationId: command.id,
              application: {
                appId: "capture.streamskope.io",
                publisher: "StreamSkope",
                state: "installed",
                version: "v26.8.2",
              },
            },
          });
        if (command.command === "edaCapture.inspect")
          return Promise.resolve({
            ...base,
            ok: true,
            result: {
              correlationId: command.id,
              inspection: {
                context: "explicit-host",
                contexts: ["explicit-host"],
                namespace: "eda-system",
                imageSetup: { state: "unconfigured" },
                sources: [
                  {
                    ...source.source,
                    name: mode === "missing" ? "another-source" : source.source.name,
                    topics: source.topics,
                  },
                ],
              },
            },
          });
        if (command.command === "edaCapture.status" || command.command === "edaCapture.remove")
          return Promise.resolve({
            ...base,
            ok: true,
            result: {
              correlationId: command.id,
              captureSession: {
                state: "failed",
                tunnel: "closed",
                source,
                detail: "Partial deployment remains.",
              },
            },
          });
        if (command.command === "edaCapture.deploy")
          return Promise.resolve({
            ...base,
            ok: true,
            result: {
              correlationId: command.id,
              deployment: {
                sessionId: "capture-session",
                broker: source.broker,
                clusterBroker: source.clusterBroker,
                context: "explicit-host",
                exporterName: source.exporterName,
                namespace: "eda-system",
                profileName: "Capture",
                topics: source.topics,
                workloadName: source.workloadName,
              },
            },
          });
        return Promise.resolve({
          ...base,
          ok: true,
          result: {
            correlationId: command.id,
            ...(command.command === "profiles.create" ? { profileId: "saved-capture" } : {}),
          },
        });
      }),
    },
  };
}

it.each(["missing", "deploy-failure", "test-failure", "save-failure"] as const)(
  "recovers safely from %s",
  async (mode) => {
    const { host, commands } = fixture(mode);
    const user = userEvent.setup();
    const saved = vi.fn();
    render(
      <EdaCaptureDialog
        host={host}
        open
        onClose={() => undefined}
        onProfileReady={saved}
        profiles={[]}
        {...(mode === "missing" ? { resume: source } : {})}
      />,
    );
    if (mode !== "missing")
      await user.type(screen.getByLabelText("EDA API URL"), "https://eda.example.test");
    await user.type(screen.getByLabelText("EDA username"), "admin");
    await user.type(screen.getByLabelText("EDA password"), "test-password");
    await user.click(screen.getByRole("button", { name: "Discover sources" }));
    if (mode !== "missing")
      await user.click(screen.getByRole("button", { name: "Set up temporary capture" }));
    await waitFor(() =>
      expect(
        screen.getByRole("button", {
          name: mode === "missing" ? "Resume capture" : "Start capture",
        }),
      ).toBeEnabled(),
    );
    await user.click(
      screen.getByRole("button", { name: mode === "missing" ? "Resume capture" : "Start capture" }),
    );
    expect(saved).not.toHaveBeenCalled();
    if (mode === "missing") {
      expect(screen.getByText(/saved capture source is no longer available/u)).toBeVisible();
      expect(commands.filter((command) => command.command === "edaCapture.deploy")).toHaveLength(0);
    } else if (mode === "deploy-failure") {
      expect(await screen.findByRole("button", { name: "Stop and remove capture" })).toBeEnabled();
    } else {
      await user.click(
        screen.getByRole("button", {
          name: mode === "test-failure" ? "Retry connection test" : "Retry saving profile",
        }),
      );
      expect(commands.filter((command) => command.command === "edaCapture.deploy")).toHaveLength(1);
      expect(commands.filter((command) => command.command === "profiles.test")).toHaveLength(
        mode === "test-failure" ? 2 : 1,
      );
    }
  },
);
