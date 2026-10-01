// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import { EdaCaptureDialog } from "../../plugins/eda/ui/EdaCaptureDialog";
import { EdaCaptureApplicationSetup } from "../../plugins/eda/ui/EdaCaptureApplicationSetup";
import { HOST_PROTOCOL_VERSION, EDA_CAPTURE_APPLICATION } from "../../plugins/eda/contracts";
import { testHostExecute, testHostResponse, testHostAccepted } from "../support/eda-ui-host";
import type {
  EdaUiCommand as HostCommand,
  EdaUiHost as StreamSkopeHost,
} from "../../plugins/eda/ui/host";

afterEach(cleanup);

it("installs the capture application with explicit consent and request-scoped administrator credentials", async () => {
  const commands: HostCommand[] = [];
  const installed = vi.fn();
  const host: StreamSkopeHost = {
    subscribe: () => () => undefined,
    openExternalUrl: () => Promise.reject(new Error("Unexpected navigation")),
    execute: testHostExecute((command) => {
      commands.push(command);
      if (command.command === "edaCapture.application.install")
        return Promise.resolve(
          testHostResponse(command, {
            command: command.command,
            id: command.id,
            version: HOST_PROTOCOL_VERSION,
            ok: true,
            result: {
              correlationId: command.id,
              application: {
                appId: "capture.streamskope.io",
                publisher: "StreamSkope",
                state: "installed",
                version: EDA_CAPTURE_APPLICATION.version,
              },
            },
          }),
        );
      return Promise.resolve(testHostAccepted(command, command.id));
    }),
  };
  const user = userEvent.setup();
  render(
    <EdaCaptureApplicationSetup
      edaApi={{
        baseUrl: "https://remote.example.test",
        password: "operator-secret",
        username: "operator",
      }}
      host={host}
      onInstalled={installed}
      requiresAdministrator
    />,
  );
  expect(screen.getByText(/register its public catalog/u)).toHaveTextContent(
    "https://github.com/asadarafat/streamskope.git",
  );
  await user.type(screen.getByLabelText("EDA administrator username"), "platform-admin");
  await user.type(screen.getByLabelText("EDA administrator password"), "one-time-secret");
  await user.click(screen.getByRole("button", { name: "Install and continue" }));
  expect(commands).toHaveLength(1);
  expect(commands[0]).toMatchObject({
    command: "edaCapture.application.install",
    payload: {
      authorization: { username: "platform-admin", password: "one-time-secret" },
      edaApi: { username: "operator", password: "operator-secret" },
    },
  });
  expect(installed).toHaveBeenCalledOnce();
  expect(screen.queryByDisplayValue("platform-admin")).not.toBeInTheDocument();
  expect(screen.queryByDisplayValue("one-time-secret")).not.toBeInTheDocument();
});

it.each([false, true])(
  "requires an entered endpoint and allows discovery with configured=%s",
  async (configured) => {
    const commands: HostCommand[] = [];
    const existingDestination = vi.fn();
    const host: StreamSkopeHost = {
      subscribe: () => () => undefined,
      openExternalUrl: () => Promise.reject(new Error("Unexpected external navigation")),
      execute: testHostExecute((command) => {
        commands.push(command);
        const base = {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true as const,
        };
        if (command.command === "edaCapture.preflight")
          return Promise.resolve({
            ...base,
            result: {
              correlationId: command.id,
              captureHost: configured
                ? {
                    state: "configured",
                    context: "explicit",
                    edaApiUrl: "https://configured.example.test",
                    detail: "Configured",
                  }
                : { state: "unavailable", detail: "Setup required" },
            },
          });
        if (command.command === "edaCapture.inspect")
          return Promise.resolve({
            ...base,
            result: {
              correlationId: command.id,
              inspection: {
                contexts: [],
                namespace: "eda-system",
                imageSetup: { state: "unconfigured" },
                sources: [
                  {
                    apiVersion: "kafka.eda.nokia.com/v1",
                    kind: "Producer",
                    name: "remote-source",
                    namespace: "eda-system",
                    topics: ["interfaces"],
                    brokers: ["remote-kafka.example.test:9093"],
                  },
                ],
              },
            },
          });
        return Promise.resolve({ ...base, result: { correlationId: command.id } });
      }),
    };
    const user = userEvent.setup();
    render(
      <EdaCaptureDialog
        host={host}
        open
        profiles={[]}
        onClose={() => undefined}
        onProfileReady={() => undefined}
        onExistingDestination={existingDestination}
      />,
    );
    expect(screen.queryByText("Capture deployment setup")).not.toBeInTheDocument();
    expect(screen.getByLabelText("EDA API URL")).toHaveValue("");
    expect(screen.getByLabelText("EDA API URL")).toBeEnabled();
    await user.type(screen.getByLabelText("EDA API URL"), "https://remote.example.test");
    await user.type(screen.getByLabelText("EDA username"), "operator");
    await user.type(screen.getByLabelText("EDA password"), "fixture");
    await user.click(screen.getByRole("button", { name: "Discover sources" }));
    await screen.findByText(/Source: Producer · remote-source/u);
    expect(commands.find((command) => command.command === "edaCapture.inspect")).toMatchObject({
      payload: { edaApi: { baseUrl: "https://remote.example.test" } },
    });
    await user.click(screen.getByRole("button", { name: "Connect to existing Kafka" }));
    expect(existingDestination).toHaveBeenCalledWith({
      name: "EDA · remote-source",
      brokers: ["remote-kafka.example.test:9093"],
    });
    await user.click(screen.getByRole("button", { name: "Set up temporary capture" }));
    expect(screen.getByRole("button", { name: "Start capture" })).toBeDisabled();
    expect(commands.some((command) => command.command === "edaCapture.deploy")).toBe(false);
    expect(screen.queryByText(/Kubernetes/u)).not.toBeInTheDocument();
    expect(screen.queryByText(/STREAMSKOPE_EDA_CAPTURE_KUBECONFIG/u)).not.toBeInTheDocument();
  },
);
