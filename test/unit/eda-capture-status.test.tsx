// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import { HOST_PROTOCOL_VERSION, type ProfileEdaCaptureSource } from "../../plugins/eda/contracts";
import { EdaCaptureStatusPanel } from "../../plugins/eda/ui/EdaCaptureStatusPanel";
import { testHostExecute, testHostResponse } from "../support/eda-ui-host";
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
  context: "capture-host",
  edaApiUrl: "https://eda.example.test",
  sessionId: "old-session",
  source: {
    apiVersion: "kafka.eda.nokia.com/v1",
    kind: "Producer",
    namespace: "eda-system",
    name: "interfaces",
  },
  topics: [],
  exporterName: "streamskope-capture",
  workloadName: "streamskope-redpanda",
};

it("withdraws running evidence when a stop fails after closing the tunnel", async () => {
  const host: StreamSkopeHost = {
    openExternalUrl: () => Promise.reject(new Error("Not used")),
    subscribe: () => () => undefined,
    execute: testHostExecute((command) =>
      Promise.resolve(
        command.command === "edaCapture.stop"
          ? {
              command: command.command,
              id: command.id,
              ok: false,
              version: HOST_PROTOCOL_VERSION,
              error: {
                code: "AUTHORIZATION_DENIED",
                stage: "authorization",
                summary: "Exporter deletion denied after tunnel closed.",
                recovery: "Refresh status.",
                retryable: false,
                activeStateChanged: false,
                correlationId: command.id,
              },
            }
          : testHostResponse(command, {
              command: command.command,
              id: command.id,
              ok: true,
              version: HOST_PROTOCOL_VERSION,
              result: {
                correlationId: command.id,
                captureSession: {
                  state: "ready",
                  tunnel: "open",
                  source,
                  detail: "Capture running.",
                },
              },
            }),
      ),
    ),
  };
  render(<EdaCaptureStatusPanel host={host} source={source} />);
  expect(await screen.findByText("Running")).toBeVisible();
  await userEvent.setup().click(screen.getByRole("button", { name: "Stop capture" }));
  expect(screen.queryByText("Running")).not.toBeInTheDocument();
  expect(screen.queryByText("Open")).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Stop and remove capture" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Refresh status" })).toBeEnabled();
});

it("does not equate a saved ready snapshot with a running capture and confirms destructive removal", async () => {
  const commands: HostCommand[] = [];
  const host: StreamSkopeHost = {
    openExternalUrl: () =>
      Promise.reject(new Error("External navigation is not part of this fixture.")),
    subscribe: () => () => undefined,
    execute: testHostExecute((command) => {
      commands.push(command);
      return Promise.resolve(
        testHostResponse(command, {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: {
            correlationId: command.id,
            captureSession: {
              state: "idle",
              tunnel: "closed",
              detail: "No capture in this host session.",
            },
          },
        }),
      );
    }),
  };
  const resume = vi.fn();
  const user = userEvent.setup();
  render(<EdaCaptureStatusPanel host={host} source={source} onResume={resume} />);
  expect(await screen.findByText("Not running for this connection")).toBeVisible();
  await user.click(screen.getByRole("button", { name: "Resume capture" }));
  expect(resume).toHaveBeenCalledOnce();
  await user.click(screen.getByRole("button", { name: "Stop and remove capture" }));
  expect(commands.some((command) => command.command === "edaCapture.remove")).toBe(false);
  expect(screen.getByRole("dialog")).toHaveTextContent("eda-system/interfaces");
  await user.click(screen.getByRole("button", { name: "Remove capture resources" }));
  expect(commands.at(-1)).toMatchObject({ command: "edaCapture.remove", payload: { source } });
});
