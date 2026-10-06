// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";

import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommandResponse,
  type HostCommand,
  type HostCommandResponse,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { ProfileDialog } from "../../src/features/kafka/ui/ProfileDialog";
import { ProfilePanel } from "../../src/features/kafka/ui/ProfilePanel";
import { testHostExecute, testHostResponse } from "../support/host-response";

afterEach(cleanup);

function fixture(fail?: string): { host: StreamSkopeHost; commands: HostCommand[] } {
  const commands: HostCommand[] = [];
  return {
    commands,
    host: {
      openExternalUrl: () =>
        Promise.reject(new Error("External navigation is not part of this fixture.")),
      subscribe: () => () => undefined,
      execute: testHostExecute((command): Promise<HostCommandResponse> => {
        commands.push(command);
        return Promise.resolve(
          command.command === fail
            ? {
                command: command.command,
                id: command.id,
                ok: false,
                version: HOST_PROTOCOL_VERSION,
                error: {
                  code: "BACKEND_UNAVAILABLE",
                  stage: "backend",
                  summary: "Fixture unavailable",
                  recovery: "Retry this operation.",
                  retryable: true,
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
                  ...(command.command === "profiles.create" ? { profileId: "saved-id" } : {}),
                },
              }),
        );
      }),
    },
  };
}

describe("connection setup", () => {
  it("offers a Kafka connection without an installed plugin", async () => {
    const { host } = fixture();
    const user = userEvent.setup();
    render(
      <ProfilePanel
        host={host}
        activityOpen={false}
        connected={false}
        connectionOperation={null}
        filter=""
        loading={false}
        onFilterChange={() => undefined}
        onOpenActivity={() => undefined}
        onProfileAction={() => undefined}
        onSelectProfile={() => undefined}
        onToggleConnection={() => undefined}
        profiles={[]}
        selectedProfileId={null}
        store={{ state: "ready", durability: "session", protection: "memory" }}
      />,
    );
    expect(screen.queryByRole("button", { name: "Connect via EDA" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Add connection" }));
    expect(screen.queryByRole("menuitem", { name: "Connect via EDA" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("menuitem", { name: "Existing Kafka cluster" }));
    expect(screen.getByRole("dialog", { name: "Add Kafka profile" })).toBeVisible();
  });

  it("preserves the saved identity through response validation", () => {
    expect(
      parseHostCommandResponse({
        command: "profiles.create",
        id: "save",
        ok: true,
        result: { correlationId: "save", profileId: "saved-id" },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({ result: { profileId: "saved-id" } });
  });

  it.each([undefined, "profiles.test", "profiles.create", "profiles.connect"])(
    "tests, saves and connects with bounded recovery when %s fails",
    async (fail) => {
      const { host, commands } = fixture(fail);
      const user = userEvent.setup();
      const close = vi.fn();
      render(<ProfileDialog host={host} onClose={close} onOpenActivity={() => undefined} open />);
      await user.type(screen.getByLabelText(/Profile name/u), "A connection");
      await user.type(screen.getByLabelText(/Bootstrap brokers/u), "broker.example.test:9092");
      await user.click(screen.getByLabelText("Plaintext (insecure)"));
      await user.click(screen.getByRole("button", { name: "Save and connect" }));
      const expected =
        fail === "profiles.test"
          ? ["profiles.test"]
          : fail === "profiles.create"
            ? ["profiles.test", "profiles.create"]
            : ["profiles.test", "profiles.create", "profiles.connect"];
      expect(commands.map((command) => command.command)).toEqual(expected);
      if (fail === undefined || fail === "profiles.connect") {
        expect(commands.at(-1)?.payload).toEqual({ profileId: "saved-id" });
      }
      if (fail === undefined) expect(close).toHaveBeenCalledOnce();
      else expect(close).not.toHaveBeenCalled();
      if (fail === "profiles.connect") {
        await user.click(screen.getByRole("button", { name: "Retry connection" }));
        expect(commands.filter((command) => command.command === "profiles.create")).toHaveLength(1);
        expect(commands.at(-1)?.command).toBe("profiles.connect");
      }
    },
  );
});
