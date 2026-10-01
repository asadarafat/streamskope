// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { StreamSkopeApp as StreamSkopeWorkbench } from "../../src/features/kafka/ui/StreamSkopeApp";
import { testHostAccepted } from "../support/host-response";

class FakeHost implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  private readonly listeners = new Set<HostEventListener>();

  emit(event: HostEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  execute<Command extends HostCommand>(
    command: Command,
  ): Promise<HostCommandResponse<Command["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    return Promise.resolve(testHostAccepted(command, `correlation-${command.id}`));
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

function publishProfilesReady(host: FakeHost, sequence = 2): void {
  act(() => {
    host.emit({
      event: "profiles.changed",
      payload: {
        profiles: [],
        store: {
          durability: "session",
          protection: "memory",
          state: "ready",
        },
      },
      sequence,
      version: HOST_PROTOCOL_VERSION,
    });
  });
}

afterEach(() => {
  cleanup();
});

describe("Material UI connection-template workflow", () => {
  it("opens the recipe manager without losing the parent profile draft", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    publishProfilesReady(host);
    await user.click(screen.getByRole("button", { name: "Add connection" }));
    await user.click(screen.getByRole("menuitem", { name: "Existing Kafka cluster" }));
    await user.type(screen.getByLabelText(/Profile name/u), "Retained draft");
    expect(
      screen.queryByRole("button", { name: "Manage connection templates" }),
    ).not.toBeInTheDocument();
    await user.click(screen.getByRole("switch", { name: "Use OAuth OAUTHBEARER" }));
    expect(
      screen.queryByRole("button", { name: "Choose OAuth endpoint template" }),
    ).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Retrieve certificates and credentials" }));
    await user.click(screen.getByRole("button", { name: "Manage retrieval presets" }));
    await screen.findByRole("dialog", { name: "Retrieval presets" });
    expect(host.commands.some((command) => command.command === "recipes.list")).toBe(true);
    await user.click(screen.getByRole("button", { name: "Close retrieval presets" }));
    await waitFor(() =>
      expect(screen.getByLabelText(/Profile name/u)).toHaveValue("Retained draft"),
    );
  });
});
