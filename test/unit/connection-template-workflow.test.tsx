// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type ConnectionTemplateSnapshot,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type StreamSkopeHost,
} from "../../src/kafka/contracts";
import { StreamSkopeApp as StreamSkopeWorkbench } from "../../src/app/StreamSkopeApp";

const sessionSnapshot: ConnectionTemplateSnapshot = {
  catalogs: [
    {
      catalog: "truststore-fetch",
      entries: [
        {
          name: "nsp-25-4",
          template: "copy source {truststorePath} using {storepass}",
        },
        {
          name: "nsp-25-11",
          template: "copy replacement {truststorePath}",
        },
      ],
      selectedName: "nsp-25-4",
    },
    {
      catalog: "truststore-password",
      entries: [{ name: "nsp-25-11", template: "read truststore password" }],
      selectedName: "nsp-25-11",
    },
    {
      catalog: "oauth-endpoint",
      entries: [
        {
          name: "nsp-25-4",
          template: "https://{kafka-cluseter-server}/rest-gateway/token",
        },
        {
          name: "local",
          template: "http://{host}:5000/rest-gateway/token",
        },
      ],
      selectedName: "nsp-25-4",
    },
  ],
  store: {
    durability: "session",
    state: "ready",
  },
};

class FakeHost implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  private readonly listeners = new Set<HostEventListener>();

  emit(event: HostEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    return Promise.resolve({
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: `correlation-${command.id}` },
      version: HOST_PROTOCOL_VERSION,
    });
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

function publishTemplates(
  host: FakeHost,
  snapshot: ConnectionTemplateSnapshot = sessionSnapshot,
  sequence = 1,
): void {
  act(() => {
    host.emit({
      event: "templates.changed",
      payload: snapshot,
      sequence,
      version: HOST_PROTOCOL_VERSION,
    });
  });
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
    publishTemplates(host);
    publishProfilesReady(host);
    await user.click(screen.getByRole("button", { name: "Add profile" }));
    await user.type(screen.getByLabelText(/Profile name/u), "Retained draft");
    expect(
      screen.queryByRole("button", { name: "Manage connection templates" }),
    ).not.toBeInTheDocument();
    await user.click(screen.getByRole("switch", { name: "Use OAuth OAUTHBEARER" }));
    expect(
      screen.queryByRole("button", { name: "Choose OAuth endpoint template" }),
    ).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Secret Retrieval Profile" }));
    await user.click(screen.getByRole("button", { name: "Manage retrieval profiles" }));
    await screen.findByRole("dialog", { name: "Secret Retrieval Profiles" });
    expect(host.commands.some((command) => command.command === "recipes.list")).toBe(true);
    await user.click(screen.getByRole("button", { name: "Close retrieval profiles" }));
    await waitFor(() =>
      expect(screen.getByLabelText(/Profile name/u)).toHaveValue("Retained draft"),
    );
  });
});
