// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEventListener,
  type StreamSkopeHost,
} from "../../src/kafka/contracts";
import { StreamSkopeApp as StreamSkopeWorkbench } from "../../src/app/StreamSkopeApp";
import {
  DESKTOP_PLATFORM_VERSION,
  type DesktopAction,
  type DesktopActionListener,
  type StreamSkopeDesktop,
} from "../../src/platform/desktop";

class RecordingHost implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  private readonly listeners = new Set<HostEventListener>();

  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    return Promise.resolve({
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: `desktop-${command.id}` },
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

class RecordingDesktop implements StreamSkopeDesktop {
  private readonly listeners = new Set<DesktopActionListener>();

  emit(action: DesktopAction["action"]): void {
    for (const listener of this.listeners) {
      listener({ action, version: DESKTOP_PLATFORM_VERSION });
    }
  }

  saveTextDocument(): Promise<never> {
    return Promise.reject(new Error("Native document save was not expected."));
  }

  subscribeActions(listener: DesktopActionListener): () => void {
    this.listeners.add(listener);
    return (): void => {
      this.listeners.delete(listener);
    };
  }
}

afterEach(() => {
  cleanup();
});

describe("StreamSkope native workbench actions", () => {
  it("routes Preferences and Activity to their canonical surfaces without host commands", async () => {
    const host = new RecordingHost();
    const desktop = new RecordingDesktop();
    const view = render(<StreamSkopeWorkbench desktop={desktop} host={host} />);
    const initialCommands = host.commands.length;

    act(() => {
      desktop.emit("preferences.open");
    });
    expect(await screen.findByRole("dialog", { name: "Workbench Preferences" })).toBeVisible();

    act(() => {
      desktop.emit("activity.open");
    });
    await waitFor(() => {
      expect(screen.getByRole("complementary", { name: "Activity log" })).toBeVisible();
    });
    expect(host.commands).toHaveLength(initialCommands);

    view.unmount();
    expect(() => desktop.emit("activity.open")).not.toThrow();
  });
});
