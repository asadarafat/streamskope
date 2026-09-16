import { describe, expect, it } from "vitest";

import {
  DESKTOP_PLATFORM_VERSION,
  DesktopPlatformContractError,
  type DesktopAction,
} from "../../src/platform/desktop";
import {
  createStreamSkopeDesktop,
  type DesktopPreloadIpcRenderer,
} from "../../src/platform/electron/preload/desktop-bridge";
import {
  DESKTOP_ACTION_CHANNEL,
  DESKTOP_DOCUMENT_SAVE_CHANNEL,
} from "../../src/platform/electron/preload/channels";

type Listener = (event: unknown, value: unknown) => void;

class FakeDesktopIpc implements DesktopPreloadIpcRenderer {
  readonly invocations: Array<{ readonly channel: string; readonly value: unknown }> = [];
  readonly listeners = new Map<string, Set<Listener>>();
  response: unknown = {
    state: "cancelled",
    version: DESKTOP_PLATFORM_VERSION,
  };

  emit(channel: string, value: unknown): void {
    for (const listener of this.listeners.get(channel) ?? []) {
      listener({}, value);
    }
  }

  invoke(channel: string, value: unknown): Promise<unknown> {
    this.invocations.push({ channel, value });
    return Promise.resolve(this.response);
  }

  on(channel: string, listener: Listener): void {
    const listeners = this.listeners.get(channel) ?? new Set();
    listeners.add(listener);
    this.listeners.set(channel, listeners);
  }

  removeListener(channel: string, listener: Listener): void {
    this.listeners.get(channel)?.delete(listener);
  }
}

describe("desktop preload bridge", () => {
  const document = {
    byteSize: 17,
    content: '{\n  "ok": true\n}\n',
    fileName: "streamskope-export.json",
    mediaType: "application/json",
  } as const;

  it("validates native save requests and responses in both directions", async () => {
    const ipc = new FakeDesktopIpc();
    const desktop = createStreamSkopeDesktop(ipc);

    await expect(desktop.saveTextDocument(document)).resolves.toEqual(ipc.response);
    expect(ipc.invocations).toEqual([{ channel: DESKTOP_DOCUMENT_SAVE_CHANNEL, value: document }]);

    await expect(
      desktop.saveTextDocument({ ...document, fileName: "../private.json" }),
    ).rejects.toBeInstanceOf(DesktopPlatformContractError);
    expect(ipc.invocations).toHaveLength(1);

    ipc.response = { state: "complete", version: DESKTOP_PLATFORM_VERSION };
    await expect(desktop.saveTextDocument(document)).rejects.toBeInstanceOf(
      DesktopPlatformContractError,
    );
  });

  it("subscribes only to declared native actions and removes its listener", () => {
    const ipc = new FakeDesktopIpc();
    const desktop = createStreamSkopeDesktop(ipc);
    const received: DesktopAction[] = [];
    const unsubscribe = desktop.subscribeActions((action) => {
      received.push(action);
    });
    const activity = {
      action: "activity.open",
      version: DESKTOP_PLATFORM_VERSION,
    } as const;

    ipc.emit(DESKTOP_ACTION_CHANNEL, activity);
    expect(received).toEqual([activity]);
    expect(() =>
      ipc.emit(DESKTOP_ACTION_CHANNEL, {
        action: "shell.execute",
        version: DESKTOP_PLATFORM_VERSION,
      }),
    ).toThrow(DesktopPlatformContractError);
    unsubscribe();
    ipc.emit(DESKTOP_ACTION_CHANNEL, activity);
    expect(received).toEqual([activity]);
  });
});
