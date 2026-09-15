import {
  parseDesktopAction,
  parseDesktopSaveResult,
  parseDesktopTextDocument,
  type DesktopActionListener,
  type DesktopSaveResult,
  type StreamSkopeDesktop,
} from "../platform/desktop";

import { DESKTOP_ACTION_CHANNEL, DESKTOP_DOCUMENT_SAVE_CHANNEL } from "./channels";
import type { PreloadContextBridge } from "./host-bridge";

type PreloadEventListener = (event: unknown, value: unknown) => void;

export interface DesktopPreloadIpcRenderer {
  invoke(channel: string, value: unknown): Promise<unknown>;
  on(channel: string, listener: PreloadEventListener): void;
  removeListener(channel: string, listener: PreloadEventListener): void;
}

export function createStreamSkopeDesktop(
  ipcRenderer: DesktopPreloadIpcRenderer,
): StreamSkopeDesktop {
  return {
    saveTextDocument: async (value): Promise<DesktopSaveResult> => {
      const document = parseDesktopTextDocument(value);
      return parseDesktopSaveResult(
        await ipcRenderer.invoke(DESKTOP_DOCUMENT_SAVE_CHANNEL, document),
      );
    },
    subscribeActions: (listener: DesktopActionListener): (() => void) => {
      const handleAction: PreloadEventListener = (_event, value): void => {
        listener(parseDesktopAction(value));
      };
      ipcRenderer.on(DESKTOP_ACTION_CHANNEL, handleAction);
      let subscribed = true;
      return (): void => {
        if (!subscribed) {
          return;
        }
        subscribed = false;
        ipcRenderer.removeListener(DESKTOP_ACTION_CHANNEL, handleAction);
      };
    },
  };
}

export function exposeStreamSkopeDesktop(
  contextBridge: PreloadContextBridge,
  ipcRenderer: DesktopPreloadIpcRenderer,
): void {
  contextBridge.exposeInMainWorld("streamSkopeDesktop", createStreamSkopeDesktop(ipcRenderer));
}
