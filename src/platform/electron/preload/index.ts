import { contextBridge, ipcRenderer } from "electron";

import { exposeStreamSkopeDesktop, type DesktopPreloadIpcRenderer } from "./desktop-bridge";
import { exposeStreamSkopeHost, type PreloadIpcRenderer } from "./host-bridge";

const preloadIpc: DesktopPreloadIpcRenderer & PreloadIpcRenderer = {
  invoke: (channel, value) => ipcRenderer.invoke(channel, value),
  on: (channel, listener) => {
    ipcRenderer.on(channel, listener);
  },
  removeListener: (channel, listener) => {
    ipcRenderer.removeListener(channel, listener);
  },
};

exposeStreamSkopeHost(contextBridge, preloadIpc);
exposeStreamSkopeDesktop(contextBridge, preloadIpc);
