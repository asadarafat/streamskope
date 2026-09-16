import { join } from "node:path";

import { app } from "electron";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEventListener,
  type StreamSkopeBackend,
} from "../../src/features/kafka/contracts";
import { createElectronShell } from "../../src/platform/electron/main/electron-shell";

class ElectronSmokeBackend implements StreamSkopeBackend {
  private readonly listeners = new Set<HostEventListener>();
  private sequence = 0;

  execute(command: HostCommand): Promise<HostCommandResponse> {
    const response: HostCommandResponse = {
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: `smoke-${command.id}` },
      version: HOST_PROTOCOL_VERSION,
    };
    return Promise.resolve(response);
  }

  subscribe(listener: HostEventListener): () => void {
    this.listeners.add(listener);
    listener({
      event: "backend.availability",
      payload: { state: "ready" },
      sequence: this.sequence++,
      version: HOST_PROTOCOL_VERSION,
    });
    return (): void => {
      this.listeners.delete(listener);
    };
  }
}

async function start(): Promise<void> {
  const rendererUrl = process.env.STREAMSKOPE_SMOKE_RENDERER_URL;
  if (rendererUrl === undefined) {
    throw new Error("STREAMSKOPE_SMOKE_RENDERER_URL is required.");
  }
  await app.whenReady();
  await createElectronShell({
    backend: new ElectronSmokeBackend(),
    preloadPath: join(__dirname, "preload.cjs"),
    rendererUrl,
  });
}

app.on("window-all-closed", () => {
  app.quit();
});

void start().catch((error: unknown) => {
  const summary = error instanceof Error ? error.message : "Unknown startup failure.";
  process.stderr.write(`Electron smoke startup failed: ${summary}\n`);
  app.exit(1);
});
