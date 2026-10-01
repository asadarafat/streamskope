import type { PluginBackend, PluginBackendHost } from "../../../src/plugins/api";
import type {
  JsonObject,
  JsonValue,
  PluginRequest,
  PluginExitPrompt,
  PluginChangeWarning,
} from "../../../src/plugins/contracts";
import { parsePluginJson } from "../../../src/plugins/validation";
import {
  EDA_PROTOCOL_VERSION,
  parseEdaCaptureCommand,
  fromPluginProfileSource,
  sameEdaCaptureSource,
  type ProfileEdaCaptureSource,
  type EdaCaptureCommandResponse,
} from "../contracts";
import { parseProfileSource } from "../contracts/profile-validation";

import type { EdaCapturePort } from "./eda-capture-port";
import { EdaAgentCapture } from "./eda-agent-capture";
import { executeEdaCaptureCommand } from "./eda-capture-facade";

export class EdaCaptureBackend implements PluginBackend {
  private readonly operations = new Map<string, AbortController>();
  private readonly pending = new Map<Promise<EdaCaptureCommandResponse>, string>();
  private closed = false;
  private unloading = false;
  constructor(
    private readonly host: PluginBackendHost,
    private readonly capture: EdaCapturePort = new EdaAgentCapture(host.probeTopics.bind(host)),
  ) {}

  async execute(request: Omit<PluginRequest, "pluginId" | "activationId">): Promise<JsonValue> {
    if (this.closed || this.unloading) throw new Error("The EDA plugin is shutting down.");
    const command = parseEdaCaptureCommand({
      command: request.method,
      id: request.requestId,
      payload: request.input,
      version: EDA_PROTOCOL_VERSION,
    });
    const pending = executeEdaCaptureCommand(command, request.correlationId, {
      capture: this.capture,
      operations: this.operations,
      connectionActive: this.host.connectionActive.bind(this.host),
      recordActivity: this.host.recordActivity.bind(this.host),
      failure: this.host.failure.bind(this.host),
      publishProgress: (progress) =>
        this.host.publish("edaCapture.progress", parsePluginJson(progress)),
      removeCaptureProfiles: (source) => this.removeCaptureProfiles(source),
    });
    this.pending.set(pending, request.requestId);
    try {
      return parsePluginJson(await pending);
    } finally {
      this.pending.delete(pending);
    }
  }
  validateProfile(data: JsonObject, brokers: readonly string[]): Promise<void> {
    return Promise.resolve().then(() => {
      const source = parseProfileSource(data, "profile.source.data");
      const status = this.capture.status();
      if (
        this.closed ||
        this.unloading ||
        this.operations.size > 0 ||
        brokers.length !== 1 ||
        brokers[0] !== source.broker ||
        status.state !== "ready" ||
        status.tunnel !== "open" ||
        status.source === undefined ||
        source.sessionId === undefined ||
        source.sessionId !== status.source.sessionId ||
        source.broker !== status.source.broker ||
        !sameEdaCaptureSource(source, status.source)
      ) {
        throw Object.assign(new Error("This capture connection has no matching running tunnel."), {
          code: "BACKEND_UNAVAILABLE",
          stage: "backend",
          retryable: false,
          recovery:
            "Resume capture from this profile, then connect. Saved capture metadata is not live readiness.",
        });
      }
    });
  }
  beforeChange(): Promise<PluginChangeWarning | undefined> {
    const status = this.capture.status();
    const source = status.source;
    if (source === undefined && this.pending.size === 0) return Promise.resolve(undefined);
    return Promise.resolve({
      message: "Stop EDA capture before changing the plugin?",
      stateKey: JSON.stringify([
        source?.sessionId,
        status.state,
        status.tunnel,
        [...this.pending.values()].sort(),
      ]),
      detail:
        source === undefined
          ? "Running EDA operations will finish or be cancelled before the plugin changes. Application installation already submitted to EDA must finish. Saved connection settings are retained."
          : `This stops ${source.source.namespace}/${source.source.name}, disconnects its capture connection, and removes its temporary broker and copied exporter. Captured messages are discarded. Saved connection settings are retained for a later capture.`,
    });
  }
  async prepareUnload(_reason: "update" | "remove"): Promise<void> {
    if (this.closed || this.unloading) throw new Error("The EDA plugin is already shutting down.");
    this.unloading = true;
    try {
      for (const operation of this.operations.values()) operation.abort();
      // Application installation and inspection cannot be cancelled. Keep their
      // backend alive until they settle, then inspect the final capture state.
      await Promise.allSettled([...this.pending.keys()]);
      await this.host.disconnectOwnedConnection();
      const source = this.capture.status().source;
      if (source !== undefined) {
        try {
          // Plugin removal/update retains saved profiles for recovery or resume.
          await this.capture.stop(source, true);
        } catch (error) {
          throw new Error(
            "Capture cleanup could not be confirmed. The plugin remains active; retry cleanup or resume the capture. Saved connection and recovery information is retained.",
            { cause: error },
          );
        }
      }
    } finally {
      // The runtime owns the change barrier. If committing the installation
      // fails after cleanup, this backend must remain available to resume.
      this.unloading = false;
    }
  }
  beforeExit(): Promise<Omit<PluginExitPrompt, "pluginId"> | undefined> {
    const source = this.capture.status().source;
    if (source === undefined && this.operations.size === 0) return Promise.resolve(undefined);
    return Promise.resolve({
      title: "Temporary EDA capture",
      message: "Remove temporary capture before exiting?",
      detail:
        source === undefined
          ? "A capture operation is still running. Cancel it and wait for completion before exiting."
          : `${source.source.namespace}/${source.source.name} in ${source.context ?? "recorded cluster"}. Cleanup removes the copied exporter and temporary broker, discarding captured messages. Keeping it leaves remote resources running and records pending cleanup.`,
      actions:
        source === undefined
          ? [{ id: "cancel", label: "Cancel" }]
          : [
              { id: "cancel", label: "Cancel" },
              { id: "cleanup", label: "Stop and remove capture" },
              { id: "keep", label: "Keep capture and exit" },
            ],
      cancelAction: "cancel",
    });
  }
  async resolveExit(action: string): Promise<boolean> {
    if (action === "cancel") return false;
    if (action === "keep") return this.operations.size === 0;
    if (action !== "cleanup") throw new Error("Unknown EDA exit action.");
    if (this.operations.size > 0) return false;
    const source = this.capture.status().source;
    if (source === undefined) return true;
    await this.host.disconnectOwnedConnection();
    const cleanup = await this.execute({
      method: "edaCapture.remove",
      input: parsePluginJson({ source }),
      requestId: crypto.randomUUID(),
      correlationId: crypto.randomUUID(),
    });
    const result = cleanup as { readonly ok?: JsonValue; readonly error?: JsonValue };
    if (result.ok !== true)
      throw new Error(
        "Capture cleanup could not be confirmed. Retry cleanup or explicitly keep the capture when exiting. Recovery information is retained.",
      );
    return true;
  }
  async close(): Promise<void> {
    this.closed = true;
    for (const operation of this.operations.values()) operation.abort();
    await Promise.allSettled([...this.pending.keys()]);
    await this.capture.close();
  }
  private async removeCaptureProfiles(source: ProfileEdaCaptureSource): Promise<void> {
    if (source.sessionId === undefined) return;
    for (const profile of await this.host.profiles()) {
      const owned = fromPluginProfileSource(profile.source);
      if (
        owned !== undefined &&
        owned.sessionId === source.sessionId &&
        sameEdaCaptureSource(owned, source)
      )
        await this.host.deleteProfile(profile.id);
    }
  }
}
export function activate(host: PluginBackendHost): PluginBackend {
  return new EdaCaptureBackend(host);
}
