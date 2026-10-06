import { createHash, randomUUID } from "node:crypto";

import type { PluginBackend, PluginBackendHost } from "../../../src/plugins/api";
import type {
  JsonObject,
  JsonValue,
  PluginChangeWarning,
  PluginExitPrompt,
  PluginRequest,
} from "../../../src/plugins/contracts";
import { parsePluginJson } from "../../../src/plugins/validation";
import {
  parseNspConnectInput,
  parseNspCredentials,
  parseNspCancelInput,
  parseNspStatusInput,
  parseNspProfileSource,
  type NspConnectInput,
  type NspCredentials,
  type NspProgress,
  type NspResult,
  type NspStatus,
} from "../contracts";

import { NspApiClient, requireNspTargetVersion } from "./api-client";
import { nspProblem as problem, nspFailure } from "./errors";
import { NspProfileError, qualifyAndSaveProfile, selectProfile } from "./profile";
import { parseRecovery, type NspRecovery } from "./recovery";

export type NspApiPort = Pick<
  NspApiClient,
  "authenticate" | "readVersion" | "ensureWorkflow" | "retrieveTrust" | "cleanupExecution" | "close"
>;
export type NspApiFactory = (credentials: NspCredentials, signal?: AbortSignal) => NspApiPort;
type Request = Omit<PluginRequest, "pluginId" | "activationId">;

export class NspCaptureBackend implements PluginBackend {
  private pending:
    | { id: string; identity: string; controller: AbortController; promise: Promise<JsonValue> }
    | undefined;
  private recovery: NspRecovery | undefined;
  private initialized: Promise<void> | undefined;
  private closed = false;
  private unloading = false;

  constructor(
    private readonly host: PluginBackendHost,
    private readonly createClient: NspApiFactory = (credentials, signal) =>
      new NspApiClient(credentials, {}, signal),
  ) {}

  initialize(): Promise<void> {
    this.initialized ??= Promise.resolve().then(async () => {
      if (this.host.recoveryState === undefined)
        throw new Error(
          "NSP Connector requires a newer StreamSkope host with persistent plugin recovery support.",
        );
      this.recovery = parseRecovery(await this.host.recoveryState.read());
    });
    return this.initialized;
  }

  private status(): NspStatus {
    return this.pending !== undefined
      ? { state: "running", requestId: this.pending.id }
      : this.recovery !== undefined
        ? {
            state: "cleanup-required",
            requestId: this.recovery.requestId,
            message: `Complete cleanup on ${this.recovery.apiUrl} using the same NSP account before starting another connection setup.`,
          }
        : { state: "idle" };
  }

  async execute(request: Request): Promise<JsonValue> {
    await this.initialize();
    if (this.closed || this.unloading) throw problem("The NSP plugin is shutting down.");
    try {
      if (request.method === "nspCapture.status") {
        parseNspStatusInput(request.input);
        return parsePluginJson({ ok: true, status: this.status() });
      }
      if (request.method === "nspCapture.cancel") {
        const { requestId } = parseNspCancelInput(request.input);
        const operation = this.pending;
        if (operation?.id === requestId) {
          operation.controller.abort();
          await operation.promise;
        }
        return parsePluginJson({
          ok: true,
          cancelled: operation?.id === requestId,
          status: this.status(),
        });
      }
      const cleanup = request.method === "nspCapture.cleanup";
      if (!cleanup && request.method !== "nspCapture.connect")
        throw problem("Unknown NSP plugin command.");
      const input = cleanup
        ? parseNspCredentials(request.input)
        : parseNspConnectInput(request.input);
      const identity = createHash("sha256")
        .update(JSON.stringify([request.method, input]))
        .digest("hex");
      if (this.pending !== undefined) {
        if (this.pending.id === request.requestId && this.pending.identity === identity)
          return this.pending.promise;
        throw problem("An NSP operation is already running. Wait for it to finish or cancel it.");
      }
      const controller = new AbortController();
      const promise = Promise.resolve().then(() =>
        this.run(request, input, cleanup, controller.signal),
      );
      this.pending = { id: request.requestId, identity, controller, promise };
      try {
        return await promise;
      } finally {
        this.pending = undefined;
      }
    } catch (error) {
      return parsePluginJson({
        ok: false,
        error: this.host.failure(error, { correlationId: request.correlationId }),
      });
    }
  }

  private async journal(value: NspRecovery | undefined): Promise<void> {
    await this.host.recoveryState!.write(value === undefined ? null : parsePluginJson(value));
    this.recovery = value;
  }

  private async clean(client: NspApiPort, input: NspCredentials): Promise<void> {
    const recovery = this.recovery;
    if (recovery === undefined) return;
    if (recovery.apiUrl !== input.apiUrl || recovery.username !== input.username)
      throw problem("Pending NSP cleanup belongs to another API URL or account.", true);
    await client.cleanupExecution(recovery.requestId, recovery.executionId);
    await this.journal(undefined);
  }

  private async run(
    request: Request,
    input: NspConnectInput,
    cleanup: boolean,
    signal: AbortSignal,
  ): Promise<JsonValue> {
    const client = this.createClient(input, signal);
    const sensitiveValues: string[] = [input.password];
    let result: NspResult;
    const progress = (step: NspProgress["step"], message: string): void =>
      this.host.publish(
        "nspCapture.progress",
        parsePluginJson({ requestId: request.requestId, step, message }),
      );
    try {
      // Ownership and active-profile checks precede remote work.
      const existing = cleanup ? undefined : selectProfile(await this.host.profiles(), input);
      if (existing?.active === true)
        throw problem("Disconnect this NSP profile before refreshing its credentials.");
      if (
        this.recovery !== undefined &&
        (this.recovery.apiUrl !== input.apiUrl || this.recovery.username !== input.username)
      )
        throw problem("Pending NSP cleanup belongs to another API URL or account.", true);
      progress("authenticate", "Authenticating with the NSP API.");
      await client.authenticate();
      progress("cleanup", "Reconciling any interrupted NSP execution.");
      await this.clean(client, input);
      signal.throwIfAborted();
      if (cleanup) result = { ok: true, status: { state: "idle" } };
      else {
        progress("authenticate", "Checking the running NSP product version.");
        requireNspTargetVersion(await client.readVersion());
        signal.throwIfAborted();
        progress("workflow", "Checking the owned NSP connection helper workflow.");
        const workflow = await client.ensureWorkflow();
        signal.throwIfAborted();
        const recovery: NspRecovery = {
          version: 1,
          apiUrl: input.apiUrl,
          username: input.username,
          requestId: randomUUID(),
        };
        await this.journal(recovery);
        progress("retrieve", "Retrieving the CA truststore and matching password inside NSP.");
        const trust = await client.retrieveTrust(recovery.requestId, {
          onExecution: async (executionId) => this.journal({ ...recovery, executionId }),
        });
        sensitiveValues.push(trust.truststoreBase64, trust.truststorePassword);
        progress("cleanup", "Removing the workflow execution and its credential output.");
        await this.clean(client, input);
        signal.throwIfAborted();
        const profileId = await qualifyAndSaveProfile(
          this.host,
          input,
          trust,
          workflow.name,
          existing,
          signal,
          progress,
        );
        result = { ok: true, profileId };
      }
    } catch (error) {
      result = {
        ok: false,
        error:
          error instanceof NspProfileError
            ? error.failure
            : this.host.failure(nspFailure(error, signal.aborted), {
                correlationId: request.correlationId,
                sensitiveValues,
              }),
      };
    }
    // Cleanup must run with an independent non-cancelled client, even after a lost POST response.
    if (
      this.recovery !== undefined &&
      this.recovery.apiUrl === input.apiUrl &&
      this.recovery.username === input.username
    ) {
      const cleanupClient = this.createClient(input);
      try {
        await this.clean(cleanupClient, input);
      } catch {
        result = {
          ok: false,
          error: this.host.failure(
            problem(
              "NSP execution cleanup could not be confirmed. Recovery information has been retained.",
              true,
            ),
            { correlationId: request.correlationId },
          ),
        };
      } finally {
        await cleanupClient.close().catch(() => undefined);
      }
    }
    await client.close().catch(() => undefined);
    this.host.recordActivity({
      correlationId: request.correlationId,
      operation: cleanup ? "Clean up NSP workflow execution" : "Create NSP connection",
      object: input.apiUrl,
      detail: result.ok
        ? cleanup
          ? "Owned NSP execution cleanup confirmed."
          : "NSP connection profile verified and saved."
        : `${result.error.summary} ${result.error.recovery}`,
      outcome: result.ok ? "succeeded" : signal.aborted ? "cancelled" : "failed",
      severity: result.ok ? "info" : "error",
      sensitiveValues,
    });
    return parsePluginJson(result);
  }

  validateProfile(data: JsonObject, brokers: readonly string[]): Promise<void> {
    return Promise.resolve().then(() => {
      const source = parseNspProfileSource(data);
      if (
        this.closed ||
        this.unloading ||
        source.brokers.length !== brokers.length ||
        source.brokers.some((broker, index) => broker !== brokers[index])
      )
        throw problem("The NSP profile no longer matches its recorded broker endpoints.");
    });
  }

  async beforeChange(): Promise<PluginChangeWarning | undefined> {
    await this.initialize();
    if (this.pending === undefined && this.recovery === undefined) return undefined;
    return {
      message: "Finish NSP workflow execution cleanup before changing the plugin?",
      detail:
        "Running retrieval is cancelled and its execution output must be removed. Saved profiles and the reusable NSP helper workflow are retained.",
      stateKey: JSON.stringify([this.pending?.id, this.recovery]),
    };
  }

  async prepareUnload(_reason: "update" | "remove"): Promise<void> {
    if (this.closed || this.unloading) throw problem("The NSP plugin is already shutting down.");
    this.unloading = true;
    try {
      this.pending?.controller.abort();
      await this.pending?.promise;
      if (this.recovery !== undefined)
        throw problem(
          "NSP cleanup is still pending. The plugin remains installed for recovery.",
          true,
        );
      await this.host.disconnectOwnedConnection();
    } finally {
      this.unloading = false;
    }
  }

  async beforeExit(): Promise<Omit<PluginExitPrompt, "pluginId"> | undefined> {
    await this.initialize();
    if (this.pending === undefined && this.recovery === undefined) return undefined;
    return {
      title: "NSP connection operation",
      message: "An NSP retrieval or cleanup is unfinished.",
      detail:
        "Cancel the running retrieval and confirm cleanup before exiting. Pending recovery identifiers are retained across restart.",
      actions: [
        { id: "cancel", label: "Keep StreamSkope open" },
        { id: "cleanup", label: "Cancel retrieval and exit" },
      ],
      cancelAction: "cancel",
    };
  }
  async resolveExit(action: string): Promise<boolean> {
    if (action !== "cleanup") return false;
    this.pending?.controller.abort();
    await this.pending?.promise;
    return this.recovery === undefined;
  }
  async close(): Promise<void> {
    this.closed = true;
    this.pending?.controller.abort();
    await this.pending?.promise;
  }
}

export function activate(host: PluginBackendHost): PluginBackend {
  if (host.recoveryState === undefined)
    throw new Error(
      "NSP Connector requires a newer StreamSkope host with persistent plugin recovery support.",
    );
  // Candidates are loaded before the old instance drains; read its journal only after activation.
  return new NspCaptureBackend(host);
}
