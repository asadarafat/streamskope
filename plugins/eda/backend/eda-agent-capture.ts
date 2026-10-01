import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";

import {
  EDA_CAPTURE_DEFAULTS,
  EDA_CAPTURE_LIMITS,
  EDA_TARGET_VERSION,
  type EdaCaptureApplicationStatus,
  type EdaCaptureDeployInput,
  type EdaCaptureDeployment,
  type EdaCaptureInspectInput,
  type EdaCaptureInspection,
  type EdaCaptureHostStatus,
  type EdaCaptureSessionStatus,
  type ProfileEdaCaptureSource,
  sameEdaCaptureSource,
} from "../contracts";

import type { EdaCapturePort, EdaCaptureProgressObserver } from "./eda-capture-port";
import { EdaAgentTunnel } from "./eda-agent-tunnel";
import { EdaApiClient, EdaApiError } from "./eda-api-client";
import { exportedTopics, sourceFromResource } from "./eda-capture-source";

const CONTEXT = "eda-agent";
const LEASE_SECONDS = 900;
const RENEW_INTERVAL_MS = 300_000;
const STARTUP_TIMEOUT_MS = 300_000;
const AGENT_EXPORTER_PREFIX = "streamskope-capture-";

function api(input: EdaCaptureInspectInput["edaApi"], signal?: AbortSignal): EdaApiClient {
  return new EdaApiClient(input, { rejectUnauthorized: input.verifyTls ?? true }, signal);
}

function captureError(message: string): EdaApiError {
  return new EdaApiError(message, {
    code: "BACKEND_UNAVAILABLE",
    recovery: "Inspect the installed StreamSkope Capture application and retry this source.",
    retryable: true,
    stage: "backend",
    target: "StreamSkope Capture",
  });
}

async function requireTargetVersion(client: EdaApiClient): Promise<void> {
  const observed = await client.clusterVersion();
  if (observed.releaseVersion !== EDA_TARGET_VERSION)
    throw new EdaApiError(
      `This EDA Capture plugin targets ${EDA_TARGET_VERSION}, but the running cluster reports ${observed.releaseVersion}.`,
      {
        code: "VALIDATION",
        recovery: `Connect to EDA ${EDA_TARGET_VERSION}, or install the EDA Capture plugin for ${observed.releaseVersion} in Preferences > Plugins. No capture resources were changed.`,
        retryable: false,
        stage: "validation",
        target: "EDA version",
      },
    );
}

export class EdaAgentCapture implements EdaCapturePort {
  private client: EdaApiClient | undefined;
  private tunnel: EdaAgentTunnel | undefined;
  private renewal: NodeJS.Timeout | undefined;
  private renewing: Promise<void> | undefined;
  private session: EdaCaptureSessionStatus = {
    state: "idle",
    tunnel: "closed",
    detail: "No EDA capture is active on this host. Resume a saved capture with EDA credentials.",
  };

  constructor(
    private readonly probeTopics: (
      brokers: readonly string[],
      signal?: AbortSignal,
    ) => Promise<readonly string[]>,
  ) {}

  async applicationStatus(
    input: import("../contracts").EdaCaptureApplicationInput,
  ): Promise<EdaCaptureApplicationStatus> {
    const credentials = input.authorization
      ? { ...input.edaApi, ...input.authorization }
      : input.edaApi;
    const client = api(credentials);
    await requireTargetVersion(client);
    return client.captureApplicationStatus();
  }

  async installApplication(
    input: import("../contracts").EdaCaptureApplicationInput,
  ): Promise<EdaCaptureApplicationStatus> {
    const credentials = input.authorization
      ? { ...input.edaApi, ...input.authorization }
      : input.edaApi;
    const client = api(credentials);
    await requireTargetVersion(client);
    return client.installCaptureApplication();
  }

  preflight(): Promise<EdaCaptureHostStatus> {
    return Promise.resolve({
      state: "configured" as const,
      context: CONTEXT,
      detail:
        "EDA application access uses the entered EDA API credentials. No Kubernetes credentials are required on this host.",
    });
  }

  status(): EdaCaptureSessionStatus {
    return this.session;
  }

  async inspect(input: EdaCaptureInspectInput): Promise<EdaCaptureInspection> {
    const client = api(input.edaApi);
    await requireTargetVersion(client);
    const sources = (await client.listProducers())
      .flatMap((resource) => {
        const kind = resource.kind;
        if (kind !== "ClusterProducer" && kind !== "Producer") return [];
        const source = sourceFromResource(
          resource,
          "kafka.eda.nokia.com/v1",
          kind,
          EDA_CAPTURE_DEFAULTS.namespace,
        );
        return source === undefined ? [] : [source];
      })
      .slice(0, EDA_CAPTURE_LIMITS.sources);
    return {
      contexts: [],
      edaApiUrl: new URL(input.edaApi.baseUrl).origin,
      imageSetup: { state: "unconfigured" },
      namespace: EDA_CAPTURE_DEFAULTS.namespace,
      sources,
    };
  }

  async deploy(
    input: EdaCaptureDeployInput,
    onProgress: EdaCaptureProgressObserver = () => undefined,
    signal: AbortSignal = new AbortController().signal,
  ): Promise<EdaCaptureDeployment> {
    if (input.context !== CONTEXT)
      throw captureError("The capture target is not the EDA application.");
    if (this.session.state === "starting" || this.session.state === "ready")
      throw captureError("Another EDA capture is active. Stop it before starting another source.");
    const client = api(input.edaApi, signal);
    onProgress("authenticating", "Checking the running EDA version and capture application.");
    await requireTargetVersion(client);
    let id = input.sessionId ?? randomUUID();
    let name = `${AGENT_EXPORTER_PREFIX}${id.slice(0, 8)}`;
    const broker = `127.0.0.1:${String(input.localPort)}`;
    let source: ProfileEdaCaptureSource = {
      kind: "eda-capture",
      edaApiUrl: new URL(input.edaApi.baseUrl).origin,
      context: CONTEXT,
      sessionId: id,
      broker,
      clusterBroker: `${name}.${input.source.namespace}.svc:9092`,
      exporterName: name,
      workloadName: name,
      source: input.source,
      state: "ready",
      topics: [],
    };
    this.session = {
      state: "starting",
      tunnel: "closed",
      source,
      detail: "Preparing EDA capture.",
    };
    let tunnel: EdaAgentTunnel | undefined;
    let created = false;
    let remoteSessionPossible = input.sessionId !== undefined;
    try {
      onProgress("authenticating", "Authenticating with EDA and checking the capture application.");
      const application = await client.captureApplicationStatus();
      if (application.state !== "installed")
        throw captureError("The StreamSkope Capture application is not installed.");
      const producer = await client.getProducer(input.source);
      const topics = exportedTopics(producer);
      if (topics.length === 0) throw captureError("The selected EDA exporter has no topics.");
      let resumeExisting = input.sessionId !== undefined;
      if (resumeExisting) {
        try {
          const existing = await client.getCaptureSession(id);
          if (existing.expiresAt !== undefined && Date.parse(existing.expiresAt) <= Date.now())
            resumeExisting = false;
        } catch (error) {
          if (!(error instanceof EdaApiError) || error.code !== "PROFILE_NOT_FOUND") throw error;
          resumeExisting = false;
        }
        if (!resumeExisting) {
          remoteSessionPossible = false;
          id = randomUUID();
          name = `${AGENT_EXPORTER_PREFIX}${id.slice(0, 8)}`;
          source = {
            ...source,
            sessionId: id,
            clusterBroker: `${name}.${input.source.namespace}.svc:9092`,
            exporterName: name,
            workloadName: name,
          };
          this.session = { ...this.session, source };
        }
      }
      signal.throwIfAborted();
      onProgress("opening-tunnel", "Reserving the local Kafka endpoint.");
      tunnel = await EdaAgentTunnel.listen(client, id, input.localPort);
      onProgress(
        "deploying-broker",
        "Asking the EDA application to start a private capture broker.",
      );
      if (!resumeExisting) {
        // A lost response may still leave the session committed in EDA.
        created = true;
        remoteSessionPossible = true;
        await client.createCaptureSession({
          id,
          leaseSeconds: LEASE_SECONDS,
          localPort: input.localPort,
          source: input.source,
        });
      } else {
        await client.getCaptureSession(id);
      }
      onProgress("waiting-broker", "Waiting for the private capture broker and exporter.");
      const deadline = Date.now() + STARTUP_TIMEOUT_MS;
      while (Date.now() < deadline) {
        signal.throwIfAborted();
        const session = await client.getCaptureSession(id);
        if (session.phase === "Ready") break;
        if (session.phase !== "Pending")
          throw captureError(`EDA capture entered ${session.phase} state.`);
        await delay(1_000, undefined, { signal });
      }
      if ((await client.getCaptureSession(id)).phase !== "Ready")
        throw captureError("The EDA capture broker did not become ready in time.");
      tunnel.activate();
      onProgress("waiting-topics", "Verifying the local Kafka protocol endpoint.");
      let available: readonly string[] | undefined;
      const topicsDeadline = Date.now() + 120_000;
      while (Date.now() < topicsDeadline) {
        signal.throwIfAborted();
        try {
          available = await this.probeTopics([broker], signal);
          break;
        } catch {
          // The broker may still be starting after the session becomes Ready.
        }
        await delay(2_000, undefined, { signal });
      }
      if (available === undefined)
        throw captureError("The local Kafka endpoint did not return topic metadata in time.");
      signal.throwIfAborted();
      this.client = client;
      this.tunnel = tunnel;
      tunnel = undefined;
      this.session = {
        state: "ready",
        tunnel: "open",
        source: { ...source, topics },
        verifiedAt: new Date().toISOString(),
        detail: topics.every((topic) => available.includes(topic))
          ? "EDA export and the local Kafka tunnel are ready. Messages appear when EDA emits matching data."
          : "The capture broker is reachable. Some selected exporter topics have not been created yet; they appear when EDA emits matching data.",
      };
      this.scheduleRenewal();
      onProgress("ready", "The EDA capture and local Kafka endpoint are ready.");
      return {
        sessionId: id,
        broker,
        clusterBroker: source.clusterBroker,
        context: CONTEXT,
        exporterName: name,
        namespace: input.source.namespace,
        profileName: `EDA capture · ${input.source.name}`.slice(0, 256),
        topics,
        workloadName: name,
      };
    } catch (error) {
      await tunnel?.close().catch(() => undefined);
      let cleanupConfirmed = false;
      if (created) {
        try {
          await api(input.edaApi).removeCaptureSession(id);
          cleanupConfirmed = true;
        } catch {
          // The remote lease remains the fallback; never report cleanup as confirmed.
        }
      }
      const cleanupPending = remoteSessionPossible && !cleanupConfirmed;
      // Deploy's signal may already be aborted. Retain a fresh client only when
      // there is a remote session to recover, so unload cleanup can be retried.
      this.client = cleanupPending ? api(input.edaApi) : undefined;
      this.session = {
        state: signal.aborted ? "cancelled" : "failed",
        tunnel: "closed",
        ...(cleanupPending ? { source } : {}),
        detail: created
          ? cleanupConfirmed
            ? "Capture failed; EDA accepted removal of its temporary resources."
            : "Capture failed and remote cleanup could not be confirmed. The EDA lease will expire; inspect the capture before retrying."
          : "Capture failed before a new EDA session was created.",
      };
      throw error;
    }
  }

  private scheduleRenewal(): void {
    this.renewal = setTimeout(() => {
      this.renewal = undefined;
      if (this.renewing !== undefined) return;
      this.renewing = this.renewLease().finally(() => {
        this.renewing = undefined;
      });
    }, RENEW_INTERVAL_MS);
    this.renewal.unref();
  }

  private async renewLease(): Promise<void> {
    const id = this.session.source?.sessionId;
    const client = this.client;
    if (client === undefined || id === undefined) return;
    try {
      await client.renewCaptureSession(id, LEASE_SECONDS);
      if (this.client !== client) return;
      if (this.session.state === "failed" && this.tunnel !== undefined)
        this.session = {
          ...this.session,
          state: "ready",
          detail: "EDA capture lease renewal recovered and the local tunnel remains open.",
        };
    } catch {
      if (this.client !== client) return;
      this.session = {
        ...this.session,
        state: "failed",
        detail:
          "EDA capture lease renewal failed. Reconnect to EDA before its lease expires; do not assume the broker remains available.",
      };
    } finally {
      if (this.client === client && this.tunnel !== undefined) this.scheduleRenewal();
    }
  }

  async stop(source: ProfileEdaCaptureSource): Promise<void> {
    if (
      !this.session.source ||
      !sameEdaCaptureSource(source, this.session.source) ||
      source.sessionId !== this.session.source.sessionId
    )
      throw captureError("The selected capture does not match this host session.");
    if (this.client === undefined || source.sessionId === undefined)
      throw captureError(
        "EDA credentials are unavailable. Resume this capture before removing it.",
      );
    await this.client.removeCaptureSession(source.sessionId);
    await this.close();
    this.session = {
      state: "stopped",
      tunnel: "closed",
      detail: "EDA accepted capture removal; owned resources are being cleaned up.",
    };
  }

  async close(): Promise<void> {
    if (this.renewal !== undefined) clearTimeout(this.renewal);
    this.renewal = undefined;
    const tunnel = this.tunnel;
    // Invalidate the renewal before awaiting tunnel cleanup. A late response must
    // neither change the stopped state nor start another renewal timer.
    this.tunnel = undefined;
    this.client = undefined;
    const results = await Promise.allSettled([tunnel?.close(), this.renewing]);
    if (this.session.tunnel === "open")
      this.session = {
        ...this.session,
        state: "stopped",
        tunnel: "closed",
        detail: "Local capture closed; remote lease will expire if not resumed.",
      };
    for (const result of results) {
      if (result.status === "rejected") throw result.reason;
    }
  }
}
