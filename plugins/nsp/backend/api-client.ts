import { createHash } from "node:crypto";
import { request } from "node:https";
import { setTimeout as delay } from "node:timers/promises";

import { isTargetVersionCompatible, parsePluginManifest } from "../../../src/plugins/validation";
import manifestJson from "../manifest.json";

import {
  NSP_WORKFLOW_DEFINITION,
  NSP_WORKFLOW_FINGERPRINT,
  NSP_WORKFLOW_NAME,
  NSP_WORKFLOW_VERSION,
} from "./workflow";

const API = "/wfm/api/v1";
const AUTH = "/rest-gateway/rest/api/v1/auth";
const MAXIMUM_RESPONSE_BYTES = 8 * 1_048_576;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;
const TERMINAL = new Set(["SUCCESS", "ERROR", "CANCELLED"]);
const manifest = parsePluginManifest(manifestJson);

export interface NspVersion {
  readonly raw: string;
  readonly product: string;
  readonly build: number;
}

function supportedVersions(): string {
  const target = manifest.compatibility?.target;
  if (target?.system !== "nsp") throw new Error("The NSP plugin target is not declared.");
  return `NSP ${target.minimum} through ${target.maximum} (inclusive)`;
}

/** Accept the documented product build, never an API version or a partial version match. */
export function parseNspVersion(value: unknown): NspVersion {
  const wrapper =
    value !== null && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>).response
      : undefined;
  const response =
    wrapper !== null && typeof wrapper === "object" && !Array.isArray(wrapper)
      ? (wrapper as Record<string, unknown>)
      : undefined;
  const raw = response?.data;
  const match =
    response?.status === 0 && typeof raw === "string" && raw.length <= 128
      ? /^NSP-CN-((?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*))-rel\.((?:0|[1-9]\d*))$/u.exec(raw)
      : null;
  if (
    !match?.[1] ||
    match[0] !== raw ||
    match[2] === undefined ||
    ![...match[1].split("."), match[2]].every((part) => Number.isSafeInteger(Number(part)))
  ) {
    throw new NspApiError(
      `The NSP API did not return a recognized product version. This plugin supports ${supportedVersions()}.`,
      "VERSION",
    );
  }
  return { raw: match[0], product: match[1], build: Number(match[2]) };
}

export function requireNspTargetVersion(version: NspVersion): void {
  if (!isTargetVersionCompatible(manifest, version.product)) {
    throw new NspApiError(
      `The running NSP product version is incompatible. This plugin supports ${supportedVersions()}.`,
      "VERSION",
    );
  }
}

type JsonRecord = Record<string, unknown>;
export interface NspApiCredentials {
  readonly apiUrl: string;
  readonly username: string;
  readonly password: string;
  readonly verifyCertificate: boolean;
}
export interface NspApiOptions {
  readonly caPem?: string;
  readonly pollIntervalMs?: number;
  readonly executionTimeoutMs?: number;
  readonly requestTimeoutMs?: number;
}
export interface NspTrustMaterial {
  readonly truststoreBase64: string;
  readonly truststorePassword: string;
  readonly sha256: string;
  readonly certificateCount: number;
}
export interface NspWorkflow {
  readonly id: string;
  readonly name: string;
  readonly fingerprint: string;
}
export interface NspExecutionCallbacks {
  readonly onExecution?: (id: string) => Promise<void>;
}
export type NspApiErrorCode =
  | "VALIDATION"
  | "VERSION"
  | "AUTHENTICATION"
  | "AUTHORIZATION"
  | "NOT_FOUND"
  | "CONFLICT"
  | "NETWORK"
  | "CANCELLED"
  | "WORKFLOW"
  | "CLEANUP";

export class NspApiError extends Error {
  constructor(
    message: string,
    readonly code: NspApiErrorCode,
    readonly status?: number,
  ) {
    super(message);
    this.name = "NspApiError";
  }
}
export class NspCleanupError extends NspApiError {
  constructor(
    readonly requestId: string,
    readonly executionId?: string,
  ) {
    super(
      "NSP execution cleanup is incomplete. Retry cleanup with the same NSP account.",
      "CLEANUP",
    );
    this.name = "NspCleanupError";
  }
}

function record(value: unknown): JsonRecord {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new NspApiError("NSP returned an invalid response.", "WORKFLOW");
  }
  return value as JsonRecord;
}
function data(value: unknown): unknown {
  return record(record(value).response).data;
}
function item(value: unknown): JsonRecord {
  const result = data(value);
  return record(Array.isArray(result) ? result[0] : result);
}
function id(value: unknown): string {
  if (typeof value !== "string" || !UUID.test(value)) {
    throw new NspApiError("NSP returned an invalid resource identity.", "WORKFLOW");
  }
  return value;
}
function description(requestId: string): string {
  return `streamskope.nsp:${id(requestId)}`;
}
function apiOrigin(value: string): URL {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new NspApiError("Enter a valid NSP HTTPS origin.", "VALIDATION");
  }
  if (
    url.protocol !== "https:" ||
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.search ||
    url.hash
  ) {
    throw new NspApiError(
      "The NSP API URL must be an HTTPS origin without a path or credentials.",
      "VALIDATION",
    );
  }
  return url;
}

export class NspApiClient {
  private readonly origin: URL;
  private token: string | undefined;
  private observedVersion: NspVersion | undefined;
  constructor(
    private readonly credentials: NspApiCredentials,
    private readonly options: NspApiOptions = {},
    private readonly signal?: AbortSignal,
  ) {
    this.origin = apiOrigin(credentials.apiUrl);
  }

  async authenticate(cleanup = false): Promise<void> {
    if (this.token) return;
    const result = record(
      await this.json(
        "POST",
        `${AUTH}/token`,
        {
          grant_type: "client_credentials",
        },
        cleanup,
        `Basic ${Buffer.from(`${this.credentials.username}:${this.credentials.password}`).toString("base64")}`,
      ),
    );
    if (typeof result.access_token !== "string" || !result.access_token) {
      throw new NspApiError("NSP did not return an access token.", "AUTHENTICATION");
    }
    this.token = result.access_token;
  }

  async readVersion(): Promise<NspVersion> {
    if (this.observedVersion !== undefined) return this.observedVersion;
    await this.authenticate();
    try {
      this.observedVersion = parseNspVersion(await this.json("GET", "/sdn/api/v4/system/version"));
      return this.observedVersion;
    } catch (error) {
      if (error instanceof NspApiError && ["VERSION", "CANCELLED"].includes(error.code))
        throw error;
      throw new NspApiError(
        `The NSP product version could not be verified through its API. This plugin supports ${supportedVersions()}.`,
        "VERSION",
      );
    }
  }

  async ensureWorkflow(): Promise<NspWorkflow> {
    requireNspTargetVersion(await this.readVersion());
    let workflow: JsonRecord;
    try {
      workflow = item(await this.json("GET", `${API}/workflow/${NSP_WORKFLOW_NAME}`));
    } catch (error) {
      if (!(error instanceof NspApiError) || error.code !== "NOT_FOUND") throw error;
      try {
        await this.json("POST", `${API}/workflow`, {
          yaml: NSP_WORKFLOW_DEFINITION,
          version: NSP_WORKFLOW_VERSION,
          provided_by: "StreamSkope",
        });
      } catch (creationError) {
        // A lost response or simultaneous install can leave the helper already created.
        // Read it back and verify exact content before adoption; never overwrite it.
        if (
          !(creationError instanceof NspApiError) ||
          !["CONFLICT", "NETWORK"].includes(creationError.code)
        )
          throw creationError;
      }
      workflow = item(await this.json("GET", `${API}/workflow/${NSP_WORKFLOW_NAME}`));
    }
    const workflowId = this.verifyWorkflow(workflow);
    if (record(workflow.details).status !== "PUBLISHED") {
      if (record(workflow.details).status !== "DRAFT") {
        throw new NspApiError(
          "The StreamSkope NSP helper is not available for publication.",
          "CONFLICT",
        );
      }
      await this.json("PUT", `${API}/workflow/${workflowId}/status`, { status: "PUBLISHED" });
      workflow = item(await this.json("GET", `${API}/workflow/${workflowId}`));
      this.verifyWorkflow(workflow);
      if (record(workflow.details).status !== "PUBLISHED") {
        throw new NspApiError("The StreamSkope NSP helper could not be published.", "WORKFLOW");
      }
    }
    return { id: workflowId, name: NSP_WORKFLOW_NAME, fingerprint: NSP_WORKFLOW_FINGERPRINT };
  }

  async retrieveTrust(
    requestId: string,
    callbacks: NspExecutionCallbacks = {},
  ): Promise<NspTrustMaterial> {
    const marker = description(requestId);
    const workflow = await this.ensureWorkflow();
    let executionId: string | undefined;
    try {
      const existing = await this.findExecutions(requestId, false);
      if (existing.length > 1) {
        throw new NspApiError(
          "Multiple NSP executions match this request. Run cleanup before retrying.",
          "CONFLICT",
        );
      }
      let execution: JsonRecord;
      if (existing[0]) {
        execution = existing[0];
      } else {
        try {
          execution = item(
            await this.json("POST", `${API}/execution`, {
              id: requestId,
              workflow_id: workflow.id,
              description: marker,
              input: {},
              params: {},
              notifyKafka: false,
            }),
          );
        } catch (error) {
          // Do not repeat an uncertain POST. Reconcile the exact request marker.
          const recovered = await this.findExecutions(requestId, true);
          if (recovered.length !== 1) throw error;
          execution = recovered[0]!;
        }
      }
      executionId = this.verifyExecution(execution, requestId);
      await callbacks.onExecution?.(executionId);
      const deadline = Date.now() + (this.options.executionTimeoutMs ?? 60_000);
      while (!TERMINAL.has(String(execution.state))) {
        if (Date.now() >= deadline) {
          throw new NspApiError("NSP trust retrieval timed out.", "WORKFLOW");
        }
        await this.pause(false);
        execution = item(await this.json("GET", `${API}/execution/${executionId}`));
        this.verifyExecution(execution, requestId);
      }
      if (execution.state !== "SUCCESS") {
        throw new NspApiError(
          "NSP could not retrieve the mounted Kafka truststore. Check workflow permissions and certificate mounts.",
          "WORKFLOW",
        );
      }
      return validateTrustMaterial(record(execution.output).result);
    } finally {
      await this.cleanupExecution(requestId, executionId);
    }
  }

  async cleanupExecution(requestId: string, executionId?: string): Promise<void> {
    description(requestId);
    if (executionId !== undefined) id(executionId);
    try {
      await this.authenticate(true);
      const executions = executionId
        ? [await this.executionIfPresent(executionId)]
        : await this.findExecutions(requestId, true);
      if (!executionId && !executions.some((execution) => execution?.id === requestId)) {
        const direct = await this.executionIfPresent(requestId);
        if (direct) executions.push(direct);
      }
      for (let execution of executions) {
        if (!execution) continue;
        const ownedId = this.verifyExecution(execution, requestId);
        if (!TERMINAL.has(String(execution.state))) {
          await this.json("PUT", `${API}/execution/${ownedId}`, { state: "CANCELLED" }, true);
          const deadline = Date.now() + (this.options.executionTimeoutMs ?? 60_000);
          while (!TERMINAL.has(String(execution.state))) {
            if (Date.now() >= deadline) throw new NspCleanupError(requestId, ownedId);
            await this.pause(true);
            execution = await this.executionIfPresent(ownedId);
            if (!execution) break;
            this.verifyExecution(execution, requestId);
          }
          if (!execution) continue;
        }
        try {
          await this.json("DELETE", `${API}/execution/${ownedId}`, undefined, true);
        } catch (error) {
          if (!(error instanceof NspApiError) || error.code !== "NOT_FOUND") throw error;
        }
        if (await this.executionIfPresent(ownedId)) throw new NspCleanupError(requestId, ownedId);
      }
    } catch {
      throw new NspCleanupError(requestId, executionId);
    }
  }

  async close(): Promise<void> {
    if (!this.token) return;
    await this.json(
      "POST",
      `${AUTH}/revocation`,
      { token: this.token, token_type_hint: "client_credentials" },
      true,
      `Basic ${Buffer.from(`${this.credentials.username}:${this.credentials.password}`).toString("base64")}`,
      true,
    );
    this.token = undefined;
    this.observedVersion = undefined;
  }

  private verifyWorkflow(workflow: JsonRecord): string {
    const definition = workflow.definition;
    const draft = record(workflow.details).local_definition;
    if (
      workflow.name !== NSP_WORKFLOW_NAME ||
      typeof definition !== "string" ||
      createHash("sha256").update(definition).digest("hex") !== NSP_WORKFLOW_FINGERPRINT ||
      (typeof draft === "string" && draft.length > 0 && draft !== NSP_WORKFLOW_DEFINITION)
    ) {
      throw new NspApiError(
        "A different workflow already uses the StreamSkope helper name. It was left unchanged.",
        "CONFLICT",
      );
    }
    return id(workflow.id);
  }

  private verifyExecution(execution: JsonRecord, requestId: string): string {
    if (
      execution.workflow_name !== NSP_WORKFLOW_NAME ||
      execution.description !== description(requestId)
    ) {
      throw new NspApiError("NSP execution ownership could not be verified.", "CONFLICT");
    }
    return id(execution.id);
  }

  private async executionIfPresent(executionId: string): Promise<JsonRecord | undefined> {
    try {
      return item(await this.json("GET", `${API}/execution/${id(executionId)}`, undefined, true));
    } catch (error) {
      if (error instanceof NspApiError && error.code === "NOT_FOUND") return undefined;
      throw error;
    }
  }

  private async findExecutions(requestId: string, cleanup: boolean): Promise<JsonRecord[]> {
    const marker = description(requestId);
    const query = new URLSearchParams({
      workflow_name: NSP_WORKFLOW_NAME,
      description: marker,
      limit: "100",
    });
    const response = record(
      record(await this.json("GET", `${API}/execution?${query.toString()}`, undefined, cleanup))
        .response,
    );
    if (
      (response.next && response.next !== "None") ||
      (typeof response.totalRows === "number" && response.totalRows > 100)
    ) {
      throw new NspApiError("NSP execution reconciliation exceeded its result limit.", "WORKFLOW");
    }
    if (!Array.isArray(response.data))
      throw new NspApiError("NSP returned invalid execution results.", "WORKFLOW");
    return response.data
      .map(record)
      .filter((entry) => entry.description === marker && entry.workflow_name === NSP_WORKFLOW_NAME);
  }

  private async pause(cleanup: boolean): Promise<void> {
    try {
      await delay(
        this.options.pollIntervalMs ?? 500,
        undefined,
        cleanup || !this.signal ? {} : { signal: this.signal },
      );
    } catch {
      throw new NspApiError("NSP connection setup was cancelled.", "CANCELLED");
    }
  }

  private async json(
    method: string,
    path: string,
    body?: unknown,
    cleanup = false,
    authorization?: string,
    form = false,
  ): Promise<unknown> {
    if (!cleanup && this.signal?.aborted)
      throw new NspApiError("NSP connection setup was cancelled.", "CANCELLED");
    const payload =
      body === undefined
        ? undefined
        : form
          ? new URLSearchParams(body as Record<string, string>).toString()
          : JSON.stringify(body);
    return new Promise((resolve, reject) => {
      const req = request(
        new URL(path, this.origin),
        {
          method,
          rejectUnauthorized: this.credentials.verifyCertificate,
          ...(this.options.caPem === undefined ? {} : { ca: this.options.caPem }),
          ...(!cleanup && this.signal ? { signal: this.signal } : {}),
          headers: {
            Accept: "application/json",
            Authorization: authorization ?? `Bearer ${this.token ?? ""}`,
            ...(payload === undefined
              ? {}
              : {
                  "Content-Type": form ? "application/x-www-form-urlencoded" : "application/json",
                  "Content-Length": Buffer.byteLength(payload),
                }),
          },
        },
        (response) => {
          let bytes = 0;
          const chunks: Buffer[] = [];
          response.on("data", (chunk: Buffer) => {
            bytes += chunk.length;
            if (bytes > MAXIMUM_RESPONSE_BYTES) {
              req.destroy();
              reject(new NspApiError("NSP response exceeded its size limit.", "WORKFLOW"));
            } else chunks.push(chunk);
          });
          response.on("error", () =>
            reject(new NspApiError("NSP response could not be read.", "NETWORK")),
          );
          response.on("end", () => {
            const status = response.statusCode ?? 500;
            if (status < 200 || status >= 300) {
              const code: NspApiErrorCode =
                status === 401
                  ? "AUTHENTICATION"
                  : status === 403
                    ? "AUTHORIZATION"
                    : status === 404
                      ? "NOT_FOUND"
                      : status === 409
                        ? "CONFLICT"
                        : "WORKFLOW";
              const message =
                status === 401
                  ? "NSP authentication failed."
                  : status === 403
                    ? "This NSP account cannot manage or execute the connection helper workflow."
                    : status === 404
                      ? "The NSP API resource was not found."
                      : "NSP rejected the workflow request.";
              reject(new NspApiError(message, code, status));
              return;
            }
            try {
              const raw = Buffer.concat(chunks).toString("utf8");
              const parsed: unknown = raw && !form ? JSON.parse(raw) : {};
              resolve(parsed);
            } catch {
              reject(new NspApiError("NSP returned invalid JSON.", "WORKFLOW"));
            }
          });
        },
      );
      const timer = setTimeout(() => {
        reject(new NspApiError("The NSP API request timed out.", "NETWORK"));
        req.destroy();
      }, this.options.requestTimeoutMs ?? 15_000);
      req.on("close", () => clearTimeout(timer));
      req.on("error", () =>
        reject(
          new NspApiError(
            !cleanup && this.signal?.aborted
              ? "NSP connection setup was cancelled."
              : "The NSP API connection failed. Check its address and certificate trust.",
            !cleanup && this.signal?.aborted ? "CANCELLED" : "NETWORK",
          ),
        ),
      );
      req.end(payload);
    });
  }
}

export function validateTrustMaterial(value: unknown): NspTrustMaterial {
  const result = record(value);
  const encoded = result.truststore_base64;
  const password = result.truststore_password;
  if (
    result.store_type !== "JKS" ||
    result.encoding !== "base64" ||
    typeof encoded !== "string" ||
    encoded.length > 2_796_204 ||
    encoded.length % 4 !== 0 ||
    !/^[A-Za-z0-9+/]*={0,2}$/u.test(encoded) ||
    typeof password !== "string" ||
    !password ||
    password.length > 4096 ||
    typeof result.certificate_count !== "number" ||
    !Number.isInteger(result.certificate_count) ||
    result.certificate_count < 1
  ) {
    throw new NspApiError("NSP returned invalid Kafka trust material.", "WORKFLOW");
  }
  const bytes = Buffer.from(encoded, "base64");
  const fingerprint = createHash("sha256").update(bytes).digest("hex");
  if (
    bytes.length < 12 ||
    bytes.length > 2_097_152 ||
    bytes.toString("base64") !== encoded ||
    bytes.length !== result.size_bytes ||
    bytes.readUInt32BE(0) !== 0xfeedfeed ||
    result.sha256 !== fingerprint
  ) {
    throw new NspApiError("NSP Kafka truststore integrity verification failed.", "WORKFLOW");
  }
  return {
    truststoreBase64: encoded,
    truststorePassword: password,
    sha256: fingerprint,
    certificateCount: result.certificate_count,
  };
}
