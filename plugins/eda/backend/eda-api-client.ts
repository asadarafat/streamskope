import { randomUUID } from "node:crypto";
import { request } from "node:https";
import { setTimeout as delay } from "node:timers/promises";

import type { HostErrorCode, HostErrorStage } from "../../../src/features/kafka/contracts";
import type {
  EdaApiCredentialsInput,
  EdaCaptureApplicationStatus,
  EdaCaptureProducerKind,
  EdaCaptureSourceIdentity,
} from "../contracts";
import { EDA_CAPTURE_APPLICATION } from "../contracts";

import { EDA_CAPTURE_PUBLIC_KEY } from "./eda-capture-public-key";

const MAXIMUM_RESPONSE_BYTES = 16 * 1_048_576;
const REQUEST_TIMEOUT_MS = 15_000;
const APPLICATION_INSTALL_TIMEOUT_MS = 180_000;
const CAPTURE_APPLICATION_PATH = "/apps/capture.streamskope.io/v1alpha1";
const CAPTURE_APPLICATION_CATALOG = "streamskope";
const CAPTURE_APPLICATION_CATALOG_COLLECTION = "/apps/appstore.eda.nokia.com/v1/catalogs";
const CAPTURE_APPLICATION_CATALOG_URL = "https://github.com/asadarafat/streamskope.git";
const CAPTURE_SIGNING_KEY_NAME = "streamskope-capture";
const CAPTURE_SIGNING_KEY_COLLECTION = "/apps/appstore.eda.nokia.com/v1/signingkeys";
const CAPTURE_AGENT_PATH = "/core/httpproxy/v1/streamskope-capture/v1/sessions";

export interface EdaAgentCaptureSession {
  readonly id: string;
  readonly phase: string;
  readonly expiresAt?: string;
}

export interface EdaAgentCaptureRequest {
  readonly id: string;
  readonly leaseSeconds: number;
  readonly localPort: number;
  readonly source: EdaCaptureSourceIdentity;
}

export interface EdaApiTlsOptions {
  readonly caPem?: string;
  readonly clientSecret?: string;
  readonly rejectUnauthorized?: boolean;
  readonly serverName?: string;
}

export class EdaApiError extends Error {
  readonly code: HostErrorCode;
  readonly recovery: string;
  readonly retryable: boolean;
  readonly stage: HostErrorStage;
  readonly target: string;

  constructor(
    message: string,
    options: {
      readonly cause?: unknown;
      readonly code: HostErrorCode;
      readonly recovery: string;
      readonly retryable?: boolean;
      readonly stage: HostErrorStage;
      readonly target: string;
    },
  ) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = "EdaApiError";
    this.code = options.code;
    this.recovery = options.recovery;
    this.retryable = options.retryable ?? false;
    this.stage = options.stage;
    this.target = options.target;
  }
}

function exactBaseUrl(value: string): URL {
  let url: URL;
  try {
    url = new URL(value);
  } catch (error) {
    throw new EdaApiError("The EDA API URL is invalid.", {
      cause: error,
      code: "VALIDATION",
      recovery: "Enter the HTTPS base URL of the EDA API.",
      stage: "validation",
      target: value,
    });
  }
  if (
    url.protocol !== "https:" ||
    url.username.length > 0 ||
    url.password.length > 0 ||
    (url.pathname !== "/" && url.pathname !== "") ||
    url.search.length > 0 ||
    url.hash.length > 0
  ) {
    throw new EdaApiError("The EDA API URL must be an HTTPS origin.", {
      code: "VALIDATION",
      recovery: "Enter an HTTPS URL without credentials, path, query, or fragment.",
      stage: "validation",
      target: value,
    });
  }
  return url;
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

export class EdaApiClient {
  private readonly baseUrl: URL;
  private token: string | undefined;

  constructor(
    private readonly credentials: EdaApiCredentialsInput,
    private readonly tls: EdaApiTlsOptions = {},
    private readonly signal?: AbortSignal,
  ) {
    this.baseUrl = exactBaseUrl(credentials.baseUrl);
  }

  async createCaptureSession(input: EdaAgentCaptureRequest): Promise<EdaAgentCaptureSession> {
    await this.authenticate();
    return this.captureSession(
      await this.json("POST", CAPTURE_AGENT_PATH, {
        id: input.id,
        leaseSeconds: input.leaseSeconds,
        localPort: input.localPort,
        source: {
          apiVersion: input.source.apiVersion,
          kind: input.source.kind,
          name: input.source.name,
          namespace: input.source.namespace,
        },
      }),
      input.id,
    );
  }

  async getCaptureSession(id: string): Promise<EdaAgentCaptureSession> {
    await this.authenticate();
    return this.captureSession(
      await this.json("GET", `${CAPTURE_AGENT_PATH}/${encodeURIComponent(id)}`),
      id,
    );
  }

  async renewCaptureSession(id: string, leaseSeconds: number): Promise<EdaAgentCaptureSession> {
    await this.authenticate(true);
    return this.captureSession(
      await this.json("PATCH", `${CAPTURE_AGENT_PATH}/${encodeURIComponent(id)}/lease`, {
        leaseSeconds,
      }),
      id,
    );
  }

  async removeCaptureSession(id: string): Promise<void> {
    await this.authenticate(true);
    await this.json("DELETE", `${CAPTURE_AGENT_PATH}/${encodeURIComponent(id)}`);
  }

  async captureTunnelAccess(id: string): Promise<{
    readonly url: string;
    readonly authorization: string;
    readonly caPem?: string;
    readonly rejectUnauthorized: boolean;
    readonly serverName?: string;
  }> {
    await this.authenticate(true);
    const url = new URL(`${CAPTURE_AGENT_PATH}/${encodeURIComponent(id)}/tunnel`, this.baseUrl);
    url.protocol = "wss:";
    return {
      url: url.toString(),
      authorization: `Bearer ${this.token!}`,
      ...(this.tls.caPem === undefined ? {} : { caPem: this.tls.caPem }),
      rejectUnauthorized: this.tls.rejectUnauthorized ?? true,
      ...(this.tls.serverName === undefined ? {} : { serverName: this.tls.serverName }),
    };
  }

  private captureSession(value: unknown, expectedId: string): EdaAgentCaptureSession {
    const record = asRecord(value);
    if (record?.id !== expectedId || typeof record.phase !== "string") {
      throw new EdaApiError("EDA returned an invalid capture session.", {
        code: "INTERNAL",
        recovery: "Inspect the installed StreamSkope Capture application and retry.",
        stage: "backend",
        target: this.baseUrl.origin,
      });
    }
    return {
      id: expectedId,
      phase: record.phase,
      ...(typeof record.expiresAt === "string" ? { expiresAt: record.expiresAt } : {}),
    };
  }

  async listProducers(): Promise<readonly Record<string, unknown>[]> {
    await this.authenticate();
    const [clusterProducers, producers] = await Promise.all([
      this.json("GET", "/apps/kafka.eda.nokia.com/v1/clusterproducers"),
      this.json("GET", "/apps/kafka.eda.nokia.com/v1/producers"),
    ]);
    return [...this.items(clusterProducers), ...this.items(producers)];
  }

  async clusterVersion(): Promise<{
    readonly releaseVersion: string;
    readonly buildVersion: string;
  }> {
    await this.authenticate();
    const versions = asRecord(await this.json("GET", "/core/about/version"));
    const buildVersion = asRecord(versions?.eda)?.version;
    // EDA production builds append their timestamp and Git revision to the release.
    const release =
      typeof buildVersion === "string"
        ? /^v?(\d+\.\d+\.\d+)(?:-\d{10}-g[0-9a-f]+)?$/u.exec(buildVersion)
        : null;
    if (!release?.[1] || typeof buildVersion !== "string") {
      throw new EdaApiError("EDA returned an invalid product version.", {
        code: "BACKEND_UNAVAILABLE",
        recovery: "Verify that /core/about/version reports the EDA product release.",
        stage: "backend",
        target: this.baseUrl.origin,
      });
    }
    return { releaseVersion: `v${release[1]}`, buildVersion };
  }

  async captureApplicationStatus(): Promise<EdaCaptureApplicationStatus> {
    await this.authenticate();
    try {
      await this.json("GET", CAPTURE_APPLICATION_PATH);
      const health = asRecord(
        await this.json("GET", "/core/httpproxy/v1/streamskope-capture/healthz"),
      );
      if (health?.status !== "ready" || health.version !== EDA_CAPTURE_APPLICATION.version)
        return { ...EDA_CAPTURE_APPLICATION, state: "missing" };
      return { ...EDA_CAPTURE_APPLICATION, state: "installed" };
    } catch (error) {
      if (error instanceof EdaApiError && error.code === "PROFILE_NOT_FOUND") {
        return { ...EDA_CAPTURE_APPLICATION, state: "missing" };
      }
      throw error;
    }
  }

  async installCaptureApplication(): Promise<EdaCaptureApplicationStatus> {
    await this.authenticate();
    await this.ensureCaptureApplicationCatalog();
    await this.ensureCaptureApplicationSigningKey();
    const workflow = await this.json(
      "POST",
      "/workflows/v1/appstore.eda.nokia.com/v1/appinstallers",
      {
        apiVersion: "appstore.eda.nokia.com/v1",
        kind: "AppInstaller",
        metadata: {
          name: `streamskope-capture-${randomUUID().slice(0, 8)}`,
          namespace: "eda-system",
        },
        spec: {
          apps: [
            {
              appId: EDA_CAPTURE_APPLICATION.appId,
              catalog: CAPTURE_APPLICATION_CATALOG,
              version: { type: "semver", value: EDA_CAPTURE_APPLICATION.version },
            },
          ],
          autoProcessRequirements: ["strict"],
          operation: "install",
        },
      },
    );
    const submittedFailure = this.captureApplicationInstallationFailure(workflow);
    if (submittedFailure !== undefined) throw submittedFailure;
    const workflowMetadata = asRecord(asRecord(workflow)?.metadata);
    const installerName =
      typeof workflowMetadata?.name === "string" ? workflowMetadata.name : undefined;
    const deadline = Date.now() + APPLICATION_INSTALL_TIMEOUT_MS;
    while (Date.now() < deadline) {
      if (installerName !== undefined) {
        try {
          const currentWorkflow = await this.json(
            "GET",
            `/workflows/v1/appstore.eda.nokia.com/v1/appinstallers/${encodeURIComponent(installerName)}`,
          );
          const failure = this.captureApplicationInstallationFailure(currentWorkflow);
          if (failure !== undefined) throw failure;
        } catch (error) {
          if (!(error instanceof EdaApiError) || error.code !== "PROFILE_NOT_FOUND") throw error;
        }
      }
      try {
        const status = await this.captureApplicationStatus();
        if (status.state === "installed") return status;
      } catch (error) {
        if (
          !(error instanceof EdaApiError) ||
          error.code !== "BACKEND_UNAVAILABLE" ||
          !error.retryable ||
          !error.target.endsWith("/core/httpproxy/v1/streamskope-capture/healthz")
        ) {
          throw error;
        }
        // A zero-surge agent upgrade briefly has no healthy proxy endpoint.
      }
      await delay(1_000, undefined, { signal: this.signal });
    }
    throw new EdaApiError("The StreamSkope Capture application did not become ready in time.", {
      code: "TIMEOUT",
      recovery:
        "Open the EDA App Store to inspect the installation, then retry the readiness check.",
      retryable: true,
      stage: "backend",
      target: this.baseUrl.origin,
    });
  }

  private async ensureCaptureApplicationCatalog(): Promise<void> {
    let catalog: unknown;
    try {
      catalog = await this.json(
        "GET",
        `${CAPTURE_APPLICATION_CATALOG_COLLECTION}/${CAPTURE_APPLICATION_CATALOG}`,
      );
    } catch (error) {
      if (!(error instanceof EdaApiError) || error.code !== "PROFILE_NOT_FOUND") throw error;
      await this.json("POST", CAPTURE_APPLICATION_CATALOG_COLLECTION, {
        apiVersion: "appstore.eda.nokia.com/v1",
        kind: "Catalog",
        metadata: { name: CAPTURE_APPLICATION_CATALOG },
        spec: {
          remoteType: "git",
          remoteURL: CAPTURE_APPLICATION_CATALOG_URL,
          skipTLSVerify: false,
          title: "StreamSkope",
        },
      });
      return;
    }

    const record = asRecord(catalog);
    const metadata = asRecord(record?.metadata);
    const spec = asRecord(record?.spec);
    if (
      record?.kind !== "Catalog" ||
      metadata?.name !== CAPTURE_APPLICATION_CATALOG ||
      (spec?.remoteType !== undefined && spec.remoteType !== "git") ||
      spec?.remoteURL !== CAPTURE_APPLICATION_CATALOG_URL ||
      spec?.skipTLSVerify !== false
    ) {
      throw new EdaApiError("The existing StreamSkope catalog points to a different source.", {
        code: "BACKEND_UNAVAILABLE",
        recovery:
          "Ask an EDA administrator to inspect the existing streamskope catalog. StreamSkope will not overwrite or trust a catalog that does not use the canonical public repository with TLS verification enabled.",
        stage: "backend",
        target: `${this.baseUrl.origin}${CAPTURE_APPLICATION_CATALOG_COLLECTION}/${CAPTURE_APPLICATION_CATALOG}`,
      });
    }
    const catalogStatus = asRecord(record?.status);
    if (catalogStatus?.operational === false) {
      throw this.captureCatalogUnavailable(
        typeof catalogStatus.error === "string" ? catalogStatus.error : "",
      );
    }
  }

  private async ensureCaptureApplicationSigningKey(): Promise<void> {
    let signingKey: unknown;
    try {
      signingKey = await this.json(
        "GET",
        `${CAPTURE_SIGNING_KEY_COLLECTION}/${CAPTURE_SIGNING_KEY_NAME}`,
      );
    } catch (error) {
      if (!(error instanceof EdaApiError) || error.code !== "PROFILE_NOT_FOUND") throw error;
      await this.json("POST", CAPTURE_SIGNING_KEY_COLLECTION, {
        apiVersion: "appstore.eda.nokia.com/v1",
        kind: "SigningKey",
        metadata: { name: CAPTURE_SIGNING_KEY_NAME },
        spec: {
          publicKeys: [{ key: EDA_CAPTURE_PUBLIC_KEY, title: "StreamSkope Capture" }],
        },
      });
      return;
    }
    const record = asRecord(signingKey);
    const keys = asRecord(record?.spec)?.publicKeys;
    if (
      record?.kind !== "SigningKey" ||
      asRecord(record.metadata)?.name !== CAPTURE_SIGNING_KEY_NAME ||
      !Array.isArray(keys) ||
      keys.length !== 1 ||
      asRecord(keys[0])?.key !== EDA_CAPTURE_PUBLIC_KEY
    ) {
      throw new EdaApiError("The existing StreamSkope signing key differs from this release.", {
        code: "BACKEND_UNAVAILABLE",
        recovery:
          "Ask an EDA administrator to inspect the existing streamskope-capture SigningKey. StreamSkope will not replace a conflicting trust anchor.",
        stage: "backend",
        target: `${this.baseUrl.origin}${CAPTURE_SIGNING_KEY_COLLECTION}/${CAPTURE_SIGNING_KEY_NAME}`,
      });
    }
  }

  private captureCatalogUnavailable(detail: string): EdaApiError {
    const normalized = detail.toLowerCase();
    if (
      normalized.includes("cannot open directory") &&
      normalized.includes("/vendors/streamskope/apps/capture")
    ) {
      return new EdaApiError(
        "EDA cannot read the published StreamSkope Capture catalog directory.",
        {
          code: "BACKEND_UNAVAILABLE",
          recovery:
            "Verify the published version tag and that vendors/streamskope/apps/capture is a physical directory with no catalog navigation symlink, then refresh the EDA catalog.",
          retryable: true,
          stage: "backend",
          target: this.baseUrl.origin,
        },
      );
    }
    return new EdaApiError("The StreamSkope EDA catalog is unavailable.", {
      code: "BACKEND_UNAVAILABLE",
      recovery:
        "Open EDA System Administration > Catalogs, inspect the streamskope catalog status, correct the reported source issue, and retry.",
      retryable: true,
      stage: "backend",
      target: `${this.baseUrl.origin}${CAPTURE_APPLICATION_CATALOG_COLLECTION}/${CAPTURE_APPLICATION_CATALOG}`,
    });
  }

  private captureApplicationInstallationFailure(value: unknown): EdaApiError | undefined {
    const status = asRecord(asRecord(value)?.status);
    const result = typeof status?.result === "string" ? status.result.toLowerCase() : "";
    if (!result.includes("fail") && !result.includes("error")) return undefined;
    const detail =
      typeof status?.error === "string"
        ? status.error.toLowerCase()
        : typeof status?.message === "string"
          ? status.message.toLowerCase()
          : "";
    if (detail.includes("cannot open directory")) return this.captureCatalogUnavailable(detail);
    if (detail.includes("catalog") && detail.includes("not found")) {
      return new EdaApiError("The published StreamSkope Capture release is unavailable to EDA.", {
        code: "BACKEND_UNAVAILABLE",
        recovery:
          "Refresh the StreamSkope catalog in EDA and confirm this exact application version is published before retrying. Source discovery and existing Kafka destinations remain available.",
        stage: "backend",
        target: this.baseUrl.origin,
      });
    }
    return new EdaApiError("EDA rejected the StreamSkope Capture application installation.", {
      code: "BACKEND_UNAVAILABLE",
      recovery:
        "Open the EDA App Store to inspect the failed installation and resolve its catalog, signature, requirement or image error before retrying.",
      stage: "backend",
      target: this.baseUrl.origin,
    });
  }

  async getProducer(identity: EdaCaptureSourceIdentity): Promise<Record<string, unknown>> {
    await this.authenticate();
    const path =
      identity.kind === "ClusterProducer"
        ? `/apps/kafka.eda.nokia.com/v1/clusterproducers/${encodeURIComponent(identity.name)}`
        : `/apps/kafka.eda.nokia.com/v1/namespaces/${encodeURIComponent(identity.namespace)}/producers/${encodeURIComponent(identity.name)}`;
    const producer = asRecord(await this.json("GET", path));
    if (producer === undefined) {
      throw new EdaApiError("EDA returned an invalid Kafka producer.", {
        code: "INTERNAL",
        recovery: "Inspect the EDA Kafka exporter resource and retry.",
        stage: "backend",
        target: `${identity.namespace}/${identity.name}`,
      });
    }
    return producer;
  }

  async replaceCaptureExporter(
    kind: EdaCaptureProducerKind,
    namespace: string,
    body: Record<string, unknown>,
  ): Promise<void> {
    await this.authenticate();
    const collection =
      kind === "ClusterProducer"
        ? "/apps/kafka.eda.nokia.com/v1/clusterproducers"
        : `/apps/kafka.eda.nokia.com/v1/namespaces/${encodeURIComponent(namespace)}/producers`;
    try {
      await this.json("DELETE", `${collection}/streamskope-capture`);
    } catch (error) {
      if (!(error instanceof EdaApiError) || error.code !== "PROFILE_NOT_FOUND") throw error;
    }
    await this.json("POST", collection, body);
  }

  private items(value: unknown): readonly Record<string, unknown>[] {
    const items = asRecord(value)?.items;
    return Array.isArray(items)
      ? items.flatMap((item) => {
          const parsed = asRecord(item);
          return parsed === undefined ? [] : [parsed];
        })
      : [];
  }

  private async authenticate(force = false): Promise<void> {
    if (this.token !== undefined && !force) return;
    const clientSecret = this.tls.clientSecret ?? (await this.discoverClientSecret());
    const token = asRecord(
      await this.form("/core/httpproxy/v1/keycloak/realms/eda/protocol/openid-connect/token", {
        client_id: "eda",
        client_secret: clientSecret,
        grant_type: "password",
        password: this.credentials.password,
        scope: "openid",
        username: this.credentials.username,
      }),
    )?.access_token;
    if (typeof token !== "string" || token.length === 0) {
      throw this.authenticationError();
    }
    this.token = token;
  }

  private async discoverClientSecret(): Promise<string> {
    const adminToken = asRecord(
      await this.form("/core/httpproxy/v1/keycloak/realms/master/protocol/openid-connect/token", {
        client_id: "admin-cli",
        grant_type: "password",
        password: this.credentials.password,
        username: this.credentials.username,
      }),
    )?.access_token;
    if (typeof adminToken !== "string" || adminToken.length === 0) {
      throw this.authenticationError();
    }
    const clients = await this.json(
      "GET",
      "/core/httpproxy/v1/keycloak/admin/realms/eda/clients",
      undefined,
      adminToken,
    );
    if (!Array.isArray(clients)) throw this.authenticationError();
    const client = clients.map(asRecord).find((candidate) => candidate?.clientId === "eda");
    if (typeof client?.id !== "string") throw this.authenticationError();
    const secret = asRecord(
      await this.json(
        "GET",
        `/core/httpproxy/v1/keycloak/admin/realms/eda/clients/${encodeURIComponent(client.id)}/client-secret`,
        undefined,
        adminToken,
      ),
    )?.value;
    if (typeof secret !== "string" || secret.length === 0) throw this.authenticationError();
    return secret;
  }

  private authenticationError(): EdaApiError {
    return new EdaApiError("EDA API authentication failed.", {
      code: "AUTHORIZATION_DENIED",
      recovery:
        "Verify the EDA administrator credentials, or have an administrator configure the EDA API client secret.",
      stage: "authorization",
      target: this.baseUrl.origin,
    });
  }

  private form(path: string, values: Readonly<Record<string, string>>): Promise<unknown> {
    return this.request(
      "POST",
      path,
      new URLSearchParams(values).toString(),
      "application/x-www-form-urlencoded",
    );
  }

  private json(
    method: "DELETE" | "GET" | "PATCH" | "POST",
    path: string,
    body?: unknown,
    token = this.token,
  ): Promise<unknown> {
    return this.request(
      method,
      path,
      body === undefined ? undefined : JSON.stringify(body),
      "application/json",
      token,
    );
  }

  private request(
    method: "DELETE" | "GET" | "PATCH" | "POST",
    path: string,
    body: string | undefined,
    contentType: string,
    token?: string,
  ): Promise<unknown> {
    const url = new URL(path, this.baseUrl);
    return new Promise((resolve, reject) => {
      const request_ = request(
        url,
        {
          ...(this.tls.caPem === undefined ? {} : { ca: this.tls.caPem }),
          ...(this.tls.serverName === undefined ? {} : { servername: this.tls.serverName }),
          headers: {
            accept: "application/json",
            ...(body === undefined
              ? {}
              : {
                  "content-length": String(Buffer.byteLength(body)),
                  "content-type": contentType,
                }),
            ...(token === undefined ? {} : { authorization: `Bearer ${token}` }),
          },
          method,
          rejectUnauthorized: this.tls.rejectUnauthorized ?? true,
          timeout: REQUEST_TIMEOUT_MS,
          ...(this.signal === undefined ? {} : { signal: this.signal }),
        },
        (response) => {
          const chunks: Buffer[] = [];
          let bytes = 0;
          response.on("data", (chunk: Buffer) => {
            bytes += chunk.byteLength;
            if (bytes > MAXIMUM_RESPONSE_BYTES) {
              response.destroy(new Error("EDA API response exceeded its size limit."));
              return;
            }
            chunks.push(chunk);
          });
          response.once("error", (error) => reject(this.transportError(error)));
          response.once("end", () => {
            const status = response.statusCode ?? 0;
            if (status === 404) {
              reject(
                new EdaApiError("The EDA API resource was not found.", {
                  code: "PROFILE_NOT_FOUND",
                  recovery: "Refresh EDA resources and retry.",
                  stage: "backend",
                  target: url.toString(),
                }),
              );
              return;
            }
            if (status === 401 || status === 403) {
              reject(this.authenticationError());
              return;
            }
            if (status < 200 || status > 299) {
              reject(
                new EdaApiError(`EDA API request failed with status ${String(status)}.`, {
                  code: "BACKEND_UNAVAILABLE",
                  recovery: "Verify EDA API availability and retry.",
                  retryable: status >= 500,
                  stage: "backend",
                  target: url.toString(),
                }),
              );
              return;
            }
            const content = Buffer.concat(chunks).toString("utf8");
            if (content.length === 0) {
              resolve({});
              return;
            }
            try {
              resolve(JSON.parse(content) as unknown);
            } catch (error) {
              reject(this.transportError(error));
            }
          });
        },
      );
      request_.once("timeout", () => request_.destroy(new Error("EDA API request timed out.")));
      request_.once("error", (error) => reject(this.transportError(error)));
      if (body !== undefined) request_.write(body);
      request_.end();
    });
  }

  private transportError(error: unknown): EdaApiError {
    return error instanceof EdaApiError
      ? error
      : new EdaApiError("The EDA API could not be reached securely.", {
          cause: error,
          code: "BACKEND_UNAVAILABLE",
          recovery: "Verify the EDA API URL, certificate trust, and network path.",
          retryable: true,
          stage: "backend",
          target: this.baseUrl.origin,
        });
  }
}
