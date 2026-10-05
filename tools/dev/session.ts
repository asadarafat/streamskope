import { createHash, randomUUID } from "node:crypto";
import { lstat, mkdir, open, readdir, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  BROWSER_DEVELOPMENT_GATEWAY_PATH,
  browserDevelopmentSessionCookie,
  HOST_PROTOCOL_VERSION,
} from "../../src/features/kafka/contracts";
import {
  launchWebDevelopment,
  type DevelopmentBackend,
  type DevelopmentProviderSource,
  type RunningWebDevelopment,
  type WebDevelopmentLaunchOptions,
} from "../../src/platform/dev-host";
import { developmentOrigin, resolveDevelopmentNetwork } from "../../src/platform/dev-host/network";
import type { ProviderHostRegistry } from "../../src/platform/node/provider-host";
import type { PluginRendererAsset } from "../../src/platform/node/plugins/runtime";

import { stopWebDevelopmentOwner } from "./stop-owner";

const SESSION_DIRECTORY_PREFIX = "streamskope-web-development";
const SESSION_RECORD_BYTES = 8 * 1_024;
const SESSION_RECORD_MARKER = "streamskope-web-development";
const SESSION_RECORD_VERSION = 1;
const SESSION_READINESS_TIMEOUT_MS = 2_000;

export interface WebDevelopmentCommandOptions {
  readonly hostPort: number;
  readonly publicHostname?: string;
  readonly rendererPort: number;
  readonly rendererRoot: string;
  readonly sessionDirectory?: string;
}

export interface WebDevelopmentSessionIdentity {
  readonly hostPort: number;
  readonly publicHostname: string;
  readonly rendererPort: number;
  readonly rendererRoot: string;
}

interface WebDevelopmentCommandBaseDependencies {
  readonly stopOwnedSession?: (
    pid: number,
    identity: WebDevelopmentSessionIdentity,
  ) => Promise<void>;
  readonly prepare?: () => Promise<void>;
  readonly isExistingSessionReady?: (
    browserUrl: string,
    identity: WebDevelopmentSessionIdentity,
  ) => Promise<boolean>;
  readonly isProcessAlive?: (pid: number) => boolean;
  readonly launch?: (options: WebDevelopmentLaunchOptions) => Promise<RunningWebDevelopment>;
  readonly openBrowser?: (browserUrl: string) => Promise<void>;
  readonly ownerPid?: number;
}

export interface WebDevelopmentProviderApplication {
  readonly providers: ProviderHostRegistry;
  readonly pluginAsset?: (pathname: string) => Promise<PluginRendererAsset | undefined>;
}

export type WebDevelopmentCommandDependencies = WebDevelopmentCommandBaseDependencies &
  (
    | {
        readonly createBackend: () => DevelopmentBackend | Promise<DevelopmentBackend>;
        readonly createProviders?: never;
      }
    | {
        readonly createProviders: () =>
          WebDevelopmentProviderApplication | Promise<WebDevelopmentProviderApplication>;
        readonly createBackend?: never;
      }
  );

export interface RunningWebDevelopmentCommand {
  readonly browserOpenError: string | null;
  readonly browserUrl: string;
  readonly reused: boolean;
  close(): Promise<void>;
}

interface SessionRecord {
  readonly browserUrl: string | null;
  readonly identity: WebDevelopmentSessionIdentity;
  readonly marker: typeof SESSION_RECORD_MARKER;
  readonly nonce: string;
  readonly ownerPid: number;
  readonly state: "ready" | "starting";
  readonly version: typeof SESSION_RECORD_VERSION;
}

interface SessionLease {
  publish(browserUrl: string): Promise<void>;
  release(): Promise<void>;
}

type SessionClaim =
  | {
      readonly kind: "existing";
      readonly browserUrl: string;
    }
  | {
      readonly kind: "owner";
      readonly lease: SessionLease;
    };

export class WebDevelopmentSessionError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "WebDevelopmentSessionError";
  }
}

export class WebDevelopmentSessionConflictError extends WebDevelopmentSessionError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "WebDevelopmentSessionConflictError";
  }
}

function isNodeError(error: unknown): error is NodeJS.ErrnoException {
  return error instanceof Error && "code" in error;
}

async function attemptBrowserOpen(
  browserUrl: string,
  openBrowser: ((url: string) => Promise<void>) | undefined,
): Promise<string | null> {
  if (openBrowser === undefined) {
    return null;
  }
  try {
    await openBrowser(browserUrl);
    return null;
  } catch (error) {
    const summary = error instanceof Error ? error.message : "Unknown browser opener failure.";
    return summary.replaceAll(browserUrl, "<launch URL>");
  }
}

function defaultSessionDirectory(): string {
  const owner = typeof process.getuid === "function" ? String(process.getuid()) : "user";
  return join(tmpdir(), `${SESSION_DIRECTORY_PREFIX}-${owner}`);
}

function sessionFileName(identity: WebDevelopmentSessionIdentity): string {
  const digest = createHash("sha256")
    .update(
      JSON.stringify([
        identity.rendererRoot,
        identity.publicHostname,
        identity.rendererPort,
        identity.hostPort,
      ]),
    )
    .digest("hex")
    .slice(0, 24);
  return `session-${digest}.json`;
}

function privateMode(mode: number): boolean {
  return process.platform === "win32" || (mode & 0o077) === 0;
}

async function ensurePrivateDirectory(path: string): Promise<void> {
  try {
    await mkdir(path, { mode: 0o700, recursive: false });
  } catch (error) {
    if (!isNodeError(error) || error.code !== "EEXIST") {
      throw error;
    }
  }

  const metadata = await lstat(path);
  const ownedByCurrentUser =
    typeof process.getuid !== "function" || metadata.uid === process.getuid();
  if (
    !metadata.isDirectory() ||
    metadata.isSymbolicLink() ||
    !ownedByCurrentUser ||
    !privateMode(metadata.mode)
  ) {
    throw new WebDevelopmentSessionError(
      `Development session directory ${path} must be a private directory owned by the current user.`,
    );
  }
}

function sameIdentity(
  left: WebDevelopmentSessionIdentity,
  right: WebDevelopmentSessionIdentity,
): boolean {
  return (
    left.hostPort === right.hostPort &&
    left.publicHostname === right.publicHostname &&
    left.rendererPort === right.rendererPort &&
    left.rendererRoot === right.rendererRoot
  );
}

function parseIdentity(value: unknown): WebDevelopmentSessionIdentity | null {
  if (typeof value !== "object" || value === null) {
    return null;
  }
  const candidate = value as Record<string, unknown>;
  if (
    !Number.isSafeInteger(candidate.hostPort) ||
    typeof candidate.publicHostname !== "string" ||
    !Number.isSafeInteger(candidate.rendererPort) ||
    typeof candidate.rendererRoot !== "string"
  ) {
    return null;
  }
  return {
    hostPort: candidate.hostPort as number,
    publicHostname: candidate.publicHostname,
    rendererPort: candidate.rendererPort as number,
    rendererRoot: candidate.rendererRoot,
  };
}

function parseSessionRecord(
  input: string,
  expectedIdentity?: WebDevelopmentSessionIdentity,
): SessionRecord {
  let value: unknown;
  try {
    value = JSON.parse(input) as unknown;
  } catch (error) {
    throw new WebDevelopmentSessionError(
      "The existing development session record is not valid JSON. Stop its owner before removing the record.",
      { cause: error },
    );
  }
  if (typeof value !== "object" || value === null) {
    throw new WebDevelopmentSessionError("The existing development session record is malformed.");
  }
  const candidate = value as Record<string, unknown>;
  const identity = parseIdentity(candidate.identity);
  const browserUrl =
    candidate.browserUrl === null || typeof candidate.browserUrl === "string"
      ? candidate.browserUrl
      : undefined;
  if (
    candidate.marker !== SESSION_RECORD_MARKER ||
    candidate.version !== SESSION_RECORD_VERSION ||
    identity === null ||
    (expectedIdentity !== undefined && !sameIdentity(identity, expectedIdentity)) ||
    typeof candidate.nonce !== "string" ||
    candidate.nonce.length < 16 ||
    !Number.isSafeInteger(candidate.ownerPid) ||
    (candidate.ownerPid as number) <= 0 ||
    (candidate.state !== "ready" && candidate.state !== "starting") ||
    browserUrl === undefined ||
    (candidate.state === "ready" && browserUrl === null) ||
    (candidate.state === "starting" && browserUrl !== null)
  ) {
    throw new WebDevelopmentSessionError(
      "The existing development session record does not match the requested launch.",
    );
  }
  return {
    browserUrl,
    identity,
    marker: SESSION_RECORD_MARKER,
    nonce: candidate.nonce,
    ownerPid: candidate.ownerPid as number,
    state: candidate.state,
    version: SESSION_RECORD_VERSION,
  };
}

async function readSessionRecord(
  path: string,
  identity: WebDevelopmentSessionIdentity | undefined,
): Promise<SessionRecord> {
  const handle = await open(path, "r");
  try {
    const metadata = await handle.stat();
    const ownedByCurrentUser =
      typeof process.getuid !== "function" || metadata.uid === process.getuid();
    if (
      !metadata.isFile() ||
      !ownedByCurrentUser ||
      !privateMode(metadata.mode) ||
      metadata.size > SESSION_RECORD_BYTES
    ) {
      throw new WebDevelopmentSessionError(
        "The existing development session record is not a bounded private file.",
      );
    }
    return parseSessionRecord(await handle.readFile("utf8"), identity);
  } finally {
    await handle.close();
  }
}

async function writeOwnedRecord(
  path: string,
  identity: WebDevelopmentSessionIdentity,
  nonce: string,
  record: SessionRecord,
): Promise<void> {
  const handle = await open(path, "r+");
  try {
    const current = parseSessionRecord(await handle.readFile("utf8"), identity);
    if (current.nonce !== nonce) {
      throw new WebDevelopmentSessionConflictError(
        "Development session ownership changed before the launch became ready.",
      );
    }
    const serialized = `${JSON.stringify(record)}\n`;
    await handle.write(serialized, 0, "utf8");
    await handle.truncate(Buffer.byteLength(serialized));
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function removeOwnedRecord(
  path: string,
  identity: WebDevelopmentSessionIdentity,
  nonce: string,
): Promise<void> {
  let current: SessionRecord;
  try {
    current = await readSessionRecord(path, identity);
  } catch (error) {
    if (isNodeError(error) && error.code === "ENOENT") {
      return;
    }
    throw error;
  }
  if (current.nonce === nonce) {
    await rm(path, { force: true });
  }
}

async function createSessionLease(
  path: string,
  identity: WebDevelopmentSessionIdentity,
  ownerPid: number,
): Promise<SessionLease | null> {
  const nonce = randomUUID();
  const starting: SessionRecord = {
    browserUrl: null,
    identity,
    marker: SESSION_RECORD_MARKER,
    nonce,
    ownerPid,
    state: "starting",
    version: SESSION_RECORD_VERSION,
  };
  let handle;
  try {
    handle = await open(path, "wx", 0o600);
  } catch (error) {
    if (isNodeError(error) && error.code === "EEXIST") {
      return null;
    }
    throw error;
  }
  try {
    await handle.writeFile(`${JSON.stringify(starting)}\n`, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }

  let released = false;
  return {
    publish: async (browserUrl): Promise<void> => {
      if (released) {
        throw new WebDevelopmentSessionConflictError(
          "Development session ownership was already released.",
        );
      }
      await writeOwnedRecord(path, identity, nonce, {
        ...starting,
        browserUrl,
        state: "ready",
      });
    },
    release: async (): Promise<void> => {
      if (released) {
        return;
      }
      released = true;
      await removeOwnedRecord(path, identity, nonce);
    },
  };
}

function processIsAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return isNodeError(error) && error.code === "EPERM";
  }
}

async function claimSession(
  identity: WebDevelopmentSessionIdentity,
  directory: string,
  ownerPid: number,
  isProcessAlive: (pid: number) => boolean,
  isExistingSessionReady: (
    browserUrl: string,
    identity: WebDevelopmentSessionIdentity,
  ) => Promise<boolean>,
): Promise<SessionClaim> {
  await ensurePrivateDirectory(directory);
  const path = join(directory, sessionFileName(identity));

  for (let attempt = 0; attempt < 3; attempt += 1) {
    const lease = await createSessionLease(path, identity, ownerPid);
    if (lease !== null) {
      return { kind: "owner", lease };
    }

    let current: SessionRecord;
    try {
      current = await readSessionRecord(path, identity);
    } catch (error) {
      if (isNodeError(error) && error.code === "ENOENT") {
        continue;
      }
      throw error;
    }

    if (!isProcessAlive(current.ownerPid)) {
      const confirmed = await readSessionRecord(path, identity);
      if (confirmed.nonce === current.nonce) {
        await rm(path, { force: true });
      }
      continue;
    }
    if (current.state === "starting" || current.browserUrl === null) {
      throw new WebDevelopmentSessionConflictError(
        `Web development is already starting in process ${String(
          current.ownerPid,
        )}. Retry after it reports readiness.`,
      );
    }
    if (await isExistingSessionReady(current.browserUrl, identity)) {
      return { browserUrl: current.browserUrl, kind: "existing" };
    }
    throw new WebDevelopmentSessionConflictError(
      `Web development process ${String(
        current.ownerPid,
      )} is still running, but its recorded renderer and host did not confirm authenticated readiness. Stop that process before retrying.`,
    );
  }

  throw new WebDevelopmentSessionConflictError(
    "Development session ownership changed repeatedly. Retry after the active start operation completes.",
  );
}

function launchParameters(
  browserUrl: string,
  identity: WebDevelopmentSessionIdentity,
): { readonly rendererOrigin: string } | null {
  let renderer: URL;
  try {
    renderer = new URL(browserUrl);
  } catch {
    return null;
  }
  const rendererOrigin = developmentOrigin(identity.publicHostname, identity.rendererPort);
  if (
    renderer.origin !== rendererOrigin ||
    renderer.pathname !== "/" ||
    renderer.search.length > 0 ||
    renderer.hash.length > 0
  ) {
    return null;
  }
  return { rendererOrigin };
}

function gatewayCookie(response: Response, rendererOrigin: string): string | null {
  const setCookie = response.headers.get("set-cookie");
  if (setCookie === null) {
    return null;
  }
  const cookie = setCookie.split(";", 1)[0]?.trim();
  if (
    cookie === undefined ||
    !cookie.startsWith(`${browserDevelopmentSessionCookie(rendererOrigin)}=`) ||
    cookie.length > 1_024
  ) {
    return null;
  }
  return cookie;
}

async function existingSessionIsReady(
  browserUrl: string,
  identity: WebDevelopmentSessionIdentity,
): Promise<boolean> {
  const launch = launchParameters(browserUrl, identity);
  if (launch === null) {
    return false;
  }
  try {
    const signal = AbortSignal.timeout(SESSION_READINESS_TIMEOUT_MS);
    const rendererResponse = await fetch(`${launch.rendererOrigin}/`, {
      cache: "no-store",
      redirect: "error",
      signal,
    });
    const cookie = gatewayCookie(rendererResponse, launch.rendererOrigin);
    if (!rendererResponse.ok || cookie === null) {
      return false;
    }
    const hostResponse = await fetch(
      `${launch.rendererOrigin}${BROWSER_DEVELOPMENT_GATEWAY_PATH}/health`,
      {
        cache: "no-store",
        headers: { cookie },
        redirect: "error",
        signal,
      },
    );
    if (!hostResponse.ok) {
      return false;
    }
    const health = await hostResponse.json();
    return (
      typeof health === "object" &&
      health !== null &&
      (health as Record<string, unknown>).protocolVersion === HOST_PROTOCOL_VERSION &&
      (health as Record<string, unknown>).status === "ready"
    );
  } catch {
    return false;
  }
}

async function cleanupFailedLaunch(
  launch: RunningWebDevelopment | undefined,
  application: DevelopmentProviderSource | undefined,
  lease: SessionLease,
  error: unknown,
): Promise<never> {
  const owner = application?.providers ?? application?.backend;
  const cleanups = await Promise.allSettled([
    Promise.resolve().then(() => (launch === undefined ? owner?.shutdown() : launch.close())),
    Promise.resolve().then(() => lease.release()),
  ]);
  const failures = cleanups
    .filter((result): result is PromiseRejectedResult => result.status === "rejected")
    .map((result) => result.reason as unknown);
  if (failures.length > 0) {
    throw new AggregateError(
      [error, ...failures],
      "Web development startup failed and session cleanup did not complete.",
      { cause: error },
    );
  }
  throw error;
}

export async function startWebDevelopmentCommand(
  options: WebDevelopmentCommandOptions,
  dependencies: WebDevelopmentCommandDependencies,
): Promise<RunningWebDevelopmentCommand> {
  const network = resolveDevelopmentNetwork({
    ...(options.publicHostname === undefined ? {} : { publicHostname: options.publicHostname }),
  });
  const identity: WebDevelopmentSessionIdentity = {
    hostPort: options.hostPort,
    publicHostname: network.publicHostname,
    rendererPort: options.rendererPort,
    rendererRoot: await realpath(options.rendererRoot),
  };
  const directory = options.sessionDirectory ?? defaultSessionDirectory();
  await ensurePrivateDirectory(directory);
  for (const name of await readdir(directory)) {
    if (!/^session-[a-f0-9]{24}\.json$/u.test(name)) continue;
    const path = join(directory, name);
    let previous: SessionRecord;
    try {
      previous = await readSessionRecord(path, undefined);
    } catch (error) {
      if (isNodeError(error) && error.code === "ENOENT") continue;
      throw error;
    }
    if (
      previous.identity.rendererRoot !== identity.rendererRoot ||
      sameIdentity(previous.identity, identity)
    )
      continue;
    const ports = [identity.hostPort, identity.rendererPort];
    if (
      !ports.includes(previous.identity.hostPort) &&
      !ports.includes(previous.identity.rendererPort)
    )
      continue;
    if ((dependencies.isProcessAlive ?? processIsAlive)(previous.ownerPid)) {
      if (
        previous.state !== "ready" ||
        previous.browserUrl === null ||
        !(await (dependencies.isExistingSessionReady ?? existingSessionIsReady)(
          previous.browserUrl,
          previous.identity,
        ))
      ) {
        throw new WebDevelopmentSessionConflictError(
          "A conflicting StreamSkope session did not confirm readiness; automatic takeover was refused.",
        );
      }
      const confirmed = await readSessionRecord(path, previous.identity);
      if (confirmed.nonce !== previous.nonce)
        throw new WebDevelopmentSessionConflictError(
          "Session ownership changed during takeover; retry.",
        );
      if (dependencies.stopOwnedSession !== undefined)
        await dependencies.stopOwnedSession(previous.ownerPid, previous.identity);
      else await stopWebDevelopmentOwner(previous.ownerPid, previous.identity.rendererRoot);
    }
    try {
      const confirmed = await readSessionRecord(path, previous.identity);
      if (confirmed.nonce === previous.nonce) await rm(path);
    } catch (error) {
      if (!isNodeError(error) || error.code !== "ENOENT") throw error;
    }
  }
  const claim = await claimSession(
    identity,
    directory,
    dependencies.ownerPid ?? process.pid,
    dependencies.isProcessAlive ?? processIsAlive,
    dependencies.isExistingSessionReady ?? existingSessionIsReady,
  );
  if (claim.kind === "existing") {
    await dependencies.prepare?.();
    return {
      browserOpenError: await attemptBrowserOpen(claim.browserUrl, dependencies.openBrowser),
      browserUrl: claim.browserUrl,
      close: () => Promise.resolve(),
      reused: true,
    };
  }

  let launch: RunningWebDevelopment | undefined;
  let application: DevelopmentProviderSource | undefined;
  try {
    await dependencies.prepare?.();
    application =
      dependencies.createProviders === undefined
        ? { backend: await dependencies.createBackend() }
        : await dependencies.createProviders();
    launch = await (dependencies.launch ?? launchWebDevelopment)({
      ...application,
      hostPort: identity.hostPort,
      publicHostname: identity.publicHostname,
      rendererPort: identity.rendererPort,
      rendererRoot: identity.rendererRoot,
    });
    await claim.lease.publish(launch.browserUrl);
  } catch (error) {
    return cleanupFailedLaunch(launch, application, claim.lease, error);
  }

  let closePromise: Promise<void> | undefined;
  return {
    browserOpenError: await attemptBrowserOpen(launch.browserUrl, dependencies.openBrowser),
    browserUrl: launch.browserUrl,
    close: (): Promise<void> => {
      closePromise ??= (async (): Promise<void> => {
        await launch.close();
        await claim.lease.release();
      })();
      return closePromise;
    },
    reused: false,
  };
}
