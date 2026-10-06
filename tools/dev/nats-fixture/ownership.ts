import { X509Certificate } from "node:crypto";
import { lstat, readFile } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";

import type { NatsConnectionInput } from "../../../src/features/nats/application/profile-types";

import { NATS_SERVER_IMAGES } from "./definition";

export class NatsFixtureError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NatsFixtureError";
  }
}

export const NATS_FIXTURE_NAME = "streamskope-nats";

export interface NatsLegacyFixtureRecord {
  readonly format: 1;
  readonly name: typeof NATS_FIXTURE_NAME;
  readonly identity: string;
  readonly container: string;
  readonly directory: string;
  readonly image: string;
  readonly port: number;
}

export interface NatsLegacyFixtureIntent extends Omit<NatsLegacyFixtureRecord, "container"> {
  readonly creationStarted: boolean;
  readonly container?: string;
}

export interface NatsContainerlabFixtureIdentity {
  readonly format: 2;
  readonly name: string;
  readonly lab: string;
  readonly identity: string;
  readonly directory: string;
  readonly image: string;
  readonly port: number;
  readonly topologyPath: string;
  readonly runtimeDirectory: string;
  readonly networkName: string;
}

export interface NatsContainerlabFixtureRecord extends NatsContainerlabFixtureIdentity {
  readonly daemon: string;
  readonly container: string;
  readonly network: string;
  readonly volume: string;
}

export interface NatsContainerlabFixtureIntent extends NatsContainerlabFixtureIdentity {
  readonly creationStarted: boolean;
  /** True after deployment ended, its adapter sealed, and every begun mutation settled. */
  readonly mutationsSettled: boolean;
  readonly daemon?: string;
  readonly container?: string;
  readonly network?: string;
  readonly volume?: string;
}

export type NatsFixtureRecord = NatsLegacyFixtureRecord | NatsContainerlabFixtureRecord;
export type NatsFixtureIntent = NatsLegacyFixtureIntent | NatsContainerlabFixtureIntent;

export function natsContainerlabPaths(
  identity: string,
  directory: string,
): Pick<
  NatsContainerlabFixtureIdentity,
  "name" | "lab" | "topologyPath" | "runtimeDirectory" | "networkName"
> {
  const lab = `sk-nats-${identity}`;
  return {
    lab,
    name: `clab-${lab}-server`,
    topologyPath: join(directory, "topology.clab.yml"),
    runtimeDirectory: join(directory, `clab-${lab}`),
    // Also fits a Linux bridge name; full UUID labels and daemon ID establish ownership.
    networkName: `skn-${identity.replaceAll("-", "").slice(0, 10)}`,
  };
}

export function natsOwnershipRoot(repositoryRoot: string): string {
  return join(resolve(repositoryRoot), "aio-nats", "ownership");
}

export function natsRecordPath(repositoryRoot: string): string {
  return join(natsOwnershipRoot(repositoryRoot), "record.json");
}

export function natsIntentPath(repositoryRoot: string): string {
  return join(natsOwnershipRoot(repositoryRoot), "start-intent.json");
}

export async function readPrivateFixtureFile(path: string, limit: number): Promise<string> {
  const info = await lstat(path);
  if (!info.isFile() || info.size > limit || (info.mode & 0o077) !== 0)
    throw new NatsFixtureError("Local AIO NATS private material is invalid or not private.");
  return readFile(path, "utf8");
}

function parseLegacyFixtureRecord(value: unknown, repositoryRoot: string): NatsLegacyFixtureRecord {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new NatsFixtureError("Local AIO NATS ownership record is invalid.");
  const record = value as Partial<NatsLegacyFixtureRecord>;
  if (
    record.format !== 1 ||
    record.name !== NATS_FIXTURE_NAME ||
    typeof record.identity !== "string" ||
    !/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/u.test(
      record.identity,
    ) ||
    typeof record.container !== "string" ||
    !/^[a-f0-9]{64}$/u.test(record.container) ||
    typeof record.image !== "string" ||
    !Object.values(NATS_SERVER_IMAGES).some((image) => image === record.image) ||
    typeof record.port !== "number" ||
    !Number.isSafeInteger(record.port) ||
    record.port < 1 ||
    record.port > 65_535 ||
    typeof record.directory !== "string" ||
    resolve(record.directory) !== record.directory ||
    dirname(record.directory) !== join(natsOwnershipRoot(repositoryRoot), "instances") ||
    !/^server-[A-Za-z0-9]{6}$/u.test(basename(record.directory))
  )
    throw new NatsFixtureError(
      "Local AIO NATS ownership record is invalid; no resource was changed.",
    );
  return record as NatsLegacyFixtureRecord;
}

function containerlabIdentity(
  value: unknown,
  repositoryRoot: string,
): NatsContainerlabFixtureIdentity {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new NatsFixtureError("Local AIO NATS Containerlab ownership is invalid.");
  const candidate = value as Record<string, unknown>;
  // Reuse the established UUID, image, port and private instance-directory checks.
  const base = parseLegacyFixtureRecord(
    { ...candidate, format: 1, name: NATS_FIXTURE_NAME, container: "0".repeat(64) },
    repositoryRoot,
  );
  const paths = natsContainerlabPaths(base.identity, base.directory);
  if (
    candidate.format !== 2 ||
    Object.entries(paths).some(([key, expected]) => candidate[key] !== expected)
  )
    throw new NatsFixtureError(
      "Local AIO NATS Containerlab paths or names are invalid; no resource was changed.",
    );
  return {
    format: 2,
    identity: base.identity,
    directory: base.directory,
    image: base.image,
    port: base.port,
    ...paths,
  };
}

export function validNatsDaemonIdentity(value: unknown): value is string {
  return typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9:._-]{7,127}$/u.test(value);
}

function daemonId(value: unknown): value is string {
  return typeof value === "string" && /^[a-f0-9]{64}$/u.test(value);
}

export function parseNatsFixtureRecord(value: unknown, repositoryRoot: string): NatsFixtureRecord {
  if (
    typeof value !== "object" ||
    value === null ||
    Array.isArray(value) ||
    (value as { format?: unknown }).format !== 2
  )
    return parseLegacyFixtureRecord(value, repositoryRoot);
  const base = containerlabIdentity(value, repositoryRoot);
  const candidate = value as Record<string, unknown>;
  if (
    !validNatsDaemonIdentity(candidate.daemon) ||
    !daemonId(candidate.container) ||
    !daemonId(candidate.network) ||
    !daemonId(candidate.volume)
  )
    throw new NatsFixtureError(
      "Local AIO NATS Containerlab resource identities are invalid; no resource was changed.",
    );
  return {
    ...base,
    daemon: candidate.daemon,
    container: candidate.container,
    network: candidate.network,
    volume: candidate.volume,
  };
}

export function parseNatsFixtureIntent(value: unknown, repositoryRoot: string): NatsFixtureIntent {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new NatsFixtureError("Local AIO NATS start intent is invalid.");
  const candidate = value as Record<string, unknown>;
  if (candidate.format === 2) {
    const base = containerlabIdentity(value, repositoryRoot);
    if (
      typeof candidate.creationStarted !== "boolean" ||
      typeof candidate.mutationsSettled !== "boolean" ||
      (candidate.daemon !== undefined && !validNatsDaemonIdentity(candidate.daemon)) ||
      (candidate.creationStarted && !validNatsDaemonIdentity(candidate.daemon)) ||
      ["container", "network", "volume"].some(
        (key) => candidate[key] !== undefined && !daemonId(candidate[key]),
      ) ||
      (!candidate.creationStarted &&
        ["container", "network", "volume"].some((key) => candidate[key] !== undefined))
    )
      throw new NatsFixtureError("Local AIO NATS Containerlab start intent is invalid.");
    return {
      ...base,
      creationStarted: candidate.creationStarted,
      mutationsSettled: candidate.mutationsSettled,
      ...(typeof candidate.daemon === "string" ? { daemon: candidate.daemon } : {}),
      ...(typeof candidate.container === "string" ? { container: candidate.container } : {}),
      ...(typeof candidate.network === "string" ? { network: candidate.network } : {}),
      ...(typeof candidate.volume === "string" ? { volume: candidate.volume } : {}),
    };
  }
  const record = parseLegacyFixtureRecord(
    { ...candidate, container: "0".repeat(64) },
    repositoryRoot,
  );
  if (
    (candidate.creationStarted !== undefined && typeof candidate.creationStarted !== "boolean") ||
    (candidate.container !== undefined && !daemonId(candidate.container))
  )
    throw new NatsFixtureError("Local AIO NATS start intent is invalid.");
  return {
    format: 1,
    name: record.name,
    identity: record.identity,
    directory: record.directory,
    image: record.image,
    port: record.port,
    creationStarted: candidate.creationStarted ?? true,
    ...(typeof candidate.container === "string" ? { container: candidate.container } : {}),
  };
}

export async function loadNatsFixtureRecord(
  repositoryRoot: string,
): Promise<NatsFixtureRecord | undefined> {
  let bytes: string;
  try {
    bytes = await readPrivateFixtureFile(natsRecordPath(repositoryRoot), 8_192);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
  return parseNatsFixtureRecord(JSON.parse(bytes) as unknown, repositoryRoot);
}

export async function loadNatsFixtureIntent(
  repositoryRoot: string,
): Promise<NatsFixtureIntent | undefined> {
  let bytes: string;
  try {
    bytes = await readPrivateFixtureFile(natsIntentPath(repositoryRoot), 8_192);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
  return parseNatsFixtureIntent(JSON.parse(bytes) as unknown, repositoryRoot);
}

export async function natsFixtureConnection(
  record: NatsFixtureRecord,
): Promise<NatsConnectionInput> {
  const caPem = await readPrivateFixtureFile(join(record.directory, "ca.pem"), 16_384);
  const token = await readPrivateFixtureFile(join(record.directory, "token"), 128);
  if (!/^[a-f0-9]{64}$/u.test(token) || caPem.includes("PRIVATE KEY"))
    throw new NatsFixtureError("Local AIO NATS private connection material is invalid.");
  const ca = new X509Certificate(caPem);
  if (!ca.ca || Date.parse(ca.validFrom) > Date.now() || Date.parse(ca.validTo) <= Date.now())
    throw new NatsFixtureError(
      "Local AIO NATS certificate is expired or invalid. Stop the owned fixture and start it again.",
    );
  return {
    servers: [`nats://127.0.0.1:${record.port}`],
    authentication: { mode: "token", token },
    tls: { mode: "tls", caPem },
  };
}
