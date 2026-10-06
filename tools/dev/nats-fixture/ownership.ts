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

export interface NatsFixtureRecord {
  readonly format: 1;
  readonly name: typeof NATS_FIXTURE_NAME;
  readonly identity: string;
  readonly container: string;
  readonly directory: string;
  readonly image: string;
  readonly port: number;
}

export interface NatsFixtureIntent extends Omit<NatsFixtureRecord, "container"> {
  readonly creationStarted: boolean;
  readonly container?: string;
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

export function parseNatsFixtureRecord(value: unknown, repositoryRoot: string): NatsFixtureRecord {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new NatsFixtureError("Local AIO NATS ownership record is invalid.");
  const record = value as Partial<NatsFixtureRecord>;
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
  return record as NatsFixtureRecord;
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
  const value: unknown = JSON.parse(bytes);
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new NatsFixtureError("Local AIO NATS start intent is invalid.");
  const record = parseNatsFixtureRecord({ ...value, container: "0".repeat(64) }, repositoryRoot);
  const metadata = value as { readonly creationStarted?: unknown; readonly container?: unknown };
  if (
    (metadata.creationStarted !== undefined && typeof metadata.creationStarted !== "boolean") ||
    (metadata.container !== undefined &&
      (typeof metadata.container !== "string" || !/^[a-f0-9]{64}$/u.test(metadata.container)))
  )
    throw new NatsFixtureError("Local AIO NATS start intent is invalid.");
  return {
    format: record.format,
    name: record.name,
    identity: record.identity,
    directory: record.directory,
    image: record.image,
    port: record.port,
    creationStarted: metadata.creationStarted ?? true,
    ...(typeof metadata.container === "string" ? { container: metadata.container } : {}),
  };
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
