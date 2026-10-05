import { NATS_LIMITS, type NatsProfileStoreCapability } from "../../features/nats/contracts";
import {
  parseNatsProfileRecord,
  parseNatsProfileRecords,
} from "../../features/nats/application/profile-record-validation";
import { natsIdentifier, natsInteger } from "../../features/nats/contracts/validation-primitives";
import {
  NatsProfileError,
  type NatsProfileRecord,
  type NatsProfileStore,
} from "../../features/nats/application/profile-types";

import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
} from "./atomic-private-text-file";
import { readBoundedFile } from "./bounded-file";
import type { ProfileProtector } from "./profile-protector";

const PROFILE_FILE_VERSION = 1 as const;
const PROTECTED_ENVELOPE_BYTES = Buffer.byteLength(
  '{"provider":"nats","version":1,"profile":}',
  "utf8",
);
const RECOVERY =
  "Preserve the NATS profile file, restore a known-good copy or correct its permissions, then restart StreamSkope.";

interface ProtectedProfile {
  readonly id: string;
  readonly revision: number;
  readonly protectedValue: string;
}

export interface NatsProfileFileStoreOptions {
  readonly createTempId?: () => string;
}

export class NatsProfileFileCorruptError extends NatsProfileError {
  constructor() {
    super(
      "storage-unavailable",
      "The NATS profile file is unreadable or uses an unsupported schema.",
      RECOVERY,
    );
    this.name = "NatsProfileFileCorruptError";
  }
}

export class NatsProfileFileWriteError extends NatsProfileError {
  constructor() {
    super(
      "storage-unavailable",
      "NATS profile storage could not commit the requested change.",
      RECOVERY,
    );
    this.name = "NatsProfileFileWriteError";
  }
}

function objectWithKeys(value: unknown, keys: readonly string[]): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new NatsProfileFileCorruptError();
  const record = value as Record<string, unknown>;
  if (Object.keys(record).length !== keys.length || keys.some((key) => !Object.hasOwn(record, key)))
    throw new NatsProfileFileCorruptError();
  return record;
}

function protectedDocument(value: unknown): readonly ProtectedProfile[] {
  const document = objectWithKeys(value, ["version", "profiles"]);
  if (
    document.version !== PROFILE_FILE_VERSION ||
    !Array.isArray(document.profiles) ||
    document.profiles.length > NATS_LIMITS.profiles
  )
    throw new NatsProfileFileCorruptError();
  const ids = new Set<string>();
  return document.profiles.map((value: unknown) => {
    const profile = objectWithKeys(value, ["id", "revision", "protectedValue"]);
    const id = natsIdentifier(profile.id);
    const revision = natsInteger(profile.revision, 1);
    if (ids.has(id) || typeof profile.protectedValue !== "string")
      throw new NatsProfileFileCorruptError();
    ids.add(id);
    return { id, revision, protectedValue: profile.protectedValue };
  });
}

function protectedBytes(value: string): Buffer {
  if (
    value.length === 0 ||
    value.length > Math.ceil(NATS_LIMITS.profileCiphertextBytes / 3) * 4 ||
    !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)
  )
    throw new NatsProfileFileCorruptError();
  const decoded = Buffer.from(value, "base64");
  if (
    decoded.length === 0 ||
    decoded.length > NATS_LIMITS.profileCiphertextBytes ||
    decoded.toString("base64") !== value
  )
    throw new NatsProfileFileCorruptError();
  return decoded;
}

function restoredProfile(plaintext: string, expected: ProtectedProfile): NatsProfileRecord {
  if (
    Buffer.byteLength(plaintext, "utf8") >
    NATS_LIMITS.profilePlaintextBytes + PROTECTED_ENVELOPE_BYTES
  )
    throw new NatsProfileFileCorruptError();
  const envelope = objectWithKeys(JSON.parse(plaintext) as unknown, [
    "provider",
    "version",
    "profile",
  ]);
  if (envelope.provider !== "nats" || envelope.version !== PROFILE_FILE_VERSION)
    throw new NatsProfileFileCorruptError();
  const profile = parseNatsProfileRecord(envelope.profile);
  if (profile.id !== expected.id || profile.revision !== expected.revision)
    throw new NatsProfileFileCorruptError();
  return profile;
}

function isMissingFile(error: unknown): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT";
}

/** Owns the private NATS file format; provider records and revision policy remain application-owned. */
export class AtomicNatsProfileFileStore implements NatsProfileStore {
  private currentCapability: NatsProfileStoreCapability = {
    durability: "durable",
    protection: "os-protected",
    state: "ready",
  };
  private hasRead = false;
  private readonly createTempId;

  constructor(
    private readonly path: string,
    private readonly protector: ProfileProtector,
    options: NatsProfileFileStoreOptions = {},
  ) {
    this.createTempId = options.createTempId ?? createAtomicPrivateFileTempId;
  }

  get capability(): NatsProfileStoreCapability {
    return { ...this.currentCapability };
  }

  async load(signal?: AbortSignal): Promise<readonly NatsProfileRecord[]> {
    signal?.throwIfAborted();
    this.assertReadable();
    let contents: Buffer;
    try {
      contents = await readBoundedFile(this.path, NATS_LIMITS.profileFileBytes, {
        rejectSymlinks: true,
        signal,
      });
    } catch (error) {
      signal?.throwIfAborted();
      if (isMissingFile(error)) {
        this.hasRead = true;
        return [];
      }
      this.markUnavailable();
      throw new NatsProfileFileCorruptError();
    }
    try {
      signal?.throwIfAborted();
      const entries = protectedDocument(
        JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(contents)) as unknown,
      );
      const restored: NatsProfileRecord[] = [];
      let shouldReEncrypt = false;
      for (const entry of entries) {
        signal?.throwIfAborted();
        const result = await this.protector.unprotect(protectedBytes(entry.protectedValue));
        signal?.throwIfAborted();
        restored.push(restoredProfile(result.plaintext, entry));
        shouldReEncrypt ||= result.shouldReEncrypt;
      }
      const validated = parseNatsProfileRecords(restored);
      if (shouldReEncrypt) await this.write(validated, signal);
      this.hasRead = true;
      return validated;
    } catch {
      signal?.throwIfAborted();
      this.markUnavailable();
      throw new NatsProfileFileCorruptError();
    }
  }

  async save(records: readonly NatsProfileRecord[], signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    this.assertReadable();
    if (!this.hasRead) await this.load(signal);
    try {
      await this.write(records, signal);
    } catch {
      signal?.throwIfAborted();
      this.markUnavailable();
      throw new NatsProfileFileWriteError();
    }
  }

  private assertReadable(): void {
    if (this.currentCapability.state === "unavailable") throw new NatsProfileFileCorruptError();
  }

  private markUnavailable(): void {
    this.currentCapability = {
      durability: "durable",
      protection: "unavailable",
      state: "unavailable",
      recovery: RECOVERY,
    };
  }

  private async write(records: readonly NatsProfileRecord[], signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    const normalized = parseNatsProfileRecords(records);
    const profiles: ProtectedProfile[] = [];
    for (const profile of normalized) {
      signal?.throwIfAborted();
      const plaintext = JSON.stringify({
        provider: "nats",
        version: PROFILE_FILE_VERSION,
        profile,
      });
      if (
        Buffer.byteLength(plaintext, "utf8") >
        NATS_LIMITS.profilePlaintextBytes + PROTECTED_ENVELOPE_BYTES
      )
        throw new NatsProfileFileWriteError();
      const protectedValue = await this.protector.protect(plaintext);
      signal?.throwIfAborted();
      if (protectedValue.length === 0 || protectedValue.length > NATS_LIMITS.profileCiphertextBytes)
        throw new NatsProfileFileWriteError();
      profiles.push({
        id: profile.id,
        revision: profile.revision,
        protectedValue: protectedValue.toString("base64"),
      });
    }
    const contents = `${JSON.stringify({ version: PROFILE_FILE_VERSION, profiles })}\n`;
    if (Buffer.byteLength(contents, "utf8") > NATS_LIMITS.profileFileBytes)
      throw new NatsProfileFileWriteError();
    await writeAtomicPrivateTextFile({
      path: this.path,
      contents,
      createTempId: this.createTempId,
      ...(signal === undefined ? {} : { signal }),
    });
  }
}
