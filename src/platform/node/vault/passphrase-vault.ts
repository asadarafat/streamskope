import { constants } from "node:fs";
import { chmod, link, lstat, mkdir, open, readdir, unlink } from "node:fs/promises";
import { randomBytes, randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { dirname, isAbsolute, join, resolve } from "node:path";

import { readBoundedFile } from "../bounded-file";
import type { ProfileProtectionCapability, ProfileProtector } from "../profile-protector";

import { decryptVaultValue, deriveVaultKey, encryptVaultValue, VAULT_KDF } from "./vault-crypto";

const METADATA_BYTES = 4096;
const VERIFIER = "StreamSkope passphrase vault v1";
const RECOVERY = "Preserve the data directory and restore its permissions or a known-good backup.";

export type PassphraseVaultErrorCode =
  | "invalid-passphrase"
  | "already-exists"
  | "not-created"
  | "unavailable"
  | "in-use"
  | "unlock-failed"
  | "locked"
  | "invalid-value";

export class PassphraseVaultError extends Error {
  readonly recovery: string;
  constructor(readonly code: PassphraseVaultErrorCode) {
    const message: Record<PassphraseVaultErrorCode, string> = {
      "invalid-passphrase": "Use a passphrase between 12 and 1024 UTF-8 bytes.",
      "already-exists": "The data directory already contains a vault or existing data.",
      "not-created": "The vault has not been created.",
      unavailable: "The vault is unreadable, corrupt, or uses an unsupported format.",
      "in-use": "Another StreamSkope host holds this data directory's lease.",
      "unlock-failed": "The vault could not be unlocked with this passphrase.",
      locked: "The vault is locked.",
      "invalid-value": "The protected value is corrupt, unsupported, or exceeds its bound.",
    };
    super(message[code]);
    this.name = "PassphraseVaultError";
    this.recovery =
      code === "in-use"
        ? "Stop the other StreamSkope host using this data directory, then retry."
        : code === "unlock-failed"
          ? "Check the passphrase. Existing vault and profile files have been preserved."
          : RECOVERY;
  }
}

export interface PassphraseVaultPaths {
  readonly dataRoot: string;
  readonly vaultFile: string;
  readonly kafkaProfiles: string;
  readonly natsProfiles: string;
}

export interface PassphraseVault {
  readonly capability: ProfileProtectionCapability;
  readonly paths: PassphraseVaultPaths;
  readonly protector: ProfileProtector;
  /** Call after all host operations and stores have stopped; destroys the key and releases the lease. */
  lock(): Promise<void>;
}

export interface OpenPassphraseVaultInput {
  readonly dataRoot: string;
  readonly passphrase: string;
  readonly mode: "create" | "unlock";
}

interface VaultMetadata {
  readonly version: 1;
  readonly cipher: "aes-256-gcm";
  readonly kdf: typeof VAULT_KDF;
  readonly salt: string;
  readonly vaultId: string;
  readonly verifier: string;
}

function missing(error: unknown): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT";
}

function paths(dataRoot: string): PassphraseVaultPaths {
  if (
    typeof dataRoot !== "string" ||
    dataRoot.length === 0 ||
    dataRoot.length > 4096 ||
    !isAbsolute(dataRoot)
  )
    throw new PassphraseVaultError("unavailable");
  const root = resolve(dataRoot);
  if (dirname(root) === root) throw new PassphraseVaultError("unavailable");
  return {
    dataRoot: root,
    vaultFile: join(root, "vault.json"),
    kafkaProfiles: join(root, "kafka-profiles.json"),
    natsProfiles: join(root, "nats-profiles.json"),
  };
}

/** Reject symlink components before granting the host ownership of a persistent directory. */
async function checkDirectoryPath(directory: string): Promise<void> {
  const parent = dirname(directory);
  if (parent !== directory) await checkDirectoryPath(parent);
  try {
    if (!(await lstat(directory)).isDirectory()) throw new PassphraseVaultError("unavailable");
  } catch (error) {
    if (!missing(error)) throw error;
  }
}

async function privateFile(path: string): Promise<void> {
  const metadata = await lstat(path);
  if (
    !metadata.isFile() ||
    metadata.nlink !== 1 ||
    (process.getuid !== undefined && metadata.uid !== process.getuid()) ||
    (process.platform !== "win32" && (metadata.mode & 0o077) !== 0)
  )
    throw new PassphraseVaultError("unavailable");
}

function exact(value: unknown, keys: readonly string[]): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new PassphraseVaultError("unavailable");
  const record = value as Record<string, unknown>;
  if (Object.keys(record).length !== keys.length || keys.some((key) => !Object.hasOwn(record, key)))
    throw new PassphraseVaultError("unavailable");
  return record;
}

function base64(value: unknown, bytes: number): Buffer {
  if (typeof value !== "string") throw new PassphraseVaultError("unavailable");
  const decoded = Buffer.from(value, "base64");
  if (decoded.length !== bytes || decoded.toString("base64") !== value)
    throw new PassphraseVaultError("unavailable");
  return decoded;
}

function parseMetadata(contents: Buffer): VaultMetadata {
  const parsed: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(contents));
  const metadata = exact(parsed, ["version", "cipher", "kdf", "salt", "vaultId", "verifier"]);
  const kdf = exact(metadata.kdf, ["name", "N", "r", "p"]);
  if (
    metadata.version !== 1 ||
    metadata.cipher !== "aes-256-gcm" ||
    Object.keys(VAULT_KDF).some((key) => kdf[key] !== VAULT_KDF[key as keyof typeof VAULT_KDF])
  )
    throw new PassphraseVaultError("unavailable");
  base64(metadata.salt, 32);
  base64(metadata.vaultId, 16);
  base64(metadata.verifier, 32 + Buffer.byteLength(VERIFIER));
  return metadata as unknown as VaultMetadata;
}

function aad(metadata: Omit<VaultMetadata, "verifier">): Buffer {
  return Buffer.from(
    JSON.stringify({
      version: metadata.version,
      cipher: metadata.cipher,
      kdf: VAULT_KDF,
      salt: metadata.salt,
      vaultId: metadata.vaultId,
    }),
    "utf8",
  );
}

async function readMetadata(path: string): Promise<VaultMetadata> {
  try {
    await privateFile(path);
    return parseMetadata(await readBoundedFile(path, METADATA_BYTES, { rejectSymlinks: true }));
  } catch (error) {
    if (missing(error)) throw new PassphraseVaultError("not-created");
    throw new PassphraseVaultError("unavailable");
  }
}

/** Inspection never creates, resets, or migrates stored data. */
export async function inspectPassphraseVault(dataRoot: string): Promise<"missing" | "present"> {
  const locations = paths(dataRoot);
  try {
    await checkDirectoryPath(locations.dataRoot);
    await readMetadata(locations.vaultFile);
    return "present";
  } catch (error) {
    if (error instanceof PassphraseVaultError && error.code === "not-created") return "missing";
    throw new PassphraseVaultError("unavailable");
  }
}

async function acquireLease(dataRoot: string): Promise<() => Promise<void>> {
  if (process.platform !== "linux") throw new PassphraseVaultError("unavailable");
  const leasePath = join(dataRoot, "vault.lock");
  const handle = await open(
    leasePath,
    constants.O_CREAT | constants.O_RDWR | (constants.O_NOFOLLOW ?? 0),
    0o600,
  );
  try {
    const identity = await handle.stat();
    if (
      !identity.isFile() ||
      identity.nlink !== 1 ||
      (process.getuid !== undefined && identity.uid !== process.getuid()) ||
      (identity.mode & 0o077) !== 0
    )
      throw new PassphraseVaultError("unavailable");
    // flock locks this shared open-file description. The parent retains it after the
    // helper exits, so closing the descriptor or process death releases the kernel lock.
    const status = await new Promise<number | null>((complete, reject) => {
      const child = spawn("/usr/bin/flock", ["-n", "-x", "-E", "75", "3"], {
        stdio: ["ignore", "ignore", "ignore", handle.fd],
        env: {},
      });
      const timeout = setTimeout(() => {
        child.kill("SIGKILL");
        reject(new PassphraseVaultError("unavailable"));
      }, 10000);
      child.once("error", (error) => {
        clearTimeout(timeout);
        reject(error);
      });
      child.once("close", (code) => {
        clearTimeout(timeout);
        complete(code);
      });
    });
    if (status !== 0) throw new PassphraseVaultError(status === 75 ? "in-use" : "unavailable");
    return () => handle.close();
  } catch (error) {
    await handle.close();
    throw error instanceof PassphraseVaultError ? error : new PassphraseVaultError("unavailable");
  }
}

/** Publish a fully synced metadata file without ever replacing an existing vault. */
async function createMetadata(path: string, metadata: VaultMetadata): Promise<void> {
  const temporary = join(dirname(path), `.vault.${randomUUID()}.tmp`);
  const handle = await open(temporary, "wx", 0o600);
  try {
    await handle.writeFile(`${JSON.stringify(metadata)}\n`, "utf8");
    await handle.sync();
    await handle.close();
    await link(temporary, path);
  } finally {
    await handle.close().catch(() => undefined);
    await unlink(temporary).catch(() => undefined);
  }
  const directory = await open(dirname(path), constants.O_RDONLY | constants.O_DIRECTORY);
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
}

export async function openPassphraseVault(
  input: OpenPassphraseVaultInput,
): Promise<PassphraseVault> {
  if (
    typeof input.passphrase !== "string" ||
    Buffer.byteLength(input.passphrase, "utf8") < 12 ||
    Buffer.byteLength(input.passphrase, "utf8") > 1024
  )
    throw new PassphraseVaultError("invalid-passphrase");
  const locations = paths(input.dataRoot);
  let release: (() => Promise<void>) | undefined;
  let key: Buffer | undefined;
  try {
    await checkDirectoryPath(locations.dataRoot);
    await mkdir(locations.dataRoot, { recursive: true, mode: 0o700 });
    const directory = await lstat(locations.dataRoot);
    if (
      !directory.isDirectory() ||
      (process.getuid !== undefined && directory.uid !== process.getuid())
    )
      throw new PassphraseVaultError("unavailable");
    await chmod(locations.dataRoot, 0o700);
    release = await acquireLease(locations.dataRoot);
    let metadata: VaultMetadata;
    if (input.mode === "create") {
      const entries = await readdir(locations.dataRoot);
      if (entries.includes("setup-code")) await privateFile(join(locations.dataRoot, "setup-code"));
      if (entries.some((name) => name !== "vault.lock" && name !== "setup-code"))
        throw new PassphraseVaultError("already-exists");
      const initial = {
        version: 1 as const,
        cipher: "aes-256-gcm" as const,
        kdf: VAULT_KDF,
        salt: randomBytes(32).toString("base64"),
        vaultId: randomBytes(16).toString("base64"),
      };
      key = await deriveVaultKey(input.passphrase, Buffer.from(initial.salt, "base64"));
      metadata = {
        ...initial,
        verifier: encryptVaultValue(key, aad(initial), VERIFIER).toString("base64"),
      };
      await createMetadata(locations.vaultFile, metadata);
    } else {
      metadata = await readMetadata(locations.vaultFile);
      key = await deriveVaultKey(input.passphrase, Buffer.from(metadata.salt, "base64"));
      try {
        if (
          decryptVaultValue(key, aad(metadata), Buffer.from(metadata.verifier, "base64")) !==
          VERIFIER
        )
          throw new Error("Invalid verifier");
      } catch {
        throw new PassphraseVaultError("unlock-failed");
      }
    }
    let liveKey: Buffer | undefined = key;
    const valueAad = Buffer.concat([aad(metadata), Buffer.from("\0profile-value", "utf8")]);
    const protector: ProfileProtector = {
      protect(plaintext) {
        if (liveKey === undefined) return Promise.reject(new PassphraseVaultError("locked"));
        try {
          return Promise.resolve(encryptVaultValue(liveKey, valueAad, plaintext));
        } catch {
          return Promise.reject(new PassphraseVaultError("invalid-value"));
        }
      },
      unprotect(protectedValue) {
        if (liveKey === undefined) return Promise.reject(new PassphraseVaultError("locked"));
        try {
          return Promise.resolve({
            plaintext: decryptVaultValue(liveKey, valueAad, protectedValue),
            shouldReEncrypt: false,
          });
        } catch {
          return Promise.reject(new PassphraseVaultError("invalid-value"));
        }
      },
    };
    const releaseLease = release;
    return {
      capability: { durability: "durable", protection: "passphrase-protected", state: "ready" },
      paths: locations,
      protector,
      async lock(): Promise<void> {
        liveKey?.fill(0);
        liveKey = undefined;
        await releaseLease();
      },
    };
  } catch (error) {
    key?.fill(0);
    await release?.().catch(() => undefined);
    throw error instanceof PassphraseVaultError ? error : new PassphraseVaultError("unavailable");
  }
}
