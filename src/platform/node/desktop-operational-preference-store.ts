import { randomUUID } from "node:crypto";
import { mkdir, readdir, rename, rmdir } from "node:fs/promises";
import { join } from "node:path";

import type {
  KafkaOperationalPreferences,
  KafkaOperationalPreferenceStoreCapability,
} from "../../features/kafka/contracts";
import type { KafkaOperationalPreferenceStore } from "../../features/kafka/application";

import {
  AtomicKafkaOperationalPreferenceFileStore,
  KafkaOperationalPreferenceFileCorruptError,
  KafkaOperationalPreferenceFileWriteError,
} from "./kafka-operational-preference-file-store";

/** Keep application settings separate from Chromium's case-sensitive spelling, Preferences. */
export class DesktopOperationalPreferenceStore implements KafkaOperationalPreferenceStore {
  private readonly current: AtomicKafkaOperationalPreferenceFileStore;
  private unavailable = false;

  constructor(private readonly userDataPath: string) {
    this.current = new AtomicKafkaOperationalPreferenceFileStore(
      join(userDataPath, "workbench", "kafka-operational-preferences.json"),
    );
  }

  capability(): KafkaOperationalPreferenceStoreCapability {
    return this.unavailable
      ? {
          durability: "durable",
          state: "unavailable",
          recovery:
            "Restore operational preferences, or reset them while disconnected and review Protection.",
        }
      : this.current.capability();
  }

  async load(signal?: AbortSignal): Promise<KafkaOperationalPreferences | undefined> {
    try {
      const current = await this.current.load(signal);
      const legacy = await this.legacyDirectory();
      if (legacy === undefined) return current;
      const stored =
        current ??
        (await new AtomicKafkaOperationalPreferenceFileStore(
          join(legacy, "kafka-operational-preferences.json"),
        ).load(signal));
      signal?.throwIfAborted();
      // Commit before moving the old directory. If interrupted, the new document
      // remains authoritative and the next load completes the archive operation.
      if (current === undefined && stored !== undefined) await this.current.commit(stored, signal);
      await this.archiveLegacyDirectory(legacy, signal);
      return stored;
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") throw error;
      this.unavailable = true;
      throw new KafkaOperationalPreferenceFileCorruptError();
    }
  }

  async commit(preferences: KafkaOperationalPreferences, signal?: AbortSignal): Promise<void> {
    try {
      await this.current.commit(preferences, signal);
      // A deliberate reset may recover malformed legacy data. Retain its bytes
      // in an isolated archive rather than deleting them or weakening protection.
      const legacy = await this.legacyDirectory();
      if (legacy !== undefined) await this.archiveLegacyDirectory(legacy, signal);
      this.unavailable = false;
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") throw error;
      this.unavailable = true;
      throw new KafkaOperationalPreferenceFileWriteError();
    }
  }

  private async legacyDirectory(): Promise<string | undefined> {
    let entries;
    try {
      entries = await readdir(this.userDataPath, { withFileTypes: true });
    } catch (error) {
      if (error instanceof Error && "code" in error && error.code === "ENOENT") return undefined;
      throw error;
    }
    const legacy =
      entries.find((entry) => entry.name === "preferences") ??
      entries.find((entry) => entry.name.toLowerCase() === "preferences");
    if (legacy === undefined) return undefined;
    // Chromium owns this regular file. Never read, reset, move or replace it.
    if (legacy.name === "Preferences" && legacy.isFile()) return undefined;
    if (!legacy.isDirectory()) throw new KafkaOperationalPreferenceFileCorruptError();
    return join(this.userDataPath, legacy.name);
  }

  private async archiveLegacyDirectory(legacy: string, signal?: AbortSignal): Promise<void> {
    const migrations = join(this.userDataPath, "workbench", "migrations");
    await mkdir(migrations, { recursive: true, mode: 0o700 });
    const archive = join(migrations, `preferences-${randomUUID()}`);
    await mkdir(archive, { mode: 0o700 });
    try {
      signal?.throwIfAborted();
      await rename(legacy, join(archive, "preferences"));
    } catch (error) {
      // Only this attempt's still-empty reservation can be removed.
      await rmdir(archive).catch(() => undefined);
      throw error;
    }
  }
}
