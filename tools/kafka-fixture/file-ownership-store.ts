import { randomUUID } from "node:crypto";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

import {
  assertFixtureName,
  parseOwnedFixtureRecord,
  type FixtureOwnershipStore,
  type OwnedFixtureRecord,
} from "./lifecycle";

function isMissingFile(error: unknown): boolean {
  return (
    error !== null &&
    typeof error === "object" &&
    "code" in error &&
    (error as { readonly code?: unknown }).code === "ENOENT"
  );
}

export class FileFixtureOwnershipStore implements FixtureOwnershipStore {
  constructor(private readonly directory: string) {}

  async load(name: string): Promise<OwnedFixtureRecord | undefined> {
    const recordPath = this.recordPath(name);

    try {
      const value: unknown = JSON.parse(await readFile(recordPath, "utf8"));
      const record = parseOwnedFixtureRecord(value);
      if (record.name !== name) {
        throw new Error(`Fixture ownership record ${recordPath} has a mismatched name.`);
      }
      return record;
    } catch (error: unknown) {
      if (isMissingFile(error)) {
        return undefined;
      }
      throw error;
    }
  }

  async save(record: OwnedFixtureRecord): Promise<void> {
    const recordPath = this.recordPath(record.name);
    const temporaryPath = join(this.directory, `.${record.name}.${randomUUID()}.tmp`);
    await mkdir(this.directory, { recursive: true });

    try {
      await writeFile(temporaryPath, `${JSON.stringify(record, undefined, 2)}\n`, {
        encoding: "utf8",
        flag: "wx",
        mode: 0o600,
      });
      await rename(temporaryPath, recordPath);
    } finally {
      await rm(temporaryPath, { force: true });
    }
  }

  async remove(name: string): Promise<void> {
    await rm(this.recordPath(name), { force: true });
  }

  private recordPath(name: string): string {
    assertFixtureName(name);
    return join(this.directory, `${name}.json`);
  }
}
