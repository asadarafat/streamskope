import { mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { KafkaOperationalPreferenceService } from "../../src/features/kafka/application";
import { KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS } from "../../src/features/kafka/contracts";
import { DesktopOperationalPreferenceStore } from "../../src/platform/node/desktop-operational-preference-store";

const roots: string[] = [];
const protectedPreferences = {
  ...KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  protection: {
    readOnly: true,
    maskKey: true,
    maskHeaders: ["authorization"],
    valuePaths: ["/customer"],
  },
};

async function root(): Promise<string> {
  const path = await mkdtemp(join(tmpdir(), "streamskope-desktop-preferences-"));
  roots.push(path);
  return path;
}

function currentPath(path: string): string {
  return join(path, "workbench", "kafka-operational-preferences.json");
}

async function legacy(path: string, bytes: string): Promise<string> {
  const directory = join(path, "preferences");
  await mkdir(directory);
  const file = join(directory, "kafka-operational-preferences.json");
  await writeFile(file, bytes);
  return file;
}

async function archivedBytes(path: string): Promise<string> {
  const migrations = join(path, "workbench", "migrations");
  const snapshots = await readdir(migrations);
  expect(snapshots).toHaveLength(1);
  return readFile(
    join(migrations, snapshots[0]!, "preferences", "kafka-operational-preferences.json"),
    "utf8",
  );
}

afterEach(async () => {
  for (const path of roots.splice(0)) await rm(path, { recursive: true, force: true });
});

describe("desktop preferences coexist with Chromium", () => {
  it("keeps defaults ready across restart when Chromium owns Preferences, without modifying that file", async () => {
    const path = await root();
    const chromium = join(path, "Preferences");
    const browserBytes = '{"browser":{"window_placement":{"left":31}}}\n';
    await writeFile(chromium, browserBytes);
    const first = new KafkaOperationalPreferenceService(
      new DesktopOperationalPreferenceStore(path),
    );
    await expect(first.get()).resolves.toMatchObject({ store: { state: "ready" } });
    await first.update({ protection: protectedPreferences.protection });
    expect(JSON.parse(await readFile(currentPath(path), "utf8"))).toMatchObject({
      preferences: { protection: protectedPreferences.protection },
    });
    const restarted = new KafkaOperationalPreferenceService(
      new DesktopOperationalPreferenceStore(path),
    );
    await expect(restarted.get()).resolves.toMatchObject({
      preferences: { protection: protectedPreferences.protection },
      store: { state: "ready" },
    });
    await expect(readFile(chromium, "utf8")).resolves.toBe(browserBytes);
    expect((await stat(chromium)).isFile()).toBe(true);
    expect((await readdir(path)).includes("preferences")).toBe(false);
  });

  it("migrates every protection choice and archives original bytes before releasing the old browser namespace", async () => {
    const path = await root();
    const bytes = `${JSON.stringify({ version: 1, preferences: protectedPreferences }, null, 2)}\n`;
    const original = await legacy(path, bytes);
    const service = new KafkaOperationalPreferenceService(
      new DesktopOperationalPreferenceStore(path),
    );
    await expect(service.get()).resolves.toMatchObject({
      preferences: protectedPreferences,
      store: { state: "ready" },
    });
    expect(JSON.parse(await readFile(currentPath(path), "utf8"))).toEqual({
      preferences: protectedPreferences,
      version: 1,
    });
    await expect(readFile(original)).rejects.toMatchObject({ code: "ENOENT" });
    await expect(archivedBytes(path)).resolves.toBe(bytes);
    await writeFile(join(path, "Preferences"), '{"browser":{}}');
    await expect(new DesktopOperationalPreferenceStore(path).load()).resolves.toEqual(
      protectedPreferences,
    );
  });

  it("keeps malformed legacy data blocked until deliberate safe reset and preserves its original bytes", async () => {
    const path = await root();
    const bytes = '{"version":99,"preferences":{"protection":"invalid"}}';
    const original = await legacy(path, bytes);
    const service = new KafkaOperationalPreferenceService(
      new DesktopOperationalPreferenceStore(path),
    );
    await expect(service.get()).resolves.toMatchObject({ store: { state: "unavailable" } });
    await expect(readFile(original, "utf8")).resolves.toBe(bytes);
    await expect(stat(currentPath(path))).rejects.toMatchObject({ code: "ENOENT" });
    await expect(
      service.update({
        protection: { readOnly: false, maskKey: false, maskHeaders: [], valuePaths: [] },
      }),
    ).rejects.toThrow();
    await expect(service.reset()).resolves.toMatchObject({
      preferences: { protection: { readOnly: true, maskKey: true, valuePaths: [""] } },
      store: { state: "ready" },
    });
    await expect(archivedBytes(path)).resolves.toBe(bytes);
  });

  it("does not archive the legacy document if its new private location cannot be committed", async () => {
    const path = await root();
    const bytes = JSON.stringify({ version: 1, preferences: protectedPreferences });
    const original = await legacy(path, bytes);
    await writeFile(join(path, "workbench"), "operator-owned obstruction");
    const service = new KafkaOperationalPreferenceService(
      new DesktopOperationalPreferenceStore(path),
    );
    await expect(service.get()).resolves.toMatchObject({ store: { state: "unavailable" } });
    await expect(readFile(original, "utf8")).resolves.toBe(bytes);
  });

  it("finishes interrupted archival using the already committed preferences, without reverting protection", async () => {
    const path = await root();
    await mkdir(join(path, "workbench"));
    await writeFile(
      currentPath(path),
      JSON.stringify({ version: 1, preferences: protectedPreferences }),
    );
    const older = JSON.stringify({
      version: 1,
      preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
    });
    await legacy(path, older);
    await expect(new DesktopOperationalPreferenceStore(path).load()).resolves.toEqual(
      protectedPreferences,
    );
    await expect(archivedBytes(path)).resolves.toBe(older);
  });

  it("never falls back to a legacy document when the current preference document is corrupt", async () => {
    const path = await root();
    await mkdir(join(path, "workbench"));
    await writeFile(currentPath(path), "unreadable current preferences");
    const bytes = JSON.stringify({
      version: 1,
      preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
    });
    const original = await legacy(path, bytes);
    const service = new KafkaOperationalPreferenceService(
      new DesktopOperationalPreferenceStore(path),
    );
    await expect(service.get()).resolves.toMatchObject({ store: { state: "unavailable" } });
    await expect(readFile(original, "utf8")).resolves.toBe(bytes);
  });
});
