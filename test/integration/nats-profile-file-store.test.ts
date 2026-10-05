import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  stat,
  symlink,
  truncate,
  writeFile,
} from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { afterEach, beforeAll, describe, expect, it } from "vitest";

import { NATS_LIMITS } from "../../src/features/nats/contracts";
import type { NatsProfileRecord } from "../../src/features/nats/application/profile-types";
import { parseNatsProfileRecord } from "../../src/features/nats/application/profile-record-validation";
import {
  AtomicNatsProfileFileStore,
  NatsProfileFileCorruptError,
  NatsProfileFileWriteError,
} from "../../src/platform/node/nats-profile-file-store";
import {
  ReversibleProfileProtector,
  protectedProfileFixtureCa,
} from "../support/protected-profile-fixture";

const temporaryDirectories: string[] = [];
const time = "2026-10-05T12:00:00.000Z";
let caPem: string;

beforeAll(async () => {
  caPem = await protectedProfileFixtureCa();
});

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

async function profilePath(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-nats-profile-"));
  temporaryDirectories.push(directory);
  return join(directory, "profiles", "nats-profiles.json");
}

function profile(id = "nats-profile"): NatsProfileRecord {
  return {
    id,
    revision: 1,
    name: `Private NATS ${id}`,
    servers: ["tls://nats.example.test:4222"],
    authentication: { mode: "token", token: `token-never-visible-${id}` },
    tls: { mode: "tls", caPem },
    createdAt: time,
    updatedAt: time,
  };
}

interface StoredDocument {
  version: number;
  profiles: Array<{ id: string; revision: number; protectedValue: string }>;
}

async function storedDocument(path: string): Promise<StoredDocument> {
  return JSON.parse(await readFile(path, "utf8")) as StoredDocument;
}

async function seed(
  path: string,
  protector: ReversibleProfileProtector,
  records: readonly NatsProfileRecord[] = [profile()],
): Promise<Buffer> {
  await new AtomicNatsProfileFileStore(path, protector).save(records);
  return readFile(path);
}

function deferred<T>(): { readonly promise: Promise<T>; resolve(value: T): void } {
  let resolve = (_value: T): void => undefined;
  const promise = new Promise<T>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

describe("protected NATS profile storage", () => {
  it("restores complete credentials after restart while keeping record data out of disk metadata", async () => {
    const path = await profilePath();
    const protector = new ReversibleProfileProtector();
    const original = profile();

    await new AtomicNatsProfileFileStore(path, protector).save([original]);
    const disk = await readFile(path, "utf8");
    const reopened = new AtomicNatsProfileFileStore(path, new ReversibleProfileProtector());

    expect(await reopened.load()).toEqual([parseNatsProfileRecord(original)]);
    expect(disk).not.toContain("token-never-visible");
    expect(disk).not.toContain("BEGIN CERTIFICATE");
    expect(disk).not.toContain(caPem.split("\n")[1]);
    expect(disk).not.toContain(original.name);
    expect(disk).not.toContain(original.servers[0]);
    expect(reopened.capability).toEqual({
      durability: "durable",
      protection: "os-protected",
      state: "ready",
    });
    if (process.platform !== "win32") {
      expect((await stat(path)).mode & 0o777).toBe(0o600);
      expect((await stat(join(path, ".."))).mode & 0o777).toBe(0o700);
    }
  });

  it("distinguishes an absent installation from a custom ENOENT-shaped cancellation", async () => {
    const path = await profilePath();
    const store = new AtomicNatsProfileFileStore(path, new ReversibleProfileProtector());
    expect(await store.load()).toEqual([]);
    const controller = new AbortController();
    const reason = { code: "ENOENT", detail: "cancelled before the private read" };
    controller.abort(reason);

    await expect(store.load(controller.signal)).rejects.toBe(reason);
    await expect(store.save([], controller.signal)).rejects.toBe(reason);
    expect(store.capability.state).toBe("ready");
    await expect(readFile(path)).rejects.toMatchObject({ code: "ENOENT" });
  });

  it.each([
    "invalid JSON",
    "unknown version",
    "unknown envelope field",
    "duplicate IDs",
    "invalid base64",
    "noncanonical base64",
  ])("preserves %s and denies an empty replacement", async (failure) => {
    const path = await profilePath();
    const protector = new ReversibleProfileProtector();
    await seed(path, protector);
    const document = await storedDocument(path);
    let contents: string;
    if (failure === "invalid JSON") contents = "{broken-json";
    else {
      if (failure === "unknown version") document.version = 2;
      if (failure === "unknown envelope field") Object.assign(document, { future: true });
      if (failure === "duplicate IDs") document.profiles.push({ ...document.profiles[0]! });
      if (failure === "invalid base64") document.profiles[0]!.protectedValue = "invalid-base64!";
      if (failure === "noncanonical base64") document.profiles[0]!.protectedValue = "AB==";
      contents = JSON.stringify(document);
    }
    await writeFile(path, contents);
    const corrupt = await readFile(path);
    const store = new AtomicNatsProfileFileStore(path, protector);
    const protectionCalls = protector.protectedInputs.length;

    await expect(store.load()).rejects.toBeInstanceOf(NatsProfileFileCorruptError);
    await expect(store.save([])).rejects.toBeInstanceOf(NatsProfileFileCorruptError);
    expect(store.capability.state).toBe("unavailable");
    expect(await readFile(path)).toEqual(corrupt);
    expect(protector.protectedInputs).toHaveLength(protectionCalls);
  });

  it.each(["transplanted ciphertext", "changed ID", "changed revision", "foreign provider"])(
    "rejects %s without exposing or replacing the original data",
    async (failure) => {
      const path = await profilePath();
      const protector = new ReversibleProfileProtector();
      await seed(path, protector, [profile("one"), profile("two")]);
      const document = await storedDocument(path);
      if (failure === "transplanted ciphertext")
        document.profiles[0]!.protectedValue = document.profiles[1]!.protectedValue;
      if (failure === "changed ID") document.profiles[0]!.id = "different";
      if (failure === "changed revision") document.profiles[0]!.revision = 2;
      if (failure === "foreign provider") {
        const restored = await protector.unprotect(
          Buffer.from(document.profiles[0]!.protectedValue, "base64"),
        );
        const envelope = JSON.parse(restored.plaintext) as Record<string, unknown>;
        envelope.provider = "kafka";
        document.profiles[0]!.protectedValue = (
          await protector.protect(JSON.stringify(envelope))
        ).toString("base64");
      }
      await writeFile(path, JSON.stringify(document));
      const original = await readFile(path);
      const store = new AtomicNatsProfileFileStore(path, protector);

      await expect(store.load()).rejects.toBeInstanceOf(NatsProfileFileCorruptError);
      await expect(store.load()).rejects.not.toThrow(/token-never-visible|BEGIN CERTIFICATE/u);
      await expect(store.save([])).rejects.toBeInstanceOf(NatsProfileFileCorruptError);
      expect(await readFile(path)).toEqual(original);
    },
  );

  it("treats an ENOENT-shaped decryption failure as corruption rather than a missing catalog", async () => {
    const path = await profilePath();
    const protector = new ReversibleProfileProtector();
    const original = await seed(path, protector);
    protector.unprotectOperation = (): Promise<never> =>
      Promise.reject(
        Object.assign(new Error("private token-never-visible diagnostic"), { code: "ENOENT" }),
      );
    const store = new AtomicNatsProfileFileStore(path, protector);

    await expect(store.load()).rejects.toBeInstanceOf(NatsProfileFileCorruptError);
    await expect(store.save([])).rejects.not.toThrow(/private|token-never-visible/u);
    expect(await readFile(path)).toEqual(original);
    expect(store.capability.state).toBe("unavailable");
  });

  it.skipIf(process.platform === "win32")(
    "refuses a symlink without reading or overwriting its target",
    async () => {
      const path = await profilePath();
      const target = join(path, "..", "owned-target.json");
      const protector = new ReversibleProfileProtector();
      const original = await seed(target, protector);
      await symlink(target, path);
      const store = new AtomicNatsProfileFileStore(path, protector);

      await expect(store.load()).rejects.toBeInstanceOf(NatsProfileFileCorruptError);
      await expect(store.save([])).rejects.toBeInstanceOf(NatsProfileFileCorruptError);
      expect(await readFile(target)).toEqual(original);
    },
  );

  it.skipIf(process.platform === "win32" || process.getuid?.() === 0)(
    "preserves an unreadable file instead of resetting the catalog",
    async () => {
      const path = await profilePath();
      const protector = new ReversibleProfileProtector();
      const original = await seed(path, protector);
      await chmod(path, 0o000);
      const store = new AtomicNatsProfileFileStore(path, protector);
      try {
        await expect(store.load()).rejects.toBeInstanceOf(NatsProfileFileCorruptError);
        await expect(store.save([])).rejects.toBeInstanceOf(NatsProfileFileCorruptError);
      } finally {
        await chmod(path, 0o600);
      }
      expect(await readFile(path)).toEqual(original);
    },
  );

  it("rejects a sparse oversized document before decryption and preserves its size", async () => {
    const path = await profilePath();
    const protector = new ReversibleProfileProtector();
    await mkdir(join(path, ".."), { recursive: true });
    await writeFile(path, "");
    await truncate(path, NATS_LIMITS.profileFileBytes + 1);
    const store = new AtomicNatsProfileFileStore(path, protector);

    await expect(store.load()).rejects.toBeInstanceOf(NatsProfileFileCorruptError);
    await expect(store.save([])).rejects.toBeInstanceOf(NatsProfileFileCorruptError);
    expect(protector.unprotectedInputs).toEqual([]);
    expect((await stat(path)).size).toBe(NATS_LIMITS.profileFileBytes + 1);
  });

  it("rejects oversized ciphertext and decrypted plaintext without replacing a valid file", async () => {
    const path = await profilePath();
    const protector = new ReversibleProfileProtector();
    const original = await seed(path, protector);
    protector.protectOperation = (): Promise<Buffer> =>
      Promise.resolve(Buffer.alloc(NATS_LIMITS.profileCiphertextBytes + 1));

    await expect(
      new AtomicNatsProfileFileStore(path, protector).save([profile("next")]),
    ).rejects.toBeInstanceOf(NatsProfileFileWriteError);
    expect(await readFile(path)).toEqual(original);
    protector.unprotectOperation = (): Promise<{
      readonly plaintext: string;
      readonly shouldReEncrypt: boolean;
    }> =>
      Promise.resolve({
        plaintext: "x".repeat(NATS_LIMITS.profilePlaintextBytes + 100),
        shouldReEncrypt: false,
      });
    await expect(new AtomicNatsProfileFileStore(path, protector).load()).rejects.toBeInstanceOf(
      NatsProfileFileCorruptError,
    );
    expect(await readFile(path)).toEqual(original);
  });

  it("discards a late protected value after custom cancellation and leaves no owned temporary file", async () => {
    const path = await profilePath();
    const protector = new ReversibleProfileProtector();
    const original = await seed(path, protector);
    const gate = deferred<Buffer>();
    const started = deferred<void>();
    protector.protectOperation = (): Promise<Buffer> => {
      started.resolve();
      return gate.promise;
    };
    const controller = new AbortController();
    const reason = { why: "caller retired during OS encryption" };
    const store = new AtomicNatsProfileFileStore(path, protector);
    const saving = store.save([profile("replacement")], controller.signal);
    await started.promise;
    controller.abort(reason);
    gate.resolve(Buffer.from("late protected result"));

    await expect(saving).rejects.toBe(reason);
    expect(await readFile(path)).toEqual(original);
    expect(await readdir(join(path, ".."))).toEqual(["nats-profiles.json"]);
    expect(store.capability.state).toBe("ready");
    expect(
      await new AtomicNatsProfileFileStore(path, new ReversibleProfileProtector()).load(),
    ).toEqual([parseNatsProfileRecord(profile())]);
  });

  it("does not publish a late decrypted catalog after caller cancellation", async () => {
    const path = await profilePath();
    const protector = new ReversibleProfileProtector();
    const original = await seed(path, protector);
    const restored = await protector.unprotect(
      Buffer.from((await storedDocument(path)).profiles[0]!.protectedValue, "base64"),
    );
    const gate = deferred<typeof restored>();
    const started = deferred<void>();
    protector.unprotectOperation = (): Promise<typeof restored> => {
      started.resolve();
      return gate.promise;
    };
    const controller = new AbortController();
    const reason = new Error("cancelled profile read");
    const store = new AtomicNatsProfileFileStore(path, protector);
    const loading = store.load(controller.signal);
    await started.promise;
    controller.abort(reason);
    gate.resolve(restored);

    await expect(loading).rejects.toBe(reason);
    expect(await readFile(path)).toEqual(original);
    expect(store.capability.state).toBe("ready");
  });

  it("retains committed data and an unrelated temporary file when atomic replacement collides", async () => {
    const path = await profilePath();
    const protector = new ReversibleProfileProtector();
    const original = await seed(path, protector);
    const collision = join(path, "..", ".nats-profiles.json.collision.tmp");
    await writeFile(collision, "unrelated owner");
    const store = new AtomicNatsProfileFileStore(path, protector, {
      createTempId: (): string => "collision",
    });

    await expect(store.save([profile("replacement")])).rejects.toBeInstanceOf(
      NatsProfileFileWriteError,
    );
    expect(await readFile(path)).toEqual(original);
    expect(await readFile(collision, "utf8")).toBe("unrelated owner");
    expect(store.capability.state).toBe("unavailable");
  });

  it("re-encrypts only after the complete catalog is validated and restores identical records", async () => {
    const path = await profilePath();
    const protector = new ReversibleProfileProtector();
    const original = await seed(path, protector, [profile("one"), profile("two")]);
    protector.rotate = true;
    const records = await new AtomicNatsProfileFileStore(path, protector).load();

    expect(records).toEqual([
      parseNatsProfileRecord(profile("one")),
      parseNatsProfileRecord(profile("two")),
    ]);
    expect(await readFile(path)).not.toEqual(original);
    expect(
      await new AtomicNatsProfileFileStore(path, new ReversibleProfileProtector()).load(),
    ).toEqual(records);
  });

  it("does not partially rotate a catalog when a later ciphertext is invalid", async () => {
    const path = await profilePath();
    const protector = new ReversibleProfileProtector();
    await seed(path, protector, [profile("one"), profile("two")]);
    const document = await storedDocument(path);
    document.profiles[1]!.protectedValue = Buffer.from("invalid protected payload").toString(
      "base64",
    );
    await writeFile(path, JSON.stringify(document));
    const original = await readFile(path);
    const protectionCalls = protector.protectedInputs.length;
    protector.rotate = true;

    await expect(new AtomicNatsProfileFileStore(path, protector).load()).rejects.toBeInstanceOf(
      NatsProfileFileCorruptError,
    );
    expect(await readFile(path)).toEqual(original);
    expect(protector.protectedInputs).toHaveLength(protectionCalls);
  });

  it("preserves a catalog with duplicate normalized names before attempting key rotation", async () => {
    const path = await profilePath();
    const protector = new ReversibleProfileProtector();
    await seed(path, protector, [profile("one"), profile("two")]);
    const document = await storedDocument(path);
    const restored = await protector.unprotect(
      Buffer.from(document.profiles[1]!.protectedValue, "base64"),
    );
    const envelope = JSON.parse(restored.plaintext) as { profile: Record<string, unknown> };
    envelope.profile.name = profile("one").name.toUpperCase();
    document.profiles[1]!.protectedValue = (
      await protector.protect(JSON.stringify(envelope))
    ).toString("base64");
    await writeFile(path, JSON.stringify(document));
    const original = await readFile(path);
    const protectionCalls = protector.protectedInputs.length;
    protector.rotate = true;
    const store = new AtomicNatsProfileFileStore(path, protector);

    await expect(store.load()).rejects.toBeInstanceOf(NatsProfileFileCorruptError);
    await expect(store.save([])).rejects.toBeInstanceOf(NatsProfileFileCorruptError);
    expect(await readFile(path)).toEqual(original);
    expect(protector.protectedInputs).toHaveLength(protectionCalls);
  });

  it("preserves the complete original catalog when rotation protection fails", async () => {
    const path = await profilePath();
    const protector = new ReversibleProfileProtector();
    const original = await seed(path, protector);
    protector.rotate = true;
    protector.protectOperation = (): Promise<never> =>
      Promise.reject(new Error("private encryption failure"));
    const store = new AtomicNatsProfileFileStore(path, protector);

    await expect(store.load()).rejects.toBeInstanceOf(NatsProfileFileCorruptError);
    await expect(store.save([])).rejects.toBeInstanceOf(NatsProfileFileCorruptError);
    expect(await readFile(path)).toEqual(original);
    expect(await readdir(join(path, ".."))).toEqual(["nats-profiles.json"]);
  });
});
