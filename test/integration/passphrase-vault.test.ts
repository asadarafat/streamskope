import { createDecipheriv, scryptSync } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { chmod, mkdtemp, readFile, readdir, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { AtomicKafkaProfileFileStore } from "../../src/platform/node/kafka-profile-file-store";
import { AtomicNatsProfileFileStore } from "../../src/platform/node/nats-profile-file-store";
import {
  inspectPassphraseVault,
  openPassphraseVault,
  type PassphraseVault,
} from "../../src/platform/node/vault/passphrase-vault";
import { parseProfileStoreCapability } from "../../src/features/kafka/contracts/profile-validation";
import { parseNatsProfileStoreCapability } from "../../src/features/nats/contracts";
import { natsProfileStorageLabel } from "../../src/features/nats/ui/profile-presentation";

const phrase = "independent vault passphrase fixture";
const roots: string[] = [];
const vaults: PassphraseVault[] = [];
const children: ChildProcess[] = [];

afterEach(async () => {
  for (const child of children.splice(0)) child.kill("SIGKILL");
  await Promise.all(vaults.splice(0).map((vault) => vault.lock()));
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function directory(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-vault-"));
  roots.push(root);
  return root;
}

async function create(dataRoot: string, passphrase = phrase): Promise<PassphraseVault> {
  const vault = await openPassphraseVault({ dataRoot, passphrase, mode: "create" });
  vaults.push(vault);
  return vault;
}

async function unlock(dataRoot: string, passphrase = phrase): Promise<PassphraseVault> {
  const vault = await openPassphraseVault({ dataRoot, passphrase, mode: "unlock" });
  vaults.push(vault);
  return vault;
}

describe.skipIf(process.platform !== "linux")(
  "Linux passphrase vault and persistent provider profiles",
  () => {
    it("persists real encrypted Kafka/NATS credentials and honestly declares vault protection after restart", async () => {
      const root = await directory();
      expect(await inspectPassphraseVault(root)).toBe("missing");
      expect(await readdir(root)).toEqual([]);
      const vault = await create(root);
      const time = "2026-10-07T10:00:00.000Z";
      const kafka = {
        id: "kafka-private",
        name: "Private Kafka",
        brokers: ["broker.example.test:9093"],
        transport: "tls" as const,
        oauth: {
          clientId: "client",
          clientSecret: "kafka-secret-never-on-disk",
          scope: "kafka",
          tokenEndpoint: "https://auth.example.test/token",
        },
        trust: {
          kind: "pem" as const,
          label: "test CA",
          material: "private-ca-material",
          password: "trust-secret",
        },
        createdAt: time,
        updatedAt: time,
      };
      const nats = {
        id: "nats-private",
        revision: 1,
        name: "Private NATS",
        servers: ["tls://nats.example.test:4222"],
        authentication: { mode: "token" as const, token: "nats-secret-never-on-disk" },
        tls: { mode: "tls" as const },
        createdAt: time,
        updatedAt: time,
      };
      await new AtomicKafkaProfileFileStore(
        vault.paths.kafkaProfiles,
        vault.protector,
        vault.capability,
      ).commit([kafka]);
      await new AtomicNatsProfileFileStore(vault.paths.natsProfiles, vault.protector, {
        protection: "passphrase-protected",
      }).save([nats]);
      const before = await Promise.all(
        [vault.paths.vaultFile, vault.paths.kafkaProfiles, vault.paths.natsProfiles].map((path) =>
          readFile(path, "utf8"),
        ),
      );
      for (const contents of before) {
        for (const secret of [
          phrase,
          "kafka-secret-never-on-disk",
          "nats-secret-never-on-disk",
          "private-ca-material",
          "trust-secret",
        ])
          expect(contents).not.toContain(secret);
      }
      await vault.lock();
      const reopened = await unlock(root);
      expect(
        await new AtomicKafkaProfileFileStore(
          reopened.paths.kafkaProfiles,
          reopened.protector,
          reopened.capability,
        ).load(),
      ).toEqual([kafka]);
      const natsStore = new AtomicNatsProfileFileStore(
        reopened.paths.natsProfiles,
        reopened.protector,
        { protection: "passphrase-protected" },
      );
      expect(await natsStore.load()).toEqual([nats]);
      expect(parseNatsProfileStoreCapability(natsStore.capability).protection).toBe(
        "passphrase-protected",
      );
      expect(parseProfileStoreCapability(reopened.capability, "store").protection).toBe(
        "passphrase-protected",
      );
      expect(natsProfileStorageLabel(natsStore.capability)).toContain("passphrase vault");
      expect(natsProfileStorageLabel(natsStore.capability)).not.toContain("operating system");
      expect(await inspectPassphraseVault(root)).toBe("present");
      for (const file of await readdir(root))
        expect((await stat(join(root, file))).mode & 0o777).toBe(0o600);
      expect((await stat(root)).mode & 0o777).toBe(0o700);
    });

    it("uses independently decryptable AES-GCM ciphertext, unique nonces and a scrypt key", async () => {
      const vault = await create(await directory());
      const metadata = JSON.parse(await readFile(vault.paths.vaultFile, "utf8")) as {
        version: number;
        cipher: string;
        salt: string;
        vaultId: string;
        kdf: { name: string; N: number; r: number; p: number };
      };
      const key = scryptSync(phrase, Buffer.from(metadata.salt, "base64"), 32, {
        N: 32768,
        r: 8,
        p: 1,
        maxmem: 64 * 1_048_576,
      });
      const first = await vault.protector.protect("sensitive fixture");
      const second = await vault.protector.protect("sensitive fixture");
      expect(first.equals(second)).toBe(false);
      expect(first.subarray(0, 4).toString("ascii")).toBe("SKV1");
      const decoder = createDecipheriv("aes-256-gcm", key, first.subarray(4, 16));
      decoder.setAAD(
        Buffer.from(
          `${JSON.stringify({ version: metadata.version, cipher: metadata.cipher, kdf: metadata.kdf, salt: metadata.salt, vaultId: metadata.vaultId })}\0profile-value`,
          "utf8",
        ),
      );
      decoder.setAuthTag(first.subarray(16, 32));
      expect(
        Buffer.concat([decoder.update(first.subarray(32)), decoder.final()]).toString("utf8"),
      ).toBe("sensitive fixture");
      key.fill(0);
    });

    it("rejects a wrong passphrase without changing stored bytes or retaining a writer lock", async () => {
      const root = await directory();
      const vault = await create(root);
      const before = await readFile(vault.paths.vaultFile);
      await vault.lock();
      await expect(unlock(root, "wrong secret phrase fixture")).rejects.toMatchObject({
        code: "unlock-failed",
      });
      expect(await readFile(vault.paths.vaultFile)).toEqual(before);
      await unlock(root);
    });

    it("locks every previously supplied protector and rejects tampering, truncation, and another vault's ciphertext", async () => {
      const first = await create(await directory());
      const second = await create(await directory());
      const ciphertext = await first.protector.protect("private fixture");
      await expect(second.protector.unprotect(ciphertext)).rejects.toMatchObject({
        code: "invalid-value",
      });
      const tampered = Buffer.from(ciphertext);
      tampered.writeUInt8(tampered.readUInt8(tampered.length - 1) ^ 1, tampered.length - 1);
      await expect(first.protector.unprotect(tampered)).rejects.toMatchObject({
        code: "invalid-value",
      });
      await expect(first.protector.unprotect(ciphertext.subarray(0, 16))).rejects.toMatchObject({
        code: "invalid-value",
      });
      await first.lock();
      await first.lock();
      await expect(first.protector.protect("new secret")).rejects.toMatchObject({ code: "locked" });
      await expect(first.protector.unprotect(ciphertext)).rejects.toMatchObject({ code: "locked" });
    });

    it.each(["not-json", "unsupported", "unsafe-kdf", "oversized"])(
      "preserves %s metadata and refuses implicit reinitialization",
      async (mutation) => {
        const root = await directory();
        const vault = await create(root);
        await vault.lock();
        const parsed = JSON.parse(await readFile(vault.paths.vaultFile, "utf8")) as {
          version: number;
          kdf: { N: number };
        };
        if (mutation === "unsupported") parsed.version = 2;
        if (mutation === "unsafe-kdf") parsed.kdf.N = 2 ** 30;
        const bad =
          mutation === "not-json"
            ? "{bad"
            : mutation === "oversized"
              ? "x".repeat(4097)
              : JSON.stringify(parsed);
        await writeFile(vault.paths.vaultFile, bad, { mode: 0o600 });
        await expect(inspectPassphraseVault(root)).rejects.toMatchObject({ code: "unavailable" });
        await expect(unlock(root)).rejects.toMatchObject({ code: "unavailable" });
        await expect(create(root)).rejects.toMatchObject({ code: "already-exists" });
        expect(await readFile(vault.paths.vaultFile, "utf8")).toBe(bad);
      },
    );

    it("rejects symlink roots, symlink metadata and loose metadata permissions", async () => {
      const real = await directory();
      const alias = join(await directory(), "linked");
      await symlink(real, alias);
      await expect(create(alias)).rejects.toMatchObject({ code: "unavailable" });
      const vault = await create(real);
      await vault.lock();
      const copy = join(await directory(), "vault-copy.json");
      await writeFile(copy, await readFile(vault.paths.vaultFile), { mode: 0o600 });
      await rm(vault.paths.vaultFile);
      await symlink(copy, vault.paths.vaultFile);
      await expect(unlock(real)).rejects.toMatchObject({ code: "unavailable" });
      await rm(vault.paths.vaultFile);
      await writeFile(vault.paths.vaultFile, await readFile(copy), { mode: 0o600 });
      await chmod(vault.paths.vaultFile, 0o644);
      await expect(unlock(real)).rejects.toMatchObject({ code: "unavailable" });
      expect((await stat(vault.paths.vaultFile)).mode & 0o777).toBe(0o644);
    });

    it("refuses to create over orphaned profile data and bounds passphrases before creating files", async () => {
      const root = await directory();
      await expect(create(root, "short")).rejects.toMatchObject({ code: "invalid-passphrase" });
      await expect(create(root, "🔒".repeat(257))).rejects.toMatchObject({
        code: "invalid-passphrase",
      });
      expect(await readdir(root)).toEqual([]);
      await writeFile(join(root, "kafka-profiles.json"), "preserve orphan", { mode: 0o600 });
      await expect(create(root)).rejects.toMatchObject({ code: "already-exists" });
      expect(await readFile(join(root, "kafka-profiles.json"), "utf8")).toBe("preserve orphan");
    });

    it("permits only a private existing browser pairing code and preserves its bytes", async () => {
      const root = await directory();
      const pairing = join(root, "setup-code");
      await writeFile(pairing, "independent browser pairing fixture", { mode: 0o600 });
      const vault = await create(root);
      await vault.lock();
      await unlock(root);
      expect(await readFile(pairing, "utf8")).toBe("independent browser pairing fixture");
      const unsafe = await directory();
      await symlink(pairing, join(unsafe, "setup-code"));
      await expect(create(unsafe)).rejects.toMatchObject({ code: "unavailable" });
    });

    it("excludes concurrent processes and automatically recovers the lease after SIGKILL", async () => {
      const root = await directory();
      const original = await create(root);
      await original.lock();
      const module = resolve("src/platform/node/vault/passphrase-vault.ts");
      const script = `import {openPassphraseVault} from ${JSON.stringify(module)}; const vault=await openPassphraseVault({dataRoot:process.argv[1],passphrase:${JSON.stringify(phrase)},mode:'unlock'}); console.log('locked'); setInterval(()=>{},1000);`;
      const child = spawn(
        process.execPath,
        ["--import", "tsx", "--input-type=module", "-e", script, root],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      children.push(child);
      await new Promise<void>((complete, reject) => {
        let output = "";
        const timeout = setTimeout(
          () => reject(new Error("Vault child did not acquire the kernel lock.")),
          10000,
        );
        child.stdout?.on("data", (chunk: Buffer) => {
          output += chunk.toString("utf8");
          if (output.includes("locked")) {
            clearTimeout(timeout);
            complete();
          }
        });
        child.once("error", reject);
        child.once("exit", (code) => {
          if (!output.includes("locked")) {
            clearTimeout(timeout);
            reject(new Error(`Vault child exited ${code}`));
          }
        });
      });
      await expect(unlock(root)).rejects.toMatchObject({ code: "in-use" });
      await new Promise<void>((complete) => {
        child.once("exit", () => complete());
        child.kill("SIGKILL");
      });
      await unlock(root);
      expect(await inspectPassphraseVault(root)).toBe("present");
    });
  },
);
