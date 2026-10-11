import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { chown, chmod, lstat, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { ProfileSummary, ProfileUpdateInput } from "../../src/features/kafka/contracts";
import {
  retainedClientIdentity,
  retainedServiceEndpoints,
} from "../../src/features/kafka/contracts/profile-retain-input";

import type { BrowserInstallerTarget } from "./browser-installer-evidence";
import { run } from "./browser-qualification-process";
import type { BrowserVaultFixture } from "./browser-vault-fixture";

/** Actual protected manual profiles only; managed capture sources retain their separate preflight refusal. */
export async function seedNativeMetricsRecovery({
  fixture,
  data,
  uid,
  gid,
}: {
  readonly fixture: BrowserVaultFixture;
  readonly data: string;
  readonly uid: number;
  readonly gid: number;
}): Promise<{
  migrate(
    container: string,
    predecessor: BrowserInstallerTarget,
    environment: NodeJS.ProcessEnv,
  ): Promise<void>;
  verifyRestore(recovery: string): Promise<void>;
  verifyRollback(): Promise<void>;
}> {
  const path = join(data, "kafka-profiles.json"),
    directory = await mkdtemp(join(tmpdir(), "streamskope-native-metrics-"));
  const secret = `native-metrics-${randomUUID()}`;
  const replace = (value: string): { mode: "replace"; value: string } => ({
    mode: "replace",
    value,
  });
  const clear = { mode: "clear" as const },
    retain = { mode: "retain" as const };
  const profiles = async (): Promise<readonly ProfileSummary[]> => fixture.kafkaProfiles();
  try {
    await run("openssl", [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      join(directory, "key.pem"),
      "-out",
      join(directory, "ca.pem"),
      "-days",
      "1",
      "-subj",
      "/CN=native-qualification.test",
      "-addext",
      "basicConstraints=critical,CA:TRUE",
    ]);
    const certificate = await readFile(join(directory, "ca.pem"), "utf8"),
      privateKey = await readFile(join(directory, "key.pem"), "utf8");
    await fixture.kafkaCommand("profiles.create", {
      profile: {
        name: "Native protected OAuth",
        brokers: ["broker.example.test:9093"],
        transport: "tls",
        trust: {
          kind: "pem",
          label: "native.pem",
          material: replace(certificate),
          password: clear,
        },
        oauth: {
          clientId: "native-broker",
          clientSecret: replace(secret),
          tokenEndpoint: "https://broker.example.test/token",
          scope: "kafka",
        },
        clientIdentity: {
          certificatePem: replace(certificate),
          privateKeyPem: replace(privateKey),
          passphrase: clear,
        },
        services: {
          schemaRegistry: {
            baseUrl: "https://registry.example.test",
            authentication: "basic",
            basic: { username: "native-registry", password: replace(secret) },
            trust: {
              mode: "custom",
              kind: "pem",
              label: "registry.pem",
              material: replace(certificate),
              password: clear,
            },
          },
          connect: {
            baseUrl: "https://connect.example.test",
            authentication: "oauth-client",
            oauth: {
              clientId: "native-connect",
              clientSecret: replace(secret),
              scope: "",
              tokenEndpoint: "https://connect.example.test/token",
            },
            trust: { mode: "system" },
          },
          redpandaAdmin: {
            baseUrl: "https://admin.example.test",
            authentication: "bearer",
            bearer: replace(secret),
            trust: { mode: "broker" },
          },
        },
      },
    });
    await fixture.kafkaCommand("profiles.create", {
      profile: {
        name: "Native protected SCRAM",
        brokers: ["broker.example.test:9092"],
        transport: "plaintext",
        sasl: { mechanism: "SCRAM-SHA-512", username: "native-scram", password: replace(secret) },
      },
    });
    const expected = await profiles();
    const stable = (summary: ProfileSummary): unknown => {
      const services = { ...summary.services };
      delete services.metrics;
      return {
        ...summary,
        revision: undefined,
        updatedAt: undefined,
        services: summary.services === undefined ? undefined : services,
      };
    };
    assert.equal(expected.length, 2);
    const original = await readFile(path),
      fingerprint = await fixture.fingerprintKafkaProtected();
    assert.equal((JSON.parse(original.toString()) as { version: number }).version, 4);
    assert.ok(!original.toString().includes(secret) && !original.toString().includes(privateKey));
    let changed: Buffer | undefined;
    const draft = (summary: ProfileSummary): ProfileUpdateInput => {
      assert.equal(summary.transport, "tls");
      assert.ok(summary.revision !== undefined);
      assert.ok(summary.trust && summary.oauth && summary.clientIdentity && summary.services);
      return {
        name: summary.name,
        brokers: summary.brokers,
        transport: "tls",
        expectedRevision: summary.revision,
        trust: {
          kind: summary.trust.kind,
          label: summary.trust.label,
          material: retain,
          password: clear,
        },
        oauth: {
          clientId: summary.oauth.clientId,
          clientSecret: retain,
          tokenEndpoint: summary.oauth.tokenEndpoint,
          scope: summary.oauth.scope,
        },
        clientIdentity: retainedClientIdentity(summary.clientIdentity),
        services: retainedServiceEndpoints(summary.services),
      };
    };
    return {
      migrate: async (container, predecessor, environment): Promise<void> => {
        const before = (await profiles()).find((p) => p.name === "Native protected OAuth")!;
        const update = draft(before);
        const metrics = {
          baseUrl: "https://metrics.example.test",
          authentication: "bearer" as const,
          bearer: replace(secret),
          trust: { mode: "system" as const },
          clientIdentity: {
            certificatePem: replace(certificate),
            privateKeyPem: replace(privateKey),
            passphrase: clear,
          },
        };
        await fixture.kafkaCommand("profiles.update", {
          profileId: before.id,
          profile: { ...update, services: { ...update.services, metrics } },
        });
        assert.equal(
          (JSON.parse((await readFile(path)).toString()) as { version: number }).version,
          5,
        );
        assert.deepEqual(await readFile(`${path}.pre-metrics-v4`), original);
        assert.equal((await lstat(`${path}.pre-metrics-v4`)).mode & 0o777, 0o600);
        assert.equal(await fixture.fingerprintKafkaProtected(true), fingerprint);
        await fixture.lock();
        await run("docker", ["restart", "--time", "120", container], environment);
        await fixture.unlockAfterReplacement();
        const restartedProfiles = await profiles();
        assert.deepEqual(restartedProfiles.map(stable), expected.map(stable));
        const restarted = restartedProfiles.find((p) => p.id === before.id)!;
        assert.equal(restarted.services?.metrics?.bearerPresent, true);
        assert.equal(restarted.services.metrics.clientIdentity?.privateKeyPresent, true);
        assert.equal(await fixture.fingerprintKafkaProtected(true), fingerprint);
        const removing = draft(restarted);
        const services = { ...removing.services };
        delete services.metrics;
        await fixture.kafkaCommand("profiles.update", {
          profileId: before.id,
          profile: { ...removing, services },
        });
        assert.equal(
          (JSON.parse((await readFile(path)).toString()) as { version: number }).version,
          5,
        );
        const removed = (await profiles()).find((p) => p.id === before.id)!;
        const retained = draft(removed);
        await fixture.kafkaCommand("profiles.update", {
          profileId: before.id,
          profile: { ...retained, services: { ...retained.services, metrics } },
        });
        assert.equal(await fixture.fingerprintKafkaProtected(true), fingerprint);
        changed = await readFile(path);
        assert.ok(!changed.toString().includes(secret) && !changed.toString().includes(privateKey));
        const isolated = join(data, "..", "metrics-inspection-fixture");
        await mkdir(isolated, { mode: 0o700 });
        await chown(isolated, uid, gid);
        const isolatedPath = join(isolated, "kafka-profiles.json");
        await writeFile(isolatedPath, changed, { mode: 0o600 });
        await chown(isolatedPath, uid, gid);
        await chmod(isolatedPath, 0o600);
        const isolatedMetadata = await lstat(isolatedPath);
        const refused = await run(
          "docker",
          [
            "run",
            "--rm",
            "--network",
            "none",
            "--read-only",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--user",
            `${uid}:${gid}`,
            "--mount",
            `type=bind,source=${isolated},target=/data,readonly`,
            "--entrypoint",
            "node",
            predecessor.imageId,
            "dist/web/data-preflight.cjs",
            "/data",
          ],
          environment,
          true,
        );
        assert.equal(refused.code, 2);
        const report = JSON.parse(refused.stdout) as {
          documents: { kind: string; state: string; reason: string }[];
        };
        assert.deepEqual(
          report.documents.find((d) => d.kind === "kafka-profiles"),
          {
            kind: "kafka-profiles",
            state: "blocked",
            count: 0,
            formats: [],
            reason: "unsupported-format",
          },
        );
        assert.deepEqual(await readFile(isolatedPath), changed);
        assert.equal((await lstat(isolatedPath)).ino, isolatedMetadata.ino);
        assert.equal((await lstat(isolatedPath)).mtimeMs, isolatedMetadata.mtimeMs);
      },
      verifyRestore: async (recovery): Promise<void> => {
        assert.ok(changed);
        assert.deepEqual(await readFile(join(recovery, "kafka-profiles.json")), changed);
        assert.deepEqual(
          await readFile(join(recovery, "kafka-profiles.json.pre-metrics-v4")),
          original,
        );
        assert.deepEqual(await readFile(path), original);
      },
      verifyRollback: async (): Promise<void> => {
        assert.deepEqual(await profiles(), expected);
        assert.equal(await fixture.fingerprintKafkaProtected(), fingerprint);
        assert.deepEqual(await readFile(path), original);
      },
    };
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
