import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

import { afterEach, describe, expect, it, vi } from "vitest";
import { utils } from "ssh2";

import type { RemoteSshTargetInput } from "../../src/features/kafka/contracts";
import {
  InMemoryKafkaConnectionTemplateStore,
  KafkaConnectionTemplateService,
  KafkaTrustAcquisitionService,
} from "../../src/features/kafka/application";
import { createHostTrustMaterialDecoder } from "../../src/platform/electron/main";
import { trustRecipeInput } from "../support/trust-recipe";
import { Ssh2KafkaRemoteTrustAdapter } from "../../src/platform/electron/main/ssh2-kafka-remote-trust-adapter";
import { startControlledSshServer, type ControlledSshServer } from "../support/ssh-fixture";

const servers: ControlledSshServer[] = [];

afterEach(async () => {
  vi.unstubAllEnvs();
  await Promise.all(servers.splice(0).map(async (server) => server.close()));
});

async function server(
  options: Parameters<typeof startControlledSshServer>[0] = {},
): Promise<ControlledSshServer> {
  const started = await startControlledSshServer(options);
  servers.push(started);
  return started;
}

function target(
  controlled: ControlledSshServer,
  overrides: Partial<Extract<RemoteSshTargetInput, { readonly password: string }>> = {},
): RemoteSshTargetInput {
  return {
    host: controlled.host,
    hostKeyFingerprint: controlled.fingerprint,
    password: "ssh-password",
    port: controlled.port,
    username: "operator",
    ...overrides,
  };
}

async function binaryFixture(kind: "jks" | "pkcs12"): Promise<Buffer> {
  const bytes = await readFile(
    join(process.cwd(), "node_modules/jks-js/examples/assets/truststore.jks"),
  );
  if (kind === "jks") return bytes;
  const directory = await mkdtemp(join(tmpdir(), "streamskope-pkcs12-fixture-"));
  try {
    const decoded = await createHostTrustMaterialDecoder().decode({
      kind: "jks",
      material: bytes.toString("base64"),
      password: "password",
    });
    const certificatePath = join(directory, "ca.pem");
    const outputPath = join(directory, "trust.p12");
    await writeFile(certificatePath, decoded.caPem);
    await promisify(execFile)(
      "openssl",
      [
        "pkcs12",
        "-export",
        "-nokeys",
        "-in",
        certificatePath,
        "-out",
        outputPath,
        "-passout",
        "pass:password",
      ],
      { timeout: 5_000 },
    );
    return await readFile(outputPath);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

describe("SSH2 remote-trust controlled integration", () => {
  it("diagnoses redirected stdout and decodes corrected raw JKS output over SSH", async () => {
    const directory = await mkdtemp(join(tmpdir(), "streamskope-stdout-fixture-"));
    try {
      const bytes = await binaryFixture("jks");
      const sourcePath = join(directory, "source.jks");
      const destinationPath = join(directory, "redirected.jks");
      await writeFile(sourcePath, bytes);
      const redirected = await promisify(execFile)(
        "/bin/sh",
        ["-c", 'cat "$1" > "$2"', "retrieval-test", sourcePath, destinationPath],
        { encoding: "buffer", timeout: 5_000 },
      );
      expect(redirected.stdout.length).toBe(0);
      expect(await readFile(destinationPath)).toEqual(bytes);
      const corrected = await promisify(execFile)(
        "/bin/sh",
        ["-c", 'cat "$1"', "retrieval-test", sourcePath],
        { encoding: "buffer", timeout: 5_000 },
      );
      expect(corrected.stdout).toEqual(bytes);
      for (const output of [redirected.stdout, corrected.stdout]) {
        const controlled = await server({ commandOutput: output });
        const templates = new KafkaConnectionTemplateService(
          new InMemoryKafkaConnectionTemplateStore({ durability: "session", state: "ready" }),
        );
        const catalog = await templates.recipes.create({
          ...trustRecipeInput(),
          name: "Raw JKS output",
          kind: "jks",
          parameters: [],
          ssh: { source: "stdout", value: "emit-truststore", password: { source: "ask" } },
        });
        const recipe = catalog.recipes.find((entry) => entry.name === "Raw JKS output")!;
        const service = new KafkaTrustAcquisitionService(
          templates,
          new Ssh2KafkaRemoteTrustAdapter(),
          createHostTrustMaterialDecoder(),
        );
        try {
          const pending = service.fetchMaterial({
            kind: "jks",
            label: "fixture.jks",
            target: target(controlled),
            truststorePassword: "password",
            recipe: {
              mode: "replace",
              recipeId: recipe.id,
              recipeRevision: recipe.revision,
              overrides: {},
            },
          });
          if (output.length === 0) {
            await expect(pending).rejects.toMatchObject({
              code: "TRUST_MATERIAL",
              message:
                "The material command completed but returned no certificate or truststore bytes on stdout.",
            });
          } else {
            const result = await pending;
            expect(result.material?.byteCount).toBe(bytes.length);
            expect(service.resolve(result.id, "jks").material).toBe(bytes.toString("base64"));
          }
          expect(controlled.removedPaths).toEqual([]);
        } finally {
          service.clear();
        }
      }
      expect(await readFile(sourcePath)).toEqual(bytes);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it.each([
    { source: "file", kind: "jks" },
    { source: "stdout", kind: "jks" },
    { source: "file", kind: "pkcs12" },
    { source: "stdout", kind: "pkcs12" },
  ] as const)(
    "acquires and decodes actual $kind through the selected $source recipe",
    async ({ source, kind }) => {
      const bytes = await binaryFixture(kind);
      const controlled = await server({ commandOutput: bytes });
      controlled.putFile("/certificates/truststore.jks", bytes);
      const templates = new KafkaConnectionTemplateService(
        new InMemoryKafkaConnectionTemplateStore({ durability: "session", state: "ready" }),
      );
      const name = `Binary fixture ${source}`;
      const catalog = await templates.recipes.create({
        ...trustRecipeInput(),
        name,
        kind,
        parameters: [],
        ssh: {
          source,
          value: source === "file" ? "/certificates/truststore.jks" : "emit-truststore",
          password: { source: "ask" },
        },
      });
      const recipe = catalog.recipes.find((candidate) => candidate.name === name);
      if (recipe === undefined) throw new Error("Missing fixture recipe");
      const service = new KafkaTrustAcquisitionService(
        templates,
        new Ssh2KafkaRemoteTrustAdapter(),
        createHostTrustMaterialDecoder(),
      );
      const input = {
        kind,
        label: "fixture.jks",
        target: target(controlled),
        recipe: {
          mode: "replace" as const,
          recipeId: recipe.id,
          recipeRevision: recipe.revision,
          overrides: {},
        },
        truststorePassword: "password",
      };
      try {
        const result = await service.fetchMaterial(input);
        expect(result.material).toMatchObject({
          byteCount: bytes.length,
          kind,
          expiredCertificates: true,
          evidence: {
            count: 1,
            certificates: [
              { subject: "C=jks-js\nST=jks-js\nL=jks-js\nO=lenchv\nOU=jks-js\nCN=jks-js" },
            ],
          },
        });
        expect(service.resolve(result.id, kind).material).toBe(bytes.toString("base64"));
        expect(controlled.commands).toEqual(source === "file" ? [] : ["emit-truststore"]);
        expect(controlled.removedPaths).toEqual([]);
        await expect(
          service.fetchMaterial({ ...input, truststorePassword: "incorrect-sentinel" }),
        ).rejects.toMatchObject({ code: "TRUSTSTORE_PASSWORD" });
        expect(service.resolve(result.id, kind).material).toBe(bytes.toString("base64"));
        expect(controlled.removedPaths).toEqual([]);
        expect(JSON.stringify(result)).not.toContain("incorrect-sentinel");
        expect(JSON.stringify(result)).not.toContain(bytes.toString("base64"));
      } finally {
        service.clear();
      }
    },
  );
  it("cancels a stalled SFTP handle close without publishing material or deleting the source", async () => {
    const controller = new AbortController();
    const controlled = await server({
      hangSftpClose: true,
      onSftpClose: () => controller.abort(new DOMException("Cancelled.", "AbortError")),
    });
    controlled.putFile("/certificates/ca.pem", new Uint8Array([1, 2, 3]));
    await expect(
      new Ssh2KafkaRemoteTrustAdapter().fetchMaterial(
        {
          source: "file",
          remotePath: "/certificates/ca.pem",
          maximumBytes: 100,
          target: target(controlled),
        },
        controller.signal,
      ),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(controlled.events).toContain("CLOSE");
    expect(controlled.removedPaths).toEqual([]);
  }, 2_000);
  it("fails an unavailable explicitly selected agent before authentication", async () => {
    vi.stubEnv("SSH_AUTH_SOCK", "");
    const controlled = await server();
    await expect(
      new Ssh2KafkaRemoteTrustAdapter().fetchPassword({
        command: "never",
        target: {
          host: controlled.host,
          port: controlled.port,
          username: "operator",
          hostKeyFingerprint: controlled.fingerprint,
          authentication: { mode: "agent" },
        },
      }),
    ).rejects.toMatchObject({ code: "SSH_AUTHENTICATION" });
    expect(controlled.events).toEqual([]);
  });

  it.skipIf(process.platform === "win32")(
    "uses the explicitly selected local agent without forwarding",
    async () => {
      const directory = await mkdtemp(join(tmpdir(), "streamskope-agent-"));
      const socket = join(directory, "agent.sock");
      const keyPath = join(directory, "fixture-key");
      let agentPid: number | undefined;
      const execute = promisify(execFile);
      try {
        const key = utils.generateKeyPairSync("ed25519");
        await writeFile(keyPath, key.private, { mode: 0o600 });
        const started = await execute("ssh-agent", ["-a", socket, "-s"], { timeout: 5_000 });
        const pid = started.stdout.match(/SSH_AGENT_PID=(\d+);/u)?.[1];
        if (pid === undefined) throw new Error("Fixture agent PID unavailable");
        agentPid = Number(pid);
        await execute("ssh-add", [keyPath], {
          env: { ...process.env, SSH_AUTH_SOCK: socket },
          timeout: 5_000,
        });
        vi.stubEnv("SSH_AUTH_SOCK", socket);
        const controlled = await server({ authorizedPublicKey: key.public });
        await expect(
          new Ssh2KafkaRemoteTrustAdapter().fetchPassword({
            command: "read-password",
            target: {
              host: controlled.host,
              port: controlled.port,
              username: "operator",
              hostKeyFingerprint: controlled.fingerprint,
              authentication: { mode: "agent" },
            },
          }),
        ).resolves.toBe("remote-password\n");
        expect(
          new Set(controlled.events.filter((event) => event.startsWith("AUTHENTICATION:"))),
        ).toEqual(new Set(["AUTHENTICATION:publickey"]));
        expect(controlled.events).not.toContain("AGENT_FORWARD");
      } finally {
        if (agentPid !== undefined) process.kill(agentPid, "SIGTERM");
        await rm(directory, { recursive: true, force: true });
      }
    },
  );

  it("uses an uploaded encrypted private key without password fallback", async () => {
    const key = utils.generateKeyPairSync("ed25519", {
      passphrase: "key-passphrase",
      cipher: "aes256-cbc",
      rounds: 16,
    });
    const controlled = await server({ authorizedPublicKey: key.public });
    const sshTarget = {
      host: controlled.host,
      port: controlled.port,
      username: "operator",
      hostKeyFingerprint: controlled.fingerprint,
      authentication: {
        mode: "private-key" as const,
        privateKey: key.private,
        passphrase: "key-passphrase",
      },
    };
    await expect(
      new Ssh2KafkaRemoteTrustAdapter().fetchPassword({
        command: "read-password",
        target: sshTarget,
      }),
    ).resolves.toBe("remote-password\n");
    expect(
      new Set(controlled.events.filter((event) => event.startsWith("AUTHENTICATION:"))),
    ).toEqual(new Set(["AUTHENTICATION:publickey"]));
  });

  it("retrieves an existing regular file without executing or removing anything", async () => {
    const controlled = await server();
    const remotePath = "/certificates/ca.pem";
    controlled.putFile(remotePath, new Uint8Array([0, 255, 128, 13]));
    const adapter = new Ssh2KafkaRemoteTrustAdapter();
    const request = {
      source: "file" as const,
      remotePath,
      maximumBytes: 4,
      target: target(controlled),
    };
    await expect(adapter.fetchMaterial(request)).resolves.toEqual(
      new Uint8Array([0, 255, 128, 13]),
    );
    await expect(adapter.fetchMaterial(request)).resolves.toEqual(
      new Uint8Array([0, 255, 128, 13]),
    );
    expect(controlled.commands).toEqual([]);
    expect(controlled.removedPaths).toEqual([]);
  });

  it("preserves binary stdout without a UTF-8 round trip", async () => {
    const controlled = await server({ commandOutput: new Uint8Array([0, 255, 128, 13, 10]) });
    const adapter = new Ssh2KafkaRemoteTrustAdapter();
    await expect(
      adapter.fetchMaterial({
        source: "command",
        command: "read-binary",
        maximumBytes: 5,
        target: target(controlled),
      }),
    ).resolves.toEqual(new Uint8Array([0, 255, 128, 13, 10]));
    expect(controlled.removedPaths).toEqual([]);
    expect(controlled.events).not.toContain("SFTP");
  });

  it.each([0o120777, 0o040700])(
    "rejects non-regular mode %s before opening the source",
    async (fileMode) => {
      const controlled = await server({ fileMode });
      controlled.putFile("/certificates/source", new Uint8Array([1]));
      await expect(
        new Ssh2KafkaRemoteTrustAdapter().fetchMaterial({
          source: "file",
          remotePath: "/certificates/source",
          maximumBytes: 4,
          target: target(controlled),
        }),
      ).rejects.toMatchObject({ code: "REMOTE_TRANSFER" });
      expect(controlled.events.some((event) => event.startsWith("OPEN:"))).toBe(false);
      expect(controlled.removedPaths).toEqual([]);
    },
  );

  it("discovers the exact host identity without authenticating or executing a command", async () => {
    const controlled = await server();
    const adapter = new Ssh2KafkaRemoteTrustAdapter();

    const discovered = await adapter.discoverHostKey({
      target: {
        host: controlled.host,
        port: controlled.port,
      },
    });
    expect(discovered).toBe(controlled.fingerprint);
    expect(controlled.commands).toEqual([]);
    expect(controlled.events.filter((event) => event.startsWith("AUTHENTICATION:"))).toEqual([]);
  });

  it("cancels discovery before opening a network connection", async () => {
    const controlled = await server();
    const adapter = new Ssh2KafkaRemoteTrustAdapter();
    const controller = new AbortController();
    controller.abort(new DOMException("Cancelled.", "AbortError"));

    await expect(
      adapter.discoverHostKey(
        {
          target: {
            host: controlled.host,
            port: controlled.port,
          },
        },
        controller.signal,
      ),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(controlled.events).toEqual([]);
  });

  it("authenticates with an exact host pin and returns bounded command output", async () => {
    const controlled = await server({ commandOutput: "fixture-password\r\n" });
    const adapter = new Ssh2KafkaRemoteTrustAdapter();

    await expect(
      adapter.fetchPassword({
        command: "fetch-password --quiet",
        target: target(controlled),
      }),
    ).resolves.toBe("fixture-password\r\n");
    expect(controlled.commands).toEqual(["fetch-password --quiet"]);
  });

  it("observes an immediate successful exit even when the remote command has no output", async () => {
    const controlled = await server({ commandOutput: "" });
    await expect(
      new Ssh2KafkaRemoteTrustAdapter().fetchPassword({
        command: "empty-command",
        target: target(controlled),
      }),
    ).resolves.toBe("");
    expect(controlled.commands).toEqual(["empty-command"]);
  });

  it("rejects host-key mismatch and password failure before executing a command", async () => {
    const controlled = await server();
    const adapter = new Ssh2KafkaRemoteTrustAdapter();
    const discovered = await adapter.discoverHostKey({
      target: {
        host: controlled.host,
        port: controlled.port,
      },
    });
    expect(discovered).toBe(controlled.fingerprint);
    const mismatchedFingerprint =
      discovered === `SHA256:${"A".repeat(43)}`
        ? `SHA256:${"B".repeat(43)}`
        : `SHA256:${"A".repeat(43)}`;

    await expect(
      adapter.fetchPassword({
        command: "must-not-run",
        target: target(controlled, {
          hostKeyFingerprint: mismatchedFingerprint,
        }),
      }),
    ).rejects.toMatchObject({
      code: "SSH_IDENTITY",
      stage: "ssh",
    });
    await expect(
      adapter.fetchPassword({
        command: "must-not-run",
        target: target(controlled, { password: "wrong-password" }),
      }),
    ).rejects.toMatchObject({
      code: "SSH_AUTHENTICATION",
      stage: "ssh",
    });
    expect(controlled.commands).toEqual([]);
  });

  it("reads one regular bounded file over SFTP and removes it remotely", async () => {
    const controlled = await server();
    const remotePath = "/tmp/streamskope-integration.trust";
    controlled.putFile(remotePath, new Uint8Array([1, 2, 3, 4]));
    const adapter = new Ssh2KafkaRemoteTrustAdapter();

    await expect(
      adapter.fetchMaterial({
        command: `copy source '${remotePath}'`,
        maximumBytes: 4,
        remotePath,
        target: target(controlled),
      }),
    ).resolves.toEqual(new Uint8Array([1, 2, 3, 4]));
    expect(controlled.commands).toEqual([`copy source '${remotePath}'`]);
    expect(controlled.removedPaths).toEqual([remotePath]);
  });

  it("rejects a hostile reported size before reading and still removes the file", async () => {
    const controlled = await server();
    const remotePath = "/tmp/streamskope-oversized.trust";
    controlled.putFile(remotePath, new Uint8Array([1, 2, 3, 4, 5]));
    const adapter = new Ssh2KafkaRemoteTrustAdapter();

    await expect(
      adapter.fetchMaterial({
        command: "copy oversized",
        maximumBytes: 4,
        remotePath,
        target: target(controlled),
      }),
    ).rejects.toMatchObject({
      code: "REMOTE_TRANSFER",
      stage: "remote-transfer",
    });
    expect(controlled.removedPaths).toEqual([remotePath]);
  });

  it("withholds output from a failed command and distinguishes cancellation", async () => {
    const failed = await server({
      commandExitCode: 7,
      commandOutput: "sensitive remote failure",
    });
    const adapter = new Ssh2KafkaRemoteTrustAdapter();
    let failure: unknown;
    try {
      await adapter.fetchPassword({
        command: "failing-command",
        target: target(failed),
      });
    } catch (error) {
      failure = error;
    }
    expect(failure).toMatchObject({
      code: "REMOTE_COMMAND",
      stage: "remote-command",
    });
    expect(failure instanceof Error ? failure.message : "").not.toContain(
      "sensitive remote failure",
    );

    const hanging = await server({ hangCommands: true });
    const controller = new AbortController();
    const operation = adapter.fetchPassword(
      {
        command: "wait",
        target: target(hanging),
      },
      controller.signal,
    );
    setTimeout(() => {
      controller.abort(new DOMException("Cancelled.", "AbortError"));
    }, 10);
    await expect(operation).rejects.toMatchObject({ name: "AbortError" });
  });
});
