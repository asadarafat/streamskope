import { readFile, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { dirname, join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

import {
  FixtureRuntimeError,
  type FixtureConnection,
  type FixtureRuntime,
  type OwnedFixtureRecord,
  type OwnedFixtureRequest,
} from "./lifecycle";
import { DEFAULT_OWNED_FIXTURE_NAME, DEFAULT_SCHEMA_REGISTRY_PORT } from "./lifecycle";
import { runCommand, DIAGNOSTIC_CHARACTER_LIMIT } from "./commands";
import { verifyFixtureReady, safeReadinessMessage } from "./readiness";

const READINESS_INTERVAL_MS = 500;
const READINESS_TIMEOUT_MS = 90_000;

export interface FixtureSourceConfig {
  readonly oauthClientId: string;
  readonly oauthClientSecret: string;
  readonly oauthImage: string;
  readonly oauthScope: string;
  readonly schemaDefinition: string;
  readonly schemaRegistryAudience: string;
  readonly schemaRegistryImage: string;
  readonly schemaRegistryRole: string;
  readonly schemaSubject: string;
  readonly seedPayload: string;
  readonly topic: string;
}

function parseFixtureSourceConfig(value: unknown): FixtureSourceConfig {
  if (value === null || typeof value !== "object") {
    throw new Error("Fixture configuration must be an object.");
  }

  const candidate = value as Partial<Record<keyof FixtureSourceConfig, unknown>>;
  const keys = [
    "oauthClientId",
    "oauthClientSecret",
    "oauthImage",
    "oauthScope",
    "schemaDefinition",
    "schemaRegistryAudience",
    "schemaRegistryImage",
    "schemaRegistryRole",
    "schemaSubject",
    "seedPayload",
    "topic",
  ] as const;

  for (const key of keys) {
    if (typeof candidate[key] !== "string" || candidate[key].length === 0) {
      throw new Error(`Fixture configuration field ${key} must be a non-empty string.`);
    }
  }

  return {
    oauthClientId: candidate.oauthClientId as string,
    oauthClientSecret: candidate.oauthClientSecret as string,
    oauthImage: candidate.oauthImage as string,
    oauthScope: candidate.oauthScope as string,
    schemaDefinition: candidate.schemaDefinition as string,
    schemaRegistryAudience: candidate.schemaRegistryAudience as string,
    schemaRegistryImage: candidate.schemaRegistryImage as string,
    schemaRegistryRole: candidate.schemaRegistryRole as string,
    schemaSubject: candidate.schemaSubject as string,
    seedPayload: candidate.seedPayload as string,
    topic: candidate.topic as string,
  };
}

export async function loadFixtureSourceConfig(
  repositoryRoot: string,
): Promise<FixtureSourceConfig> {
  const raw = await readFile(join(repositoryRoot, "aio-kafka", "fixture.config.json"), "utf8");
  return parseFixtureSourceConfig(JSON.parse(raw) as unknown);
}

export class NodeFixtureRuntime implements FixtureRuntime {
  private readonly fixtureRoot: string;

  constructor(private readonly repositoryRoot: string) {
    this.fixtureRoot = join(repositoryRoot, "aio-kafka");
  }

  async resumeOwned(request: OwnedFixtureRecord): Promise<void> {
    if (resolve(request.topologyPath) !== join(this.fixtureRoot, "topology.clab.yml")) {
      throw new Error("Refusing to resume a fixture from another repository.");
    }
    const names = ["oauth", "broker", "schema-registry"].map(
      (role) => `clab-${request.name}-${role}`,
    );
    const inspection = await runCommand({
      command: "docker",
      args: [
        "inspect",
        "--format",
        '{{.Id}}|{{.Name}}|{{.State.Status}}|{{index .Config.Labels "containerlab"}}|{{index .Config.Labels "clab-topo-file"}}',
        ...names,
      ],
      cwd: this.repositoryRoot,
    });
    const stopped: string[] = [];
    const lines = inspection.standardOutput.trim().split("\n");
    if (lines.length !== names.length) throw new Error("Owned fixture inspection was incomplete.");
    for (const [index, line] of lines.entries()) {
      const [id, name, status, lab, topology] = line.split("|");
      if (
        id === undefined ||
        !/^[a-f0-9]{64}$/u.test(id) ||
        name !== `/${names[index]}` ||
        lab !== request.name ||
        topology !== request.topologyPath
      ) {
        throw new Error(
          "Fixture container ownership could not be verified; no containers were started.",
        );
      }
      if (status === "exited" || status === "created") stopped.push(id);
      else if (status !== "running")
        throw new Error(
          `Owned fixture container ${name} is ${status}; manual recovery is required.`,
        );
    }
    if (stopped.length > 0) {
      await runCommand({
        command: "docker",
        args: ["start", ...stopped],
        cwd: this.repositoryRoot,
      });
    }
  }

  async findUnavailablePorts(ports: readonly number[]): Promise<readonly number[]> {
    const results = await Promise.all(
      ports.map(async (port) => ({ port, unavailable: await this.isPortUnavailable(port) })),
    );
    return results.filter(({ unavailable }) => unavailable).map(({ port }) => port);
  }

  async buildOAuthImage(request: OwnedFixtureRequest): Promise<void> {
    await runCommand({
      args: [
        "build",
        "--tag",
        request.oauthImage,
        join(this.fixtureRoot, "images", "oauth-service"),
      ],
      command: "docker",
      cwd: this.repositoryRoot,
    });
  }

  async generateCertificates(request: OwnedFixtureRequest): Promise<void> {
    const certificateDirectory = this.ownedCertificateDirectory(request);
    await rm(certificateDirectory, { force: true, recursive: true });
    await runCommand({
      args: [join(this.fixtureRoot, "make-certs.sh")],
      command: "bash",
      cwd: this.repositoryRoot,
      environment: {
        CERT_DIR: certificateDirectory,
        STREAMSKOPE_FIXTURE_NAME: request.name,
      },
    });
  }

  async deploy(request: OwnedFixtureRequest): Promise<void> {
    await runCommand({
      args: ["deploy", "--topo", request.topologyPath, "--name", request.name, "--reconfigure"],
      command: "containerlab",
      cwd: this.fixtureRoot,
      environment: await this.topologyEnvironment(request),
    });
  }

  async waitUntilReady(connection: FixtureConnection): Promise<void> {
    const config = await this.loadSourceConfig();
    const deadline = Date.now() + READINESS_TIMEOUT_MS;
    let lastFailure = "readiness was not attempted";

    while (Date.now() < deadline) {
      try {
        await verifyFixtureReady(connection, config);
        return;
      } catch (error: unknown) {
        lastFailure = safeReadinessMessage(error, config.oauthClientSecret);
        if (connection.ownership === "owned" && connection.name !== undefined) {
          const terminalFailure = await this.terminalContainerFailure(
            connection.name,
            config.oauthClientSecret,
          );
          if (terminalFailure !== undefined) {
            throw new FixtureRuntimeError(
              "A Kafka fixture container exited during startup.",
              terminalFailure,
            );
          }
        }
      }

      await delay(READINESS_INTERVAL_MS);
    }

    throw new FixtureRuntimeError(
      "Kafka fixture readiness timed out.",
      `Kafka fixture was not ready within ${READINESS_TIMEOUT_MS} ms: ${lastFailure}`,
    );
  }

  async destroy(request: OwnedFixtureRecord): Promise<void> {
    await runCommand({
      args: ["destroy", "--topo", request.topologyPath, "--name", request.name, "--cleanup"],
      command: "containerlab",
      cwd: this.fixtureRoot,
      environment: await this.topologyEnvironment(request),
    });
    await rm(this.ownedCertificateDirectory(request), { force: true, recursive: true });
  }

  private async isPortUnavailable(port: number): Promise<boolean> {
    return new Promise<boolean>((resolvePromise, rejectPromise) => {
      const server = createServer();
      server.unref();
      server.once("error", (error: NodeJS.ErrnoException) => {
        if (error.code === "EADDRINUSE" || error.code === "EACCES") {
          resolvePromise(true);
          return;
        }
        rejectPromise(error);
      });
      server.listen(port, "127.0.0.1", () => {
        server.close((error) => {
          if (error !== undefined) {
            rejectPromise(error);
            return;
          }
          resolvePromise(false);
        });
      });
    });
  }

  private ownedCertificateDirectory(request: Pick<OwnedFixtureRecord, "caPath" | "name">): string {
    const certificateDirectory = resolve(dirname(request.caPath));
    const expectedDirectory = resolve(this.fixtureRoot, "ownership", request.name, "certs");
    if (certificateDirectory !== expectedDirectory) {
      throw new Error(`Refusing generated-certificate operation outside ${expectedDirectory}.`);
    }
    return certificateDirectory;
  }

  private async loadSourceConfig(): Promise<FixtureSourceConfig> {
    return loadFixtureSourceConfig(this.repositoryRoot);
  }

  private async topologyEnvironment(
    request: OwnedFixtureRecord | OwnedFixtureRequest,
  ): Promise<Readonly<Record<string, string>>> {
    const config = await this.loadSourceConfig();
    return {
      STREAMSKOPE_CERT_DIR: this.ownedCertificateDirectory(request),
      STREAMSKOPE_FIXTURE_NAME: request.name,
      STREAMSKOPE_KAFKA_PORT: String(request.kafkaPort),
      STREAMSKOPE_OAUTH_CLIENT_ID: config.oauthClientId,
      STREAMSKOPE_OAUTH_CLIENT_SECRET: config.oauthClientSecret,
      STREAMSKOPE_OAUTH_IMAGE: request.oauthImage,
      STREAMSKOPE_OAUTH_ISSUER: `http://clab-${request.name}-oauth:5000`,
      STREAMSKOPE_OAUTH_PORT: String(request.oauthPort),
      STREAMSKOPE_OAUTH_SCOPE: config.oauthScope,
      STREAMSKOPE_SCHEMA_REGISTRY_AUDIENCE: config.schemaRegistryAudience,
      STREAMSKOPE_SCHEMA_REGISTRY_IMAGE: request.schemaRegistryImage ?? config.schemaRegistryImage,
      STREAMSKOPE_SCHEMA_REGISTRY_PORT: String(
        request.schemaRegistryPort ?? DEFAULT_SCHEMA_REGISTRY_PORT,
      ),
      STREAMSKOPE_SCHEMA_REGISTRY_ROLE: config.schemaRegistryRole,
      STREAMSKOPE_TOPIC: config.topic,
    };
  }

  private async terminalContainerFailure(
    name: string,
    secret: string,
  ): Promise<string | undefined> {
    const containerNames = [
      `clab-${name}-broker`,
      `clab-${name}-oauth`,
      `clab-${name}-schema-registry`,
    ];

    try {
      const inspection = await runCommand({
        args: [
          "inspect",
          "--format",
          "{{.Name}}|{{.State.Status}}|{{.State.ExitCode}}|{{.State.Error}}",
          ...containerNames,
        ],
        command: "docker",
        cwd: this.repositoryRoot,
      });
      const failedLine = inspection.standardOutput
        .trim()
        .split("\n")
        .find((line) => /\|(?:dead|exited|restarting)\|/u.test(line));
      if (failedLine === undefined) {
        return undefined;
      }

      const containerName = failedLine.split("|")[0]?.replace(/^\//u, "");
      if (containerName === undefined || !containerNames.includes(containerName)) {
        return "An owned fixture container exited, but its bounded diagnostics were unavailable.";
      }
      const logs = await runCommand({
        args: ["logs", "--tail", "80", containerName],
        command: "docker",
        cwd: this.repositoryRoot,
      });
      const logTail = (logs.standardError || logs.standardOutput).trim();
      return `${failedLine}\n${logTail}`
        .replaceAll(secret, "[REDACTED]")
        .slice(-DIAGNOSTIC_CHARACTER_LIMIT);
    } catch {
      return undefined;
    }
  }
}

export async function defaultOwnedFixtureRequest(
  repositoryRoot: string,
  name = DEFAULT_OWNED_FIXTURE_NAME,
): Promise<OwnedFixtureRequest> {
  const config = await loadFixtureSourceConfig(repositoryRoot);
  const root = join(repositoryRoot, "aio-kafka");
  return {
    caPath: join(root, "ownership", name, "certs", "ca.pem"),
    kafkaPort: 19_093,
    name,
    oauthImage: config.oauthImage,
    oauthPort: 15_000,
    schemaRegistryImage: config.schemaRegistryImage,
    schemaRegistryPort: DEFAULT_SCHEMA_REGISTRY_PORT,
    topologyPath: join(root, "topology.clab.yml"),
  };
}
