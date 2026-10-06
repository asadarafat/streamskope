import { execFile } from "node:child_process";
import { chmod, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { promisify } from "node:util";

import type { NatsDockerCommand } from "./lifecycle";
import { prepareNatsMaterial, writeNatsMaterialConfig } from "./materials";
import {
  NatsFixtureError,
  type NatsContainerlabFixtureIntent,
  type NatsContainerlabFixtureRecord,
} from "./ownership";
import type { NatsPersistentRuntime } from "./persistent-runtime";
import {
  dockerApiRequest,
  dockerContainers,
  dockerMetadata,
  dockerObject,
  fixtureLabels,
  startNatsDockerAdapter,
} from "./docker-api-adapter";

const execute = promisify(execFile);
const identifier = (value: unknown): value is string =>
  typeof value === "string" && /^[a-f0-9]{64}$/u.test(value);

/** The persistent lab uses the orchestrator; disposable qualification retains its Docker factory. */
export class ContainerlabNatsRuntime implements NatsPersistentRuntime {
  constructor(
    private readonly repositoryRoot: string,
    private readonly docker: NatsDockerCommand,
  ) {}
  private async daemon(socket: string, expected?: string): Promise<string> {
    const metadata = await dockerMetadata(socket, "/info");
    const daemon = metadata?.ID;
    if (
      typeof daemon !== "string" ||
      !/^[A-Za-z0-9][A-Za-z0-9:._-]{7,127}$/u.test(daemon) ||
      (expected !== undefined && daemon !== expected)
    )
      throw new NatsFixtureError("The original NATS Docker daemon could not be verified.");
    return daemon;
  }
  private async socket(): Promise<string> {
    const endpoint: unknown =
      process.env.DOCKER_CONTEXT === undefined && process.env.DOCKER_HOST !== undefined
        ? process.env.DOCKER_HOST
        : JSON.parse(
            await this.docker([
              "context",
              "inspect",
              ...(process.env.DOCKER_CONTEXT === undefined ? [] : [process.env.DOCKER_CONTEXT]),
              "--format",
              "{{json .Endpoints.docker.Host}}",
            ]),
          );
    if (
      typeof endpoint !== "string" ||
      !endpoint.startsWith("unix:///") ||
      endpoint.includes("\n") ||
      endpoint.includes("\0")
    )
      throw new NatsFixtureError("The NATS fixture requires a local Unix Docker endpoint.");
    return endpoint.slice("unix://".length);
  }
  private async copy(
    socket: string,
    intent: NatsContainerlabFixtureIntent,
    container: string,
  ): Promise<void> {
    const environment: NodeJS.ProcessEnv = { ...process.env, DOCKER_HOST: `unix://${socket}` };
    delete environment.DOCKER_CONTEXT;
    for (const file of ["server.pem", "server-key.pem", "nats.conf"])
      await execute(
        "docker",
        ["cp", join(intent.directory, file), `${container}:/fixture/${file}`],
        { env: environment, timeout: 30_000, maxBuffer: 1024 * 1024 },
      );
  }
  private async orchestrate(
    socket: string,
    intent: NatsContainerlabFixtureIntent,
    saveProgress: (intent: NatsContainerlabFixtureIntent) => Promise<void>,
    operation: "deploy" | "destroy",
    signal?: AbortSignal,
  ): Promise<NatsContainerlabFixtureIntent> {
    const adapter = await startNatsDockerAdapter({
      socketPath: socket,
      intent,
      saveProgress,
      copyMaterial: (container) => this.copy(socket, intent, container),
    });
    const environment: NodeJS.ProcessEnv = { ...process.env, DOCKER_HOST: adapter.host };
    delete environment.DOCKER_CONTEXT;
    try {
      signal?.throwIfAborted();
      await execute(
        "containerlab",
        [
          operation,
          "--topo",
          intent.topologyPath,
          "--runtime",
          "docker",
          "--timeout",
          "30s",
          "--log-level",
          "error",
          ...(operation === "deploy" ? ["--skip-labdir-acl", "--skip-post-deploy"] : ["--cleanup"]),
        ],
        { cwd: intent.directory, env: environment, timeout: 120_000, maxBuffer: 1024 * 1024 },
      );
      await adapter.close();
      return adapter.progress();
    } catch (error) {
      const result = error as { stdout?: unknown; stderr?: unknown };
      const diagnostic = [result.stdout, result.stderr]
        .filter((value): value is string => typeof value === "string")
        .join("\n")
        .slice(-65536);
      await writeFile(join(intent.directory, "orchestration-failure.log"), diagnostic, {
        mode: 0o600,
      }).catch(() => undefined);
      throw new NatsFixtureError(
        "Owned NATS orchestration did not complete; private ownership evidence was retained.",
      );
    } finally {
      await adapter.close();
    }
  }
  async start(
    intent: NatsContainerlabFixtureIntent,
    saveProgress: (intent: NatsContainerlabFixtureIntent) => Promise<void>,
    signal?: AbortSignal,
  ): Promise<NatsContainerlabFixtureRecord> {
    signal?.throwIfAborted();
    if (process.platform !== "linux" || !["arm64", "x64"].includes(process.arch))
      throw new NatsFixtureError("Local AIO NATS requires Linux arm64 or amd64.");
    const socket = await this.socket();
    const daemon = await this.daemon(socket, intent.daemon);
    intent = { ...intent, daemon };
    await saveProgress(intent);
    try {
      await this.docker(["image", "inspect", intent.image]);
    } catch {
      await this.docker(
        [
          "pull",
          "--platform",
          process.arch === "arm64" ? "linux/arm64" : "linux/amd64",
          intent.image,
        ],
        120_000,
      );
    }
    const material = await prepareNatsMaterial({
      directory: intent.directory,
      certificateDays: 365,
      signal,
    });
    await writeNatsMaterialConfig(material, { name: intent.name, host: "0.0.0.0", port: 4222 });
    const template = await readFile(
      join(this.repositoryRoot, "aio-nats", "topology.clab.yml"),
      "utf8",
    );
    const values: Readonly<Record<string, string>> = {
      LAB: intent.lab,
      NETWORK: intent.networkName,
      IMAGE: intent.image,
      PORT: String(intent.port),
      IDENTITY: intent.identity,
    };
    const topology = template.replace(
      /\$\{STREAMSKOPE_NATS_([A-Z]+)\}/gu,
      (_match, key: string) => {
        const value = values[key];
        if (value === undefined) throw new NatsFixtureError("NATS fixture template was invalid.");
        return value;
      },
    );
    await writeFile(intent.topologyPath, topology, { mode: 0o600 });
    await chmod(intent.topologyPath, 0o600);
    const completed = await this.orchestrate(socket, intent, saveProgress, "deploy", signal);
    if (
      !identifier(completed.container) ||
      !identifier(completed.network) ||
      !identifier(completed.volume) ||
      !completed.mutationsSettled
    )
      throw new NatsFixtureError("NATS deployment resource ownership is incomplete.");
    const record: NatsContainerlabFixtureRecord = {
      ...completed,
      daemon,
      container: completed.container,
      network: completed.network,
      volume: completed.volume,
    };
    if ((await this.status(record)) !== "running")
      throw new NatsFixtureError("Owned NATS container did not start.");
    return record;
  }
  private async inspect(
    socket: string,
    record: NatsContainerlabFixtureRecord,
  ): Promise<{ state: string; present: boolean }> {
    await this.daemon(socket, record.daemon);
    await this.exclusive(socket, record, record.container, record.volume);
    const container = await dockerMetadata(socket, `/containers/${record.container}/json`);
    const network = await dockerMetadata(socket, `/networks/${record.network}`);
    const volume = await dockerMetadata(socket, `/volumes/${record.volume}`);
    this.verifyNetwork(network, record);
    this.verifyVolume(volume, record, record.volume);
    if (
      network !== undefined &&
      Object.keys(dockerObject(network.Containers ?? {})).some((id) => id !== record.container)
    )
      throw new NatsFixtureError("An unrelated endpoint is using the owned NATS network.");
    if (container === undefined) return { state: "absent", present: false };
    if (
      container.Id !== record.container ||
      container.Name !== `/${record.name}` ||
      dockerObject(container.Config).Image !== record.image ||
      !fixtureLabels(dockerObject(container.Config).Labels, record.identity) ||
      network === undefined ||
      volume === undefined
    )
      throw new NatsFixtureError("NATS container ownership could not be verified.");
    const mounts = container.Mounts;
    if (
      !Array.isArray(mounts) ||
      mounts.length !== 1 ||
      dockerObject(mounts[0]).Type !== "volume" ||
      dockerObject(mounts[0]).Name !== record.volume ||
      dockerObject(mounts[0]).Destination !== "/fixture"
    )
      throw new NatsFixtureError("NATS private mount ownership could not be verified.");
    const config = dockerObject(container.Config);
    const labels = dockerObject(config.Labels);
    const host = dockerObject(container.HostConfig);
    const ports = dockerObject(host.PortBindings);
    const bindings = ports["4222/tcp"];
    const networks = dockerObject(dockerObject(container.NetworkSettings).Networks);
    if (
      config.User !== "0:0" ||
      JSON.stringify(config.Entrypoint) !== JSON.stringify(["nats-server"]) ||
      JSON.stringify(config.Cmd) !== JSON.stringify(["--config", "/fixture/nats.conf"]) ||
      labels.containerlab !== record.lab ||
      labels["clab-node-name"] !== "server" ||
      labels["clab-topo-file"] !== record.topologyPath ||
      host.ReadonlyRootfs !== true ||
      host.Privileged !== false ||
      JSON.stringify(host.CapDrop) !== JSON.stringify(["ALL"]) ||
      JSON.stringify(host.CapAdd) !== JSON.stringify(["DAC_OVERRIDE"]) ||
      host.PidsLimit !== 64 ||
      host.Memory !== 134217728 ||
      host.MemorySwap !== 134217728 ||
      host.NanoCpus !== 500000000 ||
      JSON.stringify(host.SecurityOpt) !== JSON.stringify(["no-new-privileges:true"]) ||
      (Array.isArray(host.Binds) && host.Binds.length !== 0) ||
      [host.PidMode, host.UTSMode, host.UsernsMode].some(
        (mode) => typeof mode === "string" && mode !== "" && mode !== "private",
      ) ||
      (typeof host.IpcMode === "string" && !["", "private", "shareable"].includes(host.IpcMode)) ||
      [host.Devices, host.DeviceRequests, host.VolumesFrom].some(
        (entries) => Array.isArray(entries) && entries.length !== 0,
      ) ||
      Object.keys(ports).length !== 1 ||
      !Array.isArray(bindings) ||
      bindings.length !== 1 ||
      dockerObject(bindings[0]).HostIp !== "127.0.0.1" ||
      dockerObject(bindings[0]).HostPort !== String(record.port) ||
      (host.NetworkMode !== record.networkName && host.NetworkMode !== record.network) ||
      Object.keys(networks).length !== 1 ||
      dockerObject(networks[record.networkName]).NetworkID !== record.network
    )
      throw new NatsFixtureError("NATS immutable isolation could not be verified.");
    const state = dockerObject(container.State).Status;
    if (typeof state !== "string") throw new NatsFixtureError("NATS container state was invalid.");
    return { state, present: true };
  }
  private verifyNetwork(
    network: Record<string, unknown> | undefined,
    identity: NatsContainerlabFixtureIntent | NatsContainerlabFixtureRecord,
  ): void {
    if (
      network !== undefined &&
      (network.Id !== identity.network ||
        network.Name !== identity.networkName ||
        !fixtureLabels(network.Labels, identity.identity))
    )
      throw new NatsFixtureError("NATS network ownership could not be verified.");
  }
  private verifyVolume(
    volume: Record<string, unknown> | undefined,
    identity: NatsContainerlabFixtureIntent | NatsContainerlabFixtureRecord,
    name: string,
  ): void {
    if (
      volume !== undefined &&
      (volume.Name !== name || !fixtureLabels(volume.Labels, identity.identity))
    )
      throw new NatsFixtureError("NATS volume ownership could not be verified.");
  }
  private async exclusive(
    socket: string,
    identity: NatsContainerlabFixtureIntent | NatsContainerlabFixtureRecord,
    container?: string,
    volume?: string,
  ): Promise<void> {
    const inventories = await Promise.all([
      dockerContainers(socket, { label: [`containerlab=${identity.lab}`] }),
      dockerContainers(socket, { label: [`io.streamskope.fixture.id=${identity.identity}`] }),
      ...(volume === undefined ? [] : [dockerContainers(socket, { volume: [volume] })]),
    ]);
    if (inventories.some((entries) => entries.some((entry) => entry.Id !== container)))
      throw new NatsFixtureError(
        "An unrecorded container is using the NATS lab or private volume.",
      );
  }
  async status(record: NatsContainerlabFixtureRecord): Promise<string> {
    return (await this.inspect(await this.socket(), record)).state;
  }
  async resume(record: NatsContainerlabFixtureRecord, signal?: AbortSignal): Promise<void> {
    const socket = await this.socket();
    const { state } = await this.inspect(socket, record);
    if (state === "running") return;
    if (state !== "created" && state !== "exited")
      throw new NatsFixtureError("Owned NATS server is not resumable.");
    await this.copy(
      socket,
      { ...record, creationStarted: true, mutationsSettled: true },
      record.container,
    );
    await this.orchestrate(
      socket,
      { ...record, creationStarted: true, mutationsSettled: true },
      () => Promise.resolve(),
      "deploy",
      signal,
    );
    signal?.throwIfAborted();
  }
  async stop(record: NatsContainerlabFixtureRecord): Promise<void> {
    const socket = await this.socket();
    const { present } = await this.inspect(socket, record);
    if (present)
      await this.orchestrate(
        socket,
        { ...record, creationStarted: true, mutationsSettled: true },
        () => Promise.resolve(),
        "destroy",
      );
    const network = await dockerMetadata(socket, `/networks/${record.network}`);
    this.verifyNetwork(network, record);
    if (network !== undefined) {
      if (Object.keys(dockerObject(network.Containers ?? {})).length !== 0)
        throw new NatsFixtureError("NATS network still has attached endpoints.");
      const removed = await dockerApiRequest(socket, "DELETE", `/networks/${record.network}`);
      if (removed.status !== 204 && removed.status !== 404)
        throw new NatsFixtureError("Owned NATS network removal failed.");
    }
    const volume = await dockerMetadata(socket, `/volumes/${record.volume}`);
    this.verifyVolume(volume, record, record.volume);
    if (volume !== undefined) {
      await this.exclusive(socket, record, undefined, record.volume);
      const removed = await dockerApiRequest(socket, "DELETE", `/volumes/${record.volume}`);
      if (removed.status !== 204 && removed.status !== 404)
        throw new NatsFixtureError("Owned NATS volume removal failed.");
    }
    const replies = await Promise.all([
      dockerMetadata(socket, `/containers/${record.container}/json`),
      dockerMetadata(socket, `/networks/${record.network}`),
      dockerMetadata(socket, `/volumes/${record.volume}`),
    ]);
    if (replies.some((value) => value !== undefined))
      throw new NatsFixtureError("Owned NATS cleanup could not be confirmed.");
    const named = await dockerMetadata(
      socket,
      `/containers/${encodeURIComponent(record.name)}/json`,
    );
    const namedNetwork = await dockerMetadata(
      socket,
      `/networks/${encodeURIComponent(record.networkName)}`,
    );
    if (named !== undefined || namedNetwork !== undefined)
      throw new NatsFixtureError(
        "NATS cleanup names are occupied; ownership evidence was retained.",
      );
    await this.exclusive(socket, record, undefined, record.volume);
  }
  async recover(intent: NatsContainerlabFixtureIntent): Promise<void> {
    if (!intent.creationStarted) return;
    const socket = await this.socket();
    if (intent.daemon === undefined)
      throw new NatsFixtureError("Pending NATS daemon identity was not recorded.");
    await this.daemon(socket, intent.daemon);
    const container = await dockerMetadata(
      socket,
      `/containers/${encodeURIComponent(intent.container ?? intent.name)}/json`,
    );
    const network = await dockerMetadata(
      socket,
      `/networks/${encodeURIComponent(intent.network ?? intent.networkName)}`,
    );
    if (
      container !== undefined &&
      (container.Name !== `/${intent.name}` ||
        !fixtureLabels(dockerObject(container.Config).Labels, intent.identity))
    )
      throw new NatsFixtureError("Pending NATS container ownership differs.");
    if (
      network !== undefined &&
      (network.Name !== intent.networkName || !fixtureLabels(network.Labels, intent.identity))
    )
      throw new NatsFixtureError("Pending NATS network ownership differs.");
    if (!intent.mutationsSettled)
      throw new NatsFixtureError(
        "NATS daemon creation has not settled; private evidence was retained.",
      );
    const containerId = container?.Id ?? intent.container;
    const networkId = network?.Id ?? intent.network;
    let volumeId = intent.volume;
    if (container !== undefined && Array.isArray(container.Mounts))
      volumeId ??= dockerObject(
        container.Mounts.find((mount) => dockerObject(mount).Destination === "/fixture"),
      ).Name as string | undefined;
    if (identifier(containerId) && identifier(networkId) && identifier(volumeId))
      await this.stop({
        ...intent,
        daemon: intent.daemon,
        container: containerId,
        network: networkId,
        volume: volumeId,
      });
    else if (container !== undefined)
      throw new NatsFixtureError(
        "Pending NATS ownership is incomplete; private evidence was retained.",
      );
    else {
      await this.exclusive(socket, intent, undefined, volumeId);
      if (network !== undefined) {
        const current = await dockerMetadata(socket, `/networks/${String(network.Id)}`);
        this.verifyNetwork(current, { ...intent, network: String(network.Id) });
        if (
          !identifier(network.Id) ||
          (current !== undefined && Object.keys(dockerObject(current.Containers ?? {})).length > 0)
        )
          throw new NatsFixtureError("Pending NATS network is still in use.");
        const removed = await dockerApiRequest(socket, "DELETE", `/networks/${network.Id}`);
        if (removed.status !== 204 && removed.status !== 404)
          throw new NatsFixtureError("Pending NATS network removal failed.");
      }
      if (volumeId !== undefined) {
        const volume = await dockerMetadata(socket, `/volumes/${volumeId}`);
        this.verifyVolume(volume, intent, volumeId);
        if (volume !== undefined) {
          const removed = await dockerApiRequest(socket, "DELETE", `/volumes/${volumeId}`);
          if (removed.status !== 204 && removed.status !== 404)
            throw new NatsFixtureError("Pending NATS volume removal failed.");
        }
      }
      const residuals = await Promise.all([
        dockerMetadata(socket, `/containers/${intent.name}/json`),
        dockerMetadata(socket, `/networks/${intent.networkName}`),
        ...(identifier(networkId) ? [dockerMetadata(socket, `/networks/${networkId}`)] : []),
        ...(volumeId === undefined ? [] : [dockerMetadata(socket, `/volumes/${volumeId}`)]),
      ]);
      if (residuals.some((value) => value !== undefined))
        throw new NatsFixtureError("Pending NATS cleanup could not be confirmed.");
    }
  }
}
