import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { createServer } from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

const execute = promisify(execFile);
/** Real reference-capable Registry. Host networking is confined to the Linux CI/qualification host. */
export async function startSchemaRegistryServerFixture(
  broker: string,
): Promise<{ url: string; image: string; dispose(): Promise<void> }> {
  if (process.platform !== "linux" || !/^127\.0\.0\.1:\d+$/u.test(broker))
    throw new Error("Isolated Registry fixture requires a Linux loopback broker.");
  const image =
    "ghcr.io/aiven-open/karapace:6.1.0@sha256:d5d2cb9dfe259ccd4df83553c272c531c2b5309d87340d5b226ab05b607cfd27";
  const reservation = createServer();
  await new Promise<void>((resolve, reject) => {
    reservation.once("error", reject);
    reservation.listen(0, "127.0.0.1", resolve);
  });
  const address = reservation.address();
  if (!address || typeof address === "string") throw new Error("Registry port unavailable.");
  const port = address.port;
  await new Promise<void>((resolve) => reservation.close(() => resolve()));
  const owner = randomUUID(),
    name = `streamskope-registry-${owner}`;
  const url = `http://127.0.0.1:${port}`;
  let id: string | undefined;
  const dispose = async (): Promise<void> => {
    if (!id) return;
    const inspected = (
      await execute(
        "docker",
        ["inspect", id, "--format", '{{.Id}} {{index .Config.Labels "streamskope.fixture"}}'],
        { timeout: 15_000 },
      )
    ).stdout.trim();
    if (inspected !== `${id} ${owner}`)
      throw new Error("Registry fixture cleanup ownership differs.");
    await execute("docker", ["rm", "--force", id], { timeout: 30_000 });
    const remaining = await execute(
      "docker",
      ["ps", "--all", "--no-trunc", "--filter", `id=${id}`, "--format", "{{.ID}}"],
      { timeout: 15_000 },
    );
    if (remaining.stdout.trim() !== "") throw new Error("Registry fixture removal is unconfirmed.");
    id = undefined;
  };
  let readiness = { ready: false, primary: false, coordinator: false, generation: -1 };
  try {
    const output = await execute(
      "docker",
      [
        "run",
        "--detach",
        "--name",
        name,
        "--label",
        `streamskope.fixture=${owner}`,
        "--network",
        "host",
        "--memory",
        "512m",
        "--cpus",
        "1",
        "--env",
        `KARAPACE_BOOTSTRAP_URI=${broker}`,
        "--env",
        "KARAPACE_HOST=127.0.0.1",
        "--env",
        `KARAPACE_PORT=${port}`,
        "--env",
        "KARAPACE_KARAPACE_REGISTRY=true",
        "--env",
        "KARAPACE_KARAPACE_REST=false",
        "--env",
        `KARAPACE_GROUP_ID=${name}`,
        "--env",
        `KARAPACE_TOPIC_NAME=${name}-schemas`,
        "--env",
        "KARAPACE_REPLICATION_FACTOR=1",
        "--env",
        "KARAPACE_COMPATIBILITY=BACKWARD",
        "--env",
        "KARAPACE_LOG_LEVEL=WARNING",
        "--entrypoint",
        "python3",
        image,
        "-m",
        "karapace",
      ],
      { timeout: 180_000 },
    );
    id = output.stdout.trim();
    if (!/^[a-f0-9]{64}$/u.test(id))
      throw new Error("Registry fixture did not return its container identity.");
    for (let attempt = 0; attempt < 60; attempt++) {
      try {
        const response = await fetch(`${url}/_health`, { signal: AbortSignal.timeout(1000) });
        const health = response.ok
          ? ((await response.json()) as {
              status?: {
                schema_registry_ready?: boolean;
                schema_registry_is_primary?: boolean;
                schema_registry_coordinator_running?: boolean;
                schema_registry_coordinator_generation_id?: number;
              };
            })
          : undefined;
        const generation = health?.status?.schema_registry_coordinator_generation_id;
        readiness = {
          ready: health?.status?.schema_registry_ready === true,
          primary: health?.status?.schema_registry_is_primary === true,
          coordinator: health?.status?.schema_registry_coordinator_running === true,
          generation: generation !== undefined && Number.isSafeInteger(generation) ? generation : -1,
        };
        if (
          health?.status?.schema_registry_ready === true &&
          health.status.schema_registry_is_primary === true
        )
          return { url, image, dispose };
      } catch {
        /* bounded startup */
      }
      await delay(1000);
    }
    throw new Error(`Registry readiness timed out: ${JSON.stringify(readiness)}.`);
  } catch (cause) {
    if (!id) {
      try {
        const inspected = (
          await execute(
            "docker",
            ["inspect", name, "--format", '{{.Id}} {{index .Config.Labels "streamskope.fixture"}}'],
            { timeout: 15_000 },
          )
        ).stdout
          .trim()
          .split(" ");
        if (inspected[1] === owner && /^[a-f0-9]{64}$/u.test(inspected[0]!)) id = inspected[0];
      } catch {
        /* No started owned container to adopt. */
      }
    }
    await dispose();
    throw new Error("Isolated reference-capable Registry fixture did not become ready.", { cause });
  }
}
