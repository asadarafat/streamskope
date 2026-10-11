import { loadFixtureSourceConfig } from "../../tools/dev/kafka-fixture/node-runtime";
import { verifyFixtureReady } from "../../tools/dev/kafka-fixture/readiness";

import { explicitFixtureConnection, type FixtureConnection } from "./development";

export class RemoteAioFixtureError extends Error {
  constructor(readonly code: "FIXTURE_NOT_CONFIGURED" | "FIXTURE_UNAVAILABLE") {
    super(
      code === "FIXTURE_NOT_CONFIGURED"
        ? "Configure the shared AIO Kafka endpoint, OAuth endpoint and local CA file; no host JVM fallback is available."
        : "The shared AIO fixture could not be verified. Check the client route or tunnel, advertised Kafka endpoint, CA and OAuth service.",
    );
    this.name = "RemoteAioFixtureError";
  }
}

/** Read-only attachment; this client has no authority to stop the remote lab. */
export async function preflightRemoteAioFixture(
  environment: Readonly<Record<string, string | undefined>> = process.env,
  verify: typeof verifyFixtureReady = verifyFixtureReady,
): Promise<FixtureConnection> {
  let connection: FixtureConnection;
  try {
    const explicit = explicitFixtureConnection(environment);
    if (explicit === undefined) throw new RemoteAioFixtureError("FIXTURE_NOT_CONFIGURED");
    connection = explicit;
  } catch {
    // URL/parser failures may contain supplied endpoints or embedded credentials.
    throw new RemoteAioFixtureError("FIXTURE_NOT_CONFIGURED");
  }
  try {
    const config = await loadFixtureSourceConfig(process.cwd());
    await verify({ ...connection, name: undefined, ownership: "external" }, config);
  } catch {
    // File, TLS and token errors must not leak CA paths, tokens or service details.
    throw new RemoteAioFixtureError("FIXTURE_UNAVAILABLE");
  }
  return connection;
}
