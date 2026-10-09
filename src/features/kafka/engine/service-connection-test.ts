import { serviceConnectionDiagnostic } from "../application/connection-diagnostics";

import type { BoundedJsonHttpPort } from "./bounded-json-http";
import { ConnectHttpAdapter } from "./connect-http";
import { KafkaEngineFailure } from "./failure";
import { SchemaRegistryHttpAdapter } from "./schema-registry-http";
import type { ConnectionCheck, KafkaEngineConnection } from "./types";

/** Qualify configured services through bounded read-only calls, with the same transport as usage. */
export async function testClusterServices(
  connection: KafkaEngineConnection,
  http: BoundedJsonHttpPort,
  signal: AbortSignal,
): Promise<readonly ConnectionCheck[]> {
  const checks: ConnectionCheck[] = [];
  for (const service of ["schemaRegistry", "connect"] as const) {
    const context = connection.clusterServiceContext?.(service);
    if (context === undefined || context === null) continue;
    const label = service === "schemaRegistry" ? "Schema Registry" : "Kafka Connect";
    try {
      if (service === "schemaRegistry")
        await new SchemaRegistryHttpAdapter(http).listSubjects(context, signal);
      else await new ConnectHttpAdapter(http).list(context, signal);
      checks.push(service === "schemaRegistry" ? "schema-registry" : "connect");
    } catch (error) {
      throw new KafkaEngineFailure({
        ...(serviceConnectionDiagnostic(error, label) ?? {
          code: "BACKEND_UNAVAILABLE",
          retryable: false,
          stage: "backend",
          summary: `${label} did not return the expected API response.`,
          recovery: `Verify the ${label} endpoint, authentication and supported API before retrying.`,
        }),
        cause: error,
        target: service,
      });
    }
  }
  return checks;
}
