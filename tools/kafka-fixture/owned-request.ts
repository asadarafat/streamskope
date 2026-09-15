import { join } from "node:path";

import { DEFAULT_OWNED_FIXTURE_NAME, DEFAULT_SCHEMA_REGISTRY_PORT } from "./defaults";
import type { OwnedFixtureRequest } from "./lifecycle";
import { loadFixtureSourceConfig } from "./node-runtime";

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
