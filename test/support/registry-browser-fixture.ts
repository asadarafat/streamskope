import { SchemaRegistryHttpAdapter } from "../../src/features/kafka/engine/schema-registry-http";
import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";

import {
  loadFixtureConfig,
  provisionSeededFixtureTopic,
  type FixtureConnection,
  type SeededFixtureTopic,
} from "./kafka-fixture";
import { startNativeKafkaFixture, disposeNativeFixtureResources } from "./native-kafka-fixture";
import { startSchemaRegistryServerFixture } from "./schema-registry-server-fixture";

/** Production-browser tests against a real, owned Registry and secured Kafka broker. */
export async function startRegistryBrowserFixture(): Promise<
  SeededFixtureTopic & { readonly connection: FixtureConnection }
> {
  const native = await startNativeKafkaFixture();
  let registry: Awaited<ReturnType<typeof startSchemaRegistryServerFixture>> | undefined;
  let seeded: SeededFixtureTopic | undefined;
  const dispose = (): Promise<void> =>
    disposeNativeFixtureResources([
      (): Promise<void> => seeded?.dispose() ?? Promise.resolve(),
      (): Promise<void> => registry?.dispose() ?? Promise.resolve(),
      (): Promise<void> => native.dispose(),
    ]);
  try {
    registry = await startSchemaRegistryServerFixture(native.internalBroker);
    const config = await loadFixtureConfig();
    const adapter = new SchemaRegistryHttpAdapter(new NodeBoundedJsonHttp());
    await adapter.register(
      {
        baseUrl: registry.url,
        authorization: (): Promise<undefined> => Promise.resolve(undefined),
      },
      {
        subject: config.schemaSubject,
        version: "latest",
        normalize: true,
        schemaType: "AVRO",
        schema: config.schemaDefinition,
        references: [],
      },
      AbortSignal.timeout(15000),
    );
    const connection: FixtureConnection = {
      kafkaEndpoint: native.environment.STREAMSKOPE_TEST_KAFKA_ENDPOINT!,
      oauthEndpoint: native.environment.STREAMSKOPE_TEST_OAUTH_ENDPOINT!,
      caPath: native.environment.STREAMSKOPE_TEST_CA_PATH!,
      schemaRegistryEndpoint: registry.url,
    };
    seeded = await provisionSeededFixtureTopic(connection);
    return { config: seeded.config, connection, dispose };
  } catch (cause) {
    try {
      await dispose();
    } catch (cleanup) {
      throw new AggregateError(
        [cause, cleanup],
        "Registry browser fixture setup and cleanup failed.",
        { cause: cleanup },
      );
    }
    throw cause;
  }
}
