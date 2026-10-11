import { SchemaRegistryHttpAdapter } from "../../src/features/kafka/engine/schema-registry-http";
import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";
import {
  loadFixtureConfig,
  provisionSeededFixtureTopic,
  type FixtureConnection,
  type SeededFixtureTopic,
} from "../../aio-kafka/fixtures/development";
import { startKafkaFixture } from "../../aio-kafka/fixtures/broker";
import { disposeFixtureResources } from "../../aio-kafka/fixtures/resources";
import { startSchemaRegistryServerFixture } from "../../aio-kafka/fixtures/registry";

/** Production-browser tests against a real, owned Registry and secured Kafka broker. */
export async function startRegistryBrowserFixture(): Promise<
  SeededFixtureTopic & { readonly connection: FixtureConnection }
> {
  const kafka = await startKafkaFixture();
  let registry: Awaited<ReturnType<typeof startSchemaRegistryServerFixture>> | undefined;
  let seeded: SeededFixtureTopic | undefined;
  const dispose = (): Promise<void> =>
    disposeFixtureResources([
      (): Promise<void> => seeded?.dispose() ?? Promise.resolve(),
      (): Promise<void> => registry?.dispose() ?? Promise.resolve(),
      (): Promise<void> => kafka.dispose(),
    ]);
  try {
    registry = await startSchemaRegistryServerFixture(kafka.internalBroker);
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
      kafkaEndpoint: kafka.environment.STREAMSKOPE_TEST_KAFKA_ENDPOINT!,
      oauthEndpoint: kafka.environment.STREAMSKOPE_TEST_OAUTH_ENDPOINT!,
      caPath: kafka.environment.STREAMSKOPE_TEST_CA_PATH!,
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
