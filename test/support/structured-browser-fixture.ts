import {
  loadFixtureConfig,
  provisionSeededFixtureTopic,
  type FixtureConnection,
  type SeededFixtureTopic,
} from "../../aio-kafka/fixtures/development";
import { startKafkaFixture } from "../../aio-kafka/fixtures/broker";
import { disposeFixtureResources } from "../../aio-kafka/fixtures/resources";

import { createSchemaRegistryProtocolFixture } from "./schema-registry-protocol-fixture";

/** Each run owns its broker, topic and Registry endpoint; no local AIO deployment is required. */
export async function startStructuredBrowserFixture(): Promise<
  SeededFixtureTopic & {
    readonly connection: FixtureConnection;
  }
> {
  const config = await loadFixtureConfig();
  const kafka = await startKafkaFixture();
  const registry = createSchemaRegistryProtocolFixture([
    {
      id: 1,
      subject: config.schemaSubject,
      version: 1,
      schemaType: "AVRO",
      schema: config.schemaDefinition,
      references: [],
    },
  ]);
  let seeded: SeededFixtureTopic | undefined;
  const dispose = (): Promise<void> =>
    disposeFixtureResources([
      (): Promise<void> => seeded?.dispose() ?? Promise.resolve(),
      (): Promise<void> => registry.close(),
      (): Promise<void> => kafka.dispose(),
    ]);
  try {
    const connection: FixtureConnection = {
      kafkaEndpoint: kafka.environment.STREAMSKOPE_TEST_KAFKA_ENDPOINT!,
      oauthEndpoint: kafka.environment.STREAMSKOPE_TEST_OAUTH_ENDPOINT!,
      caPath: kafka.environment.STREAMSKOPE_TEST_CA_PATH!,
      schemaRegistryEndpoint: await registry.listen(),
    };
    seeded = await provisionSeededFixtureTopic(connection);
    return { config: seeded.config, connection, dispose };
  } catch (error) {
    await dispose();
    throw error;
  }
}
