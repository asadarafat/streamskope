import { randomBytes, randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";

import { Admin, findErrorBy } from "@platformatic/kafka";

import type { SecureConnectionInput } from "../../src/features/kafka/contracts";
import {
  ContainerImageAvailabilityError,
  containerCommandFailureReason,
} from "../../tools/dev/container-image";

import {
  FixtureContainerOwner,
  FixtureCleanupError,
  dockerCommand,
  type DockerCommand,
} from "./container-owner";
import { kafkaPin } from "./pins";
import { loopbackPorts } from "./loopback";

class KafkaAuthorizationFixtureFailure extends Error {
  constructor(message: string) {
    super(message);
    this.name = "KafkaAuthorizationFixtureFailure";
  }
}
interface AuthorizationFixtureRuntime {
  readonly docker: DockerCommand;
  readonly createAdmin: (options: ConstructorParameters<typeof Admin>[0]) => Admin;
  readonly pause: () => Promise<void>;
}

/** FindCoordinator may create __consumer_offsets before its leader is ready. */
export async function seedConsumerGroupOffsets(
  admin: Admin,
  input: Parameters<Admin["alterConsumerGroupOffsets"]>[0],
): Promise<void> {
  const deadline = Date.now() + 15_000;
  for (;;) {
    try {
      await admin.alterConsumerGroupOffsets(input);
      return;
    } catch (error) {
      const pending =
        error instanceof Error &&
        ["COORDINATOR_NOT_AVAILABLE", "COORDINATOR_LOAD_IN_PROGRESS", "NOT_COORDINATOR"].some(
          (code) => findErrorBy(error, "apiId", code) !== null,
        );
      if (!pending || Date.now() >= deadline)
        throw new KafkaAuthorizationFixtureFailure(
          "AIO Kafka consumer-group fixture seeding failed.",
        );
      await delay(100);
    }
  }
}

/** An isolated Linux-only real broker; never changes the developer's running Kafka. */
export async function startAuthorizationFixture(
  runtime: AuthorizationFixtureRuntime = {
    docker: dockerCommand,
    createAdmin: (options) => new Admin(options),
    pause: () => delay(1_000),
  },
): Promise<{
  readonly admin: Admin;
  readonly connection: SecureConnectionInput;
  dispose(): Promise<void>;
}> {
  if (process.platform !== "linux")
    throw new Error("The local Kafka authorization fixture requires Linux Docker host networking.");
  const [clientPort, adminPort, controllerPort] = await loopbackPorts(3);
  const name = `streamskope-acl-qualification-${randomUUID()}`;
  const password = randomBytes(24).toString("hex");
  const { image } = await kafkaPin();
  const owner = new FixtureContainerOwner(runtime.docker);
  const environment: Record<string, string> = {
    CLUSTER_ID: randomBytes(16).toString("base64url"),
    KAFKA_PROCESS_ROLES: "broker,controller",
    KAFKA_NODE_ID: "1",
    KAFKA_CONTROLLER_QUORUM_VOTERS: `1@127.0.0.1:${controllerPort}`,
    KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER",
    KAFKA_LISTENERS: `CLIENT://127.0.0.1:${clientPort},ADMIN://127.0.0.1:${adminPort},CONTROLLER://127.0.0.1:${controllerPort}`,
    KAFKA_ADVERTISED_LISTENERS: `CLIENT://127.0.0.1:${clientPort},ADMIN://127.0.0.1:${adminPort}`,
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:
      "CLIENT:PLAINTEXT,ADMIN:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT",
    KAFKA_INTER_BROKER_LISTENER_NAME: "ADMIN",
    KAFKA_SASL_ENABLED_MECHANISMS: "PLAIN",
    KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: "PLAIN",
    KAFKA_LISTENER_NAME_ADMIN_PLAIN_SASL_JAAS_CONFIG: `org.apache.kafka.common.security.plain.PlainLoginModule required username="fixtureadmin" password="${password}" user_fixtureadmin="${password}";`,
    KAFKA_AUTHORIZER_CLASS_NAME: "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
    KAFKA_SUPER_USERS: "User:fixtureadmin",
    KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "true",
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1",
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1",
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1",
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false",
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0",
    KAFKA_HEAP_OPTS: "-Xms256m -Xmx512m",
    KAFKA_NUM_NETWORK_THREADS: "2",
    KAFKA_NUM_IO_THREADS: "2",
  };
  const admin = runtime.createAdmin({
    bootstrapBrokers: [`127.0.0.1:${adminPort}`],
    clientId: "streamskope-permission-qualification",
    sasl: { mechanism: "PLAIN", username: "fixtureadmin", password },
    retries: 0,
    connectTimeout: 1_000,
    requestTimeout: 2_000,
  });
  const dispose = async (): Promise<void> => {
    const failures: string[] = [];
    try {
      await admin.close();
    } catch {
      failures.push("admin close");
    }
    try {
      await owner.dispose();
    } catch (error) {
      failures.push(
        `container removal: ${error instanceof FixtureCleanupError ? error.reason : containerCommandFailureReason(error)}`,
      );
    }
    if (failures.length > 0)
      throw new KafkaAuthorizationFixtureFailure(
        `Kafka authorization fixture cleanup could not be confirmed (${failures.join(", ")}).`,
      );
  };
  let phase = "pinned image availability";
  try {
    await owner.prepare(image);
    phase = "owned container creation";
    const id = await owner.create(name, [
      "--network",
      "host",
      "--memory",
      "768m",
      "--cpus",
      "2",
      ...Object.entries(environment).flatMap(([key, value]) => ["--env", `${key}=${value}`]),
      image,
    ]);
    await owner.start(id);
    phase = "broker readiness";
    let ready = false;
    for (let attempt = 0; attempt < 60; attempt += 1) {
      try {
        await admin.listTopics();
        ready = true;
        break;
      } catch {
        await runtime.pause();
      }
    }
    if (!ready) throw new Error("The authorization fixture did not become ready.");
    return {
      admin,
      connection: {
        name: "Isolated authorization fixture",
        brokers: [`127.0.0.1:${clientPort}`],
        tls: { enabled: false },
      },
      dispose,
    };
  } catch (error) {
    const reason =
      error instanceof ContainerImageAvailabilityError
        ? error.reason
        : phase === "broker readiness"
          ? "broker-not-ready"
          : containerCommandFailureReason(error);
    try {
      await dispose();
    } catch {
      throw new KafkaAuthorizationFixtureFailure(
        `The isolated Kafka authorization fixture failed during ${phase} (${reason}); owned cleanup could not be confirmed.`,
      );
    }
    // Docker command errors can include environment arguments, so never expose them.
    throw new KafkaAuthorizationFixtureFailure(
      `The isolated Kafka authorization fixture failed during ${phase} (${reason}); owned cleanup was confirmed.`,
    );
  }
}
