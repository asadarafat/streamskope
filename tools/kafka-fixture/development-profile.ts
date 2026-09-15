import { readFile } from "node:fs/promises";
import { join } from "node:path";

import type { KafkaProfileStore } from "../../src/kafka/application";

import { DEFAULT_OWNED_FIXTURE_NAME } from "./defaults";
import { FileFixtureOwnershipStore } from "./file-ownership-store";
import { loadFixtureSourceConfig } from "./node-runtime";

const LOCAL_AIO_PROFILE_ID = "local-aio-kafka";
const LOCAL_AIO_PROFILE_NAME = "Local AIO Kafka";
const LOCAL_AIO_HOST = "127.0.0.1";
const UNAVAILABLE_RECOVERY = "Run npm run fixture:start before connecting to Local AIO Kafka.";

export type LocalAioDevelopmentProfilePreparation =
  | {
      readonly profileName: typeof LOCAL_AIO_PROFILE_NAME;
      readonly status: "seeded";
    }
  | {
      readonly status: "unchanged";
    }
  | {
      readonly recovery: typeof UNAVAILABLE_RECOVERY;
      readonly status: "unavailable";
    };

export interface LocalAioDevelopmentProfileOptions {
  readonly fixtureName?: string;
  readonly now?: () => Date;
  readonly repositoryRoot: string;
}

function fixtureUnavailable(): LocalAioDevelopmentProfilePreparation {
  return { recovery: UNAVAILABLE_RECOVERY, status: "unavailable" };
}

function currentTime(): Date {
  return new Date();
}

function completeCertificateOnlyPem(material: string): boolean {
  return (
    material.includes("-----BEGIN CERTIFICATE-----") &&
    material.includes("-----END CERTIFICATE-----") &&
    !material.includes("PRIVATE KEY")
  );
}

export async function prepareLocalAioDevelopmentProfile(
  store: KafkaProfileStore,
  options: LocalAioDevelopmentProfileOptions,
): Promise<LocalAioDevelopmentProfilePreparation> {
  const existing = await store.load();
  if (existing.length > 0) {
    return { status: "unchanged" };
  }

  const fixtureName = options.fixtureName ?? DEFAULT_OWNED_FIXTURE_NAME;
  const fixtureRoot = join(options.repositoryRoot, "aio-kafka");

  let fixtureConfig: Awaited<ReturnType<typeof loadFixtureSourceConfig>>;
  let ownership: Awaited<ReturnType<FileFixtureOwnershipStore["load"]>>;
  let trustMaterial: string;
  try {
    fixtureConfig = await loadFixtureSourceConfig(options.repositoryRoot);
    ownership = await new FileFixtureOwnershipStore(join(fixtureRoot, "ownership", "records")).load(
      fixtureName,
    );
    if (
      ownership === undefined ||
      ownership.oauthImage !== fixtureConfig.oauthImage ||
      ownership.schemaRegistryImage !== fixtureConfig.schemaRegistryImage ||
      ownership.schemaRegistryPort === undefined
    ) {
      return fixtureUnavailable();
    }
    trustMaterial = await readFile(ownership.caPath, "utf8");
  } catch {
    return fixtureUnavailable();
  }

  if (!completeCertificateOnlyPem(trustMaterial)) {
    return fixtureUnavailable();
  }

  const timestamp = (options.now ?? currentTime)().toISOString();
  await store.commit([
    {
      brokers: [`${LOCAL_AIO_HOST}:${ownership.kafkaPort}`],
      createdAt: timestamp,
      id: LOCAL_AIO_PROFILE_ID,
      name: LOCAL_AIO_PROFILE_NAME,
      oauth: {
        clientId: fixtureConfig.oauthClientId,
        clientSecret: fixtureConfig.oauthClientSecret,
        scope: fixtureConfig.oauthScope,
        tokenEndpoint: `http://${LOCAL_AIO_HOST}:${ownership.oauthPort}/rest-gateway/rest/api/v1/auth/token`,
      },
      services: {
        schemaRegistry: {
          authentication: "oauth",
          baseUrl: `http://${LOCAL_AIO_HOST}:${ownership.schemaRegistryPort}`,
        },
      },
      trust: {
        kind: "pem",
        label: "Repository fixture CA",
        material: trustMaterial,
      },
      updatedAt: timestamp,
    },
  ]);
  return { profileName: LOCAL_AIO_PROFILE_NAME, status: "seeded" };
}
