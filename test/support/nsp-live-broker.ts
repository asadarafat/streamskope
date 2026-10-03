import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";

import { Admin } from "@platformatic/kafka";

import { NspApiClient } from "../../plugins/nsp/backend/api-client";
import type { NspConnectInput } from "../../plugins/nsp/contracts";
import type { SecureConnectionInput } from "../../src/features/kafka/contracts";
import { requestOAuthToken } from "../../src/features/kafka/engine/oauth";
import { createHostTrustMaterialDecoder } from "../../src/platform/node/trust-material-decoder";

/** Fixture administration only. Product connection/produce/read still traverse the host contract. */
export function nspFixtureAdmin(connection: SecureConnectionInput): Admin {
  return new Admin({
    bootstrapBrokers: [...connection.brokers],
    clientId: `streamskope-nsp-qualification-${randomUUID()}`,
    retries: 0,
    connectTimeout: 15_000,
    requestTimeout: 15_000,
    ...(connection.tls.enabled
      ? { tls: { ca: [connection.tls.caPem], rejectUnauthorized: true } }
      : {}),
    ...(connection.oauth === undefined
      ? {}
      : {
          sasl: {
            mechanism: "OAUTHBEARER" as const,
            token: async (): Promise<string> =>
              (
                await requestOAuthToken({
                  ...connection.oauth!,
                  ...(connection.tls.enabled ? { caPem: connection.tls.caPem } : {}),
                  signal: AbortSignal.timeout(15_000),
                })
              ).value,
          },
        }),
  });
}

/** Retrieve a separate fixture-admin trust context; no trust bytes are returned to a renderer. */
export async function liveNspFixtureAdmin(
  input: NspConnectInput,
  authentication: "tls" | "oauth",
): Promise<Admin> {
  const client = new NspApiClient(input);
  try {
    await client.authenticate();
    const material = await client.retrieveTrust(randomUUID());
    const trust = await createHostTrustMaterialDecoder().decode({
      kind: "jks",
      material: material.truststoreBase64,
      password: material.truststorePassword,
    });
    return nspFixtureAdmin({
      name: "NSP qualification fixture",
      brokers: input.brokers ?? [`${new URL(input.apiUrl).hostname}:9192`],
      tls: { enabled: true, caPem: trust.caPem },
      ...(authentication === "tls"
        ? {}
        : {
            oauth: {
              clientId: input.username,
              clientSecret: input.password,
              scope: "",
              tokenEndpoint: `${input.apiUrl}/rest-gateway/rest/api/v1/auth/token`,
            },
          }),
    });
  } finally {
    await client.close();
  }
}

/** The only deletable name is a fresh fixture identity; business topics are never adopted. */
export function ownedNspTopic(): string {
  return `streamskope-qualification-${randomUUID()}`;
}

export async function removeOwnedNspTopic(admin: Admin, topic: string): Promise<void> {
  assert.match(topic, /^streamskope-qualification-[0-9a-f-]{36}$/u);
  if (!(await admin.listTopics()).includes(topic)) return;
  await admin.deleteTopics({ topics: [topic] });
  for (let attempt = 0; attempt < 40; attempt += 1) {
    if (!(await admin.listTopics()).includes(topic)) return;
    await delay(250);
  }
  throw new Error("The owned NSP qualification topic was not removed.");
}
