import type { BaseOptions } from "@platformatic/kafka";

import type { KafkaClientInput } from "./types";
import { tlsClientIdentityOptions } from "./tls-client-identity";

export function platformaticClientOptions(input: KafkaClientInput, clientId: string): BaseOptions {
  const oauthTokenProvider = input.oauthTokenProvider;
  const base: BaseOptions = {
    bootstrapBrokers: [...input.brokers],
    clientId,
    connectTimeout: input.operationTimeoutMs,
    requestTimeout: input.operationTimeoutMs,
    retries: 0,
    ...(input.tlsEnabled === false
      ? {}
      : {
          tls: {
            ca: [input.caPem],
            ...tlsClientIdentityOptions(input.clientIdentity),
            rejectUnauthorized: true,
          },
        }),
  };
  if (input.sasl !== undefined) {
    if (oauthTokenProvider !== undefined)
      throw new Error("Choose one Kafka SASL authentication mechanism.");
    return { ...base, sasl: { ...input.sasl } };
  }
  return oauthTokenProvider === undefined
    ? base
    : {
        ...base,
        sasl: {
          mechanism: "OAUTHBEARER",
          token: async (): Promise<string> => (await oauthTokenProvider()).value,
        },
      };
}
