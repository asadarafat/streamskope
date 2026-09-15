import type { BaseOptions } from "@platformatic/kafka";

import type { KafkaClientInput } from "./types";

export function platformaticClientOptions(input: KafkaClientInput, clientId: string): BaseOptions {
  const oauthTokenProvider = input.oauthTokenProvider;
  const base: BaseOptions = {
    bootstrapBrokers: [...input.brokers],
    clientId,
    connectTimeout: input.operationTimeoutMs,
    requestTimeout: input.operationTimeoutMs,
    retries: 0,
    tls: {
      ca: [input.caPem],
      rejectUnauthorized: true,
    },
  };
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
