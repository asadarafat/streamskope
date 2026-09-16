import { describe, expect, it } from "vitest";

import type { KafkaClusterServiceContext } from "../../src/features/kafka/application";
import type {
  BoundedJsonHttpPort,
  BoundedJsonHttpRequest,
  BoundedJsonHttpResponse,
} from "../../src/features/kafka/engine/bounded-json-http";
import { RedpandaTransformHttpAdapter } from "../../src/features/kafka/engine/redpanda-transform-http";

class ScriptedHttp implements BoundedJsonHttpPort {
  readonly requests: BoundedJsonHttpRequest[] = [];
  constructor(private readonly responses: readonly BoundedJsonHttpResponse[]) {}
  request(input: BoundedJsonHttpRequest): Promise<BoundedJsonHttpResponse> {
    this.requests.push(input);
    return Promise.resolve(this.responses[this.requests.length - 1] ?? { body: null, status: 500 });
  }
}

const context: KafkaClusterServiceContext = {
  authorization: () => Promise.resolve("Bearer redacted"),
  baseUrl: "https://admin.example.test:9644",
  caPem: "fixture-ca",
};

describe("Redpanda transform HTTP adapter", () => {
  it("lists metadata while redacting every environment value", async () => {
    const http = new ScriptedHttp([
      {
        body: [
          {
            compression: "none",
            environment: [
              { key: "TOKEN", value: "never-cross-host" },
              { key: "API_ENDPOINT", value: "https://service.internal" },
            ],
            input_topic: "orders.raw",
            name: "mask-orders",
            output_topics: ["orders.masked"],
            status: [{ lag: 2, node_id: 1, partition: 0, status: "running" }],
          },
        ],
        status: 200,
      },
    ]);
    const adapter = new RedpandaTransformHttpAdapter(http);

    const transforms = await adapter.list(context, new AbortController().signal);

    expect(transforms).toEqual([
      {
        aggregateStatus: "running",
        compression: "none",
        environment: [
          { name: "API_ENDPOINT", valuePresent: true },
          { name: "TOKEN", valuePresent: true },
        ],
        inputTopic: "orders.raw",
        name: "mask-orders",
        maximumLag: 2,
        offset: null,
        outputTopics: ["orders.masked"],
        statuses: [{ lag: 2, nodeId: 1, partition: 0, status: "running" }],
      },
    ]);
    expect(JSON.stringify(transforms)).not.toContain("never-cross-host");
    expect(http.requests[0]).toMatchObject({
      authorization: "Bearer redacted",
      method: "GET",
      url: "https://admin.example.test:9644/v1/transform/",
    });
  });

  it("deletes only the encoded exact transform name", async () => {
    const http = new ScriptedHttp([{ body: null, status: 204 }]);
    const adapter = new RedpandaTransformHttpAdapter(http);

    await adapter.delete(context, "mask/orders", new AbortController().signal);

    expect(http.requests[0]).toMatchObject({
      method: "DELETE",
      url: "https://admin.example.test:9644/v1/transform/mask%2Forders",
    });
  });

  it("rejects an invalid offset value instead of stringifying upstream objects", async () => {
    const adapter = new RedpandaTransformHttpAdapter(
      new ScriptedHttp([
        {
          body: [
            {
              environment: [],
              input_topic: "orders.raw",
              name: "mask-orders",
              offset: { format: "from_start", value: { unsafe: true } },
              output_topics: ["orders.masked"],
              status: [],
            },
          ],
          status: 200,
        },
      ]),
    );

    await expect(adapter.list(context, new AbortController().signal)).rejects.toMatchObject({
      name: "RedpandaTransformResponseError",
      status: null,
    });
  });
});
