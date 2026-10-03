import { expect, it } from "vitest";

import { ConnectHttpAdapter } from "../../src/features/kafka/engine/connect-http";
import type {
  BoundedJsonHttpRequest,
  BoundedJsonHttpResponse,
} from "../../src/features/kafka/engine/bounded-json-http";
import { relationshipFixture } from "../support/relationship-fixture";

it("reads Connect type/topic provenance using authenticated GETs and keeps tracking unavailability distinct from empty", async () => {
  const requests: BoundedJsonHttpRequest[] = [];
  let missing = false;
  const adapter = new ConnectHttpAdapter({
    request: (request): Promise<BoundedJsonHttpResponse> => {
      requests.push(request);
      if (request.url.endsWith("/topics"))
        return Promise.resolve({
          status: missing ? 403 : 200,
          body: missing ? { message: "private error" } : { source: { topics: ["events"] } },
        });
      if (request.url.endsWith("/source"))
        return Promise.resolve({
          status: 200,
          body: {
            type: "source",
            config: {
              topics: "events, other",
              password: "private secret",
              "topics.regex": "events-.*",
            },
          },
        });
      return Promise.resolve({ status: 200, body: { kafka_cluster_id: "cluster-1" } });
    },
  });
  const context = {
      baseUrl: "https://connect.example",
      authorization: (): Promise<string> => Promise.resolve("Bearer private credential"),
    },
    signal = new AbortController().signal;
  expect(await adapter.clusterId(context, signal)).toBe("cluster-1");
  expect(await adapter.relationships(context, "source", signal)).toEqual({
    type: "source",
    reportedTopics: ["events"],
    configuredTopics: ["events", "other"],
    regexSubscription: true,
  });
  missing = true;
  const unavailable = await adapter.relationships(context, "source", signal);
  expect(unavailable.reportedTopics).toBeNull();
  expect(JSON.stringify(unavailable)).not.toContain("private");
  expect(
    requests.every(
      (r) =>
        r.method === "GET" &&
        r.body === undefined &&
        r.authorization === "Bearer private credential",
    ),
  ).toBe(true);
});
it("refuses to associate a Connect endpoint belonging to a different Kafka cluster", async () => {
  const f = relationshipFixture();
  f.connect.clusterId = (): Promise<string> => Promise.resolve("different-cluster");
  const graph = await f.service.capture({
    topics: ["events"],
    subject: null,
    version: null,
    sampleRecords: false,
  });
  expect(graph.edges.some((e) => e.source === "Kafka Connect")).toBe(false);
  expect(graph.coverage.find((c) => c.source === "Kafka Connect")).toMatchObject({
    state: "unavailable",
    inspected: 0,
  });
});
