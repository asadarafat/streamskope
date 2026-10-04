import {
  ResponseError,
  type ClusterMetadata,
  type Group,
  type ListConsumerGroupOffsetsGroup,
} from "@platformatic/kafka";
import { expect, it, vi, type Mock } from "vitest";

import {
  PlatformaticAdminPort,
  type PlatformaticAdminClient,
} from "../../src/features/kafka/engine/platformatic-admin";
import {
  observeSelectedGroup,
  observeSelectedTopic,
} from "../../src/features/kafka/engine/platformatic-observation";
import { KafkaEngineFailure } from "../../src/features/kafka/engine/failure";

type MockAdmin = {
  [Name in keyof PlatformaticAdminClient]: Mock<NonNullable<PlatformaticAdminClient[Name]>>;
};

function client(): MockAdmin {
  return {
    close: vi.fn(() => Promise.resolve()),
    describeConfigs: vi.fn(() => Promise.resolve([])),
    describeGroups: vi.fn(() => Promise.resolve(new Map<string, Group>())),
    incrementalAlterConfigs: vi.fn(() => Promise.resolve()),
    listConsumerGroupOffsets: vi.fn(() => Promise.resolve([])),
    listGroups: vi.fn(() => Promise.resolve(new Map())),
    listOffsets: vi.fn(() => Promise.resolve([])),
    listTopics: vi.fn(() => Promise.resolve([])),
    metadata: vi.fn(() =>
      Promise.resolve({
        id: "cluster",
        controllerId: 1,
        brokers: new Map([[1, { host: "localhost", port: 9092, rack: null }]]),
        topics: new Map([
          [
            "events",
            {
              id: "topic",
              lastUpdate: 0,
              partitionsCount: 1,
              partitions: [
                { leader: 1, leaderEpoch: 0, replicas: [1], isr: [1], offlineReplicas: [] },
              ],
            },
          ],
        ]),
        lastUpdate: 0,
      } satisfies ClusterMetadata),
    ),
  };
}

it("fetches only the selected topic's committed positions without unrelated latest reads or group inventory caps", async () => {
  const admin = client();
  vi.mocked(admin.listGroups).mockResolvedValue(
    new Map([["workers", { id: "workers", state: "STABLE", protocolType: "consumer" }]]),
  );
  vi.mocked(admin.describeGroups).mockResolvedValue(
    new Map([
      [
        "workers",
        {
          id: "workers",
          state: "STABLE",
          members: new Map(Array.from({ length: 1001 }, (_, i) => [String(i), {}])),
          protocol: "range",
          protocolType: "consumer",
        } as Group,
      ],
    ]),
  );
  vi.mocked(admin.listConsumerGroupOffsets).mockResolvedValue([
    {
      groupId: "workers",
      topics: [
        {
          name: "unrelated",
          partitions: Array.from({ length: 3000 }, (_, partitionIndex) => ({
            partitionIndex,
            committedOffset: 50n,
            committedLeaderEpoch: 0,
            metadata: null,
          })),
        },
        {
          name: "events",
          partitions: [
            { partitionIndex: 0, committedOffset: 17n, committedLeaderEpoch: 0, metadata: null },
          ],
        },
      ],
    },
  ]);
  const result = await observeSelectedGroup(admin, "workers", "events", [0]);
  expect(admin.listConsumerGroupOffsets).toHaveBeenCalledWith({
    groups: [{ groupId: "workers", topics: [{ name: "events", partitionIndexes: [0] }] }],
    requireStable: false,
  });
  expect(admin.listOffsets).not.toHaveBeenCalled();
  expect(result).toMatchObject({
    state: "stable",
    members: 1000,
    offsets: [{ partition: 0, committedOffset: "17" }],
    issues: [{ measurement: "group-members", code: "OBSERVATION_INCOMPLETE" }],
  });
});

it("preserves permitted committed positions when group-member description is denied", async () => {
  const admin = client();
  vi.mocked(admin.listGroups).mockResolvedValue(
    new Map([["workers", { id: "workers", state: "STABLE", protocolType: "consumer" }]]),
  );
  vi.mocked(admin.describeGroups).mockRejectedValue(
    Object.assign(new Error("private broker detail"), { apiId: "GROUP_AUTHORIZATION_FAILED" }),
  );
  vi.mocked(admin.listConsumerGroupOffsets).mockResolvedValue([
    {
      groupId: "workers",
      topics: [
        {
          name: "events",
          partitions: [
            { partitionIndex: 0, committedOffset: 17n, committedLeaderEpoch: 0, metadata: null },
          ],
        },
      ],
    },
  ] satisfies ListConsumerGroupOffsetsGroup[]);
  const result = await observeSelectedGroup(admin, "workers", "events", [0]);
  expect(result).toMatchObject({
    members: null,
    state: null,
    offsets: [{ partition: 0, committedOffset: "17" }],
    issues: [{ measurement: "group-members", code: "AUTHORIZATION_DENIED" }],
  });
  expect(JSON.stringify(result)).not.toContain("private broker detail");
});

function offlineResponse(topicError = 0): unknown {
  return {
    throttleTimeMs: 0,
    clusterId: "cluster",
    controllerId: 2,
    brokers: [{ nodeId: 2, host: "localhost", port: 9092, rack: null }],
    topics: [
      {
        errorCode: topicError,
        name: "events",
        topicId: "topic",
        isInternal: false,
        topicAuthorizedOperations: 0,
        partitions: [
          {
            errorCode: 5,
            partitionIndex: 0,
            leaderId: -1,
            leaderEpoch: 1,
            replicaNodes: [1],
            isrNodes: [],
            offlineReplicas: [1],
          },
        ],
      },
    ],
  };
}

it("retains leader-unavailable metadata as an offline partition with unknown end position", async () => {
  const admin = client();
  vi.mocked(admin.metadata).mockRejectedValue(
    new ResponseError(3, 12, { "/topics/0/partitions/0": [5, null] }, offlineResponse()),
  );
  const health = await observeSelectedTopic(admin, "events");
  expect(health).toMatchObject({
    clusterId: "cluster",
    topicId: "topic",
    brokerCount: 1,
    controllerKnown: true,
    partitions: [{ partition: 0, leader: null, replicas: 1, inSyncReplicas: 0, endOffset: null }],
    issues: [{ measurement: "end-offsets", code: "OBSERVATION_INCOMPLETE" }],
  });
  expect(admin.listOffsets).not.toHaveBeenCalled();
});

it("does not salvage authorization or missing-topic metadata as healthy data", async () => {
  const admin = client();
  const denied = new ResponseError(3, 12, { "/topics/0": [29, null] }, offlineResponse(29));
  vi.mocked(admin.metadata).mockRejectedValueOnce(denied);
  await expect(observeSelectedTopic(admin, "events")).rejects.toBe(denied);
  vi.mocked(admin.metadata).mockRejectedValueOnce(
    new ResponseError(3, 12, { "/topics/0": [3, null] }, offlineResponse(3)),
  );
  await expect(observeSelectedTopic(admin, "events")).rejects.toMatchObject({
    code: "TOPIC_NOT_FOUND",
  });
});

it("retains safe end-offset failure causes while keeping useful metadata", async () => {
  const admin = client();
  vi.mocked(admin.listOffsets).mockRejectedValue(
    new KafkaEngineFailure({
      code: "AUTHORIZATION_DENIED",
      stage: "authorization",
      retryable: false,
      summary: "Kafka denied this operation.",
      recovery: "Request DESCRIBE permission.",
      cause: new Error("password=private"),
    }),
  );
  const result = await observeSelectedTopic(admin, "events");
  expect(result.partitions[0]?.endOffset).toBeNull();
  expect(result.issues).toMatchObject([
    {
      measurement: "end-offsets",
      code: "AUTHORIZATION_DENIED",
      summary: "Kafka denied this operation.",
      recovery: "Request DESCRIBE permission.",
      retryable: false,
    },
  ]);
  expect(JSON.stringify(result)).not.toContain("password=private");
});

it("aborting an observation closes its private admin only and starts no subsequent offset read", async () => {
  const shared = client(),
    isolated = client(),
    controller = new AbortController();
  let rejectMetadata!: (error: Error) => void;
  vi.mocked(isolated.metadata).mockImplementation(
    () =>
      new Promise((_, reject) => {
        rejectMetadata = reject;
      }),
  );
  vi.mocked(isolated.close).mockImplementation(() => {
    rejectMetadata(new Error("closed"));
    return Promise.resolve();
  });
  const port = new PlatformaticAdminPort(shared, () => isolated);
  const reading = port.observeTopicHealth("events", controller.signal);
  const stopped = expect(reading).rejects.toThrow();
  controller.abort();
  await stopped;
  expect(isolated.close).toHaveBeenCalledTimes(1);
  expect(shared.close).not.toHaveBeenCalled();
  expect(isolated.listOffsets).not.toHaveBeenCalled();
});
