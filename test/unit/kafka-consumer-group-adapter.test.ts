import type {
  ClusterMetadata,
  ConfigDescription,
  Group,
  GroupBase,
  ListConsumerGroupOffsetsGroup,
  ListedOffsetsTopic,
} from "@platformatic/kafka";
import { describe, expect, it } from "vitest";

import { PlatformaticAdminPort } from "../../src/kafka/engine/platformatic-admin";

class RecordingConsumerGroupAdmin {
  readonly calls: Array<readonly [string, unknown]> = [];
  groups = new Map<string, GroupBase>();
  descriptions = new Map<string, Group>();
  committed: ListConsumerGroupOffsetsGroup[] = [];
  latest: ListedOffsetsTopic[] = [];

  close(): Promise<void> {
    return Promise.resolve();
  }

  describeConfigs(): Promise<ConfigDescription[]> {
    return Promise.resolve([]);
  }

  describeGroups(options: unknown): Promise<Map<string, Group>> {
    this.calls.push(["describeGroups", options]);
    return Promise.resolve(this.descriptions);
  }

  incrementalAlterConfigs(): Promise<void> {
    return Promise.resolve();
  }

  listConsumerGroupOffsets(options: unknown): Promise<ListConsumerGroupOffsetsGroup[]> {
    this.calls.push(["listConsumerGroupOffsets", options]);
    return Promise.resolve(this.committed);
  }

  listGroups(options?: unknown): Promise<Map<string, GroupBase>> {
    this.calls.push(["listGroups", options]);
    return Promise.resolve(this.groups);
  }

  listOffsets(options: unknown): Promise<ListedOffsetsTopic[]> {
    this.calls.push(["listOffsets", options]);
    return Promise.resolve(this.latest);
  }

  listTopics(): Promise<readonly string[]> {
    return Promise.resolve([]);
  }

  metadata(): Promise<ClusterMetadata> {
    return Promise.resolve({
      brokers: new Map(),
      controllerId: -1,
      id: "",
      lastUpdate: 0,
      topics: new Map(),
    });
  }
}

describe("Platformatic Kafka consumer-group adapter", () => {
  it("returns a stable alphabetical bounded inventory", async () => {
    const admin = new RecordingConsumerGroupAdmin();
    admin.groups = new Map([
      [
        "zeta-worker",
        { groupType: "classic", id: "zeta-worker", protocolType: "consumer", state: "EMPTY" },
      ],
      [
        "alpha-worker",
        { groupType: "classic", id: "alpha-worker", protocolType: "consumer", state: "STABLE" },
      ],
    ]);
    const port = new PlatformaticAdminPort(admin);

    await expect(port.listConsumerGroups()).resolves.toEqual({
      groups: [
        {
          groupType: "classic",
          id: "alpha-worker",
          protocolType: "consumer",
          state: "stable",
        },
        {
          groupType: "classic",
          id: "zeta-worker",
          protocolType: "consumer",
          state: "empty",
        },
      ],
      omittedGroups: 0,
    });
    expect(admin.calls).toEqual([["listGroups", undefined]]);
  });

  it("reports omitted inventory instead of retaining an unbounded group list", async () => {
    const admin = new RecordingConsumerGroupAdmin();
    admin.groups = new Map(
      Array.from({ length: 502 }, (_, index) => {
        const id = `worker-${String(index).padStart(3, "0")}`;
        return [
          id,
          { groupType: "classic", id, protocolType: "consumer", state: "EMPTY" },
        ] as const;
      }),
    );
    const port = new PlatformaticAdminPort(admin);

    const result = await port.listConsumerGroups();

    expect(result.groups).toHaveLength(500);
    expect(result.groups.at(0)?.id).toBe("worker-000");
    expect(result.groups.at(-1)?.id).toBe("worker-499");
    expect(result.omittedGroups).toBe(2);
  });

  it("describes members, sorted assignments, precise offsets and non-negative lag", async () => {
    const admin = new RecordingConsumerGroupAdmin();
    admin.descriptions = new Map([
      [
        "orders-worker",
        {
          authorizedOperations: 0,
          id: "orders-worker",
          members: new Map([
            [
              "member-1",
              {
                assignments: new Map([
                  ["payments.events", { partitions: [2], topic: "payments.events" }],
                  ["orders.events", { partitions: [1, 0], topic: "orders.events" }],
                ]),
                clientHost: "/127.0.0.1",
                clientId: "orders-client",
                groupInstanceId: null,
                id: "member-1",
              },
            ],
          ]),
          protocol: "range",
          protocolType: "consumer",
          state: "STABLE",
        },
      ],
    ]);
    admin.committed = [
      {
        groupId: "orders-worker",
        topics: [
          {
            name: "orders.events",
            partitions: [
              {
                committedLeaderEpoch: 1,
                committedOffset: 9_007_199_254_740_993n,
                metadata: null,
                partitionIndex: 0,
              },
              {
                committedLeaderEpoch: 1,
                committedOffset: 12n,
                metadata: null,
                partitionIndex: 1,
              },
            ],
          },
        ],
      },
    ];
    admin.latest = [
      {
        name: "orders.events",
        partitions: [
          {
            leaderEpoch: 1,
            offset: 9_007_199_254_741_003n,
            partitionIndex: 0,
            timestamp: -1n,
          },
          { leaderEpoch: 1, offset: 10n, partitionIndex: 1, timestamp: -1n },
        ],
      },
    ];
    const port = new PlatformaticAdminPort(admin);

    await expect(port.describeConsumerGroup("orders-worker")).resolves.toEqual({
      id: "orders-worker",
      members: [
        {
          assignments: [
            { partitions: [0, 1], topic: "orders.events" },
            { partitions: [2], topic: "payments.events" },
          ],
          clientHost: "/127.0.0.1",
          clientId: "orders-client",
          groupInstanceId: null,
          id: "member-1",
        },
      ],
      offsets: [
        {
          committedOffset: "9007199254740993",
          endOffset: "9007199254741003",
          lag: "10",
          partition: 0,
          topic: "orders.events",
        },
        {
          committedOffset: "12",
          endOffset: "10",
          lag: "0",
          partition: 1,
          topic: "orders.events",
        },
      ],
      omittedAssignments: 0,
      omittedMembers: 0,
      omittedOffsets: 0,
      protocol: "range",
      protocolType: "consumer",
      state: "stable",
    });
    expect(admin.calls).toEqual([
      ["describeGroups", { groups: ["orders-worker"], includeAuthorizedOperations: false }],
      ["listConsumerGroupOffsets", { groups: ["orders-worker"], requireStable: false }],
      [
        "listOffsets",
        {
          topics: [
            {
              name: "orders.events",
              partitions: [
                { partitionIndex: 0, timestamp: -1n },
                { partitionIndex: 1, timestamp: -1n },
              ],
            },
          ],
        },
      ],
    ]);
  });

  it("uses unavailable offset and lag evidence for an uncommitted partition", async () => {
    const admin = new RecordingConsumerGroupAdmin();
    admin.descriptions.set("orders-worker", {
      authorizedOperations: 0,
      id: "orders-worker",
      members: new Map(),
      protocol: "",
      protocolType: "consumer",
      state: "EMPTY",
    });
    admin.committed = [
      {
        groupId: "orders-worker",
        topics: [
          {
            name: "orders.events",
            partitions: [
              {
                committedLeaderEpoch: -1,
                committedOffset: -1n,
                metadata: null,
                partitionIndex: 0,
              },
            ],
          },
        ],
      },
    ];
    admin.latest = [
      {
        name: "orders.events",
        partitions: [{ leaderEpoch: 1, offset: 5n, partitionIndex: 0, timestamp: -1n }],
      },
    ];
    const port = new PlatformaticAdminPort(admin);

    const result = await port.describeConsumerGroup("orders-worker");

    expect(result.offsets).toEqual([
      {
        committedOffset: null,
        endOffset: "5",
        lag: null,
        partition: 0,
        topic: "orders.events",
      },
    ]);
  });

  it("rejects a selected group that is absent from broker description", async () => {
    const admin = new RecordingConsumerGroupAdmin();
    const port = new PlatformaticAdminPort(admin);

    await expect(port.describeConsumerGroup("missing-worker")).rejects.toThrow(
      "Kafka did not return consumer group missing-worker",
    );
    expect(admin.calls).toEqual([
      ["describeGroups", { groups: ["missing-worker"], includeAuthorizedOperations: false }],
      ["listConsumerGroupOffsets", { groups: ["missing-worker"], requireStable: false }],
    ]);
  });
});
