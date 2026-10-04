import { describe, expect, it } from "vitest";

import {
  compileKafkaSearchFilter,
  parseKafkaInvestigationQuery,
  parseKafkaSearchFilter,
  type KafkaExploredMessage,
} from "../../src/features/kafka/contracts";
import { resolveKafkaFetchPlan } from "../../src/features/kafka/engine/fetch-plan";
import { KafkaReadTracker } from "../../src/features/kafka/engine/read-coverage";
import {
  countActiveKafkaMessageFilters,
  initialKafkaMessageFilters,
  selectFilteredKafkaMessages,
  withKafkaMessageTextFilter,
} from "../../src/features/kafka/ui/message-operations";
import { investigationCommands } from "../../src/features/kafka/ui/workbench-query-commands";

const filter = {
  key: "",
  value: "",
  offset: "",
  offsetExact: "12",
  timestamp: "",
  partition: 1,
};

function message(offset: string, partition = 1): KafkaExploredMessage {
  return {
    id: `events:${partition}:${offset}`,
    topic: "events",
    offset,
    partition,
    timestamp: "1970-01-01T00:00:01.000Z",
    key: null,
    payload: null,
    preview: "",
    originalByteSize: 0,
    headers: {},
    truncated: false,
    ruleEvaluation: {
      activeMatchCount: 0,
      activeMatches: [],
      durationMicros: 0,
      errorCount: 0,
      errors: [],
      evaluatedRules: 0,
      omittedEvidence: 0,
      omittedRules: 0,
      state: "evaluated",
      suppressedMatchCount: 0,
      suppressedMatches: [],
    },
  };
}

describe("exact sampled-record investigation", () => {
  it("matches one partition/offset without substring collisions or payload access", () => {
    const predicate = compileKafkaSearchFilter(filter);
    expect(predicate(message("12"))).toBe("matched");
    expect(predicate(message("112"))).toBe("not-matched");
    expect(predicate(message("12", 0))).toBe("not-matched");
    const filters = { ...initialKafkaMessageFilters, ...filter };
    expect(countActiveKafkaMessageFilters(filters)).toBe(2);
    expect(
      selectFilteredKafkaMessages([message("12"), message("112"), message("12", 0)], filters).map(
        (record) => record.id,
      ),
    ).toEqual(["events:1:12"]);
  });

  it("preserves the exact locator when serializing an investigation query", () => {
    const query = {
      schemaVersion: 1,
      filters: filter,
      request: {
        mode: "time-window",
        topic: "events",
        maxMessages: 1,
        startTimeMs: 0,
        endTimeMs: 2000,
        search: filter,
      },
    };
    expect(parseKafkaInvestigationQuery(JSON.parse(JSON.stringify(query)))).toEqual(query);
  });

  it.each(["", "01", "-1", "12.0", "12x", "9223372036854775808", "1".repeat(21)])(
    "rejects invalid locator %j before any read",
    (offsetExact) => {
      expect(() => parseKafkaSearchFilter({ ...filter, offsetExact }, "search")).toThrow();
    },
  );

  it("keeps ordinary offset substring behavior and clears a locator when editing that filter", () => {
    const ordinary = { ...initialKafkaMessageFilters, offset: "12" };
    expect(compileKafkaSearchFilter(ordinary)(message("112"))).toBe("matched");
    const edited = withKafkaMessageTextFilter(
      { ...initialKafkaMessageFilters, ...filter },
      "offset",
      "",
    );
    expect(edited).not.toHaveProperty("offsetExact");
    expect(countActiveKafkaMessageFilters(edited)).toBe(1);
  });

  it("preserves the locator for command-palette reads and searches", () => {
    const requests: unknown[] = [];
    const commands = investigationCommands({
      connected: true,
      selectedTopic: "events",
      selectedProfile: null,
      profileBusy: false,
      readActive: false,
      stopping: false,
      mode: "time-window",
      timeError: undefined,
      filters: { ...initialKafkaMessageFilters, ...filter },
      openQueries: () => undefined,
      toggleProfile: () => Promise.resolve(),
      startRead: (_topic, search) => {
        requests.push(search);
        return Promise.resolve();
      },
      stopRead: () => Promise.resolve(),
    });
    commands.find((command) => command.id === "read")!.run();
    commands.find((command) => command.id === "search")!.run();
    expect(requests).toEqual([filter, filter]);
  });

  it("seeks the exact position in one partition instead of scanning preceding records", async () => {
    const request = {
      mode: "time-window" as const,
      topic: "events",
      maxMessages: 1,
      startTimeMs: 0,
      endTimeMs: 2000,
      search: filter,
    };
    const plan = await resolveKafkaFetchPlan(
      {
        listTopicOffsets: (_topic, timestamp) =>
          Promise.resolve(
            timestamp === -1n || timestamp === 2000n ? [100_000n, 100_000n] : [0n, 0n],
          ),
      },
      request,
      2000,
    );
    expect([...plan.startOffsets]).toEqual([[1, 12n]]);
    expect([...plan.endOffsets!]).toEqual([[1, 13n]]);
    const tracker = new KafkaReadTracker(plan);
    expect(
      tracker.accept({
        topic: "events",
        offset: 12n,
        partition: 0,
        timestamp: 1000n,
        headers: new Map(),
      }),
    ).toBe(false);
    expect(
      tracker.accept({
        topic: "events",
        offset: 12n,
        partition: 1,
        timestamp: 1000n,
        headers: new Map(),
      }),
    ).toBe(true);
    expect(tracker.snapshot()).toMatchObject({ scannedRecords: 1, matchedRecords: 1 });
  });

  it.each(["2", "100"])(
    "returns an empty range for unavailable locator %s",
    async (offsetExact) => {
      const plan = await resolveKafkaFetchPlan(
        {
          listTopicOffsets: (_topic, timestamp) =>
            Promise.resolve(timestamp === -2n ? [5n, 5n] : [30n, 30n]),
        },
        { mode: "earliest", topic: "events", maxMessages: 1, search: { ...filter, offsetExact } },
        2000,
      );
      const tracker = new KafkaReadTracker(plan);
      expect(tracker.finished).toBe(true);
      expect(tracker.snapshot()).toMatchObject({
        reason: "range-complete",
        scannedRecords: 0,
        matchedRecords: 0,
      });
    },
  );
});
