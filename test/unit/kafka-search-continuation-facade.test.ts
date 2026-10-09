import { describe, expect, it, vi, type MockInstance } from "vitest";

import {
  InMemoryKafkaOperationalPreferenceStore,
  KafkaOperationalPreferenceService,
} from "../../src/features/kafka/application";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommandResponse,
  type HostEvent,
  type KafkaReadCoverage,
} from "../../src/features/kafka/contracts";
import type { KafkaReadCheckpoint } from "../../src/features/kafka/application/read-checkpoint";
import {
  command,
  ControlledMessageStream,
  createFacade,
  fetchCommand,
  message,
  RecordingActiveConnection,
  RecordingConnectionPort,
} from "../support/kafka-backend-facade-fixture";

class CheckpointStream extends ControlledMessageStream {
  private accepted = 0;
  private finished = false;
  constructor(
    private readonly start = 0,
    private readonly endOffset = 4,
  ) {
    super();
  }
  acknowledge(): void {
    this.accepted += 1;
  }
  override end(): void {
    this.finished = true;
    super.end();
  }
  coverage(): KafkaReadCoverage {
    const next = this.start + this.accepted;
    return {
      reason: this.finished
        ? next === this.endOffset
          ? "range-complete"
          : "result-limit"
        : "reading",
      scannedRecords: this.accepted,
      scannedBytes: this.accepted * 20,
      matchedRecords: this.accepted,
      unavailableRecords: 0,
      partitions: [
        {
          partition: 0,
          startOffset: String(this.start),
          nextOffset: String(next),
          endOffset: String(this.endOffset),
        },
      ],
    };
  }
  checkpoint(): KafkaReadCheckpoint {
    return { clusterId: "cluster", topicId: "topic", partitionCount: 1, coverage: this.coverage() };
  }
}

type ConsumptionPayload = Extract<HostEvent, { event: "consumption.state" }>["payload"];

function setup(
  streams: CheckpointStream[],
  preferences?: KafkaOperationalPreferenceService,
): {
  facade: ReturnType<typeof createFacade>;
  open: MockInstance<RecordingActiveConnection["openMessageStream"]>;
  events: HostEvent[];
  last: () => ConsumptionPayload | undefined;
  resume: (id: string) => Promise<HostCommandResponse>;
} {
  const connection = new RecordingActiveConnection();
  for (const stream of streams)
    connection.messageStreamOperations.push(() => Promise.resolve(stream));
  const open = vi.spyOn(connection, "openMessageStream");
  const port = new RecordingConnectionPort();
  port.openOperations.push(() => Promise.resolve(connection));
  const facade = createFacade(port, undefined, undefined, undefined, preferences);
  const events: HostEvent[] = [];
  facade.subscribe((event) => events.push(event));
  const last = (): ConsumptionPayload | undefined =>
    events.filter((event) => event.event === "consumption.state").at(-1)?.payload;
  const resume = (id: string): Promise<HostCommandResponse> =>
    facade.execute({
      version: HOST_PROTOCOL_VERSION,
      id: crypto.randomUUID(),
      command: "messages.continue",
      payload: { continuationId: id },
    });
  return { facade, open, events, last, resume };
}

describe("search continuation through composed host", () => {
  it("does not resurrect an older token while a new read awaits preferences", async () => {
    const preferences = new KafkaOperationalPreferenceService(
      new InMemoryKafkaOperationalPreferenceStore({ durability: "session", state: "ready" }),
    );
    const first = new CheckpointStream();
    const { facade, last } = setup([first], preferences);
    const originalGet = preferences.get.bind(preferences);
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    try {
      await facade.execute(command("connection.connect", "connect"));
      await facade.execute(
        fetchCommand({ topic: "test", mode: "earliest", maxMessages: 2 }, "first"),
      );
      first.push(message("0"));
      await vi.waitFor(() => expect(first.coverage().matchedRecords).toBe(1));
      const get = vi
        .spyOn(preferences, "get")
        .mockImplementationOnce(originalGet)
        .mockImplementationOnce(async () => {
          await gate;
          throw new Error("preference read failed");
        });
      const next = facade.execute(
        fetchCommand({ topic: "test", mode: "earliest", maxMessages: 3 }, "next"),
      );
      await vi.waitFor(() => expect(get).toHaveBeenCalledTimes(2));
      first.end();
      await vi.waitFor(() => expect(last()?.state).toBe("complete"));
      expect(last()?.searchProgress?.continuation ?? null).toBeNull();
      release();
      expect(await next).toMatchObject({ ok: false });
      get.mockRestore();
    } finally {
      release();
      await facade.shutdown();
    }
  });
  it("does not retain a failed old read while a new read awaits preferences", async () => {
    const preferences = new KafkaOperationalPreferenceService(
      new InMemoryKafkaOperationalPreferenceStore({ durability: "session", state: "ready" }),
    );
    const first = new CheckpointStream();
    const second = new CheckpointStream();
    const third = new CheckpointStream(2);
    const { facade, last, open, resume } = setup([first, second, third], preferences);
    const originalGet = preferences.get.bind(preferences);
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    try {
      await facade.execute(command("connection.connect", "connect"));
      await facade.execute(
        fetchCommand({ topic: "test", mode: "earliest", maxMessages: 2 }, "first"),
      );
      first.push(message("0"));
      await vi.waitFor(() => expect(first.coverage().matchedRecords).toBe(1));
      const get = vi
        .spyOn(preferences, "get")
        .mockImplementationOnce(originalGet)
        .mockImplementationOnce(async () => {
          await gate;
          return originalGet();
        });
      const next = facade.execute(
        fetchCommand({ topic: "test", mode: "earliest", maxMessages: 2 }, "next"),
      );
      await vi.waitFor(() => expect(get).toHaveBeenCalledTimes(2));
      first.fail(new Error("old read failed"));
      await vi.waitFor(() => expect(last()?.state).toBe("failed"));
      release();
      expect(await next).toMatchObject({ ok: true });
      get.mockRestore();
      second.push(message("0"));
      second.push(message("1"));
      second.end();
      await vi.waitFor(() => expect(last()?.state).toBe("complete"));
      const token = last()!.searchProgress!.continuation!.id;
      expect(await resume(token)).toMatchObject({ ok: true });
      expect(open).toHaveBeenCalledTimes(3);
      const resumedCall: readonly unknown[] = open.mock.calls[2] ?? [];
      expect(resumedCall[2]).toMatchObject({
        coverage: { partitions: [{ nextOffset: "2", endOffset: "4" }] },
      });
    } finally {
      release();
      await facade.shutdown();
    }
  });

  it("flushes a page before issuing a token, resumes its exact positions and rejects concurrent replay", async () => {
    const first = new CheckpointStream();
    const second = new CheckpointStream(2);
    const { facade, open, events, last, resume } = setup([first, second]);
    try {
      await facade.execute(command("connection.connect", "connect"));
      expect(
        await facade.execute(
          fetchCommand({ topic: "test", mode: "earliest", maxMessages: 2 }, "start"),
        ),
      ).toMatchObject({ ok: true });
      first.push(message("0"));
      first.push(message("1"));
      first.end();
      await vi.waitFor(() => expect(last()?.state).toBe("complete"));
      const token = last()!.searchProgress!.continuation!.id;
      expect(last()).toMatchObject({
        searchProgress: { pass: 1, scannedRecords: 2, matchedRecords: 2 },
      });
      const terminalIndex = events.findIndex(
        (event) =>
          event.event === "consumption.state" &&
          event.payload.searchProgress?.continuation?.id === token,
      );
      expect(
        events
          .slice(0, terminalIndex)
          .filter((event) => event.event === "messages.batch")
          .flatMap((event) => event.payload.messages.map((record) => record.offset)),
      ).toEqual(["0", "1"]);
      const responses = await Promise.all([resume(token), resume(token)]);
      expect(responses.filter((response) => response.ok)).toHaveLength(1);
      expect(open).toHaveBeenCalledTimes(2);
      const resumedCall: readonly unknown[] = open.mock.calls[1] ?? [];
      expect(resumedCall[2]).toMatchObject({
        coverage: { partitions: [{ nextOffset: "2", endOffset: "4" }] },
      });
      second.push(message("2"));
      second.push(message("3"));
      second.end();
      await vi.waitFor(() => expect(last()?.searchProgress?.pass).toBe(2));
      await vi.waitFor(() => expect(last()?.state).toBe("complete"));
      expect(last()).toMatchObject({
        coverage: {
          scannedRecords: 2,
          partitions: [{ startOffset: "0", endOffset: "4", nextOffset: "4" }],
        },
        searchProgress: { pass: 2, scannedRecords: 4, matchedRecords: 4, continuation: null },
      });
      expect(await resume(token)).toMatchObject({ ok: false });
    } finally {
      await facade.shutdown();
    }
  });

  it("retains a usable delivered checkpoint after confirmed Stop", async () => {
    const first = new CheckpointStream();
    const { facade, last } = setup([first]);
    try {
      await facade.execute(command("connection.connect", "connect"));
      await facade.execute(
        fetchCommand({ topic: "test", mode: "earliest", maxMessages: 2 }, "start"),
      );
      first.push(message("0"));
      await vi.waitFor(() => expect(first.coverage().matchedRecords).toBe(1));
      expect(last()?.searchProgress?.continuation ?? null).toBeNull();
      expect(await facade.execute(command("messages.stop", "stop"))).toMatchObject({ ok: true });
      expect(last()).toMatchObject({
        state: "stopped",
        searchProgress: { matchedRecords: 1 },
      });
      expect(last()?.searchProgress?.continuation?.id).toEqual(expect.any(String));
    } finally {
      await facade.shutdown();
    }
  });

  it("withholds a token after cleanup failure and revokes it on disconnect", async () => {
    const first = new CheckpointStream();
    const { facade, last, resume } = setup([first]);
    try {
      await facade.execute(command("connection.connect", "connect"));
      await facade.execute(
        fetchCommand({ topic: "test", mode: "earliest", maxMessages: 2 }, "start"),
      );
      first.push(message("0"));
      first.end();
      await vi.waitFor(() => expect(last()?.state).toBe("complete"));
      const token = last()!.searchProgress!.continuation!.id;
      await facade.execute(command("connection.disconnect", "disconnect"));
      expect(await resume(token)).toMatchObject({ ok: false });
    } finally {
      await facade.shutdown();
    }
    const failed = new CheckpointStream();
    vi.spyOn(failed, "close").mockRejectedValue(new Error("cleanup failed"));
    const broken = setup([failed]);
    try {
      await broken.facade.execute(command("connection.connect", "connect"));
      await broken.facade.execute(
        fetchCommand({ topic: "test", mode: "earliest", maxMessages: 2 }, "start"),
      );
      failed.push(message("0"));
      failed.end();
      await vi.waitFor(() => expect(broken.last()?.state).toBe("failed"));
      expect(broken.last()?.searchProgress?.continuation ?? null).toBeNull();
    } finally {
      await broken.facade.shutdown().catch(() => undefined);
    }
  });
});
