import { Producer } from "@platformatic/kafka";
import { expect } from "vitest";

import { ConnectOffsetsService } from "../../src/features/kafka/application/connect-offset-service";
import type { ConnectService } from "../../src/features/kafka/application/connect-service";
import type { KafkaApplicationSession } from "../../src/features/kafka/application";
import type { ConnectHttpAdapter } from "../../src/features/kafka/engine/connect-http";
import type {
  ConnectOffsetsSnapshot,
  ConnectOffsetsInput,
} from "../../src/features/kafka/contracts/connect-offsets";

export async function qualifyConnectOffsets(input: {
  readonly session: KafkaApplicationSession;
  readonly port: ConnectHttpAdapter;
  readonly lifecycle: ConnectService;
  readonly brokers: readonly string[];
  readonly worker: { readonly url: string; writeSource(lines: readonly string[]): Promise<void> };
}): Promise<void> {
  const service = new ConnectOffsetsService(
    () => input.session.administrationScopes.connect(),
    input.port,
  );
  const lifecycle = async (
    name: string,
    action: "pause" | "stop" | "resume" | "delete",
  ): Promise<void> => {
    const plan = await input.lifecycle.review({ name, action, config: {} });
    expect(await input.lifecycle.apply(plan.planId, plan.confirmation)).toMatchObject({
      state: "acknowledged",
      cleanup: "confirmed",
    });
  };
  const actual = async (
    name: string,
  ): Promise<{
    offsets: readonly {
      partition: Record<string, string | number>;
      offset: Record<string, number> | null;
    }[];
  }> => {
    const response = await fetch(`${input.worker.url}/connectors/${name}/offsets`);
    expect(response.ok).toBe(true);
    return (await response.json()) as {
      offsets: readonly {
        partition: Record<string, string | number>;
        offset: Record<string, number> | null;
      }[];
    };
  };
  const change = async (
    snapshot: ConnectOffsetsSnapshot,
    action: ConnectOffsetsInput["action"],
    position: number | null,
  ): Promise<void> => {
    const plan = await service.review({
      snapshotId: snapshot.snapshotId!,
      action,
      partitionRef: action === "reset" ? null : snapshot.positions[0]!.partitionRef,
      position,
    });
    expect(JSON.stringify(plan)).not.toContain("/source-data/");
    const result = await service.apply(plan.planId, plan.confirmation);
    expect(result).toMatchObject({
      state: "acknowledged",
      dispatch: "attempted",
      verification: "verified",
      cleanup: "confirmed",
    });
    expect(await service.apply(plan.planId, plan.confirmation)).toEqual(result);
  };
  const producer = new Producer({
    bootstrapBrokers: [...input.brokers],
    clientId: "connect-offset-fixture",
  });
  try {
    await producer.send({
      messages: [
        { topic: "connect-input", value: Buffer.from("offset-one") },
        { topic: "connect-input", value: Buffer.from("offset-two") },
      ],
    });
  } finally {
    await producer.close();
  }
  await expect
    .poll(async () => (await actual("fixture-sink")).offsets[0]?.offset?.kafka_offset, {
      timeout: 30_000,
    })
    .toBe(2);
  await lifecycle("fixture-sink", "pause");
  await expect
    .poll(async () => (await input.lifecycle.load("fixture-sink")).state, { timeout: 15_000 })
    .toBe("PAUSED");
  const paused = await service.inspect("fixture-sink");
  expect(paused.status).toBe("available");
  await expect(
    service.review({
      snapshotId: paused.snapshotId!,
      action: "set",
      partitionRef: paused.positions[0]!.partitionRef,
      position: 1,
    }),
  ).rejects.toThrow();
  await lifecycle("fixture-sink", "stop");
  await expect
    .poll(async () => (await input.lifecycle.load("fixture-sink")).state, { timeout: 15_000 })
    .toBe("STOPPED");
  expect((await input.lifecycle.load("fixture-sink")).tasks).toEqual([]);
  const sink = await service.inspect("fixture-sink");
  expect(sink).toMatchObject({
    status: "available",
    mapping: "kafka-sink",
    connectorState: "STOPPED",
  });
  await change(sink, "set", 1);
  expect((await actual("fixture-sink")).offsets[0]?.offset?.kafka_offset).toBe(1);
  await change(await service.inspect("fixture-sink"), "remove", null);
  expect((await actual("fixture-sink")).offsets).toEqual([]);
  expect(await service.inspect("fixture-sink")).toMatchObject({
    status: "available",
    positions: [],
  });
  // Recreate a positive observed baseline before testing reset; empty is not zero.
  await lifecycle("fixture-sink", "resume");
  await expect
    .poll(async () => (await actual("fixture-sink")).offsets[0]?.offset?.kafka_offset, {
      timeout: 30_000,
    })
    .toBe(2);
  await lifecycle("fixture-sink", "stop");
  await expect
    .poll(async () => (await input.lifecycle.load("fixture-sink")).state, { timeout: 15_000 })
    .toBe("STOPPED");
  await change(await service.inspect("fixture-sink"), "reset", null);
  expect((await actual("fixture-sink")).offsets).toEqual([]);

  await input.worker.writeSource(["first", "second"]);
  const sourcePlan = await input.lifecycle.review({
    name: "fixture-source",
    action: "create",
    config: {
      "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
      "tasks.max": "1",
      topic: "connect-source",
      file: "/source-data/source.txt",
    },
  });
  expect(await input.lifecycle.apply(sourcePlan.planId, sourcePlan.confirmation)).toMatchObject({
    state: "acknowledged",
    cleanup: "confirmed",
  });
  await expect
    .poll(async () => (await actual("fixture-source")).offsets[0]?.offset?.position, {
      timeout: 30_000,
    })
    .toBe(13);
  await lifecycle("fixture-source", "stop");
  await expect
    .poll(async () => (await input.lifecycle.load("fixture-source")).state, { timeout: 15_000 })
    .toBe("STOPPED");
  const source = await service.inspect("fixture-source");
  expect(source).toMatchObject({
    status: "available",
    mapping: "file-source",
    positions: [{ label: "Source partition 1", position: 13 }],
  });
  expect(JSON.stringify(source)).not.toContain("/source-data/");
  await change(source, "set", 6);
  expect((await actual("fixture-source")).offsets[0]?.offset?.position).toBe(6);
  await change(await service.inspect("fixture-source"), "remove", null);
  expect((await actual("fixture-source")).offsets).toEqual([]);
  await lifecycle("fixture-source", "resume");
  await expect
    .poll(async () => (await actual("fixture-source")).offsets[0]?.offset?.position, {
      timeout: 30_000,
    })
    .toBe(13);
  await lifecycle("fixture-source", "stop");
  await expect
    .poll(async () => (await input.lifecycle.load("fixture-source")).state, { timeout: 15_000 })
    .toBe("STOPPED");
  await change(await service.inspect("fixture-source"), "reset", null);
  expect((await actual("fixture-source")).offsets).toEqual([]);
  await lifecycle("fixture-source", "delete");
  expect(await service.inspect("fixture-source")).toMatchObject({
    status: "missing",
    positions: [],
  });
  await lifecycle("fixture-sink", "resume");
  await expect
    .poll(async () => (await actual("fixture-sink")).offsets[0]?.offset?.kafka_offset, {
      timeout: 30_000,
    })
    .toBe(2);
}
