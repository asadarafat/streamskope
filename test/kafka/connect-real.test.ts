import { randomUUID } from "node:crypto";
import { request } from "node:http";

import { expect, it } from "vitest";
import { Producer } from "@platformatic/kafka";

import { RecordReplayService } from "../../src/features/kafka/application/record-replay-service";
import { connectDlqContext } from "../../src/features/kafka/contracts/connect-dlq";
import {
  UNCHANGED_REPLAY_TRANSFORM,
  replayConfirmation,
} from "../../src/features/kafka/contracts/record-replay";
import type { KafkaMessage } from "../../src/features/kafka/contracts";
import { ConnectService } from "../../src/features/kafka/application/connect-service";
import { KafkaApplicationSession } from "../../src/features/kafka/application";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import { ConnectHttpAdapter } from "../../src/features/kafka/engine/connect-http";
import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";
import type { ConnectInput } from "../../src/features/kafka/contracts/connect";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";
import { startConnectFixture } from "../support/connect-fixture";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";

it("manages a real Apache Connect sink through authenticated TLS, validates without writing, recovers failed tasks and reconciles deletion", async () => {
  const privateValue = `connect-private-${randomUUID()}`;
  const kafka = await startAuthorizationFixture();
  const worker = await startConnectFixture(kafka.connection.brokers[0]!).catch(
    async (error: unknown) => {
      await kafka.dispose();
      throw error;
    },
  );
  let deny = false,
    heldRequests = 0;
  let heldResponse: { admitted(): void; allow: Promise<void> } | undefined;
  const tls = await createHttpsTrustFixture((incoming, outgoing) => {
    if (
      incoming.headers.authorization !== "Bearer fixture-token" ||
      (deny && incoming.method !== "GET")
    ) {
      outgoing.writeHead(403);
      outgoing.end("{}");
      return;
    }
    const forwarded = request(
      `${worker.url}${incoming.url ?? "/"}`,
      { method: incoming.method, headers: { "content-type": "application/json" } },
      (response) => {
        const holding =
          incoming.method === "PUT" && incoming.url?.endsWith("/pause") ? heldResponse : undefined;
        const reply = (): void => {
          if (outgoing.destroyed) {
            response.destroy();
            return;
          }
          outgoing.writeHead(response.statusCode ?? 502, { "content-type": "application/json" });
          response.pipe(outgoing);
        };
        if (holding) {
          heldResponse = undefined;
          heldRequests += 1;
          holding.admitted();
          void holding.allow.then(reply);
        } else reply();
      },
    );
    forwarded.on("error", () => {
      outgoing.writeHead(502);
      outgoing.end("{}");
    });
    incoming.pipe(forwarded);
  });
  const port = new ConnectHttpAdapter(new NodeBoundedJsonHttp({ timeoutMs: 15000 }));
  const session = new KafkaApplicationSession(new StreamSkopeKafkaEngine());
  const c = {
    baseUrl: tls.origin,
    caPem: tls.caPem,
    authorization: (): Promise<string> => Promise.resolve("Bearer fixture-token"),
  };
  try {
    await kafka.admin.createTopics({ topics: ["connect-input"], partitions: 1, replicas: 1 });
    await session.connect({
      ...kafka.connection,
      services: {
        connect: {
          baseUrl: tls.origin,
          authentication: "bearer",
          bearer: "fixture-token",
          tls: { caPem: tls.caPem },
        },
      },
    });
    const owner = session.writeContext()!;
    const service = new ConnectService(() => session.administrationScopes.connect(), port);
    expect((await service.list()).plugins).toContain(
      "org.apache.kafka.connect.file.FileStreamSinkConnector",
    );
    const create: ConnectInput = {
      name: "fixture-sink",
      action: "create",
      config: {
        "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
        "tasks.max": "1",
        topics: "connect-input",
        file: "/tmp/streamskope-sink.txt",
        "fixture.protected.value": privateValue,
        "fixture.remove.value": "retained-until-explicit-remove",
      },
    };
    expect((await service.validate(create)).issues).toEqual([]);
    expect((await service.list()).names).not.toContain(create.name);
    const apply = async (input: ConnectInput): Promise<string> => {
      const p = await service.review(input);
      return (await service.apply(p.planId, p.confirmation)).state;
    };
    expect(await apply(create)).toBe("acknowledged");
    await expect
      .poll(async () => (await service.load(create.name)).tasks[0]?.state, { timeout: 45000 })
      .toBe("RUNNING");
    const delta: ConnectInput = {
      name: create.name,
      action: "update",
      config: { "tasks.max": "2" },
      remove: ["fixture.remove.value"],
    };
    const p = await service.review(delta);
    expect(p).toMatchObject({
      fields: ["tasks.max"],
      removedFields: ["fixture.remove.value"],
      connectionName: owner.connectionName,
    });
    expect(JSON.stringify(p)).not.toContain(privateValue);
    const independentConfig = async (): Promise<Record<string, string>> => {
      const response = await fetch(`${worker.url}/connectors/${create.name}/config`);
      expect(response.ok).toBe(true);
      return (await response.json()) as Record<string, string>;
    };
    expect((await independentConfig())["fixture.remove.value"]).toBe(
      "retained-until-explicit-remove",
    );
    const result = await service.apply(p.planId, p.confirmation);
    expect(result.state).toBe("acknowledged");
    expect(result.cleanup).toBe("confirmed");
    expect(result.dispatch).toBe("attempted");
    expect(await service.apply(p.planId, p.confirmation)).toEqual(result);
    await expect
      .poll(
        async () => ({
          removed: Object.hasOwn(await independentConfig(), "fixture.remove.value"),
          tasks: (await independentConfig())["tasks.max"],
        }),
        { timeout: 15000 },
      )
      .toEqual({ removed: false, tasks: "2" });
    expect((await independentConfig())["fixture.protected.value"]).toBe(privateValue);
    expect(JSON.stringify(await service.load(create.name))).not.toContain(privateValue);
    // The full omitted secret remains host-only even when validation and readback run.
    expect(await apply({ ...create, action: "pause", config: {} })).toBe("acknowledged");
    await expect
      .poll(async () => (await service.load(create.name)).state, { timeout: 15000 })
      .toBe("PAUSED");
    expect(await apply({ ...create, action: "resume", config: {} })).toBe("acknowledged");
    await expect
      .poll(async () => (await service.load(create.name)).tasks[0]?.state, { timeout: 15000 })
      .toBe("RUNNING");
    expect(
      await apply({ ...create, action: "update", config: { file: "/missing-parent/file.txt" } }),
    ).toBe("acknowledged");
    await expect
      .poll(async () => (await service.load(create.name)).tasks[0]?.state, { timeout: 15000 })
      .toBe("FAILED");
    expect(await apply({ ...create, action: "restart-failed", config: {} })).toBe("acknowledged");
    await expect
      .poll(async () => (await service.load(create.name)).tasks[0]?.state, { timeout: 15000 })
      .toBe("FAILED");
    expect(
      await apply({ ...create, action: "update", config: { file: "/tmp/recovered.txt" } }),
    ).toBe("acknowledged");
    await expect
      .poll(async () => (await service.load(create.name)).tasks[0]?.state, { timeout: 15000 })
      .toBe("RUNNING");
    await kafka.admin.createTopics({
      topics: ["connect-dlq", "connect-replayed"],
      partitions: 1,
      replicas: 1,
    });
    expect(
      await apply({
        ...create,
        action: "update",
        config: {
          "value.converter": "org.apache.kafka.connect.json.JsonConverter",
          "value.converter.schemas.enable": "false",
          "errors.tolerance": "all",
          "errors.deadletterqueue.topic.name": "connect-dlq",
          "errors.deadletterqueue.topic.replication.factor": "1",
          "errors.deadletterqueue.context.headers.enable": "true",
        },
      }),
    ).toBe("acknowledged");
    await expect
      .poll(async () => (await service.load(create.name)).tasks[0]?.state, { timeout: 15000 })
      .toBe("RUNNING");
    const producer = new Producer({
      bootstrapBrokers: [...kafka.connection.brokers],
      clientId: "connect-fixture",
    });
    try {
      await producer.send({
        messages: [
          {
            topic: "connect-input",
            key: Buffer.from("original-key"),
            value: Buffer.from("not-json"),
          },
        ],
      });
    } finally {
      await producer.close();
    }
    const read = async (topic: string): Promise<KafkaMessage | undefined> => {
      const stream = await owner.connection.openMessageStream(
        { topic, mode: "earliest", maxMessages: 1 },
        AbortSignal.timeout(10000),
      );
      try {
        for await (const message of stream) return message;
      } finally {
        await stream.close();
      }
      return undefined;
    };
    let deadLetter: KafkaMessage | undefined;
    await expect
      .poll(
        async () => {
          deadLetter = await read("connect-dlq");
          return deadLetter;
        },
        { timeout: 30000 },
      )
      .toBeDefined();
    expect(connectDlqContext(deadLetter!)).toMatchObject({
      topic: "connect-input",
      connector: create.name,
    });
    if (deadLetter!.original?.state !== "complete") throw new Error("Missing DLQ bytes.");
    const replay = new RecordReplayService(() => session.reviewedWriteScope());
    try {
      const plan = await replay.review({
        targetProfile: null,
        topic: "connect-replayed",
        partition: 0,
        ratePerSecond: 1,
        records: [
          {
            topic: deadLetter!.topic,
            partition: deadLetter!.partition,
            offset: deadLetter!.offset,
            timestampMs: null,
            original: deadLetter!.original,
          },
        ],
        transform: UNCHANGED_REPLAY_TRANSFORM,
      });
      const result = await replay.apply(plan.planId, replayConfirmation(plan));
      expect(result.outcomes[0]?.state).toBe("acknowledged");
      expect((await read("connect-replayed"))?.original).toEqual(deadLetter!.original);
    } finally {
      await replay.invalidate();
    }
    deny = true;
    expect(await apply({ ...create, action: "pause", config: {} })).toBe("rejected");
    deny = false;
    const interrupted = await service.review({ ...create, action: "pause", config: {} });
    let admit!: () => void, release!: () => void;
    const admitted = new Promise<void>((resolve) => {
      admit = resolve;
    });
    heldResponse = {
      admitted: admit,
      allow: new Promise<void>((resolve) => {
        release = resolve;
      }),
    };
    const pending = service.apply(interrupted.planId, interrupted.confirmation);
    try {
      await admitted;
      await session.disconnect();
      const receipt = await pending;
      expect(receipt).toMatchObject({
        state: "unknown",
        dispatch: "attempted",
        verification: "not-applicable",
        cleanup: "confirmed",
      });
      expect(await service.apply(interrupted.planId, interrupted.confirmation)).toEqual(receipt);
      expect(heldRequests).toBe(1);
    } finally {
      release();
    }
    await session.connect({
      ...kafka.connection,
      services: {
        connect: {
          baseUrl: tls.origin,
          authentication: "bearer",
          bearer: "fixture-token",
          tls: { caPem: tls.caPem },
        },
      },
    });
    await expect
      .poll(async () => (await service.load(create.name)).state, { timeout: 15000 })
      .toBe("PAUSED");
    expect(await apply({ ...create, action: "delete", config: {} })).toBe("acknowledged");
    await expect
      .poll(async () => (await service.list()).names, { timeout: 15000 })
      .not.toContain(create.name);
    await expect(
      port.list({ ...c, caPem: "invalid" }, AbortSignal.timeout(2000)),
    ).rejects.toThrow();
  } finally {
    const cleanup = await Promise.allSettled([
      session.disconnect(),
      tls.close(),
      worker.dispose(),
      kafka.dispose(),
    ]);
    expect(cleanup.every((result) => result.status === "fulfilled")).toBe(true);
  }
}, 240000);
