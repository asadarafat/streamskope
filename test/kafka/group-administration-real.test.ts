import { randomUUID } from "node:crypto";

import {
  Producer,
  Consumer,
  AclOperations,
  AclPermissionTypes,
  ResourceTypes,
  ResourcePatternTypes,
} from "@platformatic/kafka";
import { expect, it, vi } from "vitest";

import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
} from "../../src/features/kafka/contracts";
import { OffsetResetService } from "../../src/features/kafka/application/offset-reset-service";
import { KafkaApplicationSession } from "../../src/features/kafka/application";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";

it("reviews real earliest/end/time offsets with protected examples, rejects stale/active/denied groups and deletes an inactive group exactly once", async () => {
  const fixture = await startAuthorizationFixture(),
    backend = createKafkaBackend();
  const topic = `groups-${randomUUID()}`,
    groupId = `group-${randomUUID()}`;
  const producer = new Producer({
    bootstrapBrokers: [...fixture.connection.brokers],
    clientId: "group-review-seed",
    retries: 0,
    idempotent: true,
  });
  const active = new Consumer({
    bootstrapBrokers: [...fixture.connection.brokers],
    clientId: "group-review-active",
    groupId,
    autocommit: false,
    autocreateTopics: false,
    retries: 0,
  });
  let stream: Awaited<ReturnType<Consumer["consume"]>> | undefined;
  const failures: unknown[] = [];
  const execute = async (
    command: string,
    payload: unknown,
  ): Promise<ReturnType<typeof parseHostCommandResponse>> =>
    parseHostCommandResponse(
      await backend.execute(
        parseHostCommand({ command, payload, id: randomUUID(), version: HOST_PROTOCOL_VERSION }),
      ),
    );
  const commit = async (offset: bigint): Promise<void> => {
    await fixture.admin.alterConsumerGroupOffsets({
      groupId,
      topics: [
        { name: topic, partitionOffsets: [0, 1, 2].map((partition) => ({ partition, offset })) },
      ],
    });
  };
  const positions = async (): Promise<string[]> => {
    const offsets = await fixture.admin.listConsumerGroupOffsets({
      groups: [groupId],
      requireStable: false,
    });
    return offsets
      .find((g) => g.groupId === groupId)!
      .topics.find((t) => t.name === topic)!
      .partitions.sort((a, b) => a.partitionIndex - b.partitionIndex)
      .map((p) => p.committedOffset.toString());
  };
  const review = async (
    kind: "earliest" | "latest" | "timestamp",
    timestampMs?: string,
  ): Promise<
    Extract<
      ReturnType<typeof parseHostCommandResponse>,
      { ok: true; command: "consumerGroups.reset.review" }
    >
  > => {
    const r = await execute("consumerGroups.reset.review", {
      groupId,
      partitions: [0, 1, 2].map((partition) => ({ topic, partition })),
      position: kind === "timestamp" ? { kind, timestampMs } : { kind },
    });
    expect(r.ok).toBe(true);
    if (!r.ok || r.command !== "consumerGroups.reset.review") throw new Error("No reset review");
    return r;
  };
  try {
    await fixture.admin.createTopics({ topics: [topic], partitions: 3, replicas: 1 });
    await expect
      .poll(
        async () => {
          try {
            const t = (
              await producer.metadata({
                topics: [topic],
                forceUpdate: true,
                autocreateTopics: false,
              })
            ).topics.get(topic);
            return (
              t?.partitions.length === 3 &&
              t.partitions.every((p) => p.leader >= 0 && p.isr.length === 1)
            );
          } catch {
            return false;
          }
        },
        { timeout: 10000 },
      )
      .toBe(true);
    const first = BigInt(Date.now() - 6000);
    for (const partition of [0, 1, 2])
      await producer.send({
        messages: [0, 1, 2].map((i) => ({
          topic,
          partition,
          timestamp: first + BigInt(i * 1000),
          key: Buffer.from("protected-key"),
          value: Buffer.from(JSON.stringify({ secret: "protected-value", visible: i })),
        })),
      });
    await expect
      .poll(
        async () => {
          try {
            await commit(2n);
            return true;
          } catch {
            return false;
          }
        },
        { timeout: 15000, interval: 250 },
      )
      .toBe(true);
    expect(
      await execute("preferences.update", {
        patch: {
          protection: { readOnly: true, maskKey: true, maskHeaders: [], valuePaths: ["/secret"] },
        },
      }),
    ).toMatchObject({ ok: true });
    expect(await execute("connection.connect", fixture.connection)).toMatchObject({ ok: true });
    const earliest = await review("earliest");
    expect(earliest.result.review.input.targets.map((t) => t.offset)).toEqual(["0", "0", "0"]);
    expect(earliest.result.review.examples).toHaveLength(3);
    expect(JSON.stringify(earliest.result.review.examples)).not.toContain("protected-key");
    expect(JSON.stringify(earliest.result.review.examples)).not.toContain("protected-value");
    expect(earliest.result.review.examples[0]?.value).toContain('"visible":0');
    expect(await positions()).toEqual(["2", "2", "2"]);
    expect(
      await execute("consumerGroups.reset.apply", {
        planId: earliest.result.review.planId,
        confirmation: groupId,
      }),
    ).toMatchObject({ ok: false, error: { code: "AUTHORIZATION_DENIED" } });
    expect((await review("latest")).result.review.input.targets.map((t) => t.offset)).toEqual([
      "3",
      "3",
      "3",
    ]);
    expect(
      (await review("timestamp", (first + 1000n).toString())).result.review.input.targets.map(
        (t) => t.offset,
      ),
    ).toEqual(["1", "1", "1"]);
    expect(
      await execute("consumerGroups.reset.review", {
        groupId,
        partitions: [{ topic, partition: 0 }],
        position: { kind: "timestamp", timestampMs: String(Date.now() + 60000) },
      }),
    ).toMatchObject({ ok: false });
    expect(await positions()).toEqual(["2", "2", "2"]);
    await execute("connection.disconnect", {});
    await execute("preferences.update", {
      patch: {
        protection: { readOnly: false, maskKey: true, maskHeaders: [], valuePaths: ["/secret"] },
      },
    });
    await execute("connection.connect", fixture.connection);
    const stale = await review("earliest");
    await commit(1n);
    expect(
      await execute("consumerGroups.reset.apply", {
        planId: stale.result.review.planId,
        confirmation: groupId,
      }),
    ).toMatchObject({
      ok: true,
      result: {
        outcome: { partitions: [{ state: "unsent" }, { state: "unsent" }, { state: "unsent" }] },
      },
    });
    const end = await review("latest");
    const applied = await execute("consumerGroups.reset.apply", {
      planId: end.result.review.planId,
      confirmation: groupId,
    });
    expect(applied).toMatchObject({
      ok: true,
      result: {
        outcome: {
          partitions: [
            { state: "acknowledged", verified: true, cleanup: "confirmed" },
            { state: "acknowledged", verified: true, cleanup: "confirmed" },
            { state: "acknowledged", verified: true, cleanup: "confirmed" },
          ],
        },
      },
    });
    expect(await positions()).toEqual(["3", "3", "3"]);
    const denied = {
      resourceType: ResourceTypes.GROUP,
      resourcePatternType: ResourcePatternTypes.LITERAL,
      resourceName: groupId,
      principal: "User:ANONYMOUS",
      host: "*",
      operation: AclOperations.DELETE,
      permissionType: AclPermissionTypes.DENY,
    };
    await fixture.admin.createAcls({ creations: [denied] });
    await expect
      .poll(async () => (await execute("consumerGroups.delete.review", { groupId })).ok, {
        timeout: 10000,
      })
      .toBe(false);
    await fixture.admin.deleteAcls({ filters: [denied] });
    stream = await active.consume({ topics: [topic], autocommit: false });
    // Demand from the real stream starts the consumer's group join.
    const iterator = stream[Symbol.asyncIterator]();
    const next = iterator.next().catch(() => ({ done: true as const, value: undefined }));
    await expect
      .poll(
        async () => {
          try {
            return (
              (await fixture.admin.describeGroups({ groups: [groupId] })).get(groupId)?.members
                .size ?? 0
            );
          } catch {
            return 0;
          }
        },
        { timeout: 15000 },
      )
      .toBe(1);
    expect(await execute("consumerGroups.delete.review", { groupId })).toMatchObject({ ok: false });
    stream.destroy();
    await active.close(true);
    await next;
    await expect
      .poll(
        async () => {
          try {
            return (await fixture.admin.describeGroups({ groups: [groupId] })).get(groupId)?.state;
          } catch {
            return "unavailable";
          }
        },
        { timeout: 15000 },
      )
      .toBe("Empty");
    const deletion = await execute("consumerGroups.delete.review", { groupId });
    expect(deletion.ok).toBe(true);
    if (!deletion.ok || deletion.command !== "consumerGroups.delete.review")
      throw new Error("No group deletion review");
    expect(
      await execute("consumerGroups.delete.apply", {
        planId: deletion.result.review.planId,
        confirmation: groupId,
      }),
    ).toMatchObject({ ok: false });
    const payload = {
      planId: deletion.result.review.planId,
      confirmation: deletion.result.review.confirmation,
    };
    const deleted = await execute("consumerGroups.delete.apply", payload);
    expect(deleted).toMatchObject({
      ok: true,
      result: {
        outcome: { state: "acknowledged", verification: "verified", cleanup: "confirmed" },
      },
    });
    await expect
      .poll(async () => (await fixture.admin.listGroups()).has(groupId), { timeout: 10000 })
      .toBe(false);
    const duplicate = await execute("consumerGroups.delete.apply", payload);
    if (
      !deleted.ok ||
      !duplicate.ok ||
      deleted.command !== "consumerGroups.delete.apply" ||
      duplicate.command !== "consumerGroups.delete.apply"
    )
      throw new Error("No actual group receipts");
    expect(duplicate.result.outcome).toEqual(deleted.result.outcome);
  } catch (error) {
    failures.push(error);
  } finally {
    stream?.destroy();
    try {
      await disposeNativeFixtureResources([
        (): Promise<void> => backend.shutdown(),
        (): Promise<void> => active.close(true),
        (): Promise<void> => producer.close(),
        (): Promise<void> => fixture.dispose(),
      ]);
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length)
    throw new AggregateError(failures, "Real group administration or owned cleanup failed", {
      cause: failures[0],
    });
}, 120000);

it("preserves the real first ACK, an independently lost second reply and the unchanged unsent suffix without resending a partial reset", async () => {
  const fixture = await startAuthorizationFixture(),
    session = new KafkaApplicationSession(new StreamSkopeKafkaEngine());
  const topic = `partial-${randomUUID()}`,
    groupId = `partial-${randomUUID()}`;
  const producer = new Producer({
    bootstrapBrokers: [...fixture.connection.brokers],
    clientId: "partial-reset-seed",
    retries: 0,
  });
  const failures: unknown[] = [];
  try {
    await fixture.admin.createTopics({ topics: [topic], partitions: 3, replicas: 1 });
    await producer.send({
      messages: [0, 1, 2].flatMap((partition) =>
        [0, 1].map((i) => ({ topic, partition, value: Buffer.from(String(i)) })),
      ),
    });
    await expect
      .poll(
        async () => {
          try {
            await fixture.admin.alterConsumerGroupOffsets({
              groupId,
              topics: [
                {
                  name: topic,
                  partitionOffsets: [0, 1, 2].map((partition) => ({ partition, offset: 2n })),
                },
              ],
            });
            return true;
          } catch {
            return false;
          }
        },
        { timeout: 15000, interval: 250 },
      )
      .toBe(true);
    await session.connect(fixture.connection);
    const connection = session.writeContext()!.connection,
      original = connection.resetGroupOffset!.bind(connection);
    let admitted = 0;
    const spy = vi
      .spyOn(connection, "resetGroupOffset")
      .mockImplementation(async (group, target, baseline) => {
        const actual = await original(group, target, baseline);
        if (++admitted === 2) {
          expect(actual.state).toBe("acknowledged");
          throw new Error("Independent response loss after the actual second acknowledgement");
        }
        return actual;
      });
    const service = new OffsetResetService(() => session.administrationScopes.offsetReset());
    const review = await service.review({
      groupId,
      partitions: [0, 1, 2].map((partition) => ({ topic, partition })),
      position: { kind: "earliest" },
    });
    const outcome = await service.apply(review.planId, groupId);
    expect(outcome.partitions.map((p) => p.state)).toEqual(["acknowledged", "unknown", "unsent"]);
    expect(await service.apply(review.planId, groupId)).toEqual(outcome);
    expect(spy).toHaveBeenCalledTimes(2);
    const offsets = await fixture.admin.listConsumerGroupOffsets({
      groups: [groupId],
      requireStable: false,
    });
    expect(
      offsets[0]!.topics[0]!.partitions.sort((a, b) => a.partitionIndex - b.partitionIndex).map(
        (p) => p.committedOffset.toString(),
      ),
    ).toEqual(["0", "0", "2"]);
    spy.mockRestore();
  } catch (error) {
    failures.push(error);
  } finally {
    try {
      await disposeNativeFixtureResources([
        (): Promise<void> => session.shutdown(),
        (): Promise<void> => producer.close(),
        (): Promise<void> => fixture.dispose(),
      ]);
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length)
    throw new AggregateError(failures, "Partial reset evidence or owned cleanup failed", {
      cause: failures[0],
    });
}, 90000);
