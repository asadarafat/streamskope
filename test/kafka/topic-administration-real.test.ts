import { randomUUID } from "node:crypto";

import {
  AclOperations,
  AclPermissionTypes,
  ResourcePatternTypes,
  ResourceTypes,
} from "@platformatic/kafka";
import { expect, it } from "vitest";

import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
} from "../../src/features/kafka/contracts";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";

it("expands and UUID-deletes real topics with no preview writes, duplicate apply, stale replacement/assignment and real permission refusal", async () => {
  const fixture = await startAuthorizationFixture(),
    backend = createKafkaBackend();
  const failures: unknown[] = [];
  const topic = `admin-${randomUUID()}`,
    replacement = `replacement-${randomUUID()}`;
  const execute = async (
    command: string,
    payload: unknown,
  ): Promise<ReturnType<typeof parseHostCommandResponse>> =>
    parseHostCommandResponse(
      await backend.execute(
        parseHostCommand({ command, payload, id: randomUUID(), version: HOST_PROTOCOL_VERSION }),
      ),
    );
  const partitions = async (name: string): Promise<number> =>
    (
      await fixture.admin.metadata({ topics: [name], forceUpdate: true, autocreateTopics: false })
    ).topics.get(name)?.partitionsCount ?? 0;
  const review = async (
    input:
      { kind: "expand"; topic: string; partitions: number } | { kind: "delete"; topic: string },
  ): Promise<{ planId: string; confirmation: string; topicId: string }> => {
    const response = await execute("topics.change.review", input);
    expect(response.ok, JSON.stringify(response)).toBe(true);
    if (!response.ok || response.command !== "topics.change.review")
      throw new Error("No topic review");
    return {
      planId: response.result.review.planId,
      confirmation: response.result.review.confirmation,
      topicId: response.result.review.baseline.identity.topicId,
    };
  };
  try {
    await fixture.admin.createTopics({ topics: [topic, replacement], partitions: 1, replicas: 1 });
    expect(
      await execute("preferences.update", {
        patch: { protection: { readOnly: true, maskKey: false, maskHeaders: [], valuePaths: [] } },
      }),
    ).toMatchObject({ ok: true });
    expect(await execute("connection.connect", fixture.connection)).toMatchObject({ ok: true });
    // The independent host policy blocks apply even when renderer controls are bypassed.
    const protectedReview = await review({ kind: "expand", topic, partitions: 2 });
    expect(
      await execute("topics.change.apply", {
        planId: protectedReview.planId,
        confirmation: protectedReview.confirmation,
      }),
    ).toMatchObject({ ok: false, error: { code: "AUTHORIZATION_DENIED" } });
    expect(await partitions(topic)).toBe(1);
    expect(await execute("connection.disconnect", {})).toMatchObject({ ok: true });
    expect(
      await execute("preferences.update", {
        patch: { protection: { readOnly: false, maskKey: false, maskHeaders: [], valuePaths: [] } },
      }),
    ).toMatchObject({ ok: true });
    expect(await execute("connection.connect", fixture.connection)).toMatchObject({ ok: true });
    const expansion = await review({ kind: "expand", topic, partitions: 2 });
    expect(await partitions(topic)).toBe(1);
    expect(
      await execute("topics.change.apply", { planId: expansion.planId, confirmation: topic }),
    ).toMatchObject({ ok: false });
    const applied = await execute("topics.change.apply", {
      planId: expansion.planId,
      confirmation: expansion.confirmation,
    });
    expect(applied).toMatchObject({
      ok: true,
      result: { outcome: { state: "acknowledged", cleanup: "confirmed" } },
    });
    await expect.poll(() => partitions(topic), { timeout: 10_000 }).toBe(2);
    const repeated = await execute("topics.change.apply", {
      planId: expansion.planId,
      confirmation: expansion.confirmation,
    });
    if (
      !applied.ok ||
      !repeated.ok ||
      applied.command !== "topics.change.apply" ||
      repeated.command !== "topics.change.apply"
    )
      throw new Error("Missing expansion receipts");
    expect(repeated.result.outcome).toEqual(applied.result.outcome);
    const stale = await review({ kind: "expand", topic, partitions: 4 });
    await fixture.admin.createPartitions({ topics: [{ name: topic, count: 3 }] });
    await expect.poll(() => partitions(topic), { timeout: 10_000 }).toBe(3);
    expect(
      await execute("topics.change.apply", {
        planId: stale.planId,
        confirmation: stale.confirmation,
      }),
    ).toMatchObject({ ok: true, result: { outcome: { state: "unsent" } } });
    expect(await partitions(topic)).toBe(3);
    const old = await review({ kind: "delete", topic: replacement });
    await fixture.admin.deleteTopics({ topics: [replacement] });
    await expect
      .poll(() => fixture.admin.listTopics(), { timeout: 10_000 })
      .not.toContain(replacement);
    await fixture.admin.createTopics({ topics: [replacement], partitions: 1, replicas: 1 });
    const newId = (
      await fixture.admin.metadata({
        topics: [replacement],
        forceUpdate: true,
        autocreateTopics: false,
      })
    ).topics.get(replacement)?.id;
    expect(newId).not.toBe(old.topicId);
    expect(
      await execute("topics.change.apply", { planId: old.planId, confirmation: old.confirmation }),
    ).toMatchObject({ ok: true, result: { outcome: { state: "unsent" } } });
    expect(await fixture.admin.listTopics()).toContain(replacement);
    const denied = {
      principal: "User:ANONYMOUS",
      host: "*",
      resourceType: ResourceTypes.TOPIC,
      resourceName: topic,
      resourcePatternType: ResourcePatternTypes.LITERAL,
      operation: AclOperations.ALTER,
      permissionType: AclPermissionTypes.DENY,
    };
    await fixture.admin.createAcls({ creations: [denied] });
    await expect
      .poll(
        async () =>
          (await execute("topics.change.review", { kind: "expand", topic, partitions: 4 })).ok,
        { timeout: 10_000 },
      )
      .toBe(false);
    await fixture.admin.deleteAcls({ filters: [denied] });
    const deletion = await review({ kind: "delete", topic });
    expect(await fixture.admin.listTopics()).toContain(topic);
    const deleted = await execute("topics.change.apply", {
      planId: deletion.planId,
      confirmation: deletion.confirmation,
    });
    expect(deleted).toMatchObject({
      ok: true,
      result: { outcome: { state: "acknowledged", cleanup: "confirmed" } },
    });
    await expect.poll(() => fixture.admin.listTopics(), { timeout: 10_000 }).not.toContain(topic);
    const duplicate = await execute("topics.change.apply", {
      planId: deletion.planId,
      confirmation: deletion.confirmation,
    });
    if (
      !deleted.ok ||
      !duplicate.ok ||
      deleted.command !== "topics.change.apply" ||
      duplicate.command !== "topics.change.apply"
    )
      throw new Error("Missing deletion receipts");
    expect(duplicate.result.outcome).toEqual(deleted.result.outcome);
  } catch (error) {
    failures.push(error);
  } finally {
    try {
      await disposeNativeFixtureResources([
        (): Promise<void> => backend.shutdown(),
        (): Promise<void> => fixture.dispose(),
      ]);
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length)
    throw new AggregateError(failures, "Real topic administration or owned cleanup failed", {
      cause: failures[0],
    });
}, 120_000);
