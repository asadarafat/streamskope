import { randomUUID } from "node:crypto";

import {
  Admin,
  AclOperations,
  AclPermissionTypes,
  ResourceTypes,
  ResourcePatternTypes,
  ClientQuotaMatchTypes,
  ResponseError,
  findErrorBy,
} from "@platformatic/kafka";
import { expect, it } from "vitest";

import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
} from "../../src/features/kafka/contracts";
import {
  parseClientQuotaEntity,
  parseClientQuotaValues,
  clientQuotaExpected,
  type ClientQuotaEntity,
  type ClientQuotaInput,
  type ClientQuotaReview,
} from "../../src/features/kafka/contracts/client-quotas";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";

it("reviews exact real named/default/combined quotas without writes, refuses zero and preserves removal and untouched keys, and refuses read-only, stale and broker-denied changes", async () => {
  const fixture = await startAuthorizationFixture(),
    backend = createKafkaBackend();
  const name = `quota-${randomUUID()}`,
    clientName = `client-${randomUUID()}`;
  const user = parseClientQuotaEntity([{ type: "user", name }]),
    client = parseClientQuotaEntity([{ type: "client-id", name: clientName }]),
    combined = parseClientQuotaEntity([
      { type: "user", name },
      { type: "client-id", name: clientName },
    ]),
    defaults = parseClientQuotaEntity([{ type: "client-id", name: null }]),
    literalDefault = parseClientQuotaEntity([{ type: "client-id", name: "(default)" }]);
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
  const values = async (
    entity: ClientQuotaEntity,
  ): Promise<ReturnType<typeof parseClientQuotaValues>> => {
    const response = await fixture.admin.describeClientQuotas({
      strict: true,
      components: entity.map((c) =>
        c.name === null
          ? { entityType: c.type, matchType: ClientQuotaMatchTypes.DEFAULT }
          : { entityType: c.type, matchType: ClientQuotaMatchTypes.EXACT, match: c.name },
      ),
    });
    expect(response.length).toBeLessThanOrEqual(1);
    if (response.length)
      expect(
        parseClientQuotaEntity(
          response[0]!.entity.map((c) => ({ type: c.entityType, name: c.entityName })),
        ),
      ).toEqual(entity);
    return parseClientQuotaValues(response[0]?.values ?? []);
  };
  const seed = async (input: ClientQuotaInput): Promise<void> => {
    const before = await values(input.entity),
      expected = clientQuotaExpected(before, input.changes);
    const request: Parameters<Admin["alterClientQuotas"]>[0] = {
      validateOnly: false,
      entries: [
        {
          entities: input.entity.map((c) => ({ entityType: c.type, entityName: c.name })),
          ops: input.changes.map((c) =>
            c.value === null
              ? { key: c.key, remove: true }
              : { key: c.key, remove: false, value: c.value },
          ),
        },
      ],
    };
    await fixture.admin.alterClientQuotas(request);
    await expect
      .poll(() => values(input.entity), { timeout: 15000, interval: 200 })
      .toEqual(expected);
  };
  const review = async (input: ClientQuotaInput): Promise<ClientQuotaReview> => {
    const result = await execute("quotas.change.review", input);
    expect(result.ok).toBe(true);
    if (!result.ok || result.command !== "quotas.change.review") throw new Error("No quota review");
    return result.result.review;
  };
  const apply = async (
    plan: ClientQuotaReview,
  ): Promise<ReturnType<typeof parseHostCommandResponse>> =>
    execute("quotas.change.apply", { planId: plan.planId, confirmation: plan.confirmation });
  try {
    await seed({
      entity: user,
      changes: [
        { key: "producer_byte_rate", value: 1000 },
        { key: "consumer_byte_rate", value: 900 },
      ],
    });
    await seed({ entity: client, changes: [{ key: "producer_byte_rate", value: 5000 }] });
    await seed({ entity: combined, changes: [{ key: "producer_byte_rate", value: 7000 }] });
    await seed({ entity: defaults, changes: [{ key: "consumer_byte_rate", value: 3000000 }] });
    await seed({
      entity: literalDefault,
      changes: [{ key: "consumer_byte_rate", value: 4000000 }],
    });
    await execute("preferences.update", {
      patch: { protection: { readOnly: true, maskKey: false, maskHeaders: [], valuePaths: [] } },
    });
    expect(await execute("connection.connect", fixture.connection)).toMatchObject({ ok: true });
    for (const entity of [user, client, combined, defaults, literalDefault]) {
      const inspected = await execute("quotas.inspect", { entity });
      expect(inspected).toMatchObject({
        ok: true,
        result: { snapshot: { entity, values: await values(entity), alterSupported: true } },
      });
    }
    const input: ClientQuotaInput = {
      entity: user,
      changes: [{ key: "producer_byte_rate", value: 512 }],
    };
    await expect(
      execute("quotas.change.review", {
        entity: user,
        changes: [{ key: "producer_byte_rate", value: 0 }],
      }),
    ).rejects.toThrow("positive");
    const readonly = await review(input);
    expect(await values(user)).toEqual(readonly.baseline.values);
    expect(await apply(readonly)).toMatchObject({
      ok: false,
      error: { code: "AUTHORIZATION_DENIED" },
    });
    await execute("connection.disconnect", {});
    await execute("preferences.update", {
      patch: { protection: { readOnly: false, maskKey: false, maskHeaders: [], valuePaths: [] } },
    });
    expect(await execute("connection.connect", fixture.connection)).toMatchObject({ ok: true });
    const stale = await review(input);
    await seed({ entity: user, changes: [{ key: "consumer_byte_rate", value: 901 }] });
    expect(await apply(stale)).toMatchObject({
      ok: true,
      result: { outcome: { state: "unsent", cleanup: "confirmed" } },
    });
    expect(await values(user)).toEqual([
      { key: "consumer_byte_rate", value: 901 },
      { key: "producer_byte_rate", value: 1000 },
    ]);
    const positive = await review(input);
    expect(
      await execute("quotas.change.apply", { planId: positive.planId, confirmation: name }),
    ).toMatchObject({ ok: false });
    expect(await values(user)).toEqual(positive.baseline.values);
    const changed = await apply(positive);
    expect(changed).toMatchObject({
      ok: true,
      result: { outcome: { state: "acknowledged", cleanup: "confirmed" } },
    });
    // Kafka applies quotas through metadata propagation. Do not infer immediate readback from its ACK.
    await expect
      .poll(() => values(user), { timeout: 15000, interval: 200 })
      .toEqual(positive.expected);
    const duplicate = await apply(positive);
    if (
      !changed.ok ||
      !duplicate.ok ||
      changed.command !== "quotas.change.apply" ||
      duplicate.command !== "quotas.change.apply"
    )
      throw new Error("No quota receipts");
    expect(duplicate.result.outcome).toEqual(changed.result.outcome);
    const remove = await review({
      entity: user,
      changes: [{ key: "producer_byte_rate", value: null }],
    });
    expect(remove.expected).toEqual([{ key: "consumer_byte_rate", value: 901 }]);
    expect(await apply(remove)).toMatchObject({
      ok: true,
      result: { outcome: { state: "acknowledged", cleanup: "confirmed" } },
    });
    await expect
      .poll(() => values(user), { timeout: 15000, interval: 200 })
      .toEqual(remove.expected);
    // Strict inspection still excludes the separate combined entity and the default client entry.
    expect(await values(combined)).toEqual([{ key: "producer_byte_rate", value: 7000 }]);
    const empty = await execute("quotas.inspect", {
      entity: [{ type: "user", name: `absent-${randomUUID()}` }],
    });
    expect(empty).toMatchObject({ ok: true, result: { snapshot: { values: [] } } });
    const denied = {
      resourceType: ResourceTypes.CLUSTER,
      resourcePatternType: ResourcePatternTypes.LITERAL,
      resourceName: "kafka-cluster",
      principal: "User:ANONYMOUS",
      host: "*",
      operation: AclOperations.ALTER_CONFIGS,
      permissionType: AclPermissionTypes.DENY,
    };
    const allowed = {
      ...denied,
      operation: AclOperations.DESCRIBE_CONFIGS,
      permissionType: AclPermissionTypes.ALLOW,
    };
    // The isolated fixture's controller listener uses the anonymous broker principal.
    // Keep forwarding authorized while denying the original client's quota operation.
    const forwarding = { ...allowed, operation: AclOperations.CLUSTER_ACTION };
    await fixture.admin.createAcls({ creations: [denied, allowed, forwarding] });
    // Establish authorization propagation with an independent read-only validation request.
    const anonymous = new Admin({
      bootstrapBrokers: [...fixture.connection.brokers],
      clientId: "quota-denial-probe",
      retries: 0,
    });
    try {
      await expect
        .poll(
          async () => {
            try {
              await anonymous.alterClientQuotas({
                validateOnly: true,
                entries: [
                  {
                    entities: [{ entityType: "user", entityName: name }],
                    ops: [{ key: "producer_byte_rate", remove: false, value: 1000 }],
                  },
                ],
              });
              return false;
            } catch (error) {
              const response: unknown = findErrorBy(
                error instanceof Error ? error : undefined,
                "code",
                ResponseError.code,
              )?.response;
              const entry: unknown =
                typeof response === "object" &&
                response !== null &&
                "entries" in response &&
                Array.isArray(response.entries) &&
                response.entries.length === 1
                  ? response.entries[0]
                  : null;
              return (
                typeof entry === "object" &&
                entry !== null &&
                "errorCode" in entry &&
                entry.errorCode === 31
              );
            }
          },
          { timeout: 15000, interval: 200 },
        )
        .toBe(true);
    } finally {
      await anonymous.close();
    }
    const deniedReview = await review({
      entity: user,
      changes: [{ key: "producer_byte_rate", value: 2000 }],
    });
    expect(await apply(deniedReview)).toMatchObject({
      ok: true,
      result: {
        outcome: {
          state: "rejected",
          verification: "unavailable",
          cleanup: "confirmed",
          detail:
            "Kafka returned an error for this quota entity (error code 31). Check cluster ALTER_CONFIGS permission and supported key/value combinations before another review.",
        },
      },
    });
    expect(await values(user)).toEqual(deniedReview.baseline.values);
    const readDenied = { ...denied, operation: AclOperations.DESCRIBE_CONFIGS };
    await fixture.admin.createAcls({ creations: [readDenied] });
    await expect
      .poll(async () => (await execute("quotas.inspect", { entity: user })).ok, {
        timeout: 15000,
        interval: 200,
      })
      .toBe(false);
    expect(await values(user)).toEqual(deniedReview.baseline.values);
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
    throw new AggregateError(failures, "Real quota administration or owned cleanup failed", {
      cause: failures[0],
    });
}, 120000);
