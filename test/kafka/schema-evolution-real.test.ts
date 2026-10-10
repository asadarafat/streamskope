import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import { expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
} from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { SchemaRegistryHttpAdapter } from "../../src/features/kafka/engine/schema-registry-http";
import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";
import {
  startNativeKafkaFixture,
  disposeNativeFixtureResources,
} from "../support/native-kafka-fixture";
import { startSchemaRegistryServerFixture } from "../support/schema-registry-server-fixture";
import { loadFixtureConfig } from "../support/kafka-fixture";

it("reviews real Registry evolution, refuses stale policy, registers once and verifies transitive compatibility", async () => {
  const fixture = await startNativeKafkaFixture();
  let registry: Awaited<ReturnType<typeof startSchemaRegistryServerFixture>> | undefined;
  const backend = createKafkaBackend(),
    http = new NodeBoundedJsonHttp(),
    adapter = new SchemaRegistryHttpAdapter(http);
  const subject = `evolution-${randomUUID()}`,
    malformedSubject = `${subject}-malformed`;
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
  try {
    registry = await startSchemaRegistryServerFixture(fixture.internalBroker);
    const signal = AbortSignal.timeout(90000),
      config = await loadFixtureConfig();
    const context = {
      baseUrl: registry.url,
      authorization: (): Promise<undefined> => Promise.resolve(undefined),
    };
    const definition = (fields: unknown[]): string =>
      JSON.stringify({ type: "record", name: "Event", fields });
    const oldSchema = definition([{ name: "source", type: "string" }]);
    const nextSchema = definition([
      { name: "source", type: "string" },
      { name: "note", type: ["null", "string"], default: null },
    ]);
    const old = await adapter.register(
      context,
      {
        subject,
        version: "latest",
        schemaType: "AVRO",
        schema: oldSchema,
        references: [],
        normalize: true,
      },
      signal,
    );
    expect(
      await execute("connection.connect", {
        name: "Evolution fixture",
        brokers: [fixture.environment.STREAMSKOPE_TEST_KAFKA_ENDPOINT!],
        oauth: {
          clientId: config.oauthClientId,
          clientSecret: config.oauthClientSecret,
          scope: config.oauthScope,
          tokenEndpoint: fixture.environment.STREAMSKOPE_TEST_OAUTH_ENDPOINT!,
        },
        tls: {
          enabled: true,
          caPem: await readFile(fixture.environment.STREAMSKOPE_TEST_CA_PATH!, "utf8"),
        },
        services: { schemaRegistry: { baseUrl: registry.url, authentication: "none" } },
      }),
    ).toMatchObject({ ok: true });
    const input = {
      draft: {
        subject,
        version: "latest" as const,
        schemaType: "AVRO" as const,
        schema: nextSchema,
        references: [],
        normalize: true,
      },
      expectedWriter: { id: old.id, version: 1 },
    };
    const prepare = async (payload = input): Promise<string> => {
      const response = await execute("schemas.change.review", payload);
      expect(response.ok, JSON.stringify(response)).toBe(true);
      if (!response.ok || response.command !== "schemas.change.review")
        throw new Error("Evolution review missing.");
      expect(response.result.review.compatible).toBe(true);
      return response.result.review.planId;
    };
    expect(
      await adapter.loadReviewSchema(context, { subject, version: "latest" }, signal),
    ).toMatchObject({ id: old.id, version: 1 });
    expect(await adapter.loadCompatibilityPolicy(context, subject, signal)).toMatchObject({
      effectiveLevel: "BACKWARD",
    });
    expect(
      await adapter.checkProposedCompatibility(context, input.draft, [1], signal),
    ).toMatchObject({
      compatible: true,
    });
    const review = await prepare();
    expect((await adapter.loadLatestSubject(context, subject, signal)).schema.version).toBe(1);
    expect(
      await execute("schemas.change.apply", { planId: review, confirmation: "wrong" }),
    ).toMatchObject({ ok: false });
    expect(
      (
        await http.request({
          method: "PUT",
          url: `${registry.url}/config/${subject}`,
          body: { compatibility: "FULL" },
          signal,
        })
      ).status,
    ).toBe(200);
    expect(
      await execute("schemas.change.apply", { planId: review, confirmation: subject }),
    ).toMatchObject({
      ok: true,
      result: { outcome: { state: "rejected", verification: "not-applicable" } },
    });
    expect((await adapter.loadLatestSubject(context, subject, signal)).schema.version).toBe(1);
    expect(
      (await http.request({ method: "DELETE", url: `${registry.url}/config/${subject}`, signal }))
        .status,
    ).toBe(200);
    const current = await prepare();
    const result = await execute("schemas.change.apply", {
      planId: current,
      confirmation: subject,
    });
    expect(result).toMatchObject({
      ok: true,
      result: {
        outcome: { state: "acknowledged", verification: "verified", observed: { version: 2 } },
      },
    });
    const repeat = await execute("schemas.change.apply", {
      planId: current,
      confirmation: subject,
    });
    if (
      !result.ok ||
      result.command !== "schemas.change.apply" ||
      !repeat.ok ||
      repeat.command !== "schemas.change.apply"
    )
      throw new Error("Registration receipt missing.");
    expect(repeat.result.outcome).toEqual(result.result.outcome);
    const second = (await adapter.loadLatestSubject(context, subject, signal)).schema;
    expect((await adapter.loadLatestSubject(context, subject, signal)).versions).toEqual([1, 2]);
    const incompatible = await execute("schemas.change.review", {
      ...input,
      draft: { ...input.draft, schema: definition([{ name: "source", type: "int" }]) },
      expectedWriter: { id: second.id, version: 2 },
    });
    expect(incompatible).toMatchObject({ ok: true, result: { review: { compatible: false } } });
    expect(
      (
        await http.request({
          method: "PUT",
          url: `${registry.url}/config/${subject}`,
          body: { compatibility: "FULL_TRANSITIVE" },
          signal,
        })
      ).status,
    ).toBe(200);
    const thirdSchema = definition([
      { name: "source", type: "string" },
      { name: "note", type: ["null", "string"], default: null },
      { name: "revision", type: "int", default: 0 },
    ]);
    const transitive = await prepare({
      ...input,
      draft: { ...input.draft, schema: thirdSchema },
      expectedWriter: { id: second.id, version: 2 },
    });
    expect(
      await execute("schemas.change.apply", { planId: transitive, confirmation: subject }),
    ).toMatchObject({
      ok: true,
      result: {
        outcome: { state: "acknowledged", verification: "verified", observed: { version: 3 } },
      },
    });
    const malformed = await execute("schemas.change.review", {
      draft: { ...input.draft, subject: malformedSubject, schema: "not-valid-avro" },
      expectedWriter: null,
    });
    expect(malformed).toMatchObject({ ok: true, result: { review: { before: null } } });
    if (!malformed.ok || malformed.command !== "schemas.change.review")
      throw new Error("New subject review missing.");
    expect(
      await execute("schemas.change.apply", {
        planId: malformed.result.review.planId,
        confirmation: malformedSubject,
      }),
    ).toMatchObject({ ok: true, result: { outcome: { state: "rejected" } } });
    expect(
      await adapter.loadReviewSchema(
        context,
        { subject: malformedSubject, version: "latest" },
        signal,
      ),
    ).toBeNull();
  } catch (error) {
    failures.push(error);
  }
  try {
    await disposeNativeFixtureResources([
      (): Promise<void> => backend.shutdown(),
      async (): Promise<void> => {
        if (registry)
          await adapter.delete(
            {
              baseUrl: registry.url,
              authorization: (): Promise<undefined> => Promise.resolve(undefined),
            },
            { confirmation: subject, target: { kind: "subject", subject }, mode: "permanent" },
            AbortSignal.timeout(15000),
          );
      },
      (): Promise<void> => registry?.dispose() ?? Promise.resolve(),
      (): Promise<void> => fixture.dispose(),
    ]);
  } catch (error) {
    failures.push(error);
  }
  if (failures.length)
    throw new AggregateError(failures, "Real evolution qualification or owned cleanup failed.", {
      cause: failures[0],
    });
}, 240000);
