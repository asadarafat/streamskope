import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import { expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
} from "../../src/features/kafka/contracts";
import type { SchemaPolicyChange } from "../../src/features/kafka/contracts/schema-policy";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";
import { startRegistryBrowserFixture } from "../support/registry-browser-fixture";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";

it("sets, coalesces and inherits real subject policy with no-op, stale policy/writer refusal and independent readback", async () => {
  const fixture = await startRegistryBrowserFixture(),
    backend = createKafkaBackend(),
    http = new NodeBoundedJsonHttp(),
    signal = AbortSignal.timeout(90000),
    subject = fixture.config.schemaSubject,
    base = fixture.connection.schemaRegistryEndpoint!;
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
  const request = async (
    method: "GET" | "PUT" | "DELETE",
    path: string,
    body?: unknown,
  ): Promise<unknown> => {
    const response = await http.request({
      method,
      url: base + path,
      signal,
      ...(body === undefined ? {} : { body }),
    });
    expect(response.status).toBe(200);
    return response.body;
  };
  try {
    expect(
      await execute("connection.connect", {
        name: "Policy fixture",
        brokers: [fixture.connection.kafkaEndpoint],
        oauth: {
          clientId: fixture.config.oauthClientId,
          clientSecret: fixture.config.oauthClientSecret,
          scope: fixture.config.oauthScope,
          tokenEndpoint: fixture.connection.oauthEndpoint,
        },
        tls: { enabled: true, caPem: await readFile(fixture.connection.caPath, "utf8") },
        services: { schemaRegistry: { baseUrl: base, authentication: "none" } },
      }),
    ).toMatchObject({ ok: true });
    const schema = (await request("GET", `/subjects/${subject}/versions/latest`)) as {
      id: number;
      version: number;
    };
    const prepare = async (
      change: SchemaPolicyChange,
      expectedWriter = { id: schema.id, version: schema.version },
    ): Promise<string> => {
      const result = await execute("schemas.policy.review", { subject, expectedWriter, change });
      expect(result.ok, JSON.stringify(result)).toBe(true);
      if (!result.ok || result.command !== "schemas.policy.review")
        throw new Error("Policy review unavailable");
      return result.result.review.planId;
    };
    expect(await execute("schemas.policy.load", { subject })).toMatchObject({
      ok: true,
      result: { baseline: { policy: { globalLevel: "BACKWARD", subjectLevel: null } } },
    });
    const stale = await prepare({ mode: "set", level: "FULL" });
    expect(await request("GET", `/config/${subject}?defaultToGlobal=true`)).toMatchObject({
      compatibilityLevel: "BACKWARD",
    });
    expect(
      await execute("schemas.policy.apply", { planId: stale, confirmation: "wrong" }),
    ).toMatchObject({ ok: false });
    await request("PUT", "/config", { compatibility: "FORWARD" });
    expect(
      await execute("schemas.policy.apply", { planId: stale, confirmation: subject }),
    ).toMatchObject({ ok: true, result: { outcome: { state: "rejected" } } });
    expect(await request("GET", `/config/${subject}?defaultToGlobal=true`)).toMatchObject({
      compatibilityLevel: "FORWARD",
    });
    await request("PUT", "/config", { compatibility: "BACKWARD" });
    const plan = await prepare({ mode: "set", level: "FULL" });
    const result = await execute("schemas.policy.apply", { planId: plan, confirmation: subject });
    expect(result).toMatchObject({
      ok: true,
      result: {
        outcome: {
          state: "acknowledged",
          verification: "verified",
          observed: { subjectLevel: "FULL" },
        },
      },
    });
    expect(await request("GET", `/config/${subject}?defaultToGlobal=false`)).toMatchObject({
      compatibilityLevel: "FULL",
    });
    await request("PUT", `/config/${subject}`, { compatibility: "FORWARD" });
    const repeat = await execute("schemas.policy.apply", { planId: plan, confirmation: subject });
    if (
      !result.ok ||
      result.command !== "schemas.policy.apply" ||
      !repeat.ok ||
      repeat.command !== "schemas.policy.apply"
    )
      throw new Error("Policy receipts missing");
    expect(repeat.result.outcome).toEqual(result.result.outcome);
    expect(await request("GET", `/config/${subject}?defaultToGlobal=false`)).toMatchObject({
      compatibilityLevel: "FORWARD",
    });
    const unchanged = await prepare({ mode: "set", level: "FORWARD" });
    expect(
      await execute("schemas.policy.apply", { planId: unchanged, confirmation: subject }),
    ).toMatchObject({
      ok: true,
      result: { outcome: { state: "unchanged", verification: "verified" } },
    });
    const inherit = await prepare({ mode: "inherit" });
    expect(
      await execute("schemas.policy.apply", { planId: inherit, confirmation: subject }),
    ).toMatchObject({
      ok: true,
      result: {
        outcome: {
          state: "acknowledged",
          verification: "verified",
          observed: { subjectLevel: null, effectiveLevel: "BACKWARD" },
        },
      },
    });
    const absent = await http.request({
      method: "GET",
      url: `${base}/config/${subject}?defaultToGlobal=false`,
      signal,
    });
    expect(absent.status).toBe(404);
    expect(await request("GET", `/config/${subject}?defaultToGlobal=true`)).toMatchObject({
      compatibilityLevel: "BACKWARD",
    });
    const inherited = await prepare({ mode: "inherit" });
    expect(
      await execute("schemas.policy.apply", { planId: inherited, confirmation: subject }),
    ).toMatchObject({ ok: true, result: { outcome: { state: "unchanged" } } });
    const writerStale = await prepare({ mode: "set", level: "FULL_TRANSITIVE" });
    const definition = JSON.parse(fixture.config.schemaDefinition) as { fields: unknown[] };
    definition.fields.push({ name: "note", type: "string", default: "" });
    await request("PUT", `/config/${subject}`, { compatibility: "FULL_TRANSITIVE" });
    const registered = await http.request({
      method: "POST",
      url: `${base}/subjects/${subject}/versions`,
      signal,
      body: { schema: JSON.stringify(definition), schemaType: "AVRO" },
    });
    expect(registered.status).toBe(200);
    expect(
      await execute("schemas.policy.apply", { planId: writerStale, confirmation: subject }),
    ).toMatchObject({
      ok: true,
      result: { outcome: { state: "rejected", verification: "not-applicable" } },
    });
    expect(await request("GET", `/subjects/${subject}/versions`)).toEqual([1, 2]);
    expect(await request("GET", "/config")).toMatchObject({ compatibilityLevel: "BACKWARD" });
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
    throw new AggregateError(failures, "Real policy or owned cleanup failed", {
      cause: failures[0],
    });
}, 240000);
