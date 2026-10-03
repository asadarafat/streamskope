import { randomUUID } from "node:crypto";

import { expect, it } from "vitest";

import { SchemaRegistryHttpAdapter } from "../../src/features/kafka/engine/schema-registry-http";
import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";
import {
  fetchFixtureToken,
  loadFixtureConfig,
  loadFixtureConnection,
} from "../support/kafka-fixture";

it("qualifies Registry references, compatibility, denial and dependency-safe deletion", async () => {
  const config = await loadFixtureConfig();
  const fixture = await loadFixtureConnection();
  if (fixture.schemaRegistryEndpoint === undefined)
    throw new Error("The real Registry fixture is required.");
  const context = {
    baseUrl: fixture.schemaRegistryEndpoint,
    authorization: async (): Promise<string> =>
      `Bearer ${await fetchFixtureToken(fixture, config)}`,
  };
  const adapter = new SchemaRegistryHttpAdapter(new NodeBoundedJsonHttp());
  const signal = new AbortController().signal;
  const root = `streamskope-registry-${randomUUID()}`;
  const dependency = `${root}-customer`;
  const subject = `${root}-order`;
  const avro = {
    subject: `${root}-avro`,
    version: "latest",
    normalize: true,
    schemaType: "AVRO",
    references: [],
    schema: '{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}',
  } as const;
  // Karapace 5 supports Protobuf references; Avro references require newer registries.
  const customer = {
    ...avro,
    subject: dependency,
    schemaType: "PROTOBUF",
    schema: 'syntax = "proto3"; message Customer { string id = 1; }',
  } as const;
  const order = {
    ...customer,
    subject,
    references: [{ name: "customer.proto", subject: dependency, version: 1 }],
    schema: 'syntax = "proto3"; import "customer.proto"; message Order { Customer customer = 1; }',
  } as const;
  const created = new Set<string>();
  try {
    await expect(
      adapter.listSubjects({ ...context, authorization: () => Promise.resolve(undefined) }, signal),
    ).rejects.toMatchObject({ status: 401 });
    expect(await adapter.checkCompatibility(context, avro, signal)).toMatchObject({
      compatible: true,
      messages: ["The subject has no registered version."],
    });
    await adapter.register(context, avro, signal);
    created.add(avro.subject);
    expect(
      (await adapter.loadLatestSubject(context, avro.subject, signal)).compatibilityLevel,
    ).toBe("BACKWARD");
    expect(await adapter.checkCompatibility(context, avro, signal)).toMatchObject({
      compatible: true,
    });
    expect(
      await adapter.checkCompatibility(
        context,
        {
          ...avro,
          schema:
            '{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"mandatory","type":"string"}]}',
        },
        signal,
      ),
    ).toMatchObject({ compatible: false });
    await adapter.register(context, customer, signal);
    created.add(dependency);
    const registered = await adapter.register(context, order, signal);
    created.add(subject);
    const detail = await adapter.loadLatestSubject(context, subject, signal);
    expect(detail.schema).toMatchObject({
      id: registered.id,
      references: order.references,
      version: 1,
    });
    await expect(
      adapter.delete(
        context,
        {
          target: { kind: "subject", subject: dependency },
          mode: "soft",
          confirmation: dependency,
        },
        signal,
      ),
    ).rejects.toMatchObject({ status: 422 });
    expect((await adapter.loadLatestSubject(context, dependency, signal)).schema.version).toBe(1);
    await adapter.delete(
      context,
      { target: { kind: "subject", subject }, mode: "soft", confirmation: subject },
      signal,
    );
    await adapter.delete(
      context,
      { target: { kind: "subject", subject }, mode: "permanent", confirmation: subject },
      signal,
    );
    created.delete(subject);
    expect((await adapter.listSubjects(context, signal)).subjects).not.toContain(subject);
  } finally {
    // Only this test's unique subjects are eligible for cleanup, dependents first.
    for (const name of [subject, dependency, avro.subject]) {
      if (created.has(name))
        await adapter.delete(
          context,
          { target: { kind: "subject", subject: name }, mode: "permanent", confirmation: name },
          signal,
        );
    }
  }
}, 60_000);
