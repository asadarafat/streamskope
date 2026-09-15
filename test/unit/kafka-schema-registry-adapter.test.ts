import { describe, expect, it } from "vitest";

import { SchemaRegistryHttpAdapter } from "../../src/kafka/engine/schema-registry-http";
import type {
  BoundedJsonHttpPort,
  BoundedJsonHttpRequest,
  BoundedJsonHttpResponse,
} from "../../src/kafka/engine/bounded-json-http";
import type { KafkaClusterServiceContext } from "../../src/kafka/application";

class ScriptedHttp implements BoundedJsonHttpPort {
  readonly requests: BoundedJsonHttpRequest[] = [];

  constructor(private readonly responses: readonly BoundedJsonHttpResponse[]) {}

  request(input: BoundedJsonHttpRequest): Promise<BoundedJsonHttpResponse> {
    this.requests.push(input);
    const response = this.responses[this.requests.length - 1];
    return response === undefined
      ? Promise.reject(new Error("Unexpected HTTP request"))
      : Promise.resolve(response);
  }
}

const context: KafkaClusterServiceContext = {
  authorization: () => Promise.resolve("Bearer redacted-token"),
  baseUrl: "https://schema.example.test:8081/registry",
  caPem: "fixture-ca",
};

describe("Schema Registry HTTP adapter", () => {
  it("lists sorted bounded subjects using an encoded owned path and authorization", async () => {
    const http = new ScriptedHttp([{ body: ["z-value", "a-key"], status: 200 }]);
    const adapter = new SchemaRegistryHttpAdapter(http);

    await expect(adapter.listSubjects(context, new AbortController().signal)).resolves.toEqual({
      omittedSubjects: 0,
      subjects: ["a-key", "z-value"],
    });
    expect(http.requests[0]).toMatchObject({
      authorization: "Bearer redacted-token",
      caPem: "fixture-ca",
      method: "GET",
      url: "https://schema.example.test:8081/registry/subjects",
    });
  });

  it("loads versions, exact version detail, and compatibility config without rewriting schema", async () => {
    const rawSchema = '{"type":"record","name":"Order","fields":[]}';
    const http = new ScriptedHttp([
      { body: [3, 1, 2], status: 200 },
      {
        body: {
          id: 42,
          references: [{ name: "Customer", subject: "customer-value", version: 2 }],
          schema: rawSchema,
          schemaType: "AVRO",
          subject: "orders/value",
          version: 3,
        },
        status: 200,
      },
      { body: { compatibilityLevel: "BACKWARD" }, status: 200 },
    ]);
    const adapter = new SchemaRegistryHttpAdapter(http);

    await expect(
      adapter.loadSubject(
        context,
        { subject: "orders/value", version: 3 },
        new AbortController().signal,
      ),
    ).resolves.toMatchObject({
      compatibilityLevel: "BACKWARD",
      schema: { id: 42, schema: rawSchema, subject: "orders/value", version: 3 },
      versions: [1, 2, 3],
    });
    expect(http.requests.map((request) => request.url)).toEqual([
      "https://schema.example.test:8081/registry/subjects/orders%2Fvalue/versions",
      "https://schema.example.test:8081/registry/subjects/orders%2Fvalue/versions/3",
      "https://schema.example.test:8081/registry/config/orders%2Fvalue",
    ]);
  });

  it("checks compatibility before registration and uses exact destructive paths", async () => {
    const http = new ScriptedHttp([
      { body: { is_compatible: false, messages: ["field status was removed"] }, status: 200 },
      { body: { id: 51 }, status: 200 },
      { body: [3], status: 200 },
      { body: [3], status: 200 },
    ]);
    const adapter = new SchemaRegistryHttpAdapter(http);
    const definition = {
      references: [],
      schema: '{"type":"record","name":"Order","fields":[]}',
      schemaType: "AVRO" as const,
    };

    await expect(
      adapter.checkCompatibility(
        context,
        { ...definition, subject: "orders-value", version: 3 },
        new AbortController().signal,
      ),
    ).resolves.toMatchObject({ compatible: false, messages: ["field status was removed"] });
    await expect(
      adapter.register(
        context,
        { ...definition, normalize: true, subject: "orders-value", version: 3 },
        new AbortController().signal,
      ),
    ).resolves.toEqual({ id: 51 });
    await expect(
      adapter.delete(
        context,
        {
          confirmation: "orders-value@3",
          mode: "permanent",
          target: { kind: "version", subject: "orders-value", version: 3 },
        },
        new AbortController().signal,
      ),
    ).resolves.toEqual([3]);
    expect(http.requests[1]).toMatchObject({ method: "POST" });
    expect(http.requests[1]?.url).toContain("normalize=true");
    expect(http.requests[2]).toMatchObject({ method: "DELETE" });
    expect(http.requests[2]?.url).toContain("/subjects/orders-value/versions/3?permanent=false");
    expect(http.requests[3]).toMatchObject({ method: "DELETE" });
    expect(http.requests[3]?.url).toContain("/subjects/orders-value/versions/3?permanent=true");
  });

  it("treats omitted AVRO type and reference metadata as protocol defaults", async () => {
    const rawSchema = '{"type":"record","name":"Fixture","fields":[]}';
    const adapter = new SchemaRegistryHttpAdapter(
      new ScriptedHttp([
        { body: [1], status: 200 },
        {
          body: {
            id: 1,
            schema: rawSchema,
            subject: "test-value",
            version: 1,
          },
          status: 200,
        },
        { body: { error_code: 40_408 }, status: 404 },
        { body: { compatibilityLevel: "BACKWARD" }, status: 200 },
      ]),
    );

    await expect(
      adapter.loadLatestSubject(context, "test-value", new AbortController().signal),
    ).resolves.toEqual({
      compatibilityLevel: "BACKWARD",
      schema: {
        id: 1,
        references: [],
        schema: rawSchema,
        schemaType: "AVRO",
        subject: "test-value",
        version: 1,
      },
      versions: [1],
    });
  });

  it("rejects malformed upstream response data instead of publishing it", async () => {
    const adapter = new SchemaRegistryHttpAdapter(
      new ScriptedHttp([{ body: { subjects: ["not-the-api-shape"] }, status: 200 }]),
    );

    await expect(adapter.listSubjects(context, new AbortController().signal)).rejects.toMatchObject(
      { name: "SchemaRegistryResponseError" },
    );
  });

  it.each([40_401, 40_402])(
    "treats missing latest error %i as compatible for first registration only",
    async (errorCode) => {
      const adapter = new SchemaRegistryHttpAdapter(
        new ScriptedHttp([{ body: { error_code: errorCode }, status: 404 }]),
      );

      await expect(
        adapter.checkCompatibility(
          context,
          {
            references: [],
            schema: '{"type":"record","name":"First","fields":[]}',
            schemaType: "AVRO",
            subject: "first-value",
            version: "latest",
          },
          new AbortController().signal,
        ),
      ).resolves.toMatchObject({ compatible: true });
    },
  );

  it("does not treat a generic missing compatibility endpoint as a new subject", async () => {
    const adapter = new SchemaRegistryHttpAdapter(
      new ScriptedHttp([{ body: { error_code: 404 }, status: 404 }]),
    );

    await expect(
      adapter.checkCompatibility(
        context,
        {
          references: [],
          schema: '{"type":"record","name":"First","fields":[]}',
          schemaType: "AVRO",
          subject: "first-value",
          version: "latest",
        },
        new AbortController().signal,
      ),
    ).rejects.toMatchObject({ name: "SchemaRegistryResponseError", status: 404 });
  });

  it("falls back to global compatibility when the subject has no override", async () => {
    const http = new ScriptedHttp([
      { body: [1], status: 200 },
      {
        body: {
          id: 1,
          references: [],
          schema: '{"type":"string"}',
          schemaType: "AVRO",
          subject: "orders-value",
          version: 1,
        },
        status: 200,
      },
      { body: { error_code: 40408 }, status: 404 },
      { body: { compatibilityLevel: "BACKWARD" }, status: 200 },
    ]);
    const adapter = new SchemaRegistryHttpAdapter(http);

    await expect(
      adapter.loadLatestSubject(context, "orders-value", new AbortController().signal),
    ).resolves.toMatchObject({ compatibilityLevel: "BACKWARD" });
    expect(http.requests.map((request) => request.url)).toEqual([
      "https://schema.example.test:8081/registry/subjects/orders-value/versions",
      "https://schema.example.test:8081/registry/subjects/orders-value/versions/latest",
      "https://schema.example.test:8081/registry/config/orders-value",
      "https://schema.example.test:8081/registry/config",
    ]);
  });
});
