import { describe, expect, it, vi } from "vitest";

import { HOST_PROTOCOL_VERSION, type HostCommand, type HostEvent } from "../../src/kafka/contracts";
import type {
  KafkaApplicationSession,
  KafkaClusterServiceContext,
  SchemaRegistryCompatibilityResult,
  SchemaRegistryPort,
  SchemaRegistrySubjectDetail,
  SchemaRegistrySubjectInventory,
} from "../../src/kafka/application";
import { SchemaRegistryFacadeController } from "../../src/kafka/facade/schema-registry-facade";

describe("Schema Registry facade", () => {
  it("does not register a schema when compatibility preflight fails", async () => {
    const events: HostEvent[] = [];
    const register = vi.fn<SchemaRegistryPort["register"]>();
    const port: SchemaRegistryPort = {
      checkCompatibility: (): Promise<SchemaRegistryCompatibilityResult> =>
        Promise.resolve({ compatible: false, messages: ["field status was removed"] }),
      delete: (): Promise<readonly number[]> => Promise.resolve([]),
      listSubjects: (): Promise<SchemaRegistrySubjectInventory> =>
        Promise.resolve({ omittedSubjects: 0, subjects: [] }),
      loadLatestSubject: (): Promise<SchemaRegistrySubjectDetail> =>
        Promise.reject(new Error("not expected")),
      loadSubject: (): Promise<SchemaRegistrySubjectDetail> =>
        Promise.reject(new Error("not expected")),
      register,
    };
    const session = {
      clusterServiceContext: (): KafkaClusterServiceContext => ({
        authorization: (): Promise<undefined> => Promise.resolve(undefined),
        baseUrl: "http://schema:8081",
        caPem: "ca",
      }),
      snapshot: (): { readonly connectionName: string; readonly state: "connected" } => ({
        connectionName: "Local",
        state: "connected",
      }),
    } as unknown as KafkaApplicationSession;
    let sequence = 0;
    const controller = new SchemaRegistryFacadeController({
      nextSequence: (): number => ++sequence,
      now: (): Date => new Date("2026-08-12T12:00:00.000Z"),
      port,
      publish: (event): void => {
        events.push(event);
      },
      recordActivity: (): void => undefined,
      session,
    });
    const command = {
      command: "schemas.register",
      id: "register-1",
      payload: {
        normalize: true,
        references: [],
        schema: '{"type":"record","name":"Order","fields":[]}',
        schemaType: "AVRO",
        subject: "orders-value",
        version: "latest",
      },
      version: HOST_PROTOCOL_VERSION,
    } as const satisfies HostCommand;

    await expect(controller.execute(command, "correlation-1")).resolves.toMatchObject({
      ok: false,
      error: { code: "VALIDATION" },
    });
    expect(register).not.toHaveBeenCalled();
    expect(
      events.some(
        (event) =>
          event.event === "schemaCompatibility.changed" && event.payload.compatible === false,
      ),
    ).toBe(true);
  });

  it("ignores a late detail response after a newer subject selection", async () => {
    type Detail = SchemaRegistrySubjectDetail;
    let resolveFirst: ((value: Detail) => void) | undefined;
    let resolveSecond: ((value: Detail) => void) | undefined;
    const first = new Promise<Detail>((resolve) => {
      resolveFirst = resolve;
    });
    const second = new Promise<Detail>((resolve) => {
      resolveSecond = resolve;
    });
    const events: HostEvent[] = [];
    const port: SchemaRegistryPort = {
      checkCompatibility: (): Promise<SchemaRegistryCompatibilityResult> =>
        Promise.reject(new Error("not expected")),
      delete: (): Promise<readonly number[]> => Promise.reject(new Error("not expected")),
      listSubjects: (): Promise<SchemaRegistrySubjectInventory> =>
        Promise.resolve({ omittedSubjects: 0, subjects: [] }),
      loadLatestSubject: (): Promise<SchemaRegistrySubjectDetail> =>
        Promise.reject(new Error("not expected")),
      loadSubject: (_context, identity) => (identity.subject === "first-value" ? first : second),
      register: (): Promise<{ readonly id: number }> => Promise.reject(new Error("not expected")),
    };
    const session = {
      clusterServiceContext: (): KafkaClusterServiceContext => ({
        authorization: (): Promise<undefined> => Promise.resolve(undefined),
        baseUrl: "http://schema:8081",
        caPem: "ca",
      }),
      snapshot: (): { readonly connectionName: string; readonly state: "connected" } => ({
        connectionName: "Local",
        state: "connected",
      }),
    } as unknown as KafkaApplicationSession;
    let sequence = 0;
    const controller = new SchemaRegistryFacadeController({
      nextSequence: (): number => ++sequence,
      now: (): Date => new Date("2026-08-12T12:00:00.000Z"),
      port,
      publish: (event): void => {
        events.push(event);
      },
      recordActivity: (): void => undefined,
      session,
    });
    const load = (
      subject: string,
    ): Promise<import("../../src/kafka/contracts").HostCommandResponse> =>
      controller.execute(
        {
          command: "schemas.load",
          id: `load-${subject}`,
          payload: { subject, version: "latest" },
          version: HOST_PROTOCOL_VERSION,
        },
        `correlation-${subject}`,
      );

    const obsolete = load("first-value");
    const current = load("second-value");
    const detail = (subject: string): Detail => ({
      compatibilityLevel: "BACKWARD",
      schema: {
        id: 2,
        references: [],
        schema: '{"type":"string"}',
        schemaType: "AVRO",
        subject,
        version: 1,
      },
      versions: [1],
    });
    resolveSecond?.(detail("second-value"));
    await current;
    resolveFirst?.(detail("first-value"));
    await obsolete;

    const detailEvents = events.filter((event) => event.event === "schema.changed");
    expect(detailEvents.at(-1)).toMatchObject({
      payload: { state: "ready", subject: "second-value" },
    });
    expect(
      detailEvents.some(
        (event) =>
          event.event === "schema.changed" &&
          event.payload.state === "ready" &&
          event.payload.subject === "first-value",
      ),
    ).toBe(false);
  });

  it.each([
    [
      Object.assign(new Error("invalid response"), {
        name: "SchemaRegistryResponseError",
        status: null,
      }),
      "invalid-response",
    ],
    [
      Object.assign(new Error("denied"), { name: "SchemaRegistryResponseError", status: 403 }),
      "denied",
    ],
  ] as const)("publishes a distinct %s inventory failure state", async (upstream, state) => {
    const events: HostEvent[] = [];
    const port: SchemaRegistryPort = {
      checkCompatibility: (): Promise<SchemaRegistryCompatibilityResult> =>
        Promise.reject(new Error("not expected")),
      delete: (): Promise<readonly number[]> => Promise.reject(new Error("not expected")),
      listSubjects: (): Promise<SchemaRegistrySubjectInventory> => Promise.reject(upstream),
      loadLatestSubject: (): Promise<SchemaRegistrySubjectDetail> =>
        Promise.reject(new Error("not expected")),
      loadSubject: (): Promise<SchemaRegistrySubjectDetail> =>
        Promise.reject(new Error("not expected")),
      register: (): Promise<{ readonly id: number }> => Promise.reject(new Error("not expected")),
    };
    const session = {
      clusterServiceContext: (): KafkaClusterServiceContext => ({
        authorization: (): Promise<undefined> => Promise.resolve(undefined),
        baseUrl: "http://schema:8081",
        caPem: "ca",
      }),
      snapshot: (): { readonly connectionName: string; readonly state: "connected" } => ({
        connectionName: "Local",
        state: "connected",
      }),
    } as unknown as KafkaApplicationSession;
    let sequence = 0;
    const controller = new SchemaRegistryFacadeController({
      nextSequence: (): number => ++sequence,
      now: (): Date => new Date("2026-08-12T12:00:00.000Z"),
      port,
      publish: (event): void => {
        events.push(event);
      },
      recordActivity: (): void => undefined,
      session,
    });

    await controller.execute(
      {
        command: "schemas.list",
        id: `schema-${state}`,
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      },
      `correlation-${state}`,
    );

    expect(events.at(-1)).toMatchObject({
      event: "schemas.changed",
      payload: { state, subjects: [] },
    });
  });
});
