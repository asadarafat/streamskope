import { describe, expect, it, vi } from "vitest";

import { HOST_PROTOCOL_VERSION, kafkaAclIdentity, type HostEvent } from "../../src/features/kafka/contracts";
import type { KafkaApplicationSession } from "../../src/features/kafka/application";
import { AclFacadeController } from "../../src/features/kafka/facade/acl-facade";

const acl = {
  host: "*",
  operation: "READ",
  patternType: "LITERAL",
  permission: "ALLOW",
  principal: "User:orders",
  resourceName: "orders.events",
  resourceType: "TOPIC",
} as const;

describe("ACL facade", () => {
  it("uses mutation confirmation followed by refreshed inventory evidence", async () => {
    const events: HostEvent[] = [];
    const createAcl = vi.fn((): Promise<void> => Promise.resolve());
    const listAcls = vi.fn((): Promise<readonly (typeof acl)[]> => Promise.resolve([acl]));
    const session = {
      createAcl,
      listAcls,
      snapshot: (): { readonly connectionName: string; readonly state: "connected" } => ({
        connectionName: "Local",
        state: "connected",
      }),
    } as unknown as KafkaApplicationSession;
    let sequence = 0;
    const controller = new AclFacadeController({
      available: (): boolean => true,
      nextSequence: (): number => ++sequence,
      now: (): Date => new Date("2026-08-12T12:00:00.000Z"),
      publish: (event): void => {
        events.push(event);
      },
      recordActivity: (): void => undefined,
      session,
    });

    await controller.execute(
      { command: "acls.create", id: "acl-1", payload: acl, version: HOST_PROTOCOL_VERSION },
      "correlation-1",
    );

    expect(createAcl).toHaveBeenCalledWith(acl);
    expect(listAcls).toHaveBeenCalledAfter(createAcl);
    expect(events.at(-1)).toMatchObject({
      event: "acls.changed",
      payload: { acls: [acl], state: "ready" },
    });
    expect(kafkaAclIdentity(acl)).toContain("orders.events");
  });

  it("refreshes authoritative ACL inventory when exact deletion verification fails", async () => {
    const events: HostEvent[] = [];
    const deleteAcl = vi.fn((): Promise<never> =>
      Promise.reject(new Error("inconsistent ACL deletion result")),
    );
    const listAcls = vi.fn((): Promise<readonly (typeof acl)[]> => Promise.resolve([acl]));
    const session = {
      deleteAcl,
      listAcls,
      snapshot: (): { readonly connectionName: string; readonly state: "connected" } => ({
        connectionName: "Local",
        state: "connected",
      }),
    } as unknown as KafkaApplicationSession;
    let sequence = 0;
    const controller = new AclFacadeController({
      available: (): boolean => true,
      nextSequence: (): number => ++sequence,
      now: (): Date => new Date("2026-08-12T12:00:00.000Z"),
      publish: (event): void => {
        events.push(event);
      },
      recordActivity: (): void => undefined,
      session,
    });

    await expect(
      controller.execute(
        {
          command: "acls.delete",
          id: "acl-delete-1",
          payload: { acl, confirmation: kafkaAclIdentity(acl) },
          version: HOST_PROTOCOL_VERSION,
        },
        "correlation-delete-1",
      ),
    ).resolves.toMatchObject({ ok: false });

    expect(listAcls).toHaveBeenCalledAfter(deleteAcl);
    expect(events.at(-1)).toMatchObject({
      event: "acls.changed",
      payload: { acls: [acl], state: "ready" },
    });
  });

  it("distinguishes unsupported ACL administration from an empty inventory", async () => {
    const events: HostEvent[] = [];
    const unsupported = Object.assign(new Error("ACL administration is unsupported"), {
      code: "UNSUPPORTED_OPERATION" as const,
      recovery: "Use a broker that supports Kafka ACL administration.",
      retryable: false,
      stage: "kafka" as const,
    });
    const session = {
      listAcls: (): Promise<never> => Promise.reject(unsupported),
      snapshot: (): { readonly connectionName: string; readonly state: "connected" } => ({
        connectionName: "Kafka-compatible",
        state: "connected",
      }),
    } as unknown as KafkaApplicationSession;
    let sequence = 0;
    const controller = new AclFacadeController({
      available: (): boolean => true,
      nextSequence: (): number => ++sequence,
      now: (): Date => new Date("2026-08-12T12:00:00.000Z"),
      publish: (event): void => {
        events.push(event);
      },
      recordActivity: (): void => undefined,
      session,
    });

    await expect(
      controller.execute(
        { command: "acls.list", id: "acl-list", payload: {}, version: HOST_PROTOCOL_VERSION },
        "correlation-list",
      ),
    ).resolves.toMatchObject({ ok: false, error: { code: "UNSUPPORTED_OPERATION" } });
    expect(events.at(-1)).toMatchObject({
      event: "acls.changed",
      payload: { acls: [], state: "unsupported" },
    });
  });
});
