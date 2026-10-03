import { Producer } from "@platformatic/kafka";
import { expect, it } from "vitest";

import { AclReviewService } from "../../src/features/kafka/application/acl-review-service";
import {
  aclChangeConfirmation,
  type AclChangeInput,
} from "../../src/features/kafka/contracts/acl-review";
import type { KafkaActiveConnection } from "../../src/features/kafka/application";
import type { KafkaAclBinding } from "../../src/features/kafka/contracts";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import { PlatformaticAdminPort } from "../../src/features/kafka/engine/platformatic-admin";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";

it("compares reviewed topic READ semantics and exact ACL application with a real StandardAuthorizer broker", async () => {
  const fixture = await startAuthorizationFixture();
  const topic = "streamskope-owned-acl-review";
  const access = { topic, principal: "User:ANONYMOUS", host: "127.0.0.1" };
  const prefix: KafkaAclBinding = {
    resourceType: "TOPIC",
    resourceName: "streamskope-owned-",
    patternType: "PREFIXED",
    principal: "User:*",
    host: "*",
    operation: "READ",
    permission: "ALLOW",
  };
  const deny: KafkaAclBinding = {
    ...prefix,
    resourceName: topic,
    patternType: "LITERAL",
    principal: access.principal,
    host: access.host,
    permission: "DENY",
  };
  let connection: KafkaActiveConnection | undefined;
  const privileged = new PlatformaticAdminPort(fixture.admin);
  try {
    await fixture.admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
    await expect
      .poll(
        async () => {
          try {
            await fixture.admin.findCoordinator({ keyType: 0, keys: ["acl-read-readiness"] });
            return true;
          } catch {
            return false;
          }
        },
        { timeout: 30_000 },
      )
      .toBe(true);
    const producer = new Producer({
      bootstrapBrokers: [...fixture.connection.brokers],
      clientId: "owned-acl-seed",
      retries: 0,
    });
    try {
      await producer.send({ messages: [{ topic, partition: 0, value: Buffer.from("fixture") }] });
    } finally {
      await producer.close();
    }
    connection = await new StreamSkopeKafkaEngine({ operationTimeoutMs: 5_000 }).openConnection(
      fixture.connection,
      AbortSignal.timeout(15_000),
    );
    const active = connection;
    const service = new AclReviewService(() => ({
      connection: active,
      connectionName: "Owned fixture",
      generation: 1,
    }));
    const canRead = async (): Promise<boolean> => {
      let stream: Awaited<ReturnType<KafkaActiveConnection["openMessageStream"]>> | undefined;
      try {
        stream = await active.openMessageStream(
          { topic, mode: "earliest", maxMessages: 1 },
          AbortSignal.timeout(10_000),
        );
        for await (const message of stream) {
          expect(message.payload).toBe("fixture");
          return true;
        }
        throw new Error("Expected seeded record");
      } catch (error) {
        if (
          typeof error === "object" &&
          error !== null &&
          "code" in error &&
          error.code === "AUTHORIZATION_DENIED"
        )
          return false;
        throw error;
      } finally {
        await stream?.close();
      }
    };
    const apply = async (input: AclChangeInput): Promise<void> => {
      const review = await service.review(input);
      const result = await service.apply(review.planId, aclChangeConfirmation(input));
      expect(result).toMatchObject({ state: "acknowledged", verification: "verified" });
      expect(await service.apply(review.planId, aclChangeConfirmation(input))).toEqual(result);
    };
    // The fixture's configured allow-no-ACL default is independently exercised.
    expect((await service.explain(access)).aclDecision).toBe("broker-default");
    expect(await canRead()).toBe(true);
    await apply({ action: "create", acl: prefix, access });
    expect((await service.explain(access)).aclDecision).toBe("allowed");
    expect(await canRead()).toBe(true);
    await apply({ action: "create", acl: deny, access });
    expect((await service.explain(access)).aclDecision).toBe("denied");
    await expect.poll(canRead, { timeout: 15_000 }).toBe(false);
    const wildcard = {
      ...prefix,
      patternType: "LITERAL",
      resourceName: "*",
      operation: "ALL",
    } as const;
    await apply({ action: "create", acl: wildcard, access });
    expect((await service.explain(access)).aclDecision).toBe("denied");
    expect(await canRead()).toBe(false);
    await apply({ action: "delete", acl: deny, access });
    expect(await canRead()).toBe(true);

    const stale = await service.review({ action: "delete", acl: prefix, access });
    const unrelated = { ...prefix, resourceName: "unrelated", patternType: "LITERAL" } as const;
    await privileged.createAcl(unrelated);
    expect(await service.apply(stale.planId, aclChangeConfirmation(stale.input))).toMatchObject({
      state: "rejected",
      verification: "not-applicable",
    });
    expect(await privileged.listAcls()).toContainEqual(prefix);

    await apply({ action: "delete", acl: prefix, access });
    await apply({ action: "delete", acl: wildcard, access });
    expect(await canRead()).toBe(true);
    await privileged.createAcl({
      ...prefix,
      resourceName: topic,
      patternType: "LITERAL",
      principal: "User:other",
      operation: "WRITE",
    });
    expect((await service.explain(access)).aclDecision).toBe("denied");
    await expect.poll(canRead, { timeout: 15_000 }).toBe(false);
  } finally {
    await connection?.close();
    await fixture.dispose();
  }
}, 180_000);
