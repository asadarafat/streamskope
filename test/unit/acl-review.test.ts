import { expect, it, vi } from "vitest";

import { AclReviewService } from "../../src/features/kafka/application/acl-review-service";
import { KafkaConnectionScopes } from "../../src/features/kafka/application/connection-scope";
import {
  explainTopicAccess,
  type BrokerAccessPolicy,
} from "../../src/features/kafka/application/topic-access";
import {
  aclChangeConfirmation,
  parseAclChangeInput,
  parseAclChangeReview,
  parseTopicAccessInput,
  parseTopicAccessExplanation,
  type AclChangeInput,
} from "../../src/features/kafka/contracts/acl-review";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
  type KafkaAclBinding,
  type KafkaConfigurationEntry,
} from "../../src/features/kafka/contracts";
import { RecordingActiveConnection } from "../support/kafka-backend-facade-fixture";

const access = { topic: "orders.events", principal: "User:reader", host: "127.0.0.1" };
const binding: KafkaAclBinding = {
  resourceType: "TOPIC",
  resourceName: "orders.",
  patternType: "PREFIXED",
  principal: "User:*",
  host: "*",
  operation: "READ",
  permission: "ALLOW",
};
const policy: BrokerAccessPolicy = {
  id: 1,
  authorizer: "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
  allowIfNoAcl: false,
  superUsers: [],
};
const change: AclChangeInput = { action: "create", acl: binding, access };

function deferred<T>(): {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: Error): void;
} {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((complete, fail) => {
    resolve = complete;
    reject = fail;
  });
  return { promise, resolve, reject };
}

it("evaluates literal/wildcard/prefix resources, wildcard identities and exact hosts, with DENY preceding ALLOW", () => {
  expect(explainTopicAccess(access, [binding], [policy]).effective).toBe("allowed");
  const deny = {
    ...binding,
    patternType: "LITERAL",
    resourceName: access.topic,
    principal: access.principal,
    host: access.host,
    permission: "DENY",
  } as const;
  expect(explainTopicAccess(access, [binding, deny], [policy]).effective).toBe("denied");
  expect(
    explainTopicAccess(
      access,
      [{ ...binding, patternType: "LITERAL", resourceName: "*", operation: "ALL" }],
      [policy],
    ).effective,
  ).toBe("allowed");
  expect(explainTopicAccess(access, [{ ...binding, host: "127.0.0.2" }], [policy]).effective).toBe(
    "denied",
  );
  expect(
    explainTopicAccess(access, [{ ...binding, principal: "Service:*" }], [policy]).effective,
  ).toBe("denied");
  expect(
    explainTopicAccess(access, [{ ...binding, operation: "DESCRIBE" }], [policy]).effective,
  ).toBe("denied");
  expect(
    explainTopicAccess(access, [{ ...binding, patternType: "LITERAL" }], [policy]).aclDecision,
  ).toBe("broker-default");
});
it("uses allow-no-ACL only when no resource binding exists, including unrelated principals and operations", () => {
  const defaults = [{ ...policy, allowIfNoAcl: true }];
  expect(explainTopicAccess(access, [], defaults).effective).toBe("allowed");
  expect(
    explainTopicAccess(
      access,
      [{ ...binding, principal: "User:other", operation: "WRITE" }],
      defaults,
    ).effective,
  ).toBe("denied");
  expect(
    explainTopicAccess(access, [{ ...binding, resourceType: "GROUP" }], defaults).effective,
  ).toBe("allowed");
});
it("keeps missing/custom/inconsistent broker policies and unknown superusers explicit", () => {
  const deny = { ...binding, permission: "DENY" } as const;
  for (const policies of [
    [],
    [{ ...policy, authorizer: null }],
    [{ ...policy, authorizer: "vendor.Policy" }],
    [{ ...policy, superUsers: null }],
  ])
    expect(explainTopicAccess(access, [deny], policies).effective).toBe("unknown");
  expect(
    explainTopicAccess(access, [deny], [{ ...policy, superUsers: [access.principal] }]).effective,
  ).toBe("allowed");
  expect(
    explainTopicAccess(
      access,
      [deny],
      [policy, { ...policy, id: 2, superUsers: [access.principal] }],
    ).effective,
  ).toBe("unknown");
  const result = explainTopicAccess(access, [binding], [{ ...policy, superUsers: null }]);
  expect(result.effective).toBe("allowed");
  expect(result.policy.superuser).toBe("unknown");
  expect(parseTopicAccessExplanation(result)).toEqual(result);
});
it("bounds displayed evidence without dropping a deny from evaluation", () => {
  const bindings = Array.from({ length: 51 }, (_, index) => ({
    ...binding,
    resourceName: index % 2 === 0 ? "orders." : "orders",
  }));
  const result = explainTopicAccess(
    access,
    [...bindings, { ...binding, permission: "DENY" }],
    [policy],
  );
  expect(result).toMatchObject({ effective: "denied", omittedBindings: 2 });
  expect(result.matching).toHaveLength(50);
});
it("rejects unconstrained identity/address queries and unrelated topic examples at the protocol boundary", () => {
  for (const host of ["*", "localhost", "999.0.0.1", "1:2:3", "127.0.0.01"])
    expect(() => parseTopicAccessInput({ ...access, host })).toThrow();
  expect(parseTopicAccessInput({ ...access, host: "::1" }).host).toBe("::1");
  expect(() => parseTopicAccessInput({ ...access, principal: "User:*" })).toThrow();
  expect(() =>
    parseAclChangeInput({ ...change, access: { ...access, topic: "unrelated" } }),
  ).toThrow();
  expect(() => parseAclChangeInput({ ...change, access: null })).toThrow();
  expect(() =>
    parseHostCommand({
      command: "acls.change.apply",
      version: HOST_PROTOCOL_VERSION,
      id: "x",
      payload: { planId: "p", confirmation: "c", acl: binding },
    }),
  ).toThrow();
});

function fixture(): {
  service: AclReviewService;
  inventory: KafkaAclBinding[];
  create: ReturnType<typeof vi.fn<(acl: KafkaAclBinding) => Promise<void>>>;
  remove: ReturnType<typeof vi.fn<(acl: KafkaAclBinding) => Promise<void>>>;
  list: ReturnType<typeof vi.fn<() => Promise<readonly KafkaAclBinding[]>>>;
  config: Map<string, string>;
  expire(): void;
  changeConnection(): void;
} {
  const inventory: KafkaAclBinding[] = [];
  const config = new Map([
    ["authorizer.class.name", policy.authorizer!],
    ["super.users", ""],
    ["allow.everyone.if.no.acl.found", "false"],
  ]);
  const create = vi.fn((acl: KafkaAclBinding): Promise<void> => {
    inventory.push(acl);
    return Promise.resolve();
  });
  const remove = vi.fn((acl: KafkaAclBinding): Promise<void> => {
    const index = inventory.findIndex((a) => JSON.stringify(a) === JSON.stringify(acl));
    if (index >= 0) inventory.splice(index, 1);
    return Promise.resolve();
  });
  const list = vi.fn((): Promise<readonly KafkaAclBinding[]> =>
    Promise.resolve(structuredClone(inventory)),
  );
  let now = 0,
    generation = 1;
  const connection = Object.assign(new RecordingActiveConnection(), {
    createAcl: create,
    deleteAcl: remove,
    listAcls: list,
    describeClusterMetadata: (): Promise<{
      brokers: { nodeId: number; host: string; port: number; rack: null }[];
      clusterId: string;
      controllerId: number;
    }> =>
      Promise.resolve({
        clusterId: "cluster",
        controllerId: 1,
        brokers: [{ nodeId: 1, host: "broker", port: 9092, rack: null }],
      }),
    describeBrokerConfiguration: (): Promise<readonly KafkaConfigurationEntry[]> =>
      Promise.resolve(
        [...config].map(([name, value]) => ({
          name,
          value,
          isDefault: false,
          isSensitive: false,
          documentation: null,
          synonyms: [],
          readOnly: true,
          source: "static-broker",
          type: "string",
        })),
      ),
  });
  const scopes = new KafkaConnectionScopes(() => ({
    connection,
    generation,
    connectionName: "Fixture",
  }));
  return {
    service: new AclReviewService(
      () => scopes.aclReview(),
      () => now,
    ),
    inventory,
    config,
    create,
    remove,
    list,
    expire(): void {
      now = 120_001;
    },
    changeConnection(): void {
      generation++;
    },
  };
}
it("reviews before/after implications without mutation, confirms exact scope, coalesces application and reads it back", async () => {
  const f = fixture(),
    review = await f.service.review(change);
  expect(parseAclChangeReview(review)).toEqual(review);
  expect(review).toMatchObject({
    beforePresent: false,
    afterPresent: true,
    beforeAccess: { effective: "denied" },
    afterAccess: { effective: "allowed" },
  });
  expect(f.create).not.toHaveBeenCalled();
  expect(
    parseHostCommandResponse({
      command: "acls.change.review",
      version: HOST_PROTOCOL_VERSION,
      id: "x",
      ok: true,
      result: { correlationId: "x", review },
    }),
  ).toMatchObject({ ok: true });
  await expect(f.service.apply(review.planId, "create another binding")).rejects.toThrow();
  const [first, second] = await Promise.all([
    f.service.apply(review.planId, aclChangeConfirmation(change)),
    f.service.apply(review.planId, aclChangeConfirmation(change)),
  ]);
  expect(first).toMatchObject({ state: "acknowledged", verification: "verified" });
  expect(second).toEqual(first);
  expect(f.create).toHaveBeenCalledTimes(1);
  const deletion = { ...change, action: "delete" } as const;
  const next = await f.service.review(deletion);
  expect(await f.service.apply(next.planId, aclChangeConfirmation(deletion))).toMatchObject({
    state: "acknowledged",
    verification: "verified",
  });
  expect(f.inventory).toEqual([]);
});
it.each(["inventory", "policy"])("rejects stale %s before mutation", async (scenario) => {
  const f = fixture(),
    review = await f.service.review(change);
  if (scenario === "inventory") f.inventory.push({ ...binding, principal: "User:another" });
  else f.config.set("allow.everyone.if.no.acl.found", "true");
  expect(await f.service.apply(review.planId, aclChangeConfirmation(change))).toMatchObject({
    state: "rejected",
    verification: "not-applicable",
  });
  expect(f.create).not.toHaveBeenCalled();
});
it.each(["expire", "changeConnection"] as const)("rejects %s plans", async (operation) => {
  const f = fixture(),
    review = await f.service.review(change);
  f[operation]();
  await expect(f.service.apply(review.planId, aclChangeConfirmation(change))).rejects.toThrow();
  expect(f.create).not.toHaveBeenCalled();
});
it("accounts for a lost acknowledgement without automatically retrying", async () => {
  const f = fixture(),
    review = await f.service.review(change);
  f.create.mockImplementationOnce((acl) => {
    f.inventory.push(acl);
    return Promise.reject(new Error("response lost"));
  });
  const result = await f.service.apply(review.planId, aclChangeConfirmation(change));
  expect(result.state).toBe("unknown");
  expect(await f.service.apply(review.planId, aclChangeConfirmation(change))).toEqual(result);
  expect(f.create).toHaveBeenCalledTimes(1);
});
it("preserves acknowledged outcomes when read-back is unavailable and avoids no-op mutations", async () => {
  const f = fixture(),
    review = await f.service.review(change);
  f.list.mockResolvedValueOnce([]).mockRejectedValueOnce(new Error("readback denied"));
  expect(await f.service.apply(review.planId, aclChangeConfirmation(change))).toMatchObject({
    state: "acknowledged",
    verification: "unavailable",
  });
  const next = await f.service.review(change);
  expect(await f.service.apply(next.planId, aclChangeConfirmation(change))).toMatchObject({
    state: "acknowledged",
    verification: "verified",
  });
  expect(f.create).toHaveBeenCalledTimes(1);
});

it.each(["review", "explain"] as const)(
  "does not publish a late ACL %s after same-adapter reconnection",
  async (operation) => {
    const f = fixture();
    const reading = deferred<readonly KafkaAclBinding[]>();
    f.list.mockReturnValueOnce(reading.promise);
    const pending = operation === "review" ? f.service.review(change) : f.service.explain(access);
    const rejected = expect(pending).rejects.toThrow(/connection changed/iu);
    f.changeConnection();
    reading.resolve([]);
    await rejected;
    expect(f.create).not.toHaveBeenCalled();
    expect(f.remove).not.toHaveBeenCalled();
  },
);

it("retains a rejected plan when authority changes during ACL apply revalidation without sending a mutation", async () => {
  const f = fixture();
  const review = await f.service.review(change);
  const reading = deferred<readonly KafkaAclBinding[]>();
  f.list.mockReturnValueOnce(reading.promise);
  const pending = f.service.apply(review.planId, aclChangeConfirmation(change));
  await vi.waitFor(() => expect(f.list).toHaveBeenCalledTimes(2));
  f.changeConnection();
  reading.resolve([]);
  expect(await pending).toMatchObject({ state: "rejected", verification: "not-applicable" });
  expect(f.service.apply(review.planId, aclChangeConfirmation(change))).toBe(pending);
  expect(f.create).not.toHaveBeenCalled();
});

it.each(["create", "delete"] as const)(
  "retains an admitted ACL %s acknowledgement after reconnection, with no stale verification or repeated dispatch",
  async (action) => {
    const f = fixture();
    if (action === "delete") f.inventory.push(binding);
    const input = { ...change, action };
    const review = await f.service.review(input);
    const writing = deferred<void>();
    const mutate = action === "create" ? f.create : f.remove;
    mutate.mockReturnValueOnce(writing.promise);
    const pending = f.service.apply(review.planId, aclChangeConfirmation(input));
    await vi.waitFor(() => expect(mutate).toHaveBeenCalledOnce());
    f.changeConnection();
    f.expire();
    writing.resolve();
    expect(await pending).toMatchObject({ state: "acknowledged", verification: "unavailable" });
    expect(f.service.apply(review.planId, aclChangeConfirmation(input))).toBe(pending);
    expect(mutate).toHaveBeenCalledOnce();
    expect(f.list).toHaveBeenCalledTimes(2);
  },
);

it("retains an admitted ACL mutation's uncertainty after reconnection without retrying", async () => {
  const f = fixture();
  const review = await f.service.review(change);
  const writing = deferred<void>();
  f.create.mockReturnValueOnce(writing.promise);
  const pending = f.service.apply(review.planId, aclChangeConfirmation(change));
  await vi.waitFor(() => expect(f.create).toHaveBeenCalledOnce());
  f.changeConnection();
  writing.reject(new Error("Acknowledgement lost"));
  expect(await pending).toMatchObject({ state: "unknown", verification: "unavailable" });
  expect(f.service.apply(review.planId, aclChangeConfirmation(change))).toBe(pending);
  expect(f.create).toHaveBeenCalledOnce();
});
