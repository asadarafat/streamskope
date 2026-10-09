import { afterEach, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  parseCorrelatedHostResponse,
  parseHostCommand,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type KafkaAclBinding,
} from "../../src/features/kafka/contracts";
import {
  InMemoryKafkaOperationalPreferenceStore,
  KafkaApplicationSession,
  KafkaOperationalPreferenceService,
} from "../../src/features/kafka/application";
import { aclChangeConfirmation } from "../../src/features/kafka/contracts/acl-review";
import type { KafkaWriteOutcome } from "../../src/features/kafka/contracts/reviewed-writes";
import type { KafkaBackendFacade } from "../../src/features/kafka/facade";
import {
  dispatchAclReviewCommand,
  type AclReviewHandlers,
} from "../../src/features/kafka/facade/acl-review-facade";
import {
  command,
  createFacade,
  RecordingActiveConnection,
  RecordingConnectionPort,
} from "../support/kafka-backend-facade-fixture";

const access = { topic: "orders.events", principal: "User:reader", host: "127.0.0.1" };
const binding: KafkaAclBinding = {
  resourceType: "TOPIC",
  resourceName: "orders.events",
  patternType: "LITERAL",
  principal: "User:reader",
  host: "*",
  operation: "READ",
  permission: "ALLOW",
};
const hosts: KafkaBackendFacade[] = [];
afterEach(async () => {
  await Promise.all(hosts.splice(0).map((host) => host.shutdown()));
});

function fixture(readOnly = false): {
  facade: KafkaBackendFacade;
  inventory: KafkaAclBinding[];
  events: HostEvent[];
  list: ReturnType<typeof vi.fn<() => Promise<readonly KafkaAclBinding[]>>>;
  create: ReturnType<typeof vi.fn<(acl: KafkaAclBinding) => Promise<void>>>;
  remove: ReturnType<typeof vi.fn<(acl: KafkaAclBinding) => Promise<void>>>;
} {
  const inventory: KafkaAclBinding[] = [];
  const list = vi.fn(() => Promise.resolve(structuredClone(inventory)));
  const create = vi.fn((acl: KafkaAclBinding): Promise<void> => {
    inventory.push(acl);
    return Promise.resolve();
  });
  const remove = vi.fn((acl: KafkaAclBinding): Promise<void> => {
    inventory.splice(
      inventory.findIndex((entry) => JSON.stringify(entry) === JSON.stringify(acl)),
      1,
    );
    return Promise.resolve();
  });
  const active = Object.assign(new RecordingActiveConnection(), {
    listAcls: list,
    createAcl: create,
    deleteAcl: remove,
    describeClusterMetadata: () =>
      Promise.resolve({
        brokers: [{ nodeId: 1, host: "broker", port: 9092, rack: null }],
        clusterId: "fixture-cluster",
        controllerId: 1,
      }),
    describeBrokerConfiguration: () =>
      Promise.resolve(
        [
          ["authorizer.class.name", "org.apache.kafka.metadata.authorizer.StandardAuthorizer"],
          ["super.users", ""],
          ["allow.everyone.if.no.acl.found", "false"],
        ].map(([name, value]) => ({
          name: name!,
          value: value!,
          isDefault: false,
          isSensitive: false,
          readOnly: true,
          documentation: null,
          synonyms: [],
          source: "static-broker" as const,
          type: "string" as const,
        })),
      ),
  });
  const port = new RecordingConnectionPort();
  port.openOperations.push(() => Promise.resolve(active));
  const preferences = new KafkaOperationalPreferenceService(
    new InMemoryKafkaOperationalPreferenceStore(
      { durability: "session", state: "ready" },
      {
        ...KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
        protection: { readOnly, maskKey: false, maskHeaders: [], valuePaths: [] },
      },
    ),
  );
  const facade = createFacade(port, undefined, undefined, undefined, preferences);
  hosts.push(facade);
  const events: HostEvent[] = [];
  facade.subscribe((event) => events.push(event));
  return { facade, inventory, events, list, create, remove };
}

async function execute<Command extends HostCommand>(
  facade: KafkaBackendFacade,
  request: Command,
): Promise<HostCommandResponse<Command["command"]>> {
  const parsed = parseHostCommand(request);
  const response = parseCorrelatedHostResponse(await facade.execute(parsed), request);
  expect(response).toMatchObject({
    command: request.command,
    id: request.id,
    version: request.version,
  });
  const correlationId = response.ok ? response.result.correlationId : response.error.correlationId;
  expect(correlationId).toMatch(/^correlation-\d+$/u);
  expect(correlationId).not.toBe(request.id);
  return response;
}

it.each(["create", "delete"] as const)(
  "routes reviewed ACL %s through the real facade and retains correlated receipts without repeated mutation",
  async (action) => {
    const f = fixture();
    if (action === "delete") f.inventory.push(binding);
    await execute(f.facade, command("connection.connect", "connect"));
    const explanation = await execute(f.facade, {
      command: "acls.access.explain",
      id: "explain-reader",
      version: HOST_PROTOCOL_VERSION,
      payload: access,
    });
    expect(explanation).toMatchObject({
      ok: true,
      result: { explanation: { effective: action === "create" ? "denied" : "allowed" } },
    });
    const input = { action, acl: binding, access };
    const reviewed = await execute(f.facade, {
      command: "acls.change.review",
      id: "review-binding",
      version: HOST_PROTOCOL_VERSION,
      payload: input,
    });
    expect(f.create).not.toHaveBeenCalled();
    expect(f.remove).not.toHaveBeenCalled();
    if (!reviewed.ok) throw new Error("Expected a successful ACL review.");
    expect(reviewed.result.review).toMatchObject({
      input,
      beforePresent: action === "delete",
      afterPresent: action === "create",
    });
    const confirmation = aclChangeConfirmation(input);
    const request = {
      command: "acls.change.apply",
      id: "apply-binding",
      version: HOST_PROTOCOL_VERSION,
      payload: { planId: reviewed.result.review.planId, confirmation },
    } as const;
    const applied = await execute(f.facade, request);
    if (!applied.ok) throw new Error("Expected an ACL mutation receipt.");
    expect(applied.result.outcome).toMatchObject({
      state: "acknowledged",
      verification: "verified",
    });
    const repeated = await execute(f.facade, { ...request, id: "apply-again" });
    expect(repeated).toMatchObject({ ok: true, result: { outcome: applied.result.outcome } });
    expect(f.create).toHaveBeenCalledTimes(action === "create" ? 1 : 0);
    expect(f.remove).toHaveBeenCalledTimes(action === "delete" ? 1 : 0);
    expect(f.inventory).toEqual(action === "create" ? [binding] : []);
    expect(
      f.events.filter((event) => event.event === "activity.recorded").map((event) => event.payload),
    ).toContainEqual(
      expect.objectContaining({
        correlationId: applied.result.correlationId,
        operation: "Apply reviewed ACL change",
        object: confirmation,
        detail: applied.result.outcome.detail,
        outcome: "succeeded",
        severity: "info",
      }),
    );
  },
);

it("permits read-only explanation and review but rejects a valid apply before acquiring a scope or reading the adapter", async () => {
  const f = fixture(true);
  const scope = vi.spyOn(KafkaApplicationSession.prototype, "aclReviewScope");
  await execute(f.facade, command("connection.connect", "connect"));
  expect(
    await execute(f.facade, {
      command: "acls.access.explain",
      id: "explain",
      version: HOST_PROTOCOL_VERSION,
      payload: access,
    }),
  ).toMatchObject({ ok: true });
  const input = { action: "create", acl: binding, access } as const;
  const reviewed = await execute(f.facade, {
    command: "acls.change.review",
    id: "review",
    version: HOST_PROTOCOL_VERSION,
    payload: input,
  });
  if (!reviewed.ok) throw new Error("Read-only review should be allowed.");
  expect(scope).toHaveBeenCalledTimes(2);
  expect(f.list).toHaveBeenCalledTimes(2);
  scope.mockClear();
  f.list.mockClear();
  const denied = await execute(f.facade, {
    command: "acls.change.apply",
    id: "apply",
    version: HOST_PROTOCOL_VERSION,
    payload: { planId: reviewed.result.review.planId, confirmation: aclChangeConfirmation(input) },
  });
  expect(denied).toMatchObject({
    ok: false,
    error: {
      code: "AUTHORIZATION_DENIED",
      summary: "Read-only mode blocks this operation.",
      retryable: false,
    },
  });
  expect(scope).not.toHaveBeenCalled();
  expect(f.list).not.toHaveBeenCalled();
  expect(f.create).not.toHaveBeenCalled();
  expect(f.remove).not.toHaveBeenCalled();
});

it.each(["lost-acknowledgement", "unavailable-readback"] as const)(
  "keeps the %s outcome and correlated Activity warning through dispatch",
  async (failure) => {
    const f = fixture();
    await execute(f.facade, command("connection.connect", "connect"));
    const input = { action: "create", acl: binding, access } as const;
    const reviewed = await execute(f.facade, {
      command: "acls.change.review",
      id: "review",
      version: HOST_PROTOCOL_VERSION,
      payload: input,
    });
    if (!reviewed.ok) throw new Error("Expected a successful ACL review.");
    if (failure === "lost-acknowledgement") {
      f.create.mockImplementationOnce((acl) => {
        f.inventory.push(acl);
        return Promise.reject(new Error("private adapter detail"));
      });
    } else {
      f.list.mockResolvedValueOnce([]).mockRejectedValueOnce(new Error("private readback detail"));
    }
    const response = await execute(f.facade, {
      command: "acls.change.apply",
      id: "apply",
      version: HOST_PROTOCOL_VERSION,
      payload: {
        planId: reviewed.result.review.planId,
        confirmation: aclChangeConfirmation(input),
      },
    });
    if (!response.ok) throw new Error("Expected a structured mutation outcome.");
    expect(response.result.outcome).toMatchObject({
      state: failure === "lost-acknowledgement" ? "unknown" : "acknowledged",
      verification: "unavailable",
    });
    expect(f.create).toHaveBeenCalledOnce();
    expect(
      f.events.filter((event) => event.event === "activity.recorded").map((event) => event.payload),
    ).toContainEqual(
      expect.objectContaining({
        correlationId: response.result.correlationId,
        operation: "Apply reviewed ACL change",
        detail: response.result.outcome.detail,
        outcome: failure === "lost-acknowledgement" ? "failed" : "succeeded",
        severity: "warning",
      }),
    );
    expect(JSON.stringify(response)).not.toContain("private");
  },
);

it("keeps the safe correlated failure envelope for an unaccepted ACL confirmation", async () => {
  const f = fixture();
  await execute(f.facade, command("connection.connect", "connect"));
  const response = await execute(f.facade, {
    command: "acls.change.apply",
    id: "unaccepted",
    version: HOST_PROTOCOL_VERSION,
    payload: { planId: "nonexistent", confirmation: "unreviewed change" },
  });
  expect(response).toMatchObject({
    ok: false,
    error: {
      code: "VALIDATION",
      stage: "authorization",
      retryable: false,
      activeStateChanged: false,
      summary: "The ACL review could not be accepted.",
    },
  });
  expect(f.create).not.toHaveBeenCalled();
  expect(f.remove).not.toHaveBeenCalled();
});

it("owns the dispatch correlation and rejects a malformed service result before publishing success", async () => {
  const outcome: KafkaWriteOutcome = {
    state: "acknowledged",
    verification: "verified",
    detail: "The reviewed binding was observed.",
    receipt: null,
  };
  const unexpected = (): Promise<never> => Promise.reject(new Error("Wrong handler dispatched."));
  const apply = vi.fn(() => Promise.resolve({ correlationId: "service-supplied", outcome }));
  const handlers: AclReviewHandlers = {
    "acls.access.explain": unexpected,
    "acls.change.review": unexpected,
    "acls.change.apply": apply,
  };
  const request = {
    command: "acls.change.apply",
    id: "renderer-request",
    version: HOST_PROTOCOL_VERSION,
    payload: { planId: "reviewed-plan", confirmation: "confirmed" },
  } as const;
  const response = await dispatchAclReviewCommand(handlers, request, "host-owned");
  expect(response).toEqual({
    command: request.command,
    id: request.id,
    version: HOST_PROTOCOL_VERSION,
    ok: true,
    result: { correlationId: "host-owned", outcome },
  });
  expect(apply).toHaveBeenCalledWith(request.payload, "host-owned");
  // Runtime values can violate their declared TypeScript shape; the wire boundary must still reject them.
  Object.assign(outcome, { state: "unsupported-state" });
  await expect(dispatchAclReviewCommand(handlers, request, "host-owned")).rejects.toThrow(
    HostContractValidationError,
  );
});
