import { expect, it, vi, type Mock } from "vitest";

import { SchemaPolicyService } from "../../src/features/kafka/application/schema-policy-service";
import { SchemaReviewOperations } from "../../src/features/kafka/application/schema-review-operations";
import { SchemaChangeService } from "../../src/features/kafka/application/schema-change-service";
import type {
  SchemaRegistrySubjectDetail,
  ReviewedSchemaPolicyPort,
} from "../../src/features/kafka/application/schema-registry-types";
import type { SchemaRegistryReviewScope } from "../../src/features/kafka/application/connection-scope";
import {
  parseSchemaPolicyInput,
  parseSchemaPolicyReview,
  parseSchemaPolicyOutcome,
  type SchemaPolicyInput,
} from "../../src/features/kafka/contracts/schema-policy";
import type { SchemaCompatibilityPolicy } from "../../src/features/kafka/contracts/schema-changes";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
  type SchemaVersionDetail,
} from "../../src/features/kafka/contracts";
import { SchemaRegistryHttpAdapter } from "../../src/features/kafka/engine/schema-registry-http";
import type {
  BoundedJsonHttpRequest,
  BoundedJsonHttpResponse,
} from "../../src/features/kafka/engine/bounded-json-http";

const input: SchemaPolicyInput = {
  subject: "events",
  expectedWriter: { id: 1, version: 1 },
  change: { mode: "set", level: "FULL" },
};
function fixture(): {
  service: SchemaPolicyService;
  operations: SchemaReviewOperations;
  scope: SchemaRegistryReviewScope;
  port: ReviewedSchemaPolicyPort;
  write: Mock<ReviewedSchemaPolicyPort["changeSubjectCompatibility"]>;
  load: Mock<ReviewedSchemaPolicyPort["loadCompatibilityPolicy"]>;
  schema: { -readonly [K in keyof SchemaVersionDetail]: SchemaVersionDetail[K] };
  policy: SchemaCompatibilityPolicy;
  revoke(): void;
  expire(): void;
} {
  let current = true,
    now = Date.now();
  const schema: SchemaVersionDetail = {
    subject: "events",
    id: 1,
    version: 1,
    schema: '"string"',
    schemaType: "AVRO",
    references: [],
  };
  const policy: SchemaCompatibilityPolicy = {
    globalLevel: "BACKWARD",
    subjectLevel: null,
    effectiveLevel: "BACKWARD",
  };
  const context = {
    baseUrl: "http://registry",
    authorization: (): Promise<undefined> => Promise.resolve(undefined),
  };
  const scope: SchemaRegistryReviewScope = {
    connectionName: "Local",
    isCurrent: () => current,
    read: async (run, signal) => {
      signal.throwIfAborted();
      if (!current) throw new Error("changed");
      const result = await run(context, signal);
      if (!current) throw new Error("changed");
      return result;
    },
    tryDispatch: (run) => (current ? { started: true, result: run(context) } : { started: false }),
  };
  const load = vi.fn<ReviewedSchemaPolicyPort["loadCompatibilityPolicy"]>(() =>
    Promise.resolve({ ...policy }),
  );
  const write = vi.fn<ReviewedSchemaPolicyPort["changeSubjectCompatibility"]>(
    (_context, _subject, change) => {
      const old = policy.subjectLevel;
      Object.assign(policy, {
        subjectLevel: change.mode === "set" ? change.level : null,
        effectiveLevel: change.mode === "set" ? change.level : policy.globalLevel,
      });
      return Promise.resolve({
        level: change.mode === "set" ? change.level : (old ?? policy.globalLevel),
      });
    },
  );
  const port = {
    loadReviewSchema: (): Promise<SchemaVersionDetail | null> => Promise.resolve({ ...schema }),
    loadCompatibilityPolicy: load,
    changeSubjectCompatibility: write,
  } satisfies ReviewedSchemaPolicyPort;
  const operations = new SchemaReviewOperations(() => (current ? scope : null)),
    service = new SchemaPolicyService(port, operations, () => now);
  return {
    service,
    operations,
    scope,
    port,
    write,
    load,
    schema,
    policy,
    revoke: (): void => {
      current = false;
    },
    expire: (): void => {
      now += 120001;
    },
  };
}
it("loads and reviews policy without writing; exact confirmation coalesces one acknowledged attempt", async () => {
  const f = fixture();
  expect(await f.service.load("events")).toMatchObject({
    writer: { id: 1 },
    policy: { subjectLevel: null },
  });
  const review = parseSchemaPolicyReview(await f.service.review(input));
  expect(review.after.effectiveLevel).toBe("FULL");
  expect(f.write).not.toHaveBeenCalled();
  await expect(f.service.apply(review.planId, "wrong")).rejects.toThrow("Confirm");
  const first = f.service.apply(review.planId, "events"),
    second = f.service.apply(review.planId, "events");
  expect(first).toBe(second);
  expect(parseSchemaPolicyOutcome(await first)).toMatchObject({
    state: "acknowledged",
    verification: "verified",
    acknowledgedLevel: "FULL",
    observed: { subjectLevel: "FULL" },
  });
  expect(await f.service.apply(review.planId, "events")).toEqual(await first);
  expect(f.write).toHaveBeenCalledTimes(1);
});
it("removes an override and verifies inheritance separately from the removed-level acknowledgement", async () => {
  const f = fixture();
  Object.assign(f.policy, { subjectLevel: "FULL", effectiveLevel: "FULL" });
  const review = await f.service.review({ ...input, change: { mode: "inherit" } });
  expect(parseSchemaPolicyOutcome(await f.service.apply(review.planId, "events"))).toMatchObject({
    state: "acknowledged",
    acknowledgedLevel: "FULL",
    observed: { globalLevel: "BACKWARD", subjectLevel: null, effectiveLevel: "BACKWARD" },
    verification: "verified",
  });
});
it("sets an explicit override even when the inherited effective level is identical", async () => {
  const f = fixture();
  const review = await f.service.review({ ...input, change: { mode: "set", level: "BACKWARD" } });
  expect(await f.service.apply(review.planId, "events")).toMatchObject({
    state: "acknowledged",
    observed: { subjectLevel: "BACKWARD" },
  });
  expect(f.write).toHaveBeenCalledTimes(1);
});
it.each(["set", "inherit"] as const)(
  "does not write when exact %s intent already holds",
  async (mode) => {
    const f = fixture();
    if (mode === "set") Object.assign(f.policy, { subjectLevel: "FULL", effectiveLevel: "FULL" });
    const review = await f.service.review({
      ...input,
      change: mode === "set" ? { mode, level: "FULL" } : { mode },
    });
    expect(parseSchemaPolicyOutcome(await f.service.apply(review.planId, "events"))).toMatchObject({
      state: "unchanged",
      verification: "verified",
      acknowledgedLevel: null,
    });
    expect(f.write).not.toHaveBeenCalled();
  },
);
it.each(["writer", "global", "subject"] as const)(
  "refuses stale %s without mutation",
  async (kind) => {
    const f = fixture(),
      review = await f.service.review(input);
    if (kind === "writer") f.schema.id = 9;
    else if (kind === "global")
      Object.assign(f.policy, { globalLevel: "FORWARD", effectiveLevel: "FORWARD" });
    else Object.assign(f.policy, { subjectLevel: "NONE", effectiveLevel: "NONE" });
    expect(await f.service.apply(review.planId, "events")).toMatchObject({
      state: "rejected",
      verification: "not-applicable",
    });
    expect(f.write).not.toHaveBeenCalled();
  },
);
it.each(["expiry", "connection", "invalidation"] as const)(
  "refuses %s after review",
  async (kind) => {
    const f = fixture(),
      review = await f.service.review(input);
    if (kind === "expiry") f.expire();
    else if (kind === "connection") f.revoke();
    else f.operations.invalidate();
    await expect(f.service.apply(review.planId, "events")).rejects.toThrow("expired");
    expect(f.write).not.toHaveBeenCalled();
  },
);
it("requires the existing latest writer and stops when preflight becomes unreadable", async () => {
  const f = fixture();
  await expect(
    f.service.review({ ...input, expectedWriter: { id: 9, version: 1 } }),
  ).rejects.toThrow("latest");
  const review = await f.service.review(input);
  f.load.mockRejectedValueOnce(new Error("secret response"));
  expect(await f.service.apply(review.planId, "events")).toMatchObject({ state: "rejected" });
  expect(f.write).not.toHaveBeenCalled();
  f.port.loadReviewSchema = (): Promise<SchemaVersionDetail | null> => Promise.resolve(null);
  await expect(f.service.review(input)).rejects.toThrow("existing");
});
it.each([400, 401, 403, 404, 409, 422])(
  "retains HTTP %s refusal without secret diagnostics or automatic retry",
  async (status) => {
    const f = fixture(),
      review = await f.service.review(input);
    f.write.mockRejectedValueOnce(Object.assign(new Error("credential-sentinel"), { status }));
    const result = parseSchemaPolicyOutcome(await f.service.apply(review.planId, "events"));
    expect(result.state).toBe("rejected");
    expect(JSON.stringify(result)).not.toContain("credential-sentinel");
    await f.service.apply(review.planId, "events");
    expect(f.write).toHaveBeenCalledTimes(1);
  },
);
it.each([503, undefined])(
  "retains unacknowledged HTTP %s as unknown and never retries it",
  async (status) => {
    const f = fixture(),
      review = await f.service.review(input);
    f.write.mockRejectedValueOnce(Object.assign(new Error("secret body"), { status }));
    const result = await f.service.apply(review.planId, "events");
    expect(result).toMatchObject({ state: "unknown", verification: "unavailable" });
    expect(JSON.stringify(result)).not.toContain("secret body");
    await f.service.apply(review.planId, "events");
    expect(f.write).toHaveBeenCalledTimes(1);
  },
);
it.each(["connection", "invalidation", "readback"] as const)(
  "preserves admitted acknowledgement after %s loss",
  async (kind) => {
    const f = fixture(),
      review = await f.service.review(input);
    f.write.mockImplementationOnce((): Promise<{ level: "FULL" }> => {
      if (kind === "connection") f.revoke();
      else if (kind === "invalidation") f.operations.invalidate();
      else f.load.mockRejectedValueOnce(new Error("readback failed"));
      return Promise.resolve({ level: "FULL" });
    });
    const result = parseSchemaPolicyOutcome(await f.service.apply(review.planId, "events"));
    expect(result).toMatchObject({
      state: "acknowledged",
      acknowledgedLevel: "FULL",
      verification: "unavailable",
      observed: null,
    });
    expect(await f.service.apply(review.planId, "events")).toEqual(result);
    expect(f.write).toHaveBeenCalledTimes(1);
  },
);
it("reports mismatch when external policy differs after admission", async () => {
  const f = fixture(),
    review = await f.service.review(input);
  f.write.mockResolvedValueOnce({ level: "FULL" });
  expect(parseSchemaPolicyOutcome(await f.service.apply(review.planId, "events"))).toMatchObject({
    state: "acknowledged",
    verification: "mismatch",
    observed: { subjectLevel: null },
  });
});
it("retains an inconsistent set acknowledgement as mismatch even if readback matches intent", async () => {
  const f = fixture(),
    review = await f.service.review(input);
  f.write.mockImplementationOnce((): Promise<{ level: "FORWARD" }> => {
    Object.assign(f.policy, { subjectLevel: "FULL", effectiveLevel: "FULL" });
    return Promise.resolve({ level: "FORWARD" });
  });
  expect(parseSchemaPolicyOutcome(await f.service.apply(review.planId, "events"))).toMatchObject({
    state: "acknowledged",
    verification: "mismatch",
    acknowledgedLevel: "FORWARD",
    observed: { subjectLevel: "FULL" },
  });
});
it("shares reviewed write admission with schema registration", async () => {
  const f = fixture();
  let finish: ((value: { id: number }) => void) | undefined;
  const register = vi.fn(
    () =>
      new Promise<{ id: number }>((resolve) => {
        finish = resolve;
      }),
  );
  const changes = new SchemaChangeService(
    () => f.scope,
    {
      ...f.port,
      register,
      checkProposedCompatibility: (): Promise<{
        compatible: boolean;
        messages: readonly string[];
      }> => Promise.resolve({ compatible: true, messages: [] }),
      loadLatestSubject: (): Promise<SchemaRegistrySubjectDetail> =>
        Promise.resolve({ schema: f.schema, versions: [1], compatibilityLevel: "BACKWARD" }),
      checkCompatibility: (): Promise<never> => Promise.reject(new Error("unused")),
      delete: (): Promise<never> => Promise.reject(new Error("unused")),
      listSubjects: (): Promise<never> => Promise.reject(new Error("unused")),
      loadSubject: (): Promise<never> => Promise.reject(new Error("unused")),
    },
    Date.now,
    f.operations,
  );
  const schemaReview = await changes.review({
      draft: {
        subject: f.schema.subject,
        schema: f.schema.schema,
        schemaType: f.schema.schemaType,
        references: f.schema.references,
        version: "latest",
        normalize: true,
      },
      expectedWriter: { id: 1, version: 1 },
    }),
    policyReview = await f.service.review(input);
  const pending = changes.apply(schemaReview.planId, "events");
  await vi.waitFor(() => expect(register).toHaveBeenCalledTimes(1));
  await expect(f.service.apply(policyReview.planId, "events")).rejects.toThrow("active");
  expect(f.write).not.toHaveBeenCalled();
  finish?.({ id: 1 });
  await pending;
});
it("bounds shared reads and aborts outstanding work on invalidation", async () => {
  const f = fixture();
  f.load.mockImplementation(
    (_context, _subject, signal) =>
      new Promise((_, reject) =>
        signal.addEventListener("abort", () => reject(new Error("aborted")), { once: true }),
      ),
  );
  const first = f.service.load("events"),
    second = f.service.load("events");
  await expect(f.service.load("events")).rejects.toThrow("Wait");
  await vi.waitFor(() => expect(f.load).toHaveBeenCalledTimes(2));
  const settled = Promise.allSettled([first, second]);
  f.operations.invalidate();
  expect((await settled).every((result) => result.status === "rejected")).toBe(true);
});
it("parses closed policy contracts and refuses inconsistent reviewed/acknowledged state", async () => {
  expect(() => parseSchemaPolicyInput({ ...input, extra: 1 })).toThrow();
  expect(() =>
    parseSchemaPolicyInput({ ...input, change: { mode: "inherit", level: "FULL" } }),
  ).toThrow();
  expect(() =>
    parseSchemaPolicyInput({ ...input, change: { mode: "set", level: "AUTO" } }),
  ).toThrow();
  const review = await fixture().service.review(input);
  expect(() =>
    parseSchemaPolicyReview({ ...review, after: { ...review.after, subjectLevel: null } }),
  ).toThrow();
  expect(() =>
    parseSchemaPolicyOutcome({
      state: "unknown",
      verification: "verified",
      acknowledgedLevel: null,
      observed: null,
      detail: "wrong",
    }),
  ).toThrow();
  for (const [command, payload] of [
    ["schemas.policy.load", { subject: "events" }],
    ["schemas.policy.review", input],
    ["schemas.policy.apply", { planId: review.planId, confirmation: "events" }],
  ] as const)
    expect(
      parseHostCommand({ command, payload, id: "policy", version: HOST_PROTOCOL_VERSION }).command,
    ).toBe(command);
  expect(
    parseHostCommandResponse({
      command: "schemas.policy.review",
      id: "policy",
      version: HOST_PROTOCOL_VERSION,
      ok: true,
      result: { correlationId: "policy", review },
    }),
  ).toMatchObject({ result: { review: { input } } });
});
it("uses subject-only PUT/DELETE with bounded acknowledgements and rejects advanced config", async () => {
  const requests: BoundedJsonHttpRequest[] = [];
  const adapter = new SchemaRegistryHttpAdapter({
    request: (request): Promise<BoundedJsonHttpResponse> => {
      requests.push(request);
      return Promise.resolve({ status: 200, body: { compatibility: "FULL" } });
    },
  });
  const context = {
      baseUrl: "http://registry",
      authorization: (): Promise<undefined> => Promise.resolve(undefined),
    },
    signal = AbortSignal.timeout(1000);
  expect(
    await adapter.changeSubjectCompatibility(
      context,
      "events 1",
      { mode: "set", level: "FULL" },
      signal,
    ),
  ).toEqual({ level: "FULL" });
  expect(
    await adapter.changeSubjectCompatibility(context, "events 1", { mode: "inherit" }, signal),
  ).toEqual({ level: "FULL" });
  expect(
    requests.map(({ method, url, body }) => ({
      method,
      url,
      ...(body === undefined ? {} : { body }),
    })),
  ).toEqual([
    { method: "PUT", url: "http://registry/config/events%201", body: { compatibility: "FULL" } },
    { method: "DELETE", url: "http://registry/config/events%201" },
  ]);
  const advanced = new SchemaRegistryHttpAdapter({
    request: (): Promise<BoundedJsonHttpResponse> =>
      Promise.resolve({ status: 200, body: { compatibilityLevel: "BACKWARD", normalize: true } }),
  });
  await expect(advanced.loadCompatibilityPolicy(context, "events", signal)).rejects.toThrow(
    "Advanced",
  );
});
