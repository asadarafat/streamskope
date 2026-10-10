import { expect, it, vi, type Mock } from "vitest";

import { SchemaChangeService } from "../../src/features/kafka/application/schema-change-service";
import type { ReviewedSchemaRegistryPort } from "../../src/features/kafka/application/schema-change-baseline";
import type { SchemaRegistryReviewScope } from "../../src/features/kafka/application/connection-scope";
import {
  parseSchemaChangeInput,
  parseSchemaChangeReview,
  parseSchemaChangeOutcome,
  SCHEMA_CHANGE_LIMITS,
  type SchemaChangeInput,
} from "../../src/features/kafka/contracts/schema-changes";
import type { SchemaVersionDetail } from "../../src/features/kafka/contracts";
import { SchemaRegistryHttpAdapter } from "../../src/features/kafka/engine/schema-registry-http";
import type {
  BoundedJsonHttpRequest,
  BoundedJsonHttpResponse,
} from "../../src/features/kafka/engine/bounded-json-http";

const old: SchemaVersionDetail = {
  subject: "events",
  version: 1,
  id: 1,
  schema: '"string"',
  schemaType: "AVRO",
  references: [],
};
const input: SchemaChangeInput = {
  draft: {
    subject: "events",
    version: "latest",
    schema: '"int"',
    references: [],
    normalize: true,
    schemaType: "AVRO",
  },
  expectedWriter: { id: 1, version: 1 },
};
function fixture(): {
  service: SchemaChangeService;
  port: ReviewedSchemaRegistryPort;
  register: Mock<ReviewedSchemaRegistryPort["register"]>;
  load: Mock<ReviewedSchemaRegistryPort["loadReviewSchema"]>;
  compatible: Mock<ReviewedSchemaRegistryPort["checkProposedCompatibility"]>;
  policy: {
    globalLevel: "BACKWARD";
    subjectLevel: "FULL" | null;
    effectiveLevel: "BACKWARD" | "FULL";
  };
  schemas: Map<string, SchemaVersionDetail>;
  revoke(): void;
  expire(): void;
} {
  let current = true,
    now = Date.now();
  const context = {
    baseUrl: "http://registry",
    authorization: (): Promise<undefined> => Promise.resolve(undefined),
  };
  const schemas = new Map<string, SchemaVersionDetail>([["events", old]]);
  const policy = {
    globalLevel: "BACKWARD" as const,
    subjectLevel: null as "FULL" | null,
    effectiveLevel: "BACKWARD" as "BACKWARD" | "FULL",
  };
  const load = vi.fn<ReviewedSchemaRegistryPort["loadReviewSchema"]>((_context, identity) =>
    Promise.resolve(schemas.get(identity.subject) ?? null),
  );
  const register = vi.fn<ReviewedSchemaRegistryPort["register"]>(() => {
    schemas.set("events", { ...old, version: 2, id: 2, schema: input.draft.schema });
    return Promise.resolve({ id: 2 });
  });
  const compatible = vi.fn<ReviewedSchemaRegistryPort["checkProposedCompatibility"]>(() =>
    Promise.resolve({ compatible: true, messages: [] }),
  );
  const port = {
    loadReviewSchema: load,
    loadCompatibilityPolicy: (): Promise<typeof policy> => Promise.resolve({ ...policy }),
    checkProposedCompatibility: compatible,
    register,
    loadLatestSubject: (): Promise<{
      schema: SchemaVersionDetail;
      versions: number[];
      compatibilityLevel: string;
    }> => Promise.resolve({ schema: old, versions: [1], compatibilityLevel: "BACKWARD" }),
    checkCompatibility: (): Promise<never> =>
      Promise.reject(new Error("Unused legacy compatibility")),
    delete: (): Promise<never> => Promise.reject(new Error("Unused deletion")),
    listSubjects: (): Promise<never> => Promise.reject(new Error("Unused inventory")),
    loadSubject: (): Promise<never> => Promise.reject(new Error("Unused detail")),
  } satisfies ReviewedSchemaRegistryPort;
  const scope: SchemaRegistryReviewScope = {
    connectionName: "Local",
    isCurrent: () => current,
    read: async (run, signal) => {
      signal.throwIfAborted();
      if (!current) throw new Error("Connection changed");
      const value = await run(context, signal);
      if (!current) throw new Error("Connection changed");
      return value;
    },
    tryDispatch: (run) => (current ? { started: true, result: run(context) } : { started: false }),
  };
  const service = new SchemaChangeService(
    () => (current ? scope : null),
    port,
    () => now,
  );
  return {
    service,
    port,
    register,
    load,
    compatible,
    policy,
    schemas,
    revoke: (): void => {
      current = false;
    },
    expire: (): void => {
      now += 120001;
    },
  };
}
it("reviews without writing, then confirms, coalesces and reads back one registration", async () => {
  const f = fixture(),
    review = parseSchemaChangeReview(await f.service.review(input));
  expect(f.register).not.toHaveBeenCalled();
  await expect(f.service.apply(review.planId, "wrong")).rejects.toThrow("Confirm");
  const first = f.service.apply(review.planId, "events");
  const second = f.service.apply(review.planId, "events");
  expect(first).toBe(second);
  expect(parseSchemaChangeOutcome(await first)).toMatchObject({
    state: "acknowledged",
    verification: "verified",
    id: 2,
    observed: { id: 2, version: 2 },
  });
  expect(f.register).toHaveBeenCalledTimes(1);
  expect(await f.service.apply(review.planId, "events")).toEqual(await first);
});
it.each(["writer", "policy", "reference"] as const)(
  "refuses changed %s before dispatch",
  async (kind) => {
    const f = fixture();
    const dependency = { ...old, subject: "dependency", id: 8 };
    f.schemas.set("dependency", dependency);
    const draft = {
      ...input,
      draft: { ...input.draft, references: [{ name: "dep", subject: "dependency", version: 1 }] },
    };
    const review = await f.service.review(draft);
    if (kind === "writer") f.schemas.set("events", { ...old, id: 3 });
    if (kind === "policy") {
      f.policy.subjectLevel = "FULL";
      f.policy.effectiveLevel = "FULL";
    }
    if (kind === "reference") f.schemas.set("dependency", { ...dependency, id: 9 });
    expect(await f.service.apply(review.planId, "events")).toMatchObject({
      state: "rejected",
      verification: "not-applicable",
    });
    expect(f.register).not.toHaveBeenCalled();
  },
);
it.each(["expiry", "connection", "invalidation"] as const)(
  "refuses %s after review",
  async (kind) => {
    const f = fixture(),
      review = await f.service.review(input);
    if (kind === "expiry") f.expire();
    if (kind === "connection") f.revoke();
    if (kind === "invalidation") f.service.invalidate();
    await expect(f.service.apply(review.planId, "events")).rejects.toThrow("expired");
    expect(f.register).not.toHaveBeenCalled();
  },
);
it("rejects historical selection and new-subject collision", async () => {
  const f = fixture();
  await expect(
    f.service.review({ ...input, expectedWriter: { id: 99, version: 1 } }),
  ).rejects.toThrow("latest");
  await expect(f.service.review({ ...input, expectedWriter: null })).rejects.toThrow("latest");
  expect(f.register).not.toHaveBeenCalled();
});
it("treats absent subject as no baseline, then relies on Registry validation", async () => {
  const f = fixture();
  f.schemas.delete("events");
  const review = await f.service.review({ ...input, expectedWriter: null });
  expect(review.before).toBeNull();
  expect(f.compatible).not.toHaveBeenCalled();
  f.register.mockRejectedValueOnce({ status: 422, message: "private-schema-default" });
  const result = await f.service.apply(review.planId, "events");
  expect(result.state).toBe("rejected");
  expect(JSON.stringify(result)).not.toContain("private-schema-default");
});
it.each([401, 403, 400, 409, 422, 500, null])(
  "classifies registration failure %s without retry or diagnostic reflection",
  async (status) => {
    const f = fixture(),
      review = await f.service.review(input);
    f.register.mockRejectedValue({ status, message: "private-schema-default" });
    const result = await f.service.apply(review.planId, "events");
    expect(result.state).toBe(
      [401, 403, 400, 409, 422].includes(status ?? 0) ? "rejected" : "unknown",
    );
    expect(JSON.stringify(result)).not.toContain("private-schema-default");
    await f.service.apply(review.planId, "events");
    expect(f.register).toHaveBeenCalledTimes(1);
  },
);
it.each(["connection", "invalidate", "readback"] as const)(
  "preserves acknowledged ID after %s ends read authority",
  async (kind) => {
    const f = fixture(),
      review = await f.service.review(input);
    f.register.mockImplementation(() => {
      if (kind === "connection") f.revoke();
      if (kind === "invalidate") f.service.invalidate();
      if (kind === "readback") f.load.mockRejectedValue(new Error("private-schema-default"));
      return Promise.resolve({ id: 2 });
    });
    expect(await f.service.apply(review.planId, "events")).toMatchObject({
      state: "acknowledged",
      id: 2,
      verification: "unavailable",
    });
  },
);
it("reports concurrent readback mismatch while preserving acknowledgement", async () => {
  const f = fixture(),
    review = await f.service.review(input);
  f.register.mockResolvedValue({ id: 9 });
  expect(await f.service.apply(review.planId, "events")).toMatchObject({
    state: "acknowledged",
    id: 9,
    verification: "mismatch",
    observed: { id: 1 },
  });
});
it("stops incompatible review and failed preflight without a write", async () => {
  const f = fixture();
  f.compatible.mockResolvedValueOnce({ compatible: false, messages: [] });
  const review = await f.service.review(input);
  expect(review.compatible).toBe(false);
  await expect(f.service.apply(review.planId, "events")).rejects.toThrow("Confirm");
  const compatible = await f.service.review(input);
  f.load.mockRejectedValueOnce(new Error("private-schema-default"));
  expect(await f.service.apply(compatible.planId, "events")).toMatchObject({ state: "rejected" });
  expect(f.register).not.toHaveBeenCalled();
});
it("rechecks compatibility after review", async () => {
  const f = fixture(),
    review = await f.service.review(input);
  f.compatible.mockResolvedValueOnce({ compatible: false, messages: [] });
  expect(await f.service.apply(review.planId, "events")).toMatchObject({ state: "rejected" });
  expect(f.register).not.toHaveBeenCalled();
});
it("rejects state changed while compatibility is being checked", async () => {
  const f = fixture(),
    review = await f.service.review(input);
  f.compatible.mockImplementationOnce(() => {
    f.schemas.set("events", { ...old, id: 9 });
    return Promise.resolve({ compatible: true, messages: [] });
  });
  const result = await f.service.apply(review.planId, "events");
  expect(result.state).toBe("rejected");
  expect(result.detail).toContain("during compatibility");
  expect(f.register).not.toHaveBeenCalled();
});
it("rejects oversized UTF8 drafts and inconsistent receipts", () => {
  expect(() =>
    parseSchemaChangeInput({
      ...input,
      draft: { ...input.draft, schema: "ø".repeat(SCHEMA_CHANGE_LIMITS.draftBytes) },
    }),
  ).toThrow("128 KiB");
  expect(() =>
    parseSchemaChangeInput({ ...input, draft: { ...input.draft, version: 1 } }),
  ).toThrow();
  expect(() =>
    parseSchemaChangeOutcome({
      state: "unknown",
      verification: "verified",
      id: 4,
      observed: old,
      detail: "bad",
    }),
  ).toThrow();
});
it("refuses oversized Registry snapshots and excessive reference graphs without writing", async () => {
  const f = fixture();
  f.schemas.set("events", {
    ...old,
    schema: `"${"x".repeat(SCHEMA_CHANGE_LIMITS.snapshotBytes)}"`,
  });
  await expect(f.service.review(input)).rejects.toThrow("512 KiB");
  f.schemas.set("events", old);
  const references = Array.from({ length: 32 }, (_, index) => ({
    name: `d${String(index)}`,
    subject: `dep${String(index)}`,
    version: 1,
  }));
  for (const reference of references)
    f.schemas.set(reference.subject, {
      ...old,
      subject: reference.subject,
      id: 10 + Number(reference.subject.slice(3)),
    });
  await expect(
    f.service.review({ ...input, draft: { ...input.draft, references } }),
  ).rejects.toThrow("node bound");
  expect(f.register).not.toHaveBeenCalled();
});
it("uses pinned-version compatibility and fails closed on advanced policy configuration", async () => {
  const requests: BoundedJsonHttpRequest[] = [];
  let advanced = false;
  const adapter = new SchemaRegistryHttpAdapter({
    request: (request): Promise<BoundedJsonHttpResponse> => {
      requests.push(request);
      const path = new URL(request.url).pathname;
      return Promise.resolve({
        status: path === "/config/events" ? 404 : 200,
        body:
          path === "/config/events"
            ? { error_code: 40408 }
            : path === "/config"
              ? {
                  compatibilityLevel: "FULL_TRANSITIVE",
                  ...(advanced ? { alias: "other-subject" } : {}),
                }
              : { is_compatible: false, messages: ["private-default"] },
      });
    },
  });
  const context = {
    baseUrl: "http://registry",
    authorization: (): Promise<undefined> => Promise.resolve(undefined),
  };
  expect(
    await adapter.loadCompatibilityPolicy(context, "events", AbortSignal.timeout(1000)),
  ).toEqual({
    globalLevel: "FULL_TRANSITIVE",
    subjectLevel: null,
    effectiveLevel: "FULL_TRANSITIVE",
  });
  expect(
    await adapter.checkProposedCompatibility(
      context,
      input.draft,
      [1, 2],
      AbortSignal.timeout(1000),
    ),
  ).toEqual({ compatible: false, messages: [] });
  expect(requests.at(-1)?.url).toBe(
    "http://registry/compatibility/subjects/events/versions/1?verbose=true&normalize=true",
  );
  advanced = true;
  await expect(
    adapter.loadCompatibilityPolicy(context, "events", AbortSignal.timeout(1000)),
  ).rejects.toThrow("Advanced");
});
