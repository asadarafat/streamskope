import { expect, it, vi } from "vitest";

import {
  createHarness,
  deferred,
  scopedMaterialInput,
} from "../support/trust-acquisition-application-fixture";
import type { HttpsTrustMaterialFetchInput } from "../../src/features/kafka/contracts/remote-trust-types";
import { parseRemoteTrustAcquisitionSummary } from "../../src/features/kafka/contracts/remote-trust-validation";

async function input(
  harness: ReturnType<typeof createHarness>,
): Promise<HttpsTrustMaterialFetchInput> {
  const catalog = await harness.templates.recipes.create({
    name: "API certificates",
    method: "https",
    syntax: "named-v1",
    kind: "pem",
    parameters: [],
    timeoutSeconds: 30,
    oauth: { endpoint: "https://localhost/token", clientId: "kafka-client", scope: "kafka" },
    https: {
      authentication: "none",
      material: {
        url: "https://localhost/certificates",
        headers: [],
        query: [],
        extraction: { mode: "raw" },
      },
      password: { source: "none" },
    },
  });
  const recipe = catalog.recipes.find((entry) => entry.name === "API certificates");
  if (recipe === undefined) throw new Error("Missing fixture recipe");
  return {
    kind: "pem",
    label: "API CA",
    editor: harness.service.openEditor(),
    recipe: {
      mode: "replace",
      recipeId: recipe.id,
      recipeRevision: recipe.revision,
      overrides: {},
    },
    api: { host: "", authentication: { mode: "none" }, tls: { mode: "system" } },
  };
}

it("uses shared candidate review, expiry and consumption without fabricating SSH identity", async () => {
  const harness = createHarness({
    https: { fetch: () => Promise.resolve({ bytes: new TextEncoder().encode("fixture-ca") }) },
  });
  const request = await input(harness);
  expect(harness.service.capabilities()).toMatchObject({ methods: ["ssh", "https"] });
  const result = await harness.service.fetchHttpsMaterial(request);
  expect(result.target).toEqual({ host: "localhost", port: 443, origin: "https://localhost" });
  expect(result.material?.kind).toBe("pem");
  expect(result.oauth).toEqual({
    endpoint: "https://localhost/token",
    clientId: "kafka-client",
    scope: "kafka",
  });
  expect(parseRemoteTrustAcquisitionSummary(result, "result")).toEqual(result);
  harness.service.apply(result.id, request.editor.id);
  expect(harness.service.resolve(result.id, "pem", request.editor.id).identity).toBeUndefined();
  expect(harness.service.resolve(result.id, "pem", request.editor.id).material).toBe("fixture-ca");
  expect(harness.remote.materialCalls).toEqual([]);
  harness.service.consume(result.id);
  expect(() => harness.service.resolve(result.id, "pem", request.editor.id)).toThrow();
});

it("does not advertise HTTPS when the transport is unavailable", () => {
  expect(createHarness().service.capabilities()).toMatchObject({ methods: ["ssh"] });
});

it("suppresses late HTTPS results when the shared editor is cancelled", async () => {
  const started = deferred<AbortSignal>();
  const finish = deferred<{ bytes: Uint8Array }>();
  const harness = createHarness({
    https: {
      fetch: (request) => {
        started.resolve(request.signal);
        return finish.promise;
      },
    },
  });
  const request = await input(harness);
  const pending = harness.service.fetchHttpsMaterial(request, undefined, "https-request");
  const rejection = expect(pending).rejects.toThrow();
  const signal = await started.promise;
  harness.service.closeEditor(request.editor.id);
  expect(signal.aborted).toBe(true);
  finish.resolve({ bytes: new TextEncoder().encode("late-ca") });
  await rejection;
  expect(harness.decode).not.toHaveBeenCalled();
});

it("shares the eight-candidate limit across SSH/HTTPS and releases discarded and expired slots", async () => {
  const harness = createHarness({
    https: { fetch: () => Promise.resolve({ bytes: new TextEncoder().encode("fixture-ca") }) },
  });
  const request = await input(harness);
  const candidates = [];
  for (let index = 0; index < 4; index += 1) {
    const editor = request.editor;
    candidates.push(
      await harness.service.fetchMaterial(await scopedMaterialInput(harness.service, editor)),
    );
    candidates.push(await harness.service.fetchHttpsMaterial(request));
  }
  await expect(harness.service.fetchHttpsMaterial(request)).rejects.toMatchObject({
    code: "ACQUISITION_CAPACITY",
  });
  const first = candidates[0]!;
  harness.service.discard(first.id, first.editor?.id);
  const replacement = await harness.service.fetchHttpsMaterial(request);
  harness.service.apply(replacement.id, replacement.editor!.id);
  harness.advance(10 * 60_000 + 1);
  expect(() => harness.service.resolve(replacement.id, "pem", replacement.editor!.id)).toThrow();
  await expect(
    harness.service.fetchHttpsMaterial({ ...request, editor: harness.service.openEditor() }),
  ).resolves.toMatchObject({ material: { kind: "pem" } });
  harness.service.clear();
});

it("enforces the shared deadline while the HTTPS transport is pending", async () => {
  vi.useFakeTimers();
  const started = deferred<AbortSignal>();
  const harness = createHarness({
    https: {
      fetch: ({ signal }) =>
        new Promise((_resolve, reject) => {
          started.resolve(signal);
          signal.addEventListener(
            "abort",
            () =>
              reject(
                signal.reason instanceof Error ? signal.reason : new Error("Transport aborted"),
              ),
            {
              once: true,
            },
          );
        }),
    },
  });
  try {
    const request = await input(harness);
    const pending = harness.service.fetchHttpsMaterial(request);
    const rejected = expect(pending).rejects.toMatchObject({ code: "TIMEOUT" });
    const signal = await started.promise;
    await vi.advanceTimersByTimeAsync(30_001);
    await rejected;
    expect(signal.aborted).toBe(true);
    expect(harness.decode).not.toHaveBeenCalled();
  } finally {
    harness.service.clear();
    vi.useRealTimers();
  }
});
