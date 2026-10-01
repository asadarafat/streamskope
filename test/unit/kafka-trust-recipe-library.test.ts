import { describe, expect, it } from "vitest";

import { DEFAULT_CONNECTION_TEMPLATE_DOCUMENT } from "../support/legacy-template-document";
import { createRecipeLibrary, LegacyTemplateFixture } from "../support/recipe-library";
import type { TrustAcquisitionRecipeDocument } from "../../src/features/kafka/contracts";
import { InMemoryKafkaTrustRecipeStore } from "../../src/features/kafka/application/trust-recipe-store";
import { trustRecipeInput } from "../support/trust-recipe";
import { resolveTrustRecipeExecution } from "../../src/features/kafka/application/trust-recipe-execution";

const capability = { durability: "session", state: "ready" } as const;

function library(
  store = new InMemoryKafkaTrustRecipeStore(capability, { version: 1, recipes: [] }),
): ReturnType<typeof createRecipeLibrary> {
  let next = 0;
  return createRecipeLibrary(new LegacyTemplateFixture(capability), {
    store,
    createId: () => `recipe-${++next}`,
  });
}

describe("Connection template recipe library", () => {
  it("preserves both stores after interrupted conversion and commits a retry once", async () => {
    class InterruptedStore extends InMemoryKafkaTrustRecipeStore {
      fail = true;
      override commit(
        document: TrustAcquisitionRecipeDocument,
        signal?: AbortSignal,
      ): Promise<void> {
        return this.fail
          ? Promise.reject(new Error("interrupted write"))
          : super.commit(document, signal);
      }
    }
    const legacy = new LegacyTemplateFixture(capability, DEFAULT_CONNECTION_TEMPLATE_DOCUMENT);
    const store = new InterruptedStore(capability, { version: 1, recipes: [] });
    const service = createRecipeLibrary(legacy, { store });
    const selection = {
      name: "Retry conversion",
      kind: "jks" as const,
      materialName: "nsp-25-4",
      passwordName: "nsp-25-11",
      oauthName: "nsp-25-4",
    };
    await expect(service.convertLegacy(selection)).rejects.toMatchObject({
      code: "TEMPLATE_STORE_UNAVAILABLE",
    });
    expect((await service.list()).recipes).toEqual([]);
    expect(legacy.document()).toEqual(DEFAULT_CONNECTION_TEMPLATE_DOCUMENT);
    store.fail = false;
    const restarted = createRecipeLibrary(legacy, { store });
    const result = await restarted.convertLegacy(selection);
    expect(result.recipes).toHaveLength(1);
    expect(await restarted.convertLegacy(selection)).toEqual(result);
    const abort = new AbortController();
    abort.abort();
    await expect(
      restarted.convertLegacy({ ...selection, name: "Cancelled" }, abort.signal),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(await restarted.list()).toEqual(result);
  });
  it("converts only an explicitly selected legacy combination and retries without duplicates", async () => {
    const legacy = new LegacyTemplateFixture(capability, DEFAULT_CONNECTION_TEMPLATE_DOCUMENT);
    const recipes = new InMemoryKafkaTrustRecipeStore(capability, { version: 1, recipes: [] });
    const service = createRecipeLibrary(legacy, { store: recipes });
    expect((await service.list()).recipes).toEqual([]);
    expect(await service.legacy()).toEqual(DEFAULT_CONNECTION_TEMPLATE_DOCUMENT);
    const input = {
      name: "Reviewed recipe",
      kind: "jks" as const,
      materialName: "nsp-25-4",
      passwordName: "nsp-25-11",
      oauthName: "nsp-25-4",
    };
    const result = await service.convertLegacy(input);
    expect(result.recipes).toHaveLength(1);
    expect(result.recipes[0]).toMatchObject({
      name: "Reviewed recipe",
      kind: "jks",
      syntax: "legacy-v1",
      ssh: { source: "legacy-tempfile" },
    });
    expect(result.recipes[0]?.ssh?.value).toBe(
      DEFAULT_CONNECTION_TEMPLATE_DOCUMENT.catalogs[0]?.entries[0]?.template,
    );
    expect(await service.convertLegacy(input)).toEqual(result);
    const restarted = createRecipeLibrary(legacy, { store: recipes });
    expect(await restarted.convertLegacy(input)).toEqual(result);
    expect(legacy.document()).toEqual(DEFAULT_CONNECTION_TEMPLATE_DOCUMENT);
    await expect(
      service.convertLegacy({ ...input, materialName: "missing" }),
    ).rejects.toMatchObject({ code: "TEMPLATE_NOT_FOUND" });
  });
  it("creates, renames and duplicates with immutable identity and no-op revisions", async () => {
    const service = library();
    const first = await service.create(trustRecipeInput());
    expect(first.recipes).toEqual([{ ...trustRecipeInput(), id: "recipe-1", revision: 1 }]);
    const renamed = await service.update("recipe-1", 1, { ...trustRecipeInput(), name: "Renamed" });
    expect(renamed.recipes[0]).toMatchObject({ id: "recipe-1", revision: 2, name: "Renamed" });
    expect(await service.update("recipe-1", 2, { ...trustRecipeInput(), name: "Renamed" })).toEqual(
      renamed,
    );
    expect((await service.duplicate("recipe-1", 2, "Copy")).recipes).toHaveLength(2);
    expect((await service.list()).recipes[1]).toMatchObject({
      id: "recipe-2",
      revision: 1,
      name: "Copy",
    });
    expect(first.recipes[0]?.name).toBe("Certificate file");
  });

  it("rejects concurrent stale writes and equivalent names without overwriting", async () => {
    const service = library();
    await service.create(trustRecipeInput());
    const results = await Promise.allSettled([
      service.update("recipe-1", 1, { ...trustRecipeInput(), name: "First" }),
      service.update("recipe-1", 1, { ...trustRecipeInput(), name: "Second" }),
    ]);
    expect(results.map((result) => result.status)).toEqual(["fulfilled", "rejected"]);
    await expect(service.create({ ...trustRecipeInput(), name: " ＦIRST " })).rejects.toMatchObject(
      { code: "TEMPLATE_DUPLICATE" },
    );
    expect((await service.list()).recipes[0]).toMatchObject({ name: "First", revision: 2 });
    await expect(service.delete("recipe-1", 1)).rejects.toMatchObject({ code: "VALIDATION" });
  });

  it("publishes no failed write and allows an explicit retry", async () => {
    class FailingStore extends InMemoryKafkaTrustRecipeStore {
      fail = false;
      override commit(
        document: TrustAcquisitionRecipeDocument,
        signal?: AbortSignal,
      ): Promise<void> {
        if (this.fail) return Promise.reject(new Error("private storage detail"));
        return super.commit(document, signal);
      }
    }
    const store = new FailingStore(capability, { version: 1, recipes: [] });
    const service = library(store);
    const before = await service.create(trustRecipeInput());
    store.fail = true;
    await expect(
      service.update("recipe-1", 1, { ...trustRecipeInput(), name: "Failed" }),
    ).rejects.toMatchObject({ code: "TEMPLATE_STORE_UNAVAILABLE" });
    expect(service.currentSnapshot()).toEqual(before);
    store.fail = false;
    await expect(
      service.update("recipe-1", 1, { ...trustRecipeInput(), name: "Retry" }),
    ).resolves.toMatchObject({ recipes: [{ name: "Retry", revision: 2 }] });
  });

  it("rejects aborted operations and never reseeds an intentionally empty library", async () => {
    const store = new InMemoryKafkaTrustRecipeStore(capability, { version: 1, recipes: [] });
    const service = library(store);
    const controller = new AbortController();
    controller.abort();
    await expect(service.create(trustRecipeInput(), controller.signal)).rejects.toMatchObject({
      name: "AbortError",
    });
    await service.create(trustRecipeInput());
    await service.delete("recipe-1", 1);
    expect((await library(store).list()).recipes).toEqual([]);
  });

  it("seeds generic and NSP recipes only when neither store existed independently of legacy storage", async () => {
    const service = library(new InMemoryKafkaTrustRecipeStore(capability));
    const snapshot = await service.list();
    expect(snapshot.recipes.map((recipe) => [recipe.name, recipe.kind])).toEqual([
      ["SSH certificate file", "pem"],
      ["SSH truststore", "jks"],
      ["nsp-26-04", "jks"],
    ]);
    expect(snapshot.recipes.slice(0, 2).every((recipe) => recipe.oauth === undefined)).toBe(true);
    expect((await service.list()).recipes).toEqual(snapshot.recipes);
  });

  it("plans the confirmed NSP kubectl commands without credentials or temporary placeholders", async () => {
    const service = library(new InMemoryKafkaTrustRecipeStore(capability));
    const snapshot = await service.list();
    const preset = snapshot.recipes.find((recipe) => recipe.name === "nsp-26-04");
    expect(preset).toBeDefined();
    if (preset === undefined) throw new Error("NSP starter missing");
    expect(resolveTrustRecipeExecution(preset, {}, {}, "nsp.example.test")).toEqual({
      material: {
        source: "stdout",
        value:
          "kubectl exec -n nsp-psa-restricted $(kubectl get pods -n nsp-psa-restricted -o name | grep -m1 nspos-tomcat | awk -F/ '{print $2}') -- cat /opt/nsp/os/ssl/nsp.truststore",
      },
      password: {
        source: "command",
        command:
          "kubectl get secret -o jsonpath='{.data.truststore-pass}' -n nsp-psa-restricted nsp-tls-truststore-pass-nspdeployer | base64 -d; echo",
      },
      oauth: {
        endpoint: "https://nsp.example.test/rest-gateway/rest/api/v1/auth/token",
        clientId: "",
        scope: "",
      },
    });
  });

  it("isolates returned snapshots from mutations and reports unavailable data without defaults", async () => {
    const service = library();
    await service.create(trustRecipeInput());
    const result = await service.list();
    Reflect.set(result.recipes[0] ?? {}, "name", "Caller mutation");
    expect((await service.list()).recipes[0]?.name).toBe("Certificate file");
    const unavailable = new InMemoryKafkaTrustRecipeStore({
      durability: "durable",
      state: "unavailable",
    });
    expect(await library(unavailable).list()).toMatchObject({
      recipes: [],
      store: { state: "unavailable" },
    });
    await expect(library(unavailable).create(trustRecipeInput())).rejects.toMatchObject({
      code: "TEMPLATE_STORE_UNAVAILABLE",
    });
  });
});
