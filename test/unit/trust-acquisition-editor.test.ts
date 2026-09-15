import { describe, expect, it, vi } from "vitest";

import {
  createHarness,
  deferred,
  scopedMaterialInput,
  target,
} from "../support/trust-acquisition-application-fixture";
import { trustRecipeInput } from "../support/trust-recipe";

describe("Trust acquisition editor ownership and identity", () => {
  it("shares the recipe budget with discovery but excludes human identity review time", async () => {
    vi.useFakeTimers();
    try {
      const harness = createHarness();
      const catalog = await harness.templates.recipes.create({
        ...trustRecipeInput(),
        timeoutSeconds: 1,
      });
      const recipe = catalog.recipes[0]!;
      vi.spyOn(harness.remote, "discoverHostKey").mockImplementation(() => {
        harness.advance(400);
        return Promise.resolve(target.hostKeyFingerprint);
      });
      const editor = harness.service.openEditor();
      const discovery = await harness.service.discoverHostKey({ editor, target });
      harness.advance(30_000);
      const entered = deferred<void>();
      let signal: AbortSignal | undefined;
      vi.spyOn(harness.remote, "fetchMaterial").mockImplementation((_, operationSignal) => {
        signal = operationSignal;
        entered.resolve();
        return new Promise((_, reject) =>
          operationSignal?.addEventListener(
            "abort",
            () =>
              reject(
                operationSignal.reason instanceof Error
                  ? operationSignal.reason
                  : new Error("Aborted"),
              ),
            { once: true },
          ),
        );
      });
      const pending = harness.service.fetchMaterial({
        kind: "pem",
        label: "ca",
        target,
        editor,
        identityId: discovery.review!.id,
        acceptIdentity: true,
        recipe: {
          mode: "replace",
          recipeId: recipe.id,
          recipeRevision: recipe.revision,
          overrides: { certificate_path: "/cert.pem" },
        },
      });
      const outcome = pending.then(
        () => null,
        (error: unknown) => error,
      );
      await entered.promise;
      await vi.advanceTimersByTimeAsync(599);
      expect(signal?.aborted).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      const abortedAtBudget = signal?.aborted;
      await vi.advanceTimersByTimeAsync(400);
      expect(await outcome).toMatchObject({ code: "TIMEOUT" });
      expect(abortedAtBudget).toBe(true);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });
  it("requires first-use acceptance of a host-owned identity challenge before authentication", async () => {
    const { service, remote } = createHarness();
    const editor = service.openEditor();
    const discovered = await service.discoverHostKey({ editor, target });
    expect(discovered.review?.confirmationRequired).toBe(true);
    const input = {
      kind: "jks" as const,
      label: "trust",
      target,
      editor,
      identityId: discovered.review!.id,
    };
    await expect(service.fetchMaterial(input)).rejects.toThrow("Accept");
    expect(remote.passwordCalls).toEqual([]);
    expect(remote.materialCalls).toEqual([]);
    await expect(service.fetchMaterial({ ...input, acceptIdentity: true })).resolves.toMatchObject({
      material: { kind: "jks" },
    });
    await expect(service.fetchMaterial({ ...input, acceptIdentity: true })).rejects.toThrow();
  });
  it("rejects expired and cross-editor identity challenges and blocks a changed saved identity", async () => {
    const { service, remote, advance } = createHarness();
    const editor = service.openEditor();
    const discovered = await service.discoverHostKey({ editor, target });
    const input = {
      kind: "jks" as const,
      label: "trust",
      target,
      editor,
      identityId: discovered.review!.id,
      acceptIdentity: true,
    };
    await expect(
      service.fetchMaterial({ ...input, editor: service.openEditor() }),
    ).rejects.toThrow();
    advance(120_001);
    await expect(service.fetchMaterial(input)).rejects.toThrow();
    const known = service.openEditor({
      host: target.host,
      port: target.port,
      fingerprint: target.hostKeyFingerprint,
    });
    expect(
      (await service.discoverHostKey({ editor: known, target })).review?.confirmationRequired,
    ).toBe(false);
    remote.fingerprintResult = `SHA256:${"B".repeat(43)}`;
    await expect(service.discoverHostKey({ editor: known, target })).rejects.toMatchObject({
      code: "SSH_IDENTITY",
    });
    expect(remote.passwordCalls).toEqual([]);
    expect(remote.materialCalls).toEqual([]);
  });
  it("returns bounded recipe provenance and expanded OAuth suggestions only with a complete candidate", async () => {
    const harness = createHarness();
    const catalog = await harness.templates.recipes.create({
      ...trustRecipeInput(),
      oauth: { endpoint: "https://{{host}}/token", clientId: "operator", scope: "kafka" },
    });
    const recipe = catalog.recipes.find((item) => item.name === "Certificate file")!;
    const result = await harness.service.fetchMaterial({
      kind: "pem",
      label: "ca.pem",
      target,
      recipe: {
        mode: "replace",
        recipeId: recipe.id,
        recipeRevision: recipe.revision,
        overrides: { certificate_path: "/private/location.pem" },
      },
    });
    expect(result).toMatchObject({
      createdAt: "2026-07-26T13:00:00.000Z",
      recipe: { id: recipe.id, revision: recipe.revision, source: "file" },
      oauth: {
        endpoint: "https://kafka-lab.example.test/token",
        clientId: "operator",
        scope: "kafka",
      },
    });
    expect(JSON.stringify(result)).not.toContain("/private/location.pem");
  });
  it("scopes candidates to their initiating editor and releases them on close", async () => {
    const { service } = createHarness();
    const owner = service.openEditor();
    const other = service.openEditor();
    const result = await service.fetchMaterial(await scopedMaterialInput(service, owner));
    expect(() => service.resolve(result.id, "jks")).toThrow();
    expect(() => service.resolve(result.id, "jks", other.id)).toThrow();
    expect(() => service.discard(result.id, other.id)).toThrow();
    expect(service.resolve(result.id, "jks", owner.id).kind).toBe("jks");
    service.closeEditor(owner.id);
    expect(() => service.resolve(result.id, "jks", owner.id)).toThrow();
    expect(() => service.closeEditor(owner.id)).not.toThrow();
  });

  it("rejects a late decoder completion after the editor generation changes", async () => {
    const { service, decode } = createHarness();
    const editor = service.openEditor();
    let release!: (value: { kind: "jks"; caPem: string }) => void;
    let started!: () => void;
    const entered = new Promise<void>((resolve) => {
      started = resolve;
    });
    vi.mocked(decode).mockImplementation(() => {
      started();
      return new Promise((resolve) => {
        release = resolve;
      });
    });
    const pending = service.fetchMaterial(await scopedMaterialInput(service, editor));
    const failure = expect(pending).rejects.toThrow();
    await entered;
    service.advanceEditor(editor.id, 2);
    release({ kind: "jks", caPem: "late" });
    await failure;
    await expect(
      service.fetchMaterial({ kind: "jks", label: "trust", target, editor }),
    ).rejects.toThrow();
  });

  it("retains applied trust while invalidating unused candidates on context change", async () => {
    const { service } = createHarness();
    const editor = service.openEditor();
    const applied = await service.fetchMaterial(
      await scopedMaterialInput(service, editor, "applied"),
    );
    const unused = await service.fetchMaterial(
      await scopedMaterialInput(service, editor, "unused"),
    );
    service.apply(applied.id, editor.id);
    service.advanceEditor(editor.id, 2);
    expect(service.resolve(applied.id, "jks", editor.id).label).toBe("applied");
    expect(() => service.resolve(unused.id, "jks", editor.id)).toThrow();
    service.closeEditor(editor.id);
    expect(() => service.resolve(applied.id, "jks", editor.id)).toThrow();
  });
  it("allows only one pending operation per editor without blocking another editor", async () => {
    const { service, remote } = createHarness();
    const editor = service.openEditor();
    const other = service.openEditor();
    const barrier = deferred<string>();
    vi.spyOn(remote, "discoverHostKey").mockReturnValueOnce(barrier.promise);
    const pending = service.discoverHostKey({ editor, target });
    await expect(service.discoverHostKey({ editor, target })).rejects.toThrow(
      "operation in progress",
    );
    await expect(service.discoverHostKey({ editor: other, target })).resolves.toMatchObject({
      fingerprint: target.hostKeyFingerprint,
    });
    barrier.resolve(target.hostKeyFingerprint);
    await pending;
  });
});
