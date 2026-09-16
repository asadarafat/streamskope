import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
  type ProfileCreateInput,
  type ProfileBindingInput,
  type TrustAcquisitionRecipe,
} from "../../src/features/kafka/contracts";
import {
  InMemoryKafkaProfileStore,
  KafkaProfileService,
  KafkaConnectionTemplateService,
  InMemoryKafkaConnectionTemplateStore,
  InMemoryKafkaTrustRecipeStore,
  KafkaTrustAcquisitionService,
} from "../../src/features/kafka/application";
import { trustRecipeInput } from "../support/trust-recipe";

async function setup(): Promise<{
  templates: KafkaConnectionTemplateService;
  recipe: TrustAcquisitionRecipe;
  store: InMemoryKafkaProfileStore;
  service: KafkaProfileService;
  input: ProfileCreateInput & { binding: Extract<ProfileBindingInput, { mode: "replace" }> };
}> {
  const templates = new KafkaConnectionTemplateService(
    new InMemoryKafkaConnectionTemplateStore({ durability: "session", state: "ready" }),
    { store: new InMemoryKafkaTrustRecipeStore({ durability: "session", state: "ready" }) },
  );
  const catalog = await templates.recipes.create(trustRecipeInput());
  const recipe = catalog.recipes.find((entry) => entry.name === "Certificate file");
  if (!recipe) throw new Error("Missing fixture recipe");
  const store = new InMemoryKafkaProfileStore({
    durability: "session",
    protection: "memory",
    state: "ready",
  });
  const options = {
    createId: (): string => "bound-profile",
    resolveRecipe: templates.recipes.resolve.bind(templates.recipes),
  };
  const service = new KafkaProfileService(
    store,
    {
      decode: (): Promise<{ kind: "pem"; caPem: string }> =>
        Promise.resolve({ kind: "pem", caPem: "decoded fixture" }),
    },
    options,
  );
  const input = {
    name: "Bound profile",
    brokers: ["localhost:19093"],
    trust: {
      kind: "pem",
      label: "ca.pem",
      material: { mode: "replace", value: "fixture certificate" },
      password: { mode: "clear" },
    },
    binding: {
      mode: "replace",
      recipeId: recipe.id,
      recipeRevision: recipe.revision,
      overrides: { certificate_path: "/remote/ca.pem" },
    },
  } as const;
  return { templates, recipe, store, service, input };
}

describe("profile-owned recipe snapshots", () => {
  it("accepts only a host candidate reference or an explicit reset for stored SSH identity", async () => {
    const { input } = await setup();
    const command = {
      command: "profiles.create",
      id: "identity",
      version: HOST_PROTOCOL_VERSION,
      payload: {
        profile: {
          ...input,
          binding: {
            ...input.binding,
            identity: { mode: "acquired", acquisitionId: "candidate", editorId: "owner" },
          },
        },
      },
    };
    expect(parseHostCommand(command)).toEqual(command);
    expect(() =>
      parseHostCommand({
        ...command,
        payload: {
          profile: {
            ...input,
            binding: {
              ...input.binding,
              identity: { mode: "acquired", acquisitionId: "candidate" },
            },
          },
        },
      }),
    ).toThrow();
    expect(() =>
      parseHostCommand({
        ...command,
        payload: {
          profile: {
            ...input,
            binding: {
              ...input.binding,
              identity: { host: "ssh.test", port: 22, fingerprint: `SHA256:${"A".repeat(43)}` },
            },
          },
        },
      }),
    ).toThrow();
  });
  it("adopts an explicit revision and resets one override without changing saved trust", async () => {
    const { service, input, recipe, templates, store } = await setup();
    await service.create(input);
    const trust = store.records()[0]?.trust;
    await templates.recipes.update(recipe.id, 1, {
      ...trustRecipeInput(),
      parameters: [
        {
          key: "certificate_path",
          label: "Path",
          type: "path",
          required: true,
          defaultValue: "/default/ca.pem",
        },
      ],
    });
    await service.update("bound-profile", {
      ...input,
      expectedRevision: 1,
      binding: { ...input.binding, recipeRevision: 2 },
    });
    expect(store.records()[0]?.binding).toMatchObject({
      recipe: { revision: 2 },
      overrides: { certificate_path: "/remote/ca.pem" },
    });
    await service.update("bound-profile", {
      ...input,
      expectedRevision: 2,
      binding: { ...input.binding, recipeRevision: 2, overrides: {} },
    });
    expect(store.records()[0]?.binding).toMatchObject({
      recipe: { revision: 2, parameters: [{ defaultValue: "/default/ca.pem" }] },
      overrides: {},
    });
    expect(store.records()[0]?.trust).toEqual(trust);
  });

  it("rejects incompatible overrides and active or stale binding updates without mutation", async () => {
    const { service, input, recipe, templates, store } = await setup();
    await service.create(input);
    const before = store.records();
    await templates.recipes.update(recipe.id, 1, {
      ...trustRecipeInput(),
      parameters: [{ key: "certificate_path", label: "Number", type: "number", required: true }],
    });
    await expect(
      service.update("bound-profile", {
        ...input,
        expectedRevision: 1,
        binding: { ...input.binding, recipeRevision: 2 },
      }),
    ).rejects.toThrow();
    await expect(
      service.update("bound-profile", { ...input, expectedRevision: 2 }),
    ).rejects.toThrow();
    await service.markActive("bound-profile");
    await expect(
      service.update("bound-profile", { ...input, expectedRevision: 1 }),
    ).rejects.toThrow();
    expect(store.records()).toEqual(before);
  });

  it("strictly validates usage review and confirmed deletion contracts", () => {
    const command = {
      command: "recipes.usage",
      id: "usage",
      version: HOST_PROTOCOL_VERSION,
      payload: { id: "recipe", revision: 1 },
    };
    expect(parseHostCommand(command)).toEqual(command);
    const envelope = {
      command: command.command,
      id: command.id,
      version: command.version,
      ok: true,
      result: { correlationId: "review", usage: [{ id: "profile", name: "Profile", revision: 1 }] },
    };
    expect(parseHostCommandResponse(envelope)).toEqual(envelope);
    expect(() =>
      parseHostCommandResponse({
        ...envelope,
        result: {
          ...envelope.result,
          usage: [{ ...envelope.result.usage[0], password: "sentinel" }],
        },
      }),
    ).toThrow();
    expect(() =>
      parseHostCommand({
        ...command,
        command: "recipes.delete",
        payload: { ...command.payload, confirmedProfileIds: ["profile", "profile"] },
      }),
    ).toThrow();
  });
  it("reports exact pinned-profile deletion impact without exposing access or trust", async () => {
    const { service, input, recipe, templates } = await setup();
    await service.create(input);
    expect(await service.recipeUsage(recipe.id)).toEqual([
      { id: "bound-profile", name: "Bound profile", revision: 1 },
    ]);
    const before = await service.bindingDetail("bound-profile");
    await expect(
      service.withConfirmedRecipeUsage(recipe.id, [], () => templates.recipes.delete(recipe.id, 1)),
    ).rejects.toThrow();
    expect(await templates.recipes.resolve(recipe.id, 1)).toEqual(recipe);
    await service.withConfirmedRecipeUsage(recipe.id, ["bound-profile"], () =>
      templates.recipes.delete(recipe.id, 1),
    );
    expect(await service.bindingDetail("bound-profile")).toEqual(before);
    expect(await service.recipeUsage(recipe.id)).toEqual([
      { id: "bound-profile", name: "Bound profile", revision: 1 },
    ]);
  });
  it("persists only explicit non-secret SSH access and exposes it only in binding detail", async () => {
    const { service, input, store } = await setup();
    const access = {
      host: "ssh.test",
      port: 2222,
      username: "operator",
      authentication: "private-key" as const,
    };
    await service.create({ ...input, binding: { ...input.binding, access } });
    expect((await service.bindingDetail("bound-profile")).binding).toMatchObject({ access });
    expect(store.records()[0]?.binding).toMatchObject({ access });
    expect(JSON.stringify(service.currentSnapshot())).not.toContain("ssh.test");
    await service.update("bound-profile", {
      ...input,
      expectedRevision: 1,
      binding: { ...input.binding, access: null },
    });
    expect((await service.bindingDetail("bound-profile")).binding).not.toHaveProperty("access");
  });

  it.each(["password", "privateKey", "passphrase", "agentPath", "hostKeyFingerprint"])(
    "rejects %s in persisted retrieval access",
    async (field) => {
      const { input } = await setup();
      expect(() =>
        parseHostCommand({
          command: "profiles.create",
          id: "unsafe-access",
          version: HOST_PROTOCOL_VERSION,
          payload: {
            profile: {
              ...input,
              binding: {
                ...input.binding,
                access: {
                  host: "ssh.test",
                  port: 22,
                  username: "operator",
                  authentication: "password",
                  [field]: "sentinel",
                },
              },
            },
          },
        }),
      ).toThrow();
    },
  );
  it("executes a retained profile recipe after library deletion and rejects stale profile revisions", async () => {
    const { service, input, templates, recipe } = await setup();
    await service.create(input);
    await templates.recipes.delete(recipe.id, recipe.revision);
    const requests: unknown[] = [];
    const acquisitions = new KafkaTrustAcquisitionService(
      templates,
      {
        discoverHostKey: (): Promise<string> => Promise.resolve(`SHA256:${"A".repeat(43)}`),
        fetchPassword: (): Promise<string> => Promise.reject(new Error("PEM needs no password")),
        fetchMaterial: (request): Promise<Uint8Array> => {
          requests.push(request);
          return Promise.resolve(new TextEncoder().encode("fixture pem"));
        },
      },
      {
        decode: (): Promise<{ kind: "pem"; caPem: string }> =>
          Promise.resolve({ kind: "pem", caPem: "decoded" }),
      },
      { resolveProfileBinding: service.resolveAcquisitionBinding.bind(service) },
    );
    const request = {
      kind: "pem" as const,
      label: "ca.pem",
      target: {
        host: "ssh.test",
        port: 22,
        username: "operator",
        password: "ssh",
        hostKeyFingerprint: `SHA256:${"A".repeat(43)}`,
      },
      recipe: input.binding,
      profile: { id: "bound-profile", revision: 1 },
    };
    const result = await acquisitions.fetchMaterial(request);
    expect(result.material?.templateName).toBe("Certificate file");
    expect(requests).toHaveLength(1);
    await expect(
      acquisitions.fetchMaterial({ ...request, profile: { ...request.profile, revision: 2 } }),
    ).rejects.toMatchObject({ code: "VALIDATION" });
    expect(requests).toHaveLength(1);
  });
  it("strictly parses detail responses without accepting protected values", async () => {
    const { service, input } = await setup();
    await service.create(input);
    const command = {
      command: "profiles.binding.get",
      id: "detail",
      version: HOST_PROTOCOL_VERSION,
      payload: { profileId: "bound-profile" },
    } as const;
    expect(parseHostCommand(command)).toEqual(command);
    const result = {
      correlationId: "detail-correlation",
      bindingDetail: await service.bindingDetail("bound-profile"),
    };
    const response = {
      command: command.command,
      id: command.id,
      version: HOST_PROTOCOL_VERSION,
      ok: true,
      result,
    };
    expect(parseHostCommandResponse(response)).toEqual(response);
    expect(() =>
      parseHostCommandResponse({
        ...response,
        result: {
          ...result,
          bindingDetail: { ...result.bindingDetail, trust: "protected sentinel" },
        },
      }),
    ).toThrow();
  });
  it("returns only explicit binding detail without saved trust or credentials", async () => {
    const { service, input, recipe } = await setup();
    await service.create(input);
    expect(await service.bindingDetail("bound-profile")).toEqual({
      profileId: "bound-profile",
      revision: 1,
      binding: { recipe, overrides: { certificate_path: "/remote/ca.pem" } },
    });
    const detail = await service.bindingDetail("bound-profile");
    expect(JSON.stringify(detail)).not.toContain("fixture certificate");
    expect(service.currentSnapshot().profiles[0]).not.toHaveProperty("binding");
  });
  it("does not expose mutable references to stored snapshots", async () => {
    const { service, store, input } = await setup();
    await service.create(input);
    Object.assign(store.records()[0]?.binding?.overrides ?? {}, { certificate_path: "/mutated" });
    expect(store.records()[0]?.binding?.overrides.certificate_path).toBe("/remote/ca.pem");
  });

  it("keeps the old binding and trust after a failed adoption commit", async () => {
    const { templates, recipe, service, store, input } = await setup();
    await service.create(input);
    const before = store.records();
    await templates.recipes.update(recipe.id, 1, { ...trustRecipeInput(), name: "New revision" });
    const failing = new KafkaProfileService(
      {
        capability: store.capability.bind(store),
        load: store.load.bind(store),
        commit: (): Promise<void> => Promise.reject(new Error("fixture commit failure")),
      },
      {
        decode: (): Promise<{ kind: "pem"; caPem: string }> =>
          Promise.resolve({ kind: "pem", caPem: "decoded" }),
      },
      {
        resolveRecipe: templates.recipes.resolve.bind(templates.recipes),
      },
    );
    await expect(
      failing.update("bound-profile", {
        ...input,
        expectedRevision: 1,
        binding: { ...input.binding, recipeRevision: 2 },
      }),
    ).rejects.toThrow("fixture commit failure");
    expect(store.records()).toEqual(before);
    expect(failing.currentSnapshot().profiles[0]?.revision).toBe(1);
  });

  it("edits non-secret overrides against the retained revision after library deletion", async () => {
    const { templates, recipe, service, store, input } = await setup();
    await service.create(input);
    await templates.recipes.delete(recipe.id, recipe.revision);
    await service.update("bound-profile", {
      ...input,
      expectedRevision: 1,
      binding: { ...input.binding, overrides: { certificate_path: "/new/ca.pem" } },
    });
    expect(store.records()[0]).toMatchObject({
      binding: { recipe, overrides: { certificate_path: "/new/ca.pem" } },
    });
  });

  it("accepts a reference, but not a renderer-supplied executable snapshot", async () => {
    const { input } = await setup();
    const command = {
      command: "profiles.create",
      id: "binding-create",
      version: HOST_PROTOCOL_VERSION,
      payload: { profile: input },
    };
    expect(parseHostCommand(command)).toMatchObject({
      payload: { profile: { binding: input.binding } },
    });
    expect(() =>
      parseHostCommand({
        ...command,
        payload: {
          profile: { ...input, binding: { ...input.binding, recipe: trustRecipeInput() } },
        },
      }),
    ).toThrow();
  });

  it("pins host-owned definitions and retains them through library edit/delete and ordinary profile edits", async () => {
    const { templates, recipe, store, service, input } = await setup();
    const summary = await service.create(input);
    expect(store.records()[0]).toMatchObject({
      binding: { recipe, overrides: { certificate_path: "/remote/ca.pem" } },
    });
    expect(JSON.stringify(summary)).not.toContain("/remote/ca.pem");
    expect(JSON.stringify(summary)).not.toContain("{{certificate_path}}");
    await templates.recipes.update(recipe.id, recipe.revision, {
      ...trustRecipeInput(),
      name: "Renamed recipe",
    });
    await templates.recipes.delete(recipe.id, recipe.revision + 1);
    const manual = { name: input.name, brokers: input.brokers, trust: input.trust };
    await service.update("bound-profile", {
      ...manual,
      expectedRevision: 1,
      name: "Renamed profile",
    });
    expect(store.records()[0]).toMatchObject({
      binding: { recipe },
      trust: { material: "fixture certificate" },
    });
  });

  it("clears a binding explicitly without clearing working trust", async () => {
    const { service, store, input } = await setup();
    await service.create(input);
    await service.update("bound-profile", {
      ...input,
      expectedRevision: 1,
      binding: { mode: "clear" },
    });
    expect(store.records()[0]).not.toHaveProperty("binding");
    expect(store.records()[0]?.trust.material).toBe("fixture certificate");
  });

  it("rejects a stale recipe reference before committing", async () => {
    const { templates, recipe, service, store, input } = await setup();
    await templates.recipes.update(recipe.id, recipe.revision, {
      ...trustRecipeInput(),
      name: "Changed",
    });
    await expect(service.create(input)).rejects.toThrow();
    expect(store.records()).toEqual([]);
  });

  it.each(["undeclared", "credential"])("rejects persisted %s overrides", async (key) => {
    const { templates, recipe, service, store, input } = await setup();
    await templates.recipes.update(recipe.id, 1, {
      ...trustRecipeInput(),
      parameters: [
        ...trustRecipeInput().parameters,
        { key: "credential", label: "Credential", type: "secret", required: true },
      ],
    });
    await expect(
      service.create({
        ...input,
        binding: {
          ...input.binding,
          recipeRevision: 2,
          overrides: { [key]: "sentinel-not-for-storage" },
        },
      }),
    ).rejects.toThrow();
    expect(store.records()).toEqual([]);
  });
});
