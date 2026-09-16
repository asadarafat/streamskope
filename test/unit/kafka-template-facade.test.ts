import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
  parseHostEvent,
  type ConnectionTemplateCatalog,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
} from "../../src/features/kafka/contracts";
import {
  DEFAULT_CONNECTION_TEMPLATE_DOCUMENT,
  InMemoryKafkaConnectionTemplateStore,
  InMemoryKafkaProfileStore,
  InMemoryKafkaTopicConfigurationHistoryStore,
  InMemoryKafkaRuleStore,
  KafkaApplicationSession,
  KafkaConnectionTemplateService,
  KafkaLiveRuleRuntime,
  KafkaProfileService,
  KafkaRuleService,
  KafkaTopicConfigurationService,
  type KafkaActiveConnection,
  type KafkaConnectionPort,
  type KafkaConnectionTestResult,
} from "../../src/features/kafka/application";
import { KafkaBackendFacade } from "../../src/features/kafka/facade";
import { StreamSkopeKafkaRuleEvaluator } from "../../src/features/kafka/engine";
import { trustRecipeInput } from "../support/trust-recipe";

class UnusedConnectionPort implements KafkaConnectionPort {
  openConnection(): Promise<KafkaActiveConnection> {
    return Promise.reject(new Error("Kafka connection was not expected in template tests."));
  }

  testConnection(): Promise<KafkaConnectionTestResult> {
    return Promise.reject(new Error("Kafka connection test was not expected."));
  }
}

describe("Unified recipe facade", () => {
  it("blocks recipe deletion when protected profile usage cannot be inspected", async () => {
    const { facade, events } = setup(
      new InMemoryKafkaProfileStore({
        durability: "durable",
        protection: "unavailable",
        state: "unavailable",
      }),
    );
    await facade.execute(
      parseHostCommand({
        command: "recipes.create",
        id: "create",
        payload: trustRecipeInput(),
        version: HOST_PROTOCOL_VERSION,
      }),
    );
    const recipe = events.filter((event) => event.event === "recipes.changed").at(-1)?.payload
      .recipes[0];
    const response = await facade.execute(
      parseHostCommand({
        command: "recipes.delete",
        id: "delete",
        payload: { id: recipe?.id, revision: 1 },
        version: HOST_PROTOCOL_VERSION,
      }),
    );
    expect(response).toMatchObject({ ok: false, error: { code: "PROFILE_STORE_UNAVAILABLE" } });
    expect(
      events.filter((event) => event.event === "recipes.changed").at(-1)?.payload.recipes,
    ).toHaveLength(1);
  });
  it("requires an unchanged reviewed legacy source before converting one selected recipe", async () => {
    const { facade, events } = setup();
    const execute = (command: string, payload: unknown): Promise<HostCommandResponse> =>
      facade.execute(
        parseHostCommand({ command, payload, id: command, version: HOST_PROTOCOL_VERSION }),
      );
    const preview = await execute("recipes.legacy.preview", {});
    expect(parseHostCommandResponse(preview)).toMatchObject({
      ok: true,
      result: {
        legacy: {
          catalogs: DEFAULT_CONNECTION_TEMPLATE_DOCUMENT.catalogs,
        },
      },
    });
    if (!preview.ok || !("legacy" in preview.result) || preview.result.legacy === null)
      throw new Error("Missing legacy review");
    expect(preview.result.legacy.sourceRevision).toMatch(/^[a-f0-9]{64}$/u);
    const selection = {
      name: "Reviewed legacy",
      kind: "jks",
      materialName: "nsp-25-4",
      passwordName: "nsp-25-11",
      oauthName: "nsp-25-4",
      expectedSourceRevision: preview.result.legacy.sourceRevision,
    };
    await expect(execute("recipes.legacy.convert", selection)).resolves.toMatchObject({ ok: true });
    await expect(execute("recipes.legacy.convert", selection)).resolves.toMatchObject({ ok: true });
    expect(
      events.filter((event) => event.event === "recipes.changed").at(-1)?.payload.recipes,
    ).toHaveLength(1);
    await execute("templates.create", {
      catalog: "oauth-endpoint",
      name: "Later edit",
      template: "https://{host}/token",
    });
    await expect(
      execute("recipes.legacy.convert", { ...selection, name: "Stale review" }),
    ).resolves.toMatchObject({ ok: false, error: { code: "VALIDATION" } });
    expect(
      events.filter((event) => event.event === "recipes.changed").at(-1)?.payload.recipes,
    ).toHaveLength(1);
  });
  it("reviews imports without mutation and exports only a selected committed definition", async () => {
    const { facade, events } = setup();
    const recipe = {
      ...trustRecipeInput(),
      parameters: [
        {
          key: "certificate_path",
          label: "Certificate path",
          type: "path",
          required: true,
          defaultValue: "/private/default-sentinel",
        },
      ],
    };
    const execute = (command: string, payload: unknown): Promise<HostCommandResponse> =>
      facade.execute(
        parseHostCommand({ command, payload, id: command, version: HOST_PROTOCOL_VERSION }),
      );
    const contents = JSON.stringify({ format: "streamskope-trust-recipe", version: 1, recipe });
    const preview = await execute("recipes.import.preview", { contents });
    expect(parseHostCommandResponse(preview)).toMatchObject({
      ok: true,
      result: { draft: recipe },
    });
    expect(events.some((event) => event.event === "recipes.changed")).toBe(false);
    await execute("recipes.create", recipe);
    const created = events.filter((event) => event.event === "recipes.changed").at(-1)?.payload
      .recipes[0];
    expect(created).toBeDefined();
    const exported = await execute("recipes.export", { id: created?.id, revision: 1 });
    expect(parseHostCommandResponse(exported)).toMatchObject({
      ok: true,
      result: {
        document: { fileName: "trust-acquisition-template.json", mediaType: "application/json" },
      },
    });
    if (!exported.ok || !("document" in exported.result)) throw new Error("Export document absent");
    expect("warning" in exported.result && exported.result.warning).toContain("hard-coded secrets");
    expect(JSON.parse(exported.result.document.content)).toMatchObject({
      recipe: { name: recipe.name },
    });
    expect(exported.result.document.content).not.toContain("default-sentinel");
    expect(exported.result.document.content).not.toContain('"revision"');
    const rejected = await execute("recipes.import.preview", {
      contents: '{"secret":"rejected-body-sentinel"}',
    });
    expect(rejected.ok).toBe(false);
    expect(events.filter((event) => event.event === "recipes.changed")).toHaveLength(1);
    expect(
      JSON.stringify(events.filter((event) => event.event === "activity.recorded")),
    ).not.toMatch(/default-sentinel|rejected-body-sentinel/);
    await expect(
      execute("recipes.export", { id: created?.id, revision: 2 }),
    ).resolves.toMatchObject({ ok: false });
  });

  it("creates and updates inert recipes through strict host commands with redacted activity", async () => {
    const { facade, events } = setup();
    const payload = {
      ...trustRecipeInput(),
      ssh: { source: "stdout", value: "literal-command-sentinel", password: { source: "none" } },
    };
    const response = await facade.execute(
      parseHostCommand({
        command: "recipes.create",
        id: "recipe-create",
        version: HOST_PROTOCOL_VERSION,
        payload,
      }),
    );
    expect(response.ok).toBe(true);
    const changed = [...events].reverse().find((event) => event.event === "recipes.changed");
    expect(changed?.event).toBe("recipes.changed");
    if (changed?.event !== "recipes.changed") throw new Error("Missing recipe snapshot");
    expect(parseHostEvent(changed)).toEqual(changed);
    const recipe = changed.payload.recipes[0];
    expect(recipe).toMatchObject({ name: "Certificate file", revision: 1 });
    const update = await facade.execute(
      parseHostCommand({
        command: "recipes.update",
        id: "recipe-update",
        version: HOST_PROTOCOL_VERSION,
        payload: { id: recipe?.id, revision: 1, recipe: { ...payload, name: "Renamed" } },
      }),
    );
    expect(update.ok).toBe(true);
    const stale = await facade.execute(
      parseHostCommand({
        command: "recipes.delete",
        id: "recipe-delete",
        version: HOST_PROTOCOL_VERSION,
        payload: { id: recipe?.id, revision: 1 },
      }),
    );
    expect(stale.ok).toBe(false);
    const activity = events.filter((event) => event.event === "activity.recorded");
    expect(activity).toHaveLength(3);
    expect(activity[0]?.payload).toMatchObject({
      operation: "Create trust acquisition template",
      object: `Certificate file · ${recipe?.id} · revision 1`,
    });
    expect(JSON.stringify(activity)).not.toContain("literal-command-sentinel");
  });
});

function ruleServices(): {
  readonly liveRules: KafkaLiveRuleRuntime;
  readonly rules: KafkaRuleService;
} {
  const evaluator = new StreamSkopeKafkaRuleEvaluator();
  const rules = new KafkaRuleService(
    new InMemoryKafkaRuleStore({ durability: "session", state: "ready" }),
    evaluator,
  );
  return {
    liveRules: new KafkaLiveRuleRuntime(rules, evaluator),
    rules,
  };
}

function templateCommand(
  command:
    | "templates.create"
    | "templates.delete"
    | "templates.list"
    | "templates.select"
    | "templates.update",
  payload: HostCommand["payload"],
): HostCommand {
  return {
    command,
    id: command,
    payload,
    version: HOST_PROTOCOL_VERSION,
  } as HostCommand;
}

function setup(
  profileStore = new InMemoryKafkaProfileStore({
    durability: "session",
    protection: "memory",
    state: "ready",
  }),
): {
  readonly events: HostEvent[];
  readonly facade: KafkaBackendFacade;
  readonly store: InMemoryKafkaConnectionTemplateStore;
} {
  const store = new InMemoryKafkaConnectionTemplateStore(
    { durability: "session", state: "ready" },
    DEFAULT_CONNECTION_TEMPLATE_DOCUMENT,
  );
  const templates = new KafkaConnectionTemplateService(store);
  const profiles = new KafkaProfileService(profileStore, {
    decode: (): Promise<never> => Promise.reject(new Error("Profile decoding was not expected.")),
  });
  const { liveRules, rules } = ruleServices();
  let correlation = 0;
  const session = new KafkaApplicationSession(new UnusedConnectionPort());
  const facade = new KafkaBackendFacade(
    session,
    profiles,
    templates,
    rules,
    liveRules,
    new KafkaTopicConfigurationService(
      session,
      new InMemoryKafkaTopicConfigurationHistoryStore({
        durability: "session",
        state: "ready",
      }),
    ),
    {
      createCorrelationId: (): string => `template-correlation-${++correlation}`,
      now: (): Date => new Date("2026-07-25T20:00:00.000Z"),
    },
  );
  const events: HostEvent[] = [];
  facade.subscribe((event) => {
    events.push(event);
  });
  return { events, facade, store };
}

function snapshots(
  events: readonly HostEvent[],
): readonly Extract<HostEvent, { readonly event: "templates.changed" }>[] {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "templates.changed" }> =>
      event.event === "templates.changed",
  );
}

describe("Kafka connection-template facade", () => {
  it("publishes one confirmed snapshot and bounded activity for each catalog operation", async () => {
    const { events, facade } = setup();

    await expect(facade.execute(templateCommand("templates.list", {}))).resolves.toMatchObject({
      ok: true,
    });
    await expect(
      facade.execute(
        templateCommand("templates.create", {
          catalog: "oauth-endpoint",
          name: "Local endpoint",
          template: "http://{host}:15000/token",
        }),
      ),
    ).resolves.toMatchObject({ ok: true });
    await expect(
      facade.execute(
        templateCommand("templates.update", {
          catalog: "oauth-endpoint",
          name: "Local OAuth",
          originalName: "Local endpoint",
          template: "http://{kafka-cluster-server}:15000/token",
        }),
      ),
    ).resolves.toMatchObject({ ok: true });
    await expect(
      facade.execute(
        templateCommand("templates.select", {
          catalog: "oauth-endpoint",
          name: "Local OAuth",
        }),
      ),
    ).resolves.toMatchObject({ ok: true });
    await expect(
      facade.execute(
        templateCommand("templates.delete", {
          catalog: "oauth-endpoint",
          name: "nsp-25-4",
        }),
      ),
    ).resolves.toMatchObject({ ok: true });

    const templateSnapshots = snapshots(events);
    expect(templateSnapshots).toHaveLength(5);
    expect(templateSnapshots.at(-1)?.payload.catalogs[2]).toEqual({
      catalog: "oauth-endpoint",
      entries: [
        {
          name: "Local OAuth",
          template: "http://{kafka-cluster-server}:15000/token",
        },
      ],
      selectedName: "Local OAuth",
    });
    expect(
      events
        .filter((event) => event.event === "activity.recorded")
        .map((event) => [event.payload.operation, event.payload.object, event.payload.outcome]),
    ).toEqual([
      ["Load templates", "Connection templates", "succeeded"],
      ["Create template", "oauth-endpoint · Local endpoint", "succeeded"],
      ["Update template", "oauth-endpoint · Local OAuth", "succeeded"],
      ["Select template", "oauth-endpoint · Local OAuth", "succeeded"],
      ["Delete template", "oauth-endpoint · nsp-25-4", "succeeded"],
    ]);
    expect(
      JSON.stringify(events.filter((event) => event.event === "activity.recorded")),
    ).not.toMatch(/15000|kafka-cluster-server|rest-gateway/);
  });

  it.each([
    {
      command: "templates.create" as const,
      expectedCode: "TEMPLATE_DUPLICATE",
      payload: {
        catalog: "truststore-fetch" as const,
        name: "NSP-25-4",
        template: "copy source {truststorePath}",
      },
    },
    {
      command: "templates.select" as const,
      expectedCode: "TEMPLATE_NOT_FOUND",
      payload: {
        catalog: "truststore-password" as const,
        name: "missing",
      },
    },
    {
      command: "templates.update" as const,
      expectedCode: "VALIDATION",
      payload: {
        catalog: "truststore-fetch" as const,
        name: "Broken",
        originalName: "nsp-25-4",
        template: "copy source {unknown}",
      },
    },
  ])(
    "returns structured $expectedCode failure and republishes unchanged state",
    async ({ command, expectedCode, payload }) => {
      const { events, facade } = setup();
      await facade.execute(templateCommand("templates.list", {}));
      const before = snapshots(events).at(-1)?.payload;

      const response = await facade.execute(templateCommand(command, payload));

      expect(response).toMatchObject({
        error: {
          activeStateChanged: false,
          code: expectedCode,
        },
        ok: false,
      });
      expect(snapshots(events).at(-1)?.payload).toEqual(before);
      expect(events.filter((event) => event.event === "activity.recorded").at(-1)).toMatchObject({
        payload: {
          outcome: "failed",
          severity: "error",
        },
      });
    },
  );

  it("fails closed and publishes only unavailable template state when loaded data is invalid", async () => {
    const invalidStore = new InMemoryKafkaConnectionTemplateStore(
      { durability: "session", state: "ready" },
      {
        catalogs: DEFAULT_CONNECTION_TEMPLATE_DOCUMENT.catalogs.map((catalog) =>
          catalog.catalog === "oauth-endpoint"
            ? {
                ...catalog,
                entries: [
                  { name: "Duplicate", template: "https://{host}/one" },
                  { name: " duplicate ", template: "https://{host}/two" },
                ],
                selectedName: "Duplicate",
              }
            : catalog,
        ),
      },
    );
    const templates = new KafkaConnectionTemplateService(invalidStore);
    const profiles = new KafkaProfileService(
      new InMemoryKafkaProfileStore({
        durability: "session",
        protection: "memory",
        state: "ready",
      }),
      {
        decode: (): Promise<never> =>
          Promise.reject(new Error("Profile decoding was not expected.")),
      },
    );
    const { liveRules, rules } = ruleServices();
    const session = new KafkaApplicationSession(new UnusedConnectionPort());
    const facade = new KafkaBackendFacade(
      session,
      profiles,
      templates,
      rules,
      liveRules,
      new KafkaTopicConfigurationService(
        session,
        new InMemoryKafkaTopicConfigurationHistoryStore({
          durability: "session",
          state: "ready",
        }),
      ),
    );
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });

    const response = await facade.execute(templateCommand("templates.list", {}));

    expect(response).toMatchObject({
      error: { code: "TEMPLATE_STORE_UNAVAILABLE" },
      ok: false,
    });
    expect(snapshots(events).at(-1)).toMatchObject({
      payload: {
        catalogs: [
          { catalog: "truststore-fetch", entries: [], selectedName: null },
          { catalog: "truststore-password", entries: [], selectedName: null },
          { catalog: "oauth-endpoint", entries: [], selectedName: null },
        ],
        store: { durability: "session", state: "unavailable" },
      },
    });
  });

  it("does not confuse identical names in different catalogs", async () => {
    const { events, facade } = setup();
    const name = "Shared";
    for (const catalog of [
      "truststore-fetch",
      "truststore-password",
      "oauth-endpoint",
    ] satisfies ConnectionTemplateCatalog[]) {
      const template =
        catalog === "truststore-fetch"
          ? "copy {truststorePath}"
          : catalog === "truststore-password"
            ? "password command"
            : "https://{host}/token";
      await facade.execute(templateCommand("templates.create", { catalog, name, template }));
    }

    expect(
      snapshots(events)
        .at(-1)
        ?.payload.catalogs.map((catalog) => catalog.entries.at(-1)?.name),
    ).toEqual([name, name, name]);
  });
});
