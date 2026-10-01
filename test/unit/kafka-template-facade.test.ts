import { describe, expect, it } from "vitest";

import { DEFAULT_CONNECTION_TEMPLATE_DOCUMENT } from "../support/legacy-template-document";
import { createRecipeLibrary, LegacyTemplateFixture } from "../support/recipe-library";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
  parseHostEvent,
  type HostCommandResponse,
  type HostEvent,
} from "../../src/features/kafka/contracts";
import { InMemoryKafkaProfileStore } from "../../src/features/kafka/application/in-memory-profile-store";
import { InMemoryKafkaTopicConfigurationHistoryStore } from "../../src/features/kafka/application/in-memory-topic-configuration-history-store";
import { InMemoryKafkaRuleStore } from "../../src/features/kafka/application/in-memory-rule-store";
import { KafkaApplicationSession } from "../../src/features/kafka/application/session";
import { KafkaLiveRuleRuntime } from "../../src/features/kafka/application/live-rule-runtime";
import { KafkaProfileService } from "../../src/features/kafka/application/profile-service";
import { KafkaRuleService } from "../../src/features/kafka/application/rule-service";
import { KafkaTopicConfigurationService } from "../../src/features/kafka/application/topic-configuration-service";
import type {
  KafkaActiveConnection,
  KafkaConnectionPort,
  KafkaConnectionTestResult,
} from "../../src/features/kafka/application/types";
import { KafkaBackendFacade } from "../../src/features/kafka/facade/facade";
import { StreamSkopeKafkaRuleEvaluator } from "../../src/features/kafka/engine/rule-evaluator";
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
    const { facade, events, store } = setup();
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
    const preserved = store.document()!;
    await store.commit({
      catalogs: preserved.catalogs.map((catalog) =>
        catalog.catalog === "oauth-endpoint"
          ? {
              ...catalog,
              entries: [
                ...catalog.entries,
                { name: "Later edit", template: "https://{host}/token" },
              ],
            }
          : catalog,
      ),
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

function setup(
  profileStore = new InMemoryKafkaProfileStore({
    durability: "session",
    protection: "memory",
    state: "ready",
  }),
): {
  readonly events: HostEvent[];
  readonly facade: KafkaBackendFacade;
  readonly store: LegacyTemplateFixture;
} {
  const store = new LegacyTemplateFixture(
    { durability: "session", state: "ready" },
    DEFAULT_CONNECTION_TEMPLATE_DOCUMENT,
  );
  const templates = createRecipeLibrary(store);
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
