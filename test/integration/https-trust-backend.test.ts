import { expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommandResponse,
  type HostEvent,
} from "../../src/kafka/contracts";
import { createKafkaBackend, createBrowserKafkaProfileStore } from "../../src/main/kafka-backend";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";

it("runs verified HTTPS through the production composition and redacts failure diagnostics", async () => {
  const sentinel = "secret-must-not-reach-activity";
  const fixture = await createHttpsTrustFixture((request, response) => {
    if (request.url?.startsWith("/denied")) {
      response.writeHead(401);
      response.end(sentinel);
    } else response.end(fixture.caPem);
  });
  const store = createBrowserKafkaProfileStore();
  const backend = createKafkaBackend(store);
  const events: HostEvent[] = [];
  backend.subscribe((event) => events.push(event));
  try {
    for (const route of ["/ca", "/denied"]) {
      const created = await backend.execute({
        command: "recipes.create",
        id: `recipe-${route}`,
        version: HOST_PROTOCOL_VERSION,
        payload: {
          name: `HTTPS ${route}`,
          method: "https",
          syntax: "named-v1",
          kind: "pem",
          parameters: [],
          timeoutSeconds: 30,
          https: {
            authentication: "bearer",
            material: {
              url: `${fixture.origin}${route}?private=${sentinel}`,
              headers: [],
              query: [],
              extraction: { mode: "raw" },
            },
            password: { source: "none" },
          },
        },
      });
      expect(created.ok).toBe(true);
      const opened = await backend.execute({
        command: "trustAcquisition.editor.open",
        id: `editor-${route}`,
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      });
      if (!opened.ok || !("editor" in opened.result)) throw new Error("Missing editor");
      const inventory = events.filter((event) => event.event === "recipes.changed").at(-1);
      if (inventory?.event !== "recipes.changed") throw new Error("Missing recipe inventory");
      const recipe = inventory.payload.recipes.find((entry) => entry.name === `HTTPS ${route}`)!;
      const result = await backend.execute({
        command: "trustAcquisition.https.fetch",
        id: `fetch-${route}`,
        version: HOST_PROTOCOL_VERSION,
        payload: {
          editor: opened.result.editor,
          recipe: {
            mode: "replace",
            recipeId: recipe.id,
            recipeRevision: recipe.revision,
            overrides: {},
          },
          label: "Fixture trust",
          kind: "pem",
          api: {
            host: "",
            authentication: { mode: "bearer", token: sentinel },
            tls: { mode: "custom", caPem: fixture.caPem },
          },
        },
      });
      expect(parseHostCommandResponse(result)).toEqual(result);
      if (route === "/ca") {
        expect(result).toMatchObject({
          ok: true,
          result: {
            acquisition: { target: { origin: fixture.origin }, material: { kind: "pem" } },
          },
        });
        if (!result.ok || !("acquisition" in result.result)) throw new Error("Expected candidate");
        const candidate = result.result.acquisition;
        const acquired = {
          mode: "acquired" as const,
          acquisitionId: candidate.id,
          editorId: opened.result.editor.id,
        };
        expect(
          await backend.execute({
            command: "trustAcquisition.apply",
            id: "apply-candidate",
            version: HOST_PROTOCOL_VERSION,
            payload: { acquisitionId: candidate.id, editorId: opened.result.editor.id },
          }),
        ).toMatchObject({ ok: true });
        const profile = {
          name: "API acquired",
          brokers: ["localhost:9093"],
          trust: {
            kind: "pem" as const,
            label: "Acquired CA",
            material: acquired,
            password: { mode: "clear" as const },
          },
          apiCa: { mode: "replace" as const, value: fixture.caPem },
          binding: {
            mode: "replace" as const,
            recipeId: recipe.id,
            recipeRevision: recipe.revision,
            overrides: {},
            apiAccess: { host: "", username: "", tls: "custom" as const },
          },
        };
        vi.spyOn(store, "commit").mockRejectedValueOnce(
          new Error("Controlled atomic write failure"),
        );
        expect(
          await backend.execute({
            command: "profiles.create",
            id: "failed-save",
            version: HOST_PROTOCOL_VERSION,
            payload: { profile },
          }),
        ).toMatchObject({ ok: false });
        expect(await store.load()).toHaveLength(0);
        expect(
          await backend.execute({
            command: "profiles.create",
            id: "retry-save",
            version: HOST_PROTOCOL_VERSION,
            payload: { profile },
          }),
        ).toMatchObject({ ok: true });
        expect(await store.load()).toHaveLength(1);
        expect((await store.load())[0]?.apiCaPem).toContain("BEGIN CERTIFICATE");
        expect(JSON.stringify(await store.load())).not.toContain(`Bearer ${sentinel}`);
        expect(
          await backend.execute({
            command: "trustAcquisition.apply",
            id: "consumed-candidate",
            version: HOST_PROTOCOL_VERSION,
            payload: { acquisitionId: candidate.id, editorId: opened.result.editor.id },
          }),
        ).toMatchObject({ ok: false });
      } else {
        expect(result).toMatchObject({
          ok: false,
          error: {
            code: "HTTPS_AUTHENTICATION",
            target: fixture.origin,
            activeStateChanged: false,
          },
        });
      }
      expect(JSON.stringify(result)).not.toContain(sentinel);
    }
    expect(
      JSON.stringify(events.filter((event) => event.event === "activity.recorded")),
    ).not.toContain(sentinel);
  } finally {
    await backend.shutdown();
    await fixture.close();
  }
});
