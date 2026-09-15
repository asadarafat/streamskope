import { expect, it } from "vitest";

import { HOST_PROTOCOL_VERSION, parseHostCommand } from "../../src/kafka/contracts";

const payload = {
  editor: { id: "editor", generation: 1 },
  recipe: { mode: "replace", recipeId: "recipe", recipeRevision: 1, overrides: {} },
  kind: "pem",
  label: "API trust",
  api: {
    host: "api.example.test",
    authentication: { mode: "bearer", token: "ephemeral" },
    tls: { mode: "system" },
  },
};
const command = (value: unknown): ReturnType<typeof parseHostCommand> =>
  parseHostCommand({
    version: HOST_PROTOCOL_VERSION,
    id: "https-fetch",
    command: "trustAcquisition.https.fetch",
    payload: value,
  });

it("accepts method-specific HTTPS access in the existing editor workflow", () => {
  expect(command(payload)).toMatchObject({ payload });
});
it("rejects unknown access fields, unsafe authentication and missing editor before dispatch", () => {
  for (const api of [
    { ...payload.api, insecure: true },
    { ...payload.api, authentication: { mode: "bearer", token: "token\r\nInjected: true" } },
    {
      ...payload.api,
      authentication: { mode: "basic", username: "user:other", password: "secret" },
    },
    { ...payload.api, tls: { mode: "custom", caPem: "" } },
  ])
    expect(() => command({ ...payload, api })).toThrow();
  expect(() => command({ ...payload, editor: undefined })).toThrow();
});
