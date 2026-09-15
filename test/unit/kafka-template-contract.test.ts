import { describe, expect, it } from "vitest";

import {
  CONNECTION_TEMPLATE_CATALOGS,
  CONNECTION_TEMPLATE_LIMITS,
  HOST_COMMANDS,
  HOST_EVENTS,
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  TemplateExpansionError,
  expandOAuthEndpointTemplate,
  parseHostCommand,
  parseHostEvent,
  parseTrustAcquisitionRecipeInput,
  parseTrustAcquisitionRecipe,
  parseTrustAcquisitionRecipeDocument,
  previewCommandTemplate,
  validateConnectionTemplateInput,
} from "../../src/kafka/contracts";
import { trustRecipeInput } from "../support/trust-recipe";

const sessionStore = {
  durability: "session",
  state: "ready",
} as const;

describe("Unified trust acquisition recipe contract", () => {
  it("parses one complete normalized recipe without catalog/provider or credentials", () => {
    const input = trustRecipeInput();
    expect(parseTrustAcquisitionRecipeInput({ ...input, name: "  Ｃertificate file  " })).toEqual(
      input,
    );
    expect(parseTrustAcquisitionRecipe({ ...input, id: "recipe-1", revision: 1 })).toEqual({
      ...input,
      id: "recipe-1",
      revision: 1,
    });
  });

  it.each([
    { provider: "vendor" },
    { password: "sentinel" },
    { method: "local" },
    { name: " " },
    { name: "x".repeat(129) },
    { timeoutSeconds: 0 },
    { timeoutSeconds: 121 },
    { timeoutSeconds: 1.5 },
    { ssh: { source: "stdout", value: "x".repeat(8193), password: { source: "none" } } },
    { ssh: { source: "file", value: "", password: { source: "none" } } },
    { ssh: { source: "file", value: "{{missing}}", password: { source: "none" } } },
    { ssh: { source: "file", value: "{{path", password: { source: "none" } } },
    { kind: "jks" },
  ])("rejects malformed or unbounded definitions %#", (patch) => {
    expect(() => parseTrustAcquisitionRecipeInput({ ...trustRecipeInput(), ...patch })).toThrow(
      HostContractValidationError,
    );
  });

  it.each([
    { key: "host", type: "text" },
    { key: "bad-key", type: "text" },
    { key: "secret", type: "secret", defaultValue: "sentinel" },
    { key: "number", type: "number", defaultValue: "NaN" },
    { key: "number", type: "number", defaultValue: " " },
    { key: "choice", type: "choice", choices: [] },
    { key: "choice", type: "choice", choices: ["one", "one"] },
    { key: "choice", type: "choice", choices: ["one"], defaultValue: "two" },
    { key: "server", type: "host", defaultValue: "user@server/path" },
  ])("rejects invalid parameter definitions %#", (parameter) => {
    expect(() =>
      parseTrustAcquisitionRecipeInput({
        ...trustRecipeInput(),
        parameters: [
          ...trustRecipeInput().parameters,
          { label: "Parameter", required: true, ...parameter },
        ],
      }),
    ).toThrow(HostContractValidationError);
  });

  it("accepts finite typed defaults and rejects duplicate keys/count overflow", () => {
    const input = trustRecipeInput();
    const parameters = [
      ...input.parameters,
      { key: "limit", label: "Limit", type: "number", required: false, defaultValue: "42" },
      {
        key: "site",
        label: "Site",
        type: "choice",
        required: true,
        choices: ["one", "two"],
        defaultValue: "one",
      },
    ];
    expect(parseTrustAcquisitionRecipeInput({ ...input, parameters }).parameters).toEqual(
      parameters,
    );
    expect(() =>
      parseTrustAcquisitionRecipeInput({
        ...input,
        parameters: [...input.parameters, ...input.parameters],
      }),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseTrustAcquisitionRecipeInput({
        ...input,
        parameters: Array.from({ length: 33 }, (_, i) => ({
          key: `p${i}`,
          type: "text",
          label: "P",
          required: true,
        })),
      }),
    ).toThrow(HostContractValidationError);
  });

  it("preserves literal shell braces and requires a matching binary password source", () => {
    const input = {
      ...trustRecipeInput(),
      kind: "jks",
      ssh: {
        source: "stdout",
        value: "fetch {{certificate_path}} && awk '{print $2}'",
        password: { source: "command", command: "read-password" },
      },
    };
    expect(parseTrustAcquisitionRecipeInput(input).ssh).toEqual(input.ssh);
    expect(() => parseTrustAcquisitionRecipeInput({ ...input, kind: "pem" })).toThrow(
      HostContractValidationError,
    );
  });

  it("keeps legacy syntax distinct and validates its established contract", () => {
    const input = {
      ...trustRecipeInput(),
      syntax: "legacy-v1",
      ssh: {
        source: "legacy-tempfile",
        value: "cp source {truststorePath} && awk '{print $2}'",
        password: { source: "none" },
      },
      oauth: { endpoint: "http://{kafka-cluseter-server}/token", clientId: "admin", scope: "" },
    };
    expect(parseTrustAcquisitionRecipeInput(input).ssh?.value).toBe(input.ssh.value);
    expect(() => parseTrustAcquisitionRecipeInput({ ...input, syntax: "named-v1" })).toThrow(
      HostContractValidationError,
    );
  });

  it("accepts local OAuth suggestions but rejects secrets and undeclared URL variables", () => {
    const input = {
      ...trustRecipeInput(),
      oauth: { endpoint: "http://{{host}}:15000/token", clientId: "admin", scope: "" },
    };
    expect(parseTrustAcquisitionRecipeInput(input).oauth).toEqual(input.oauth);
    for (const endpoint of [
      "https://u:p@example.test/token",
      "https://example.test/token#fragment",
      "file:///tmp/token",
      "https://{{ missing }}/token",
      "https://example.test/?secret={{ secret }}",
    ]) {
      expect(() =>
        parseTrustAcquisitionRecipeInput({
          ...input,
          parameters: [
            ...input.parameters,
            { key: "secret", type: "secret", label: "Secret", required: true },
          ],
          oauth: { ...input.oauth, endpoint },
        }),
      ).toThrow(HostContractValidationError);
    }
  });

  it("rejects duplicate identities/names, unsupported documents and library overflow", () => {
    const recipe = { ...trustRecipeInput(), id: "recipe-1", revision: 1 };
    expect(parseTrustAcquisitionRecipeDocument({ version: 1, recipes: [recipe] })).toEqual({
      version: 1,
      recipes: [recipe],
    });
    for (const document of [
      { version: 2, recipes: [] },
      { version: 1, recipes: [], credentials: {} },
      { version: 1, recipes: [recipe, { ...recipe, name: "Other" }] },
      { version: 1, recipes: [recipe, { ...recipe, id: "other", name: "CERTIFICATE FILE" }] },
      {
        version: 1,
        recipes: Array.from({ length: 101 }, (_, i) => ({ ...recipe, id: `r${i}`, name: `R${i}` })),
      },
    ])
      expect(() => parseTrustAcquisitionRecipeDocument(document)).toThrow(
        HostContractValidationError,
      );
    for (const revision of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => parseTrustAcquisitionRecipe({ ...recipe, revision })).toThrow(
        HostContractValidationError,
      );
    }
    const oversized = Array.from({ length: 40 }, (_, i) => ({
      ...recipe,
      id: `r${i}`,
      name: `R${i}`,
      parameters: Array.from({ length: 32 }, (_, p) => ({
        key: `p${p}`,
        label: "Text",
        type: "text",
        required: false,
        defaultValue: "a".repeat(4096),
      })),
      ssh: { source: "file", value: "/etc/certificate", password: { source: "none" } },
    }));
    expect(() => parseTrustAcquisitionRecipeDocument({ version: 1, recipes: oversized })).toThrow(
      "library: exceeds the library byte limit",
    );
  });
});

const catalogs = [
  {
    catalog: "truststore-fetch",
    entries: [
      {
        name: "nsp-25-4",
        template:
          "kubectl cp pod:/opt/nsp/os/ssl/nsp.truststore {truststorePath} && awk '{print $2}'",
      },
    ],
    selectedName: "nsp-25-4",
  },
  {
    catalog: "truststore-password",
    entries: [{ name: "nsp-25-11", template: "kubectl get secret" }],
    selectedName: "nsp-25-11",
  },
  {
    catalog: "oauth-endpoint",
    entries: [
      {
        name: "nsp-25-4",
        template: "https://{kafka-cluseter-server}/rest-gateway/token",
      },
    ],
    selectedName: "nsp-25-4",
  },
] as const;

describe("Kafka connection-template contract", () => {
  it("retains the complete bounded template vocabulary on protocol version 15", () => {
    expect(HOST_PROTOCOL_VERSION).toBe(17);
    expect(CONNECTION_TEMPLATE_CATALOGS).toEqual([
      "truststore-fetch",
      "truststore-password",
      "oauth-endpoint",
    ]);
    expect(CONNECTION_TEMPLATE_LIMITS).toEqual({
      entriesPerCatalog: 100,
      nameCharacters: 128,
      commandCharacters: 8_192,
      endpointCharacters: 2_048,
    });
    expect(HOST_COMMANDS).toEqual(
      expect.arrayContaining([
        "templates.list",
        "templates.create",
        "templates.update",
        "templates.delete",
        "templates.select",
      ]),
    );
    expect(HOST_EVENTS).toContain("templates.changed");
  });

  it("parses exact list, create, update, delete and select commands", () => {
    expect(
      parseHostCommand({
        command: "templates.list",
        id: "templates-list",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({ command: "templates.list", payload: {} });
    expect(
      parseHostCommand({
        command: "templates.create",
        id: "templates-create",
        payload: {
          catalog: "truststore-fetch",
          name: "Custom",
          template: "cp source {truststorePath}",
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      command: "templates.create",
      payload: {
        catalog: "truststore-fetch",
        name: "Custom",
        template: "cp source {truststorePath}",
      },
    });
    expect(
      parseHostCommand({
        command: "templates.update",
        id: "templates-update",
        payload: {
          catalog: "oauth-endpoint",
          name: "Renamed",
          originalName: "Original",
          template: "https://{host}/token",
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      command: "templates.update",
      payload: {
        catalog: "oauth-endpoint",
        name: "Renamed",
        originalName: "Original",
        template: "https://{host}/token",
      },
    });
    for (const command of ["templates.delete", "templates.select"] as const) {
      expect(
        parseHostCommand({
          command,
          id: command,
          payload: { catalog: "truststore-password", name: "nsp-25-11" },
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toMatchObject({
        command,
        payload: { catalog: "truststore-password", name: "nsp-25-11" },
      });
    }
  });

  it("parses one safe snapshot and rejects invalid selection or undeclared data", () => {
    expect(
      parseHostEvent({
        event: "templates.changed",
        payload: { catalogs, store: sessionStore },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toEqual({
      event: "templates.changed",
      payload: { catalogs, store: sessionStore },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });

    expect(() =>
      parseHostEvent({
        event: "templates.changed",
        payload: {
          catalogs: [{ ...catalogs[0], selectedName: "missing" }, catalogs[1], catalogs[2]],
          store: sessionStore,
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);

    for (const extra of [
      { clientSecret: "leaked" },
      { commandOutput: "secret output" },
      { storagePath: "/private/templates.json" },
    ]) {
      expect(() =>
        parseHostEvent({
          event: "templates.changed",
          payload: {
            catalogs: [
              {
                ...catalogs[0],
                entries: [{ ...catalogs[0].entries[0], ...extra }],
              },
              catalogs[1],
              catalogs[2],
            ],
            store: sessionStore,
          },
          sequence: 3,
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toThrow(HostContractValidationError);
    }
  });

  it("rejects oversized, malformed, duplicate-catalog and semantically invalid transport data", () => {
    expect(() =>
      parseHostCommand({
        command: "templates.create",
        id: "oversized-template",
        payload: {
          catalog: "truststore-fetch",
          name: "large",
          template: `cp source {truststorePath}${"x".repeat(
            CONNECTION_TEMPLATE_LIMITS.commandCharacters,
          )}`,
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);

    expect(() =>
      parseHostCommand({
        command: "templates.create",
        id: "unknown-field",
        payload: {
          catalog: "oauth-endpoint",
          name: "unsafe",
          secret: "not-declared",
          template: "https://{host}/token",
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);

    expect(() =>
      parseHostEvent({
        event: "templates.changed",
        payload: {
          catalogs: [catalogs[0], catalogs[0], catalogs[2]],
          store: sessionStore,
        },
        sequence: 4,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);

    expect(() =>
      parseHostCommand({
        command: "templates.create",
        id: "invalid-placeholder",
        payload: {
          catalog: "truststore-fetch",
          name: "invalid",
          template: "cp source {unknownPath}",
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("validates catalog-compatible placeholders while preserving literal shell braces", () => {
    expect(
      validateConnectionTemplateInput({
        catalog: "truststore-fetch",
        name: " NSP Custom ",
        template: "cp source {truststorePath} && awk '{print $2}'",
      }),
    ).toEqual([]);
    expect(
      validateConnectionTemplateInput({
        catalog: "truststore-fetch",
        name: "missing destination",
        template: "kubectl get secret",
      }),
    ).toEqual([
      {
        field: "template",
        message: "Truststore fetch templates must include {truststorePath}.",
      },
    ]);
    expect(
      validateConnectionTemplateInput({
        catalog: "truststore-password",
        name: "unsupported",
        template: "echo {storepass}",
      }),
    ).toEqual([
      {
        field: "template",
        message: "Placeholder {storepass} is not supported in truststore-password templates.",
      },
    ]);
    expect(
      validateConnectionTemplateInput({
        catalog: "oauth-endpoint",
        name: "credentials",
        template: "https://user:password@{host}/token",
      }),
    ).toEqual([
      {
        field: "template",
        message:
          "OAuth endpoint templates must expand to an HTTP or HTTPS URL without credentials or a fragment.",
      },
    ]);
  });

  it("previews commands without accepting credentials or changing shell text", () => {
    const template =
      "copy {truststorePath} --dir {destDir} --password {storepass} && awk '{print $2}'";
    expect(previewCommandTemplate("truststore-fetch", template)).toBe(
      "copy <truststore-path> --dir <destination-directory> --password <masked-password> && awk '{print $2}'",
    );
    expect(previewCommandTemplate("truststore-password", "kubectl get secret")).toBe(
      "kubectl get secret",
    );
  });

  it("expands every source-compatible endpoint alias with only the first broker host", () => {
    expect(
      expandOAuthEndpointTemplate(
        "https://{HOST}/a?one={kafka_host}&two={kafka-host}&three={kafka-cluster-server}&four={kafka-cluseter-server}",
        ["broker.example.test:9093", "ignored.example.test:9093"],
      ),
    ).toBe(
      "https://broker.example.test/a?one=broker.example.test&two=broker.example.test&three=broker.example.test&four=broker.example.test",
    );
    expect(expandOAuthEndpointTemplate("https://{host}/token", ["[2001:db8::1]:9093"])).toBe(
      "https://[2001:db8::1]/token",
    );
  });

  it.each([
    ["missing broker", []],
    ["malformed broker", ["not-a-broker"]],
    ["credential broker", ["user:password@broker.example.test:9093"]],
  ])("rejects endpoint expansion with a %s", (_label, brokers) => {
    expect(() => expandOAuthEndpointTemplate("https://{host}/token", brokers)).toThrow(
      TemplateExpansionError,
    );
  });
});
