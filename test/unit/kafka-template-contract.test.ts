import { describe, expect, it } from "vitest";

import {
  HostContractValidationError,
  TemplateExpansionError,
  expandOAuthEndpointTemplate,
  parseTrustAcquisitionRecipeInput,
  parseTrustAcquisitionRecipe,
  parseTrustAcquisitionRecipeDocument,
  previewCommandTemplate,
  validateConnectionTemplateInput,
} from "../../src/features/kafka/contracts";
import { trustRecipeInput } from "../support/trust-recipe";

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

describe("Kafka connection-template contract", () => {
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
