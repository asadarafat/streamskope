import { fileURLToPath } from "node:url";

import { ESLint } from "eslint";
import tseslint from "typescript-eslint";
import { describe, expect, it } from "vitest";

const repositoryRoot = fileURLToPath(new URL("../..", import.meta.url));

const forbiddenRendererCases = [
  ...["application", "engine", "facade"].map((layer) => ({
    fileName: `${layer}-import.ts`,
    source: `import * as host from "../../../../src/features/kafka/${layer}"; export const access = host;`,
  })),
  {
    fileName: "electron-import.ts",
    source: 'import { contextBridge } from "electron"; export const access = contextBridge;',
  },
  {
    fileName: "kafkajs-import.ts",
    source: 'import { Kafka } from "kafkajs"; export const access = Kafka;',
  },
  {
    fileName: "platformatic-kafka-import.ts",
    source: 'import { Admin } from "@platformatic/kafka"; export const access = Admin;',
  },
  {
    fileName: "kubernetes-import.ts",
    source:
      'import { KubeConfig } from "@kubernetes/client-node"; export const access = KubeConfig;',
  },
  {
    fileName: "nats-client-import.ts",
    source: 'import { connect } from "@nats-io/transport-node"; export const access = connect;',
  },
  ...["application", "engine", "facade"].map((layer) => ({
    fileName: `nats-${layer}-import.ts`,
    source: `import * as host from "../../../../src/features/nats/${layer}"; export const access = host;`,
  })),
  {
    fileName: "node-import.ts",
    source: 'import { readFile } from "node:fs/promises"; export const access = readFile;',
  },
  {
    fileName: "undeclared-platform-import.ts",
    source:
      'import { access } from "../../../../src/platform/undeclared"; export const result = access;',
  },
];

describe("optional plugin renderer boundary", () => {
  it("permits the public renderer API and rejects plugin backend or Node access", async () => {
    const eslint = new ESLint({
      cwd: repositoryRoot,
      overrideConfig: [tseslint.configs.disableTypeChecked],
    });
    const sources = [
      'import type { PluginRenderer } from "../../../src/plugins/renderer-api"; export type { PluginRenderer };',
      'import { EdaAgentCapture } from "../backend/eda-agent-capture"; export const access = EdaAgentCapture;',
      'import { readFile } from "node:fs/promises"; export const access = readFile;',
    ];
    const results = await Promise.all(
      sources.map(async (source) => {
        const [result] = await eslint.lintText(source, { filePath: "plugins/eda/ui/renderer.tsx" });
        return result;
      }),
    );
    expect(results[0]?.messages).toEqual([]);
    expect(results[1]?.messages.map(({ ruleId }) => ruleId)).toContain("boundaries/dependencies");
    expect(results[2]?.messages.map(({ ruleId }) => ruleId)).toContain("no-restricted-imports");
  }, 60_000);

  it("keeps NSP renderer isolated from backend, Node, and other plugins", async () => {
    const eslint = new ESLint({
      cwd: repositoryRoot,
      overrideConfig: [tseslint.configs.disableTypeChecked],
    });
    const sources = [
      'import type { PluginRenderer } from "../../../src/plugins/renderer-api"; export type { PluginRenderer };',
      'import { NspCaptureBackend } from "../backend/index"; export const access = NspCaptureBackend;',
      'import { readFile } from "node:fs/promises"; export const access = readFile;',
      'import { EdaCaptureDialog } from "../../eda/ui/EdaCaptureDialog"; export const access = EdaCaptureDialog;',
    ];
    const results = await Promise.all(
      sources.map(async (source) => {
        const [result] = await eslint.lintText(source, { filePath: "plugins/nsp/ui/renderer.tsx" });
        return result;
      }),
    );
    expect(results[0]?.messages).toEqual([]);
    expect(results[1]?.messages.map(({ ruleId }) => ruleId)).toContain("boundaries/dependencies");
    expect(results[2]?.messages.map(({ ruleId }) => ruleId)).toContain("no-restricted-imports");
    expect(results[3]?.messages.map(({ ruleId }) => ruleId)).toContain("boundaries/dependencies");
  }, 60_000);
});

describe("messaging provider isolation", () => {
  it("keeps Kafka and NATS as siblings and confines NATS SDK access to its engine", async () => {
    const eslint = new ESLint({
      cwd: repositoryRoot,
      overrideConfig: [tseslint.configs.disableTypeChecked],
    });
    const fixtures = [
      {
        file: "src/features/nats/application/session.ts",
        source:
          'import { KafkaApplicationSession } from "../../kafka/application"; export const access=KafkaApplicationSession;',
        rule: "boundaries/dependencies",
      },
      {
        file: "src/features/kafka/application/session.ts",
        source:
          'import { NatsApplicationSession } from "../../nats/application"; export const access=NatsApplicationSession;',
        rule: "boundaries/dependencies",
      },
      {
        file: "src/features/nats/application/session.ts",
        source: 'import { connect } from "@nats-io/transport-node"; export const access=connect;',
        rule: "no-restricted-imports",
      },
      {
        file: "src/features/nats/facade/facade.ts",
        source:
          'import { StreamSkopeNatsEngine } from "../engine/engine"; export const access=StreamSkopeNatsEngine;',
        rule: "boundaries/dependencies",
      },
    ];
    for (const fixture of fixtures) {
      const [result] = await eslint.lintText(fixture.source, { filePath: fixture.file });
      expect(
        result?.messages.map((message) => message.ruleId),
        fixture.file,
      ).toContain(fixture.rule);
    }
  }, 60_000);
});

describe("renderer dependency boundary", () => {
  it("rejects Electron, Kafka clients, and Node imports while accepting renderer dependencies", async () => {
    // These fixtures exercise import restrictions; source lint owns type analysis.
    const eslint = new ESLint({
      cwd: repositoryRoot,
      overrideConfig: [tseslint.configs.disableTypeChecked],
    });
    const [validResult] = await eslint.lintText(
      'import { createElement } from "react"; export const access = createElement;',
      {
        filePath: "src/platform/electron/renderer/main.tsx",
      },
    );
    const forbiddenResults = await Promise.all(
      forbiddenRendererCases.map(async ({ source }) => {
        const [result] = await eslint.lintText(source, {
          filePath: "src/platform/electron/renderer/main.tsx",
        });

        return result;
      }),
    );

    expect(validResult?.messages).toEqual([]);
    expect(forbiddenResults).toHaveLength(forbiddenRendererCases.length);

    for (const result of forbiddenResults) {
      expect(result?.messages.map(({ ruleId }) => ruleId)).toContain("no-restricted-imports");
    }
  }, 60_000);
});

describe("shared Node host boundary", () => {
  it("accepts Node services and rejects Electron dependencies", async () => {
    const eslint = new ESLint({
      cwd: repositoryRoot,
      overrideConfig: [tseslint.configs.disableTypeChecked],
    });
    const sources = [
      'import { readFile } from "node:fs/promises"; export const access = readFile;',
      'import { app } from "electron"; export const access = app;',
      'import { createElectronShell } from "../electron/main"; export const access = createElectronShell;',
    ];
    const results = await Promise.all(
      sources.map(async (source) => {
        const [result] = await eslint.lintText(source, {
          filePath: "src/platform/node/kafka-backend.ts",
        });
        return result;
      }),
    );

    expect(results[0]?.messages).toEqual([]);
    for (const result of results.slice(1)) {
      expect(result?.messages.map(({ ruleId }) => ruleId)).toContain("no-restricted-imports");
    }
  }, 60_000);
});
