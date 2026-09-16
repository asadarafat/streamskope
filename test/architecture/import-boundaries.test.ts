import { fileURLToPath } from "node:url";

import { ESLint } from "eslint";
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
    fileName: "node-import.ts",
    source: 'import { readFile } from "node:fs/promises"; export const access = readFile;',
  },
  {
    fileName: "undeclared-platform-import.ts",
    source:
      'import { access } from "../../../../src/platform/undeclared"; export const result = access;',
  },
];

describe("renderer dependency boundary", () => {
  it("rejects Electron, Kafka clients, and Node imports while accepting renderer dependencies", async () => {
    const eslint = new ESLint({ cwd: repositoryRoot });
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
  }, 20_000);
});
