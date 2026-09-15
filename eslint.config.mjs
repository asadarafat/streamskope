import eslint from "@eslint/js";
import boundaries from "eslint-plugin-boundaries";
import importPlugin from "eslint-plugin-import-x";
import tseslint from "typescript-eslint";

const typedFiles = ["**/*.ts", "**/*.tsx"];
const rendererFiles = [
  "src/app/**/*.{ts,tsx}",
  "src/kafka/ui/**/*.{ts,tsx}",
  "src/renderer/**/*.{ts,tsx}",
  "src/ui/**/*.{ts,tsx}",
  "test/architecture/fixtures/renderer/**/*.{ts,tsx}",
];

export default [
  {
    ignores: [
      ".codex/**",
      "coverage/**",
      "dist/**",
      "node_modules/**",
      "**/node_modules/**",
      "website/.site/**",
      "website/.preview/**",
      "website/.cache/**",
      "openspec/**",
      "playwright-report/**",
      "test-results/**",
      // Retired evaluation output may remain in existing checkouts.
      "wails/frontend/dist/**",
    ],
  },
  eslint.configs.recommended,
  ...tseslint.configs.recommendedTypeChecked.map((configuration) => ({
    ...configuration,
    files: typedFiles,
  })),
  {
    files: typedFiles,
    languageOptions: {
      parser: tseslint.parser,
      parserOptions: {
        projectService: {
          allowDefaultProject: ["test/architecture/fixtures/renderer/*.ts"],
        },
        tsconfigRootDir: import.meta.dirname,
      },
    },
    plugins: {
      boundaries,
      "import-x": importPlugin,
    },
    settings: {
      "boundaries/elements": [
        { pattern: "src/app/**", type: "app" },
        { pattern: "src/kafka/contracts/**", type: "kafka-contracts" },
        { pattern: "src/kafka/application/**", type: "kafka-application" },
        { pattern: "src/kafka/facade/**", type: "kafka-facade" },
        { pattern: "src/kafka/engine/**", type: "kafka-engine" },
        { pattern: "src/kafka/ui/**", type: "kafka-renderer" },
        { pattern: "src/platform/desktop/**", type: "platform-desktop" },
        { pattern: "src/platform/activity/**", type: "platform-activity" },
        { pattern: "src/platform/dev-host/**", type: "platform-dev-host" },
        { pattern: "src/main/**", type: "main" },
        { pattern: "src/preload/**", type: "preload" },
        { pattern: "src/renderer/**", type: "renderer" },
        { pattern: "src/ui/**", type: "ui" },
        { pattern: "test/architecture/fixtures/renderer/**", type: "renderer" },
      ],
    },
    rules: {
      "@typescript-eslint/explicit-function-return-type": "error",
      "@typescript-eslint/no-explicit-any": "error",
      "@typescript-eslint/no-floating-promises": "error",
      "@typescript-eslint/no-misused-promises": "error",
      "@typescript-eslint/no-unused-vars": ["error", { argsIgnorePattern: "^_" }],
      "import-x/no-duplicates": "error",
      "import-x/order": [
        "error",
        {
          groups: ["builtin", "external", "internal", "parent", "sibling", "index"],
          "newlines-between": "always",
        },
      ],
      "max-lines": ["error", { max: 1000, skipBlankLines: true, skipComments: true }],
      "no-console": "error",
      "no-trailing-spaces": "error",
      "boundaries/dependencies": [
        "error",
        {
          default: "disallow",
          policies: [
            {
              allow: {
                to: {
                  element: { types: { anyOf: ["kafka-contracts", "platform-desktop"] } },
                },
              },
              from: { element: { type: "kafka-contracts" } },
            },
            {
              allow: { to: { element: { type: "platform-desktop" } } },
              from: { element: { type: "platform-desktop" } },
            },
            {
              allow: {
                to: { element: { types: { anyOf: ["kafka-application", "kafka-contracts"] } } },
              },
              from: { element: { type: "kafka-application" } },
            },
            {
              allow: {
                to: {
                  element: {
                    types: {
                      anyOf: ["kafka-application", "kafka-contracts", "kafka-engine"],
                    },
                  },
                },
              },
              from: { element: { type: "kafka-engine" } },
            },
            {
              allow: {
                to: {
                  element: {
                    types: {
                      anyOf: [
                        "kafka-application",
                        "kafka-contracts",
                        "kafka-facade",
                        "platform-activity",
                      ],
                    },
                  },
                },
              },
              from: { element: { type: "kafka-facade" } },
            },
            {
              allow: {
                to: {
                  element: {
                    types: {
                      anyOf: ["kafka-contracts", "kafka-renderer", "platform-desktop", "ui"],
                    },
                  },
                },
              },
              from: { element: { type: "kafka-renderer" } },
            },
            {
              allow: {
                to: {
                  element: {
                    types: {
                      anyOf: ["kafka-contracts", "kafka-renderer", "platform-desktop", "ui"],
                    },
                  },
                },
              },
              from: { element: { type: "app" } },
            },
            {
              allow: { to: { element: { type: "ui" } } },
              from: { element: { type: "ui" } },
            },
            {
              allow: {
                to: { element: { types: { anyOf: ["kafka-contracts", "platform-activity"] } } },
              },
              from: { element: { type: "platform-activity" } },
            },
            {
              allow: {
                to: {
                  element: {
                    types: {
                      anyOf: [
                        "kafka-contracts",
                        "kafka-facade",
                        "platform-activity",
                        "platform-desktop",
                        "platform-dev-host",
                      ],
                    },
                  },
                },
              },
              from: { element: { type: "platform-dev-host" } },
            },
            {
              allow: {
                to: {
                  element: {
                    types: {
                      anyOf: [
                        "kafka-application",
                        "kafka-contracts",
                        "kafka-engine",
                        "kafka-facade",
                        "main",
                        "platform-activity",
                        "platform-dev-host",
                        "preload",
                      ],
                    },
                  },
                },
              },
              from: { element: { type: "main" } },
            },
            {
              allow: {
                to: {
                  element: {
                    types: { anyOf: ["kafka-contracts", "platform-desktop", "preload"] },
                  },
                },
              },
              from: { element: { type: "preload" } },
            },
            {
              allow: {
                to: {
                  element: {
                    types: {
                      anyOf: [
                        "app",
                        "kafka-contracts",
                        "kafka-renderer",
                        "platform-desktop",
                        "renderer",
                        "ui",
                      ],
                    },
                  },
                },
              },
              from: { element: { type: "renderer" } },
            },
          ],
        },
      ],
    },
  },
  {
    files: rendererFiles,
    rules: {
      "no-restricted-imports": [
        "error",
        {
          paths: [
            {
              message: "Renderer code must use the typed StreamSkopeHost contract.",
              name: "electron",
            },
            {
              message: "Renderer code must not access the Kafka adapter.",
              name: "kafkajs",
            },
            {
              message: "Renderer code must not access the Kafka adapter.",
              name: "@platformatic/kafka",
            },
          ],
          patterns: [
            {
              group: ["node:*"],
              message: "Renderer code must not access Node.js APIs.",
            },
            {
              group: [
                "**/application/**",
                "**/application",
                "**/facade/**",
                "**/facade",
                "**/engine/**",
                "**/engine",
                "**/main/**",
                "**/preload/**",
              ],
              message: "Renderer code must depend only on renderer modules and contracts.",
            },
            {
              regex: "(^|/)platform/(?!desktop(?:/|$))",
              message: "Renderer code may use only the declared desktop platform contract.",
            },
          ],
        },
      ],
    },
  },
  {
    files: ["eslint.config.mjs"],
    languageOptions: {
      globals: {
        console: "readonly",
      },
    },
  },
  {
    files: ["tools/build-electron.mjs", "tools/run-playwright-e2e.mjs"],
    languageOptions: {
      globals: {
        process: "readonly",
      },
    },
  },
];
