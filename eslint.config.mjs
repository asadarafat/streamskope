import eslint from "@eslint/js";
import boundaries from "eslint-plugin-boundaries";
import importPlugin from "eslint-plugin-import-x";
import tseslint from "typescript-eslint";

const typedFiles = ["**/*.ts", "**/*.tsx"];
const rendererFiles = [
  "plugins/eda/ui/**/*.{ts,tsx}",
  "plugins/nsp/ui/**/*.{ts,tsx}",
  "src/features/kafka/ui/**/*.{ts,tsx}",
  "src/platform/electron/renderer/**/*.{ts,tsx}",
  "src/platform/ui/**/*.{ts,tsx}",
  "test/architecture/fixtures/renderer/**/*.{ts,tsx}",
];

export default [
  {
    ignores: [
      ".codex/**",
      ".cache/**",
      ".artifacts/**",
      "coverage/**",
      "dist/**",
      "node_modules/**",
      "**/node_modules/**",
      "website/.site/**",
      "website/.preview/**",
      "website/.cache/**",
      "website/docs/launch/assets/**",
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
      "import/resolver": {
        node: { extensions: [".js", ".mjs", ".cjs", ".ts", ".tsx", ".json"] },
      },
      "boundaries/elements": [
        { pattern: "src/plugins/**", type: "plugin-api" },
        { pattern: "plugins/eda/contracts/**", type: "eda-contracts" },
        { pattern: "plugins/eda/backend/**", type: "eda-backend" },
        { pattern: "plugins/eda/ui/**", type: "eda-ui" },
        { pattern: "plugins/nsp/contracts/**", type: "nsp-contracts" },
        { pattern: "plugins/nsp/backend/**", type: "nsp-backend" },
        { pattern: "plugins/nsp/ui/**", type: "nsp-ui" },
        { pattern: "src/features/kafka/contracts/**", type: "kafka-contracts" },
        { pattern: "src/features/kafka/application/**", type: "kafka-application" },
        { pattern: "src/features/kafka/facade/**", type: "kafka-facade" },
        { pattern: "src/features/kafka/engine/**", type: "kafka-engine" },
        { pattern: "src/features/kafka/ui/**", type: "kafka-renderer" },
        { pattern: "src/platform/desktop/**", type: "platform-desktop" },
        { pattern: "src/platform/activity/**", type: "platform-activity" },
        { pattern: "src/platform/dev-host/**", type: "platform-dev-host" },
        { pattern: "src/platform/node/**", type: "platform-node" },
        { pattern: "src/platform/electron/main/**", type: "main" },
        { pattern: "src/platform/electron/preload/**", type: "preload" },
        { pattern: "src/platform/electron/renderer/**", type: "renderer" },
        { pattern: "src/platform/ui/**", type: "ui" },
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
              from: {
                element: {
                  types: {
                    anyOf: [
                      "kafka-contracts",
                      "kafka-application",
                      "kafka-facade",
                      "kafka-engine",
                      "kafka-renderer",
                      "platform-desktop",
                      "platform-activity",
                      "platform-dev-host",
                      "platform-node",
                      "main",
                      "preload",
                      "renderer",
                    ],
                  },
                },
              },
              allow: { to: { element: { type: "plugin-api" } } },
            },
            {
              from: { element: { type: "plugin-api" } },
              allow: { to: { element: { types: { anyOf: ["plugin-api", "kafka-contracts"] } } } },
            },
            {
              from: { element: { type: "eda-contracts" } },
              allow: {
                to: {
                  element: { types: { anyOf: ["eda-contracts", "plugin-api", "kafka-contracts"] } },
                },
              },
            },
            {
              from: { element: { type: "eda-backend" } },
              allow: {
                to: {
                  element: {
                    types: {
                      anyOf: ["eda-backend", "eda-contracts", "plugin-api", "kafka-contracts"],
                    },
                  },
                },
              },
            },
            {
              from: { element: { type: "eda-ui" } },
              allow: {
                to: {
                  element: {
                    types: {
                      anyOf: [
                        "eda-ui",
                        "eda-contracts",
                        "plugin-api",
                        "kafka-contracts",
                        "kafka-renderer",
                        "platform-desktop",
                        "ui",
                      ],
                    },
                  },
                },
              },
            },
            {
              from: { element: { type: "nsp-contracts" } },
              allow: {
                to: {
                  element: { types: { anyOf: ["nsp-contracts", "plugin-api", "kafka-contracts"] } },
                },
              },
            },
            {
              from: { element: { type: "nsp-backend" } },
              allow: {
                to: {
                  element: {
                    types: {
                      anyOf: ["nsp-backend", "nsp-contracts", "plugin-api", "kafka-contracts"],
                    },
                  },
                },
              },
            },
            {
              from: { element: { type: "nsp-ui" } },
              allow: {
                to: {
                  element: {
                    types: {
                      anyOf: [
                        "nsp-ui",
                        "nsp-contracts",
                        "plugin-api",
                        "kafka-contracts",
                        "kafka-renderer",
                        "platform-desktop",
                        "ui",
                      ],
                    },
                  },
                },
              },
            },
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
                        "platform-node",
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
                        "platform-desktop",
                        "platform-node",
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
                    types: {
                      anyOf: [
                        "kafka-application",
                        "kafka-contracts",
                        "kafka-engine",
                        "kafka-facade",
                        "platform-activity",
                        "platform-node",
                      ],
                    },
                  },
                },
              },
              from: { element: { type: "platform-node" } },
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
    files: ["src/platform/node/**/*.ts"],
    rules: {
      "no-restricted-imports": [
        "error",
        {
          paths: [{ name: "electron", message: "Shared Node hosts must not depend on Electron." }],
          patterns: [
            {
              group: ["**/electron/**"],
              message: "Keep desktop dependencies in the Electron host.",
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
            {
              message: "Renderer code must not access the Kubernetes adapter.",
              name: "@kubernetes/client-node",
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
              regex: "(^|/)platform/(?!desktop(?:/|$)|ui(?:/|$))",
              message:
                "Renderer code may use only declared desktop and shared UI platform contracts.",
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
    files: ["tools/build.mjs", "tools/package/e2e.mjs"],
    languageOptions: {
      globals: {
        process: "readonly",
      },
    },
  },
];
