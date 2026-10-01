import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import { EDA_CAPTURE_PUBLIC_KEY } from "../../plugins/eda/backend/eda-capture-public-key";

const repositoryRoot = fileURLToPath(new URL("../..", import.meta.url));
const appRoot = `${repositoryRoot}/vendors/streamskope/apps/capture`;

describe("EDA application release policy", () => {
  it("packages exactly the public key published with the EDA application", async () => {
    const source = await readFile(`${appRoot}/signing/streamskope-eda.pub`, "utf8");
    expect(EDA_CAPTURE_PUBLIC_KEY).toBe(source);
  });
});
