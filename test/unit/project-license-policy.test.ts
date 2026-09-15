import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

interface ProjectManifest {
  readonly license?: string;
  readonly private?: boolean;
}

interface ProjectLockfile {
  readonly packages?: Readonly<Record<string, { readonly license?: string }>>;
}

const APACHE_2_LICENSE_SHA256 = "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30";

describe("StreamSkope project license policy", () => {
  it("provides canonical Apache-2.0 terms with consistent metadata while publication stays private", async () => {
    const [license, manifestText, lockfileText] = await Promise.all([
      readFile(new URL("../../LICENSE", import.meta.url)),
      readFile(new URL("../../package.json", import.meta.url), "utf8"),
      readFile(new URL("../../package-lock.json", import.meta.url), "utf8"),
    ]);
    const manifest = JSON.parse(manifestText) as ProjectManifest;
    const lockfile = JSON.parse(lockfileText) as ProjectLockfile;

    expect(createHash("sha256").update(license).digest("hex")).toBe(APACHE_2_LICENSE_SHA256);
    expect(manifest.license).toBe("Apache-2.0");
    expect(lockfile.packages?.[""]?.license).toBe("Apache-2.0");
    expect(manifest.private).toBe(true);
  });
});
