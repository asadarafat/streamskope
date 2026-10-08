import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, expect, it } from "vitest";

import { assertDependencyMaintenance } from "../../tools/check/dependency-maintenance";

const temporary: string[] = [];
afterEach(async () => {
  await Promise.all(temporary.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

interface MaintenanceFixture {
  root: string;
  lock: { lockfileVersion: number; packages: Record<string, Record<string, unknown>> };
  entries: Record<string, unknown>[];
}

async function fixture(): Promise<MaintenanceFixture> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-dependency-maintenance-"));
  temporary.push(root);
  await mkdir(join(root, "test", "unit"), { recursive: true });
  await writeFile(
    join(root, "test", "unit", "fixture.test.ts"),
    "// Independent policy fixture.\n",
  );
  // Keep fixture identities independent from the definitions that enforce patch applicability.
  const reviewed = [
    ["node-forge", "1.4.0", "proposed-fix"],
    ["braces", "3.0.3", "reported-unfixed"],
    ["http-cache-semantics", "4.2.0", "disputed-local-policy"],
    ["@nats-io/transport-node", "3.4.0", "local-runtime-correction"],
  ];
  return {
    root,
    lock: {
      lockfileVersion: 3,
      packages: {
        "": {},
        "node_modules/node-forge": { version: "1.4.0" },
        "node_modules/braces": { version: "3.0.3", dev: true },
        "node_modules/http-cache-semantics": { version: "4.2.0", dev: true },
        "node_modules/@nats-io/transport-node": { version: "3.4.0" },
      },
    },
    entries: reviewed.map(([name, reviewedVersion, upstreamStatus]) => ({
      name,
      reviewedVersion,
      owner: "@asadarafat",
      reviewedOn: "2026-10-08",
      upstreamStatus,
      evidence: ["https://github.com/asadarafat/streamskope/issues/1"],
      retirementCriteria:
        "Remove the mitigation only after the unpatched upstream candidate passes its owned behavior regression.",
      regressions: ["test/unit/fixture.test.ts"],
    })),
  };
}

it("qualifies the maintained repository inventory and its real regression references offline", async () => {
  const lock: unknown = JSON.parse(await readFile("package-lock.json", "utf8"));
  const result = await assertDependencyMaintenance(process.cwd(), lock);
  expect(result.mitigationCount).toBe(4);
  expect(result.lockedInstanceCount).toBeGreaterThanOrEqual(4);
});

it("identifies all four mitigations without optional package names or a review-expiry gate", async () => {
  const f = await fixture();
  f.entries[0]!.reviewedOn = "2000-02-29";
  await expect(assertDependencyMaintenance(f.root, f.lock, f.entries)).resolves.toEqual({
    mitigationCount: 4,
    lockedInstanceCount: 4,
  });
});

it("counts scoped and unscoped nested copies, including a mitigation with no root copy", async () => {
  const f = await fixture();
  f.lock.packages["node_modules/build-tool/node_modules/braces"] = { version: "3.0.3" };
  f.lock.packages["node_modules/@fixture/host/node_modules/@nats-io/transport-node"] = {
    version: "3.4.0",
  };
  await expect(assertDependencyMaintenance(f.root, f.lock, f.entries)).resolves.toEqual({
    mitigationCount: 4,
    lockedInstanceCount: 6,
  });
  delete f.lock.packages["node_modules/@nats-io/transport-node"];
  await expect(assertDependencyMaintenance(f.root, f.lock, f.entries)).resolves.toEqual({
    mitigationCount: 4,
    lockedInstanceCount: 5,
  });
});

it.each([
  ["node_modules/node-forge", "1.4.1"],
  ["node_modules/build-tool/node_modules/braces", "3.0.4"],
  ["node_modules/@fixture/host/node_modules/@nats-io/transport-node", "3.5.0"],
])(
  "rejects an unreviewed version at %s while the other locked instances remain valid",
  async (path, version) => {
    const f = await fixture();
    f.lock.packages[path] = { version };
    await expect(assertDependencyMaintenance(f.root, f.lock, f.entries)).rejects.toThrow(path);
  },
);

it("does not count a matching name reached through a noncanonical lock path", async () => {
  const f = await fixture();
  f.lock.packages["node_modules/build-tool/../node_modules/braces"] = { version: "3.0.3" };
  await expect(assertDependencyMaintenance(f.root, f.lock, f.entries)).rejects.toThrow(
    "node_modules/build-tool/../node_modules/braces",
  );
});

it("cannot authorize a version bump by changing the review and lock together", async () => {
  const f = await fixture();
  f.entries[0]!.reviewedVersion = "1.4.1";
  f.lock.packages["node_modules/node-forge"] = { version: "1.4.1" };
  await expect(assertDependencyMaintenance(f.root, f.lock, f.entries)).rejects.toThrow(
    /node-forge/u,
  );
});

it("requires a locked occurrence for every declared mitigation", async () => {
  const f = await fixture();
  delete f.lock.packages["node_modules/http-cache-semantics"];
  await expect(assertDependencyMaintenance(f.root, f.lock, f.entries)).rejects.toThrow(
    /http-cache-semantics/u,
  );
});

it.each([
  ["node_modules/forge-alias", "node-forge", "1.4.0"],
  ["node_modules/@fixture/host/node_modules/nats-alias", "@nats-io/transport-node", "3.4.0"],
  ["node_modules/node-forge", "unrelated-package", "1.4.0"],
])("rejects aliased or contradictory identity at %s", async (path, name, version) => {
  const f = await fixture();
  f.lock.packages[path] = { name, version };
  await expect(assertDependencyMaintenance(f.root, f.lock, f.entries)).rejects.toThrow();
});

it.each(["missing", "duplicate", "unexpected"])(
  "rejects %s maintenance entries even when the lock inventory is complete",
  async (mutation) => {
    const f = await fixture();
    if (mutation === "missing") f.entries.shift();
    else if (mutation === "duplicate") f.entries[3] = { ...f.entries[0] };
    else f.entries[3] = { ...f.entries[3], name: "unrelated-package" };
    await expect(assertDependencyMaintenance(f.root, f.lock, f.entries)).rejects.toThrow();
  },
);

it.each([
  ["owner", ""],
  ["owner", "not a GitHub handle"],
  ["reviewedOn", "2026-02-30"],
  ["reviewedOn", "2026-10-08T00:00:00Z"],
  ["upstreamStatus", "fixed-probably"],
  ["retirementCriteria", ""],
  ["evidence", []],
  ["regressions", []],
  ["undocumentedOverride", true],
] as const)("rejects invalid maintenance field %s=%s", async (field, value) => {
  const f = await fixture();
  f.entries[0]![field] = value;
  await expect(assertDependencyMaintenance(f.root, f.lock, f.entries)).rejects.toThrow();
});

it.each([
  ["HTTP", "http://github.com/digitalbazaar/forge/pull/1152"],
  ["credentials", "https://reviewer:private-token@github.com/digitalbazaar/forge/pull/1152"],
  ["unreviewed host", "https://unreviewed.example/mitigation"],
  ["deceptive host", "https://github.com.evil.example/digitalbazaar/forge/pull/1152"],
  ["overlong", `https://github.com/${"x".repeat(5_000)}`],
])("rejects %s evidence URLs", async (_label, url) => {
  const f = await fixture();
  f.entries[0]!.evidence = [url];
  await expect(assertDependencyMaintenance(f.root, f.lock, f.entries)).rejects.toThrow();
});

it.each([
  "test/unit/missing.test.ts",
  "test/unit",
  "../outside.test.ts",
  "/tmp/outside.test.ts",
  "test/unit/../../package.json",
])("requires a contained regular regression file for %s", async (path) => {
  const f = await fixture();
  f.entries[0]!.regressions = [path];
  await expect(assertDependencyMaintenance(f.root, f.lock, f.entries)).rejects.toThrow();
});

it("rejects regression symlinks and linked parent directories", async () => {
  const f = await fixture();
  await writeFile(join(f.root, "outside.test.ts"), "// Outside the allowed test tree.\n");
  await symlink(join(f.root, "outside.test.ts"), join(f.root, "test", "unit", "linked.test.ts"));
  f.entries[0]!.regressions = ["test/unit/linked.test.ts"];
  await expect(assertDependencyMaintenance(f.root, f.lock, f.entries)).rejects.toThrow();
  await symlink(join(f.root, "test", "unit"), join(f.root, "test", "unit", "linked"));
  f.entries[0]!.regressions = ["test/unit/linked/fixture.test.ts"];
  await expect(assertDependencyMaintenance(f.root, f.lock, f.entries)).rejects.toThrow();
});
