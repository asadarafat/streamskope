import process from "node:process";

import { afterEach, describe, expect, it, vi } from "vitest";

import { evaluateDependencyAudit, runNpmAudit, type AuditExecution } from "../../tools/check/audit";
import { BUILD_DEPENDENCY_PATCHES } from "../../tools/check/build-dependency-patch-data";

const verifiedPaths = ["node_modules/node-forge"];
const knownAdvisory = {
  source: 1240912,
  name: "node-forge",
  dependency: "node-forge",
  title: "RSA signature verification accepts extra DigestAlgorithm elements",
  url: "https://github.com/advisories/GHSA-86w9-cpqp-85rv",
  severity: "high",
  range: "<=1.4.0",
};

interface FixtureFinding {
  name: string;
  severity: string;
  isDirect: boolean;
  range: string;
  nodes: string[];
  via: unknown[];
}

function forgeFinding(): FixtureFinding {
  return {
    name: "node-forge",
    severity: "high",
    isDirect: true,
    range: "*",
    nodes: ["node_modules/node-forge"],
    via: [{ ...knownAdvisory }],
  };
}

function inheritedFinding(name = "jks-js", dependency = "node-forge"): FixtureFinding {
  return {
    name,
    severity: "high",
    isDirect: true,
    range: ">=1.0.0",
    nodes: [`node_modules/${name}`],
    via: [dependency],
  };
}

function report(findings: readonly FixtureFinding[]): Record<string, unknown> {
  const counts = Object.fromEntries(
    ["info", "low", "moderate", "high", "critical"].map((severity) => [
      severity,
      findings.filter((finding) => finding.severity === severity).length,
    ]),
  );
  return {
    auditReportVersion: 2,
    vulnerabilities: Object.fromEntries(findings.map((finding) => [finding.name, finding])),
    metadata: { vulnerabilities: { ...counts, total: findings.length } },
  };
}

function execution(findings: readonly FixtureFinding[]): AuditExecution {
  return {
    status: findings.some(
      (finding) => finding.severity === "high" || finding.severity === "critical",
    )
      ? 1
      : 0,
    stdout: JSON.stringify(report(findings)),
  };
}

describe("dependency audit qualification", () => {
  afterEach(() => vi.unstubAllEnvs());

  it.each(BUILD_DEPENDENCY_PATCHES)(
    "accepts only verified $name mitigation and exclusively inherited findings",
    (patch) => {
      const direct: FixtureFinding = {
        name: patch.name,
        severity: "high",
        isDirect: false,
        range: "*",
        nodes: [`node_modules/${patch.name}`],
        via: [
          {
            source: 9991001,
            name: patch.name,
            dependency: patch.name,
            severity: "high",
            url: patch.advisoryUrl,
            range: `<=${patch.version}`,
          },
        ],
      };
      const verified = [{ name: patch.name, advisoryUrl: patch.advisoryUrl, paths: direct.nodes }];
      const parent = inheritedFinding("build-consumer", patch.name);
      const ancestor = inheritedFinding("build-tool", parent.name);
      parent.via.push(ancestor.name);
      expect(
        evaluateDependencyAudit(execution([direct, parent, ancestor]), [], verified)
          .remainingHighOrCriticalPackages,
      ).toEqual([]);
      expect(
        evaluateDependencyAudit(execution([direct, parent, ancestor]), [])
          .remainingHighOrCriticalPackages,
      ).toHaveLength(3);
      parent.via.pop();
      for (const change of [
        { url: `${patch.advisoryUrl}?changed` },
        { range: "*" },
        { severity: "critical" },
        { dependency: "other" },
      ]) {
        const changed = { ...direct, via: [{ ...(direct.via[0] as object), ...change }] };
        const changedParent = { ...parent, severity: change.severity ?? parent.severity };
        changed.severity = change.severity ?? changed.severity;
        expect(
          evaluateDependencyAudit(execution([changed, changedParent]), [], verified)
            .remainingHighOrCriticalPackages,
        ).toEqual([parent.name, patch.name].sort());
      }
      const unverified = {
        ...direct,
        nodes: [...direct.nodes, `node_modules/other/node_modules/${patch.name}`],
      };
      expect(
        evaluateDependencyAudit(execution([unverified, parent]), [], verified)
          .remainingHighOrCriticalPackages,
      ).toHaveLength(2);
      const independent = {
        ...parent,
        via: [
          ...parent.via,
          {
            ...knownAdvisory,
            name: parent.name,
            dependency: parent.name,
            url: "https://github.com/advisories/GHSA-new-advisory",
          },
        ],
      };
      expect(
        evaluateDependencyAudit(execution([direct, independent]), [], verified)
          .remainingHighOrCriticalPackages,
      ).toEqual([parent.name]);
    },
  );

  it("overrides inherited offline, registry and omitted dependency configuration at the npm CLI", () => {
    vi.stubEnv("npm_config_offline", "true");
    vi.stubEnv("npm_config_prefer_offline", "true");
    vi.stubEnv("npm_config_registry", "https://registry.invalid/");
    vi.stubEnv("npm_config_omit", "dev optional peer");
    vi.stubEnv("NODE_ENV", "production");
    const execute = vi.fn(() => execution([forgeFinding(), inheritedFinding()]));
    const result = runNpmAudit("/project", "/npm/bin/npm-cli.js", execute);
    expect(execute).toHaveBeenCalledWith(
      process.execPath,
      expect.arrayContaining([
        "/npm/bin/npm-cli.js",
        "audit",
        "--package-lock-only",
        "--audit-level=high",
        "--json",
        "--registry=https://registry.npmjs.org/",
        "--offline=false",
        "--prefer-offline=false",
        "--audit=true",
        "--include=prod",
        "--include=dev",
        "--include=optional",
        "--include=peer",
      ]),
      expect.objectContaining({ cwd: "/project", encoding: "utf8", timeout: 120_000 }),
    );
    expect(evaluateDependencyAudit(result, verifiedPaths).upstreamAffectedPackages).toBe(2);
  });

  it("accounts for only the verified direct backport and its jks-js propagation", () => {
    expect(
      evaluateDependencyAudit(execution([forgeFinding(), inheritedFinding()]), verifiedPaths),
    ).toEqual({
      upstreamAffectedPackages: 2,
      backportedPackages: ["jks-js", "node-forge"],
      remainingHighOrCriticalPackages: [],
      otherReportedPackages: [],
    });
  });

  it("reports a clean upstream audit without claiming a backported advisory was reported", () => {
    expect(evaluateDependencyAudit(execution([]), verifiedPaths)).toEqual({
      upstreamAffectedPackages: 0,
      backportedPackages: [],
      remainingHighOrCriticalPackages: [],
      otherReportedPackages: [],
    });
  });

  it.each([{ paths: [] }, { paths: ["node_modules/unrelated/node_modules/node-forge"] }])(
    "refuses the exception when the reported forge node has not been verified (%j)",
    ({ paths }) => {
      expect(
        evaluateDependencyAudit(execution([forgeFinding(), inheritedFinding()]), paths)
          .remainingHighOrCriticalPackages,
      ).toEqual(["jks-js", "node-forge"]);
    },
  );

  it("refuses the exception if even one forge installation is unverified", () => {
    const forge = forgeFinding();
    forge.nodes.push("node_modules/jks-js/node_modules/node-forge");
    expect(
      evaluateDependencyAudit(execution([forge, inheritedFinding()]), verifiedPaths)
        .remainingHighOrCriticalPackages,
    ).toEqual(["jks-js", "node-forge"]);
  });

  it("accepts multiple installations only when every reported path was verified", () => {
    const forge = forgeFinding();
    forge.nodes.push("node_modules/jks-js/node_modules/node-forge");
    expect(evaluateDependencyAudit(execution([forge]), forge.nodes).backportedPackages).toEqual([
      "node-forge",
    ]);
  });

  it("does not extend the reviewed jks-js propagation to an unknown consumer installation", () => {
    const inherited = inheritedFinding();
    inherited.nodes.push("node_modules/unknown-copy/node_modules/jks-js");
    const result = evaluateDependencyAudit(execution([forgeFinding(), inherited]), verifiedPaths);
    expect(result.backportedPackages).toEqual(["node-forge"]);
    expect(result.remainingHighOrCriticalPackages).toEqual(["jks-js"]);
  });

  it.each([
    { url: "https://github.com/advisories/GHSA-new-advisory" },
    { url: `${knownAdvisory.url}?other=true` },
    { dependency: "different-package" },
    { name: "different-package" },
    { range: "<1.5.0" },
    { severity: "critical" },
  ])("does not allow a changed advisory identity or scope: %j", (change) => {
    const forge = forgeFinding();
    forge.via = [{ ...knownAdvisory, ...change }];
    if (change.severity) forge.severity = change.severity;
    expect(
      evaluateDependencyAudit(execution([forge]), verifiedPaths).remainingHighOrCriticalPackages,
    ).toEqual(["node-forge"]);
  });

  it("keeps new forge advisories and their jks-js propagation blocking", () => {
    const forge = forgeFinding();
    forge.via.push({
      ...knownAdvisory,
      source: 9999999,
      url: "https://github.com/advisories/GHSA-new-advisory",
    });
    expect(
      evaluateDependencyAudit(execution([forge, inheritedFinding()]), verifiedPaths)
        .remainingHighOrCriticalPackages,
    ).toEqual(["jks-js", "node-forge"]);
  });

  it("does not hide an independent jks-js advisory behind its backported dependency", () => {
    const inherited = inheritedFinding();
    inherited.via.push({
      ...knownAdvisory,
      name: "jks-js",
      dependency: "jks-js",
      url: "https://github.com/advisories/GHSA-new-advisory",
    });
    const result = evaluateDependencyAudit(execution([forgeFinding(), inherited]), verifiedPaths);
    expect(result.backportedPackages).toEqual(["node-forge"]);
    expect(result.remainingHighOrCriticalPackages).toEqual(["jks-js"]);
  });

  it("keeps unrelated high and critical packages blocking", () => {
    const unrelated = inheritedFinding("unrelated");
    unrelated.severity = "critical";
    unrelated.via = [
      {
        ...knownAdvisory,
        name: "unrelated",
        dependency: "unrelated",
        severity: "critical",
        url: "https://github.com/advisories/GHSA-new-advisory",
      },
    ];
    const result = evaluateDependencyAudit(execution([forgeFinding(), unrelated]), verifiedPaths);
    expect(result.backportedPackages).toEqual(["node-forge"]);
    expect(result.remainingHighOrCriticalPackages).toEqual(["unrelated"]);
  });

  it("reports moderate findings without changing the existing high severity threshold", () => {
    const moderate = inheritedFinding("moderate-package");
    moderate.severity = "moderate";
    moderate.via = [
      {
        ...knownAdvisory,
        name: moderate.name,
        dependency: moderate.name,
        severity: "moderate",
        url: "https://github.com/advisories/GHSA-moderate-advisory",
      },
    ];
    expect(evaluateDependencyAudit(execution([moderate]), verifiedPaths)).toMatchObject({
      upstreamAffectedPackages: 1,
      backportedPackages: [],
      remainingHighOrCriticalPackages: [],
      otherReportedPackages: ["moderate-package"],
    });
  });

  it.each([
    { findings: [inheritedFinding()] },
    { findings: [inheritedFinding("node-forge", "node-forge")] },
    { findings: [inheritedFinding("node-forge", "jks-js"), inheritedFinding()] },
  ])("fails closed on missing or cyclic dependency references (%j)", ({ findings }) => {
    expect(() => evaluateDependencyAudit(execution(findings), verifiedPaths)).toThrow(
      /missing or cyclic/u,
    );
  });

  it.each([
    "",
    "not json",
    "null",
    "[]",
    JSON.stringify({ error: { code: "ENOAUDIT" } }),
    JSON.stringify({ ...report([]), error: { code: "ECONNRESET" } }),
    JSON.stringify({ ...report([]), auditReportVersion: 1 }),
    JSON.stringify({ auditReportVersion: 2, vulnerabilities: {} }),
    JSON.stringify({ ...report([]), metadata: { vulnerabilities: { total: 0 } } }),
  ])("fails closed on malformed or error reports: %s", (stdout) => {
    expect(() => evaluateDependencyAudit({ status: 0, stdout }, verifiedPaths)).toThrow();
  });

  it.each([
    { nodes: [] },
    { via: [] },
    { via: [null] },
    { via: [""] },
    { nodes: ["../node_modules/node-forge"] },
    { nodes: ["node_modules/../node-forge"] },
    { nodes: ["node_modules/node-forge", "node_modules/node-forge"] },
    { severity: "unknown" },
    { isDirect: undefined },
    { name: "different-package" },
    { via: [{ ...knownAdvisory, source: -1 }] },
  ])("fails closed on malformed findings: %j", (change) => {
    const source = report([forgeFinding()]);
    source.vulnerabilities = { "node-forge": { ...forgeFinding(), ...change } };
    expect(() =>
      evaluateDependencyAudit({ status: 1, stdout: JSON.stringify(source) }, verifiedPaths),
    ).toThrow();
  });

  it("rejects reports whose metadata omits a finding", () => {
    const source = report([forgeFinding()]);
    source.metadata = {
      vulnerabilities: { info: 0, low: 0, moderate: 0, high: 0, critical: 0, total: 0 },
    };
    expect(() =>
      evaluateDependencyAudit({ status: 1, stdout: JSON.stringify(source) }, verifiedPaths),
    ).toThrow(/counts/u);
  });

  it("rejects reports that hide a high advisory in a lower-severity package", () => {
    const forge = forgeFinding();
    forge.severity = "moderate";
    expect(() => evaluateDependencyAudit(execution([forge]), verifiedPaths)).toThrow(
      /understates/u,
    );
  });

  it.each([
    { status: null },
    { status: 2 },
    { status: 0, signal: "SIGTERM" },
    { status: 0, error: new Error("registry connection failed") },
    { status: 1 },
  ])("rejects npm execution failures even alongside valid JSON: %j", (change) => {
    expect(() => evaluateDependencyAudit({ ...execution([]), ...change }, verifiedPaths)).toThrow();
  });

  it("rejects successful exit status paired with unresolved high findings", () => {
    expect(() =>
      evaluateDependencyAudit({ ...execution([forgeFinding()]), status: 0 }, verifiedPaths),
    ).toThrow(/exit status/u);
  });
});
