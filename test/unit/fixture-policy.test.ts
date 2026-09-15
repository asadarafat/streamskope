import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

const repositoryRoot = fileURLToPath(new URL("../..", import.meta.url));
const fixtureRoot = new URL("../../aio-kafka/", import.meta.url);

function readFixture(relativePath: string): string {
  return readFileSync(new URL(relativePath, fixtureRoot), "utf8");
}

describe("aio-kafka fixture source policy", () => {
  it("pins external container images by immutable digest", () => {
    const topology = readFixture("topology.clab.yml");
    const dockerfile = readFixture("images/oauth-service/Dockerfile");

    expect(topology).not.toMatch(/image:\s+\S+:latest\b/u);
    expect(topology).toMatch(/image:\s+apache\/kafka:[^\s@]+@sha256:[a-f0-9]{64}\b/u);
    expect(topology).toMatch(
      /schema-registry:[\s\S]*image:\s+\$\{STREAMSKOPE_SCHEMA_REGISTRY_IMAGE\}/u,
    );
    expect(readFixture("fixture.config.json")).toMatch(
      /"schemaRegistryImage":\s*"ghcr\.io\/aiven-open\/karapace:5\.0\.3@sha256:[a-f0-9]{64}"/u,
    );
    expect(dockerfile).toMatch(/^FROM python:[^\s@]+@sha256:[a-f0-9]{64}$/mu);
  });

  it("pins every Python runtime package", () => {
    const dockerfile = readFixture("images/oauth-service/Dockerfile");
    const requirements = readFixture("images/oauth-service/requirements.txt").trim().split("\n");

    expect(dockerfile).toContain("--requirement /opt/oauth/requirements.txt");
    expect(requirements).toEqual(
      expect.arrayContaining([
        expect.stringMatching(/^flask==/u),
        expect.stringMatching(/^pyjwt==/u),
        expect.stringMatching(/^cryptography==/u),
      ]),
    );

    for (const requirement of requirements) {
      expect(requirement).toMatch(/^[a-z][a-z0-9-]*==[a-z0-9.]+$/u);
    }
  });

  it("excludes generated certificates and Containerlab runtime output", () => {
    const repositoryFiles = execFileSync(
      "git",
      ["ls-files", "--cached", "--others", "--exclude-standard", "--", "aio-kafka"],
      { cwd: repositoryRoot, encoding: "utf8" },
    )
      .trim()
      .split("\n")
      .filter(Boolean);

    expect(repositoryFiles).not.toEqual(
      expect.arrayContaining([
        expect.stringMatching(
          /(?:^|\/)(?:clab-[^/]+|certs)(?:\/|$)|\.annotations\.json$|\.DS_Store$/u,
        ),
      ]),
    );
  });

  it("never prints the certificate password", () => {
    const certificateScript = readFixture("make-certs.sh");

    expect(certificateScript).not.toMatch(/echo\b[^\n]*(?:PASSWORD=|Password is)/u);
  });

  it("requires invocation-owned names and host ports", () => {
    const topology = readFixture("topology.clab.yml");

    expect(topology).toContain("name: ${STREAMSKOPE_FIXTURE_NAME}");
    expect(topology).toContain("${STREAMSKOPE_KAFKA_PORT}:9093/tcp");
    expect(topology).toContain("${STREAMSKOPE_OAUTH_PORT}:5000/tcp");
    expect(topology).toContain("${STREAMSKOPE_SCHEMA_REGISTRY_PORT}:8081/tcp");
    expect(readFixture("config/kafka-broker/server.properties")).toContain("REGISTRY:PLAINTEXT");
    expect(topology).not.toContain("clab-aio-kafka");
  });

  it("separates bounded container liveness from complete fixture readiness", () => {
    const topology = readFixture("topology.clab.yml");
    const dockerfile = readFixture("images/oauth-service/Dockerfile");

    expect(topology.match(/interval:\s+10\b/gu)).toHaveLength(3);
    expect(topology.match(/timeout:\s+2\b/gu)).toHaveLength(3);
    expect(topology.match(/retries:\s+6\b/gu)).toHaveLength(3);
    expect(topology.match(/-\s+awk\b/gu)).toHaveLength(3);
    expect(topology.match(/\/proc\/net\/tcp\b/gu)).toHaveLength(3);
    expect(topology.match(/\/proc\/net\/tcp6/gu)).toHaveLength(3);
    for (const port of ["1388", "1F91", "2384"]) {
      expect(topology).toContain(`$2 ~ /:${port}$/ && $4 == "0A"`);
    }
    expect(topology).not.toContain("/dev/tcp");
    expect(topology).not.toMatch(/kafka-broker-api-versions\.sh|python3?\s+-c/u);
    expect(dockerfile).not.toMatch(/^HEALTHCHECK\b/mu);
    expect(topology).toContain('KAFKA_HEAP_OPTS: "-Xms256M -Xmx512M"');
    expect(topology).toContain('PRODUCER_ENABLED: "false"');
  });

  it("requires the owned Schema Registry to validate the fixture OAuth bearer", () => {
    const topology = readFixture("topology.clab.yml");
    const oauthService = readFixture("images/oauth-service/server.py");
    const brokerConfig = readFixture("config/kafka-broker/server.properties");

    expect(topology).toContain(
      'OAUTH_EXPECTED_AUDIENCE: "${STREAMSKOPE_SCHEMA_REGISTRY_AUDIENCE}"',
    );
    expect(topology).toContain('OAUTH_EXPECTED_ISSUER: "${STREAMSKOPE_OAUTH_ISSUER}"');
    expect(brokerConfig).toContain("sasl.oauthbearer.expected.audience=${OAUTH_EXPECTED_AUDIENCE}");
    expect(brokerConfig).toContain("sasl.oauthbearer.expected.issuer=${OAUTH_EXPECTED_ISSUER}");
    expect(topology).toContain('KARAPACE_SASL_OAUTHBEARER_AUTHORIZATION_ENABLED: "true"');
    expect(topology).toContain("KARAPACE_SASL_OAUTHBEARER_JWKS_ENDPOINT_URL:");
    expect(topology).toContain("KARAPACE_SASL_OAUTHBEARER_EXPECTED_ISSUER:");
    expect(topology).toContain("KARAPACE_SASL_OAUTHBEARER_EXPECTED_AUDIENCE:");
    expect(topology).toContain("KARAPACE_SASL_OAUTHBEARER_ROLES_CLAIM_PATH:");
    expect(topology).toContain("KARAPACE_SASL_OAUTHBEARER_METHOD_ROLES:");
    expect(oauthService).toContain('"iss": OAUTH_ISSUER');
    expect(oauthService).toContain('"aud": SCHEMA_REGISTRY_AUDIENCE');
    expect(oauthService).toContain('"roles": [SCHEMA_REGISTRY_ROLE]');
  });
});
