import { expect, it, vi } from "vitest";

import { explicitFixtureConnection } from "../../aio-kafka/fixtures/development";
import { preflightRemoteAioFixture } from "../../aio-kafka/fixtures/remote";

const configured = {
  STREAMSKOPE_TEST_KAFKA_ENDPOINT: "aio.example.test:19093",
  STREAMSKOPE_TEST_OAUTH_ENDPOINT: "https://oauth.example.test/rest-gateway/rest/api/v1/auth/token",
  STREAMSKOPE_TEST_CA_PATH: "/client/aio-ca.pem",
};

it.each([
  {},
  { STREAMSKOPE_TEST_KAFKA_ENDPOINT: configured.STREAMSKOPE_TEST_KAFKA_ENDPOINT },
  { ...configured, STREAMSKOPE_TEST_OAUTH_ENDPOINT: undefined },
  { ...configured, STREAMSKOPE_TEST_CA_PATH: "  " },
])(
  "refuses missing remote settings without invoking readiness or creating a fallback",
  async (environment) => {
    const verify = vi.fn();
    await expect(preflightRemoteAioFixture(environment, verify)).rejects.toMatchObject({
      code: "FIXTURE_NOT_CONFIGURED",
    });
    expect(verify).not.toHaveBeenCalled();
  },
);

it.each([
  {
    ...configured,
    STREAMSKOPE_TEST_KAFKA_ENDPOINT: "user:private-sentinel@aio.example.test:19093",
  },
  { ...configured, STREAMSKOPE_TEST_KAFKA_ENDPOINT: "aio.example.test:65536" },
  { ...configured, STREAMSKOPE_TEST_KAFKA_ENDPOINT: "aio.example.test:19093/topic" },
  { ...configured, STREAMSKOPE_TEST_OAUTH_ENDPOINT: "private-sentinel" },
  {
    ...configured,
    STREAMSKOPE_TEST_OAUTH_ENDPOINT: "https://user:private-sentinel@oauth.example.test/token",
  },
])("sanitizes invalid endpoint settings before contacting a service", async (environment) => {
  const verify = vi.fn();
  const failure = await preflightRemoteAioFixture(environment, verify).catch(
    (error: unknown) => error,
  );
  expect(failure).toMatchObject({ code: "FIXTURE_NOT_CONFIGURED" });
  expect(String(failure)).not.toContain("private-sentinel");
  expect(JSON.stringify(failure)).not.toContain("private-sentinel");
  expect(verify).not.toHaveBeenCalled();
});

it("uses read-only external readiness and returns the same configured connection", async () => {
  const verify = vi.fn().mockResolvedValue(undefined);
  expect(await preflightRemoteAioFixture(configured, verify)).toEqual({
    kafkaEndpoint: configured.STREAMSKOPE_TEST_KAFKA_ENDPOINT,
    oauthEndpoint: configured.STREAMSKOPE_TEST_OAUTH_ENDPOINT,
    caPath: configured.STREAMSKOPE_TEST_CA_PATH,
  });
  expect(verify).toHaveBeenCalledOnce();
  expect(verify).toHaveBeenCalledWith(
    expect.objectContaining({ ownership: "external", name: undefined }),
    expect.objectContaining({ topic: "test" }),
  );
});

it("reports an unavailable remote fixture without leaking raw TLS or token failures", async () => {
  const verify = vi.fn().mockRejectedValue(new Error("private-sentinel token or CA details"));
  const failure = await preflightRemoteAioFixture(configured, verify).catch(
    (error: unknown) => error,
  );
  expect(failure).toMatchObject({ code: "FIXTURE_UNAVAILABLE" });
  expect(String(failure)).not.toContain("private-sentinel");
  expect(JSON.stringify(failure)).not.toContain("private-sentinel");
});

it("keeps loopback tunnels and IPv6 endpoints valid without a platform-specific fixture", () => {
  expect(
    explicitFixtureConnection({ ...configured, STREAMSKOPE_TEST_KAFKA_ENDPOINT: "[::1]:19093" })
      ?.kafkaEndpoint,
  ).toBe("[::1]:19093");
  expect(
    explicitFixtureConnection({ ...configured, STREAMSKOPE_TEST_KAFKA_ENDPOINT: "127.0.0.1:19093" })
      ?.kafkaEndpoint,
  ).toBe("127.0.0.1:19093");
});
