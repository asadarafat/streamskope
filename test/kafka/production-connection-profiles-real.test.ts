import { randomBytes, randomUUID } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostEvent,
  type HostCommand,
  type HostEvent,
  type ProfileCreateInput,
  type ClusterServiceEndpointInput,
  type ProtectedValueCreateInput,
} from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { AtomicKafkaProfileFileStore } from "../../src/platform/node/kafka-profile-file-store";
import {
  openPassphraseVault,
  type PassphraseVault,
} from "../../src/platform/node/vault/passphrase-vault";
import { loadFixtureConfig } from "../support/kafka-fixture";
import { startNativeKafkaFixture } from "../support/native-kafka-fixture";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";
import { undecodableClientKey } from "../support/tls-client-identity-fixture";

const mechanisms = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] as const;
let fixture: Awaited<ReturnType<typeof startNativeKafkaFixture>>;

beforeAll(async () => {
  fixture = await startNativeKafkaFixture(undefined, { authenticationMatrix: true });
}, 240_000);
afterAll(async () => {
  await fixture?.dispose();
}, 30_000);

function material(value: string): { readonly mode: "replace"; readonly value: string } {
  return { mode: "replace", value };
}
function authentication(): NonNullable<typeof fixture.authentication> {
  if (fixture.authentication === undefined) throw new Error("Authentication fixture unavailable.");
  return fixture.authentication;
}
function tlsProfile(): Extract<ProfileCreateInput, { trust: unknown }> {
  const a = authentication();
  return {
    name: "Connection matrix",
    brokers: [a.tlsBroker],
    trust: {
      kind: "pem",
      label: "fixture CA",
      material: material(a.caPem),
      password: { mode: "clear" },
    },
  };
}
function identity(): NonNullable<
  Extract<ProfileCreateInput, { trust: unknown }>["clientIdentity"]
> {
  const a = authentication();
  return {
    certificatePem: material(a.certificatePem),
    privateKeyPem: material(a.privateKeyPem),
    passphrase: material(a.passphrase),
  };
}
function safe(value: unknown): void {
  const a = authentication();
  const serialized = typeof value === "string" ? value : JSON.stringify(value);
  for (const secret of [a.password, a.passphrase, a.privateKeyPem]) {
    expect(serialized.includes(secret), "Protected credential escaped host projection").toBe(false);
    expect(
      serialized.includes(JSON.stringify(secret).slice(1, -1)),
      "Protected credential escaped host projection",
    ).toBe(false);
  }
}
async function execute(
  backend: ReturnType<typeof createKafkaBackend>,
  command: HostCommand["command"],
  payload: unknown,
): ReturnType<ReturnType<typeof createKafkaBackend>["execute"]> {
  const result = await backend.execute({
    command,
    payload,
    id: randomUUID(),
    version: HOST_PROTOCOL_VERSION,
  } as HostCommand);
  safe(result);
  return result;
}
async function ownedHost(
  work: (context: {
    readonly events: HostEvent[];
    readonly file: string;
    current(): ReturnType<typeof createKafkaBackend>;
    restart(): Promise<void>;
  }) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-auth-profile-"));
  const dataRoot = join(root, "data");
  const passphrase = randomBytes(32).toString("base64");
  let vault: PassphraseVault | undefined;
  let backend: ReturnType<typeof createKafkaBackend> | undefined;
  const events: HostEvent[] = [];
  const start = async (mode: "create" | "unlock"): Promise<void> => {
    vault = await openPassphraseVault({ dataRoot, passphrase, mode });
    backend = createKafkaBackend({
      profileStore: new AtomicKafkaProfileFileStore(
        vault.paths.kafkaProfiles,
        vault.protector,
        vault.capability,
      ),
    });
    backend.subscribe((event) => {
      safe(event);
      events.push(parseHostEvent(event));
    });
  };
  try {
    await start("create");
    await work({
      events,
      file: join(dataRoot, "kafka-profiles.json"),
      current: () => {
        if (!backend) throw new Error("Host unavailable");
        return backend;
      },
      restart: async () => {
        await backend?.shutdown();
        backend = undefined;
        await vault?.lock();
        vault = undefined;
        events.length = 0;
        await start("unlock");
      },
    });
  } finally {
    try {
      await backend?.shutdown();
    } finally {
      try {
        await vault?.lock();
      } finally {
        await rm(root, { recursive: true, force: true });
      }
    }
  }
}
async function save(
  backend: ReturnType<typeof createKafkaBackend>,
  events: HostEvent[],
  profile: ProfileCreateInput,
): Promise<string> {
  expect(await execute(backend, "profiles.create", { profile })).toMatchObject({ ok: true });
  const event = events.filter((item) => item.event === "profiles.changed").at(-1);
  const id = event?.payload.profiles.find((entry) => entry.name === profile.name)?.id;
  if (!id) throw new Error("The saved profile was not emitted.");
  return id;
}
async function reconnect(
  backend: ReturnType<typeof createKafkaBackend>,
  profileId: string,
): Promise<void> {
  expect(await execute(backend, "profiles.connect", { profileId })).toMatchObject({ ok: true });
  expect(await execute(backend, "topics.list", {})).toMatchObject({ ok: true });
  expect(await execute(backend, "connection.disconnect", {})).toMatchObject({ ok: true });
}

const cases = mechanisms.flatMap((mechanism) =>
  ["tls", "mutual", "plaintext"].map((transport) => ({ mechanism, transport })),
);
describe("real production profile authentication and encrypted restart", () => {
  it.each(cases)(
    "connects, reconnects and restores $mechanism over $transport",
    async ({ mechanism, transport }) => {
      const a = authentication();
      const sasl = { mechanism, username: a.username, password: material(a.password) };
      const profile: ProfileCreateInput =
        transport === "plaintext"
          ? {
              name: "Explicit isolated plaintext",
              brokers: [a.saslPlaintextBroker],
              transport: "plaintext",
              sasl,
            }
          : {
              ...tlsProfile(),
              sasl,
              ...(transport === "mutual"
                ? { brokers: [a.mutualSaslBroker], clientIdentity: identity() }
                : {}),
            };
      await ownedHost(async (host) => {
        expect(
          await execute(host.current(), "profiles.test", { mode: "create", profile }),
        ).toMatchObject({ ok: true });
        const id = await save(host.current(), host.events, profile);
        await reconnect(host.current(), id);
        await reconnect(host.current(), id);
        safe(await readFile(host.file, "utf8"));
        await host.restart();
        await reconnect(host.current(), id);
      });
    },
    45_000,
  );
  it("preserves SCRAM and mutual TLS through direct connection test/connect routing", async () => {
    const a = authentication();
    await ownedHost(async (host) => {
      const connection = {
        name: "Direct protected connection",
        brokers: [a.mutualSaslBroker],
        sasl: { mechanism: "SCRAM-SHA-512", username: a.username, password: a.password },
        tls: {
          enabled: true,
          caPem: a.caPem,
          clientIdentity: {
            certificatePem: a.certificatePem,
            privateKeyPem: a.privateKeyPem,
            passphrase: a.passphrase,
          },
        },
      };
      expect(await execute(host.current(), "connection.test", connection)).toMatchObject({
        ok: true,
      });
      for (let attempt = 0; attempt < 2; attempt += 1) {
        expect(await execute(host.current(), "connection.connect", connection)).toMatchObject({
          ok: true,
        });
        expect(await execute(host.current(), "topics.list", {})).toMatchObject({ ok: true });
        expect(await execute(host.current(), "connection.disconnect", {})).toMatchObject({
          ok: true,
        });
      }
    });
  });
  it("restores a mutually authenticated TLS profile without SASL", async () => {
    await ownedHost(async (host) => {
      const profile = {
        ...tlsProfile(),
        brokers: [authentication().mutualTlsBroker],
        clientIdentity: identity(),
      };
      const id = await save(host.current(), host.events, profile);
      await reconnect(host.current(), id);
      await host.restart();
      await reconnect(host.current(), id);
    });
  });
  it("combines OAuth and mutual TLS across reconnect and encrypted restart", async () => {
    const config = await loadFixtureConfig();
    await ownedHost(async (host) => {
      const profile = {
        ...tlsProfile(),
        brokers: [authentication().mutualSaslBroker],
        clientIdentity: identity(),
        oauth: {
          clientId: config.oauthClientId,
          clientSecret: material(config.oauthClientSecret),
          scope: config.oauthScope,
          tokenEndpoint: fixture.environment.STREAMSKOPE_TEST_OAUTH_ENDPOINT!,
        },
      };
      expect(
        await execute(host.current(), "profiles.test", { mode: "create", profile }),
      ).toMatchObject({ ok: true });
      const id = await save(host.current(), host.events, profile);
      await reconnect(host.current(), id);
      await reconnect(host.current(), id);
      await host.restart();
      await reconnect(host.current(), id);
    });
  });
  it("preserves the existing OAuth profile through an encrypted restart", async () => {
    const config = await loadFixtureConfig();
    await ownedHost(async (host) => {
      const profile = {
        ...tlsProfile(),
        oauth: {
          clientId: config.oauthClientId,
          clientSecret: material(config.oauthClientSecret),
          scope: config.oauthScope,
          tokenEndpoint: fixture.environment.STREAMSKOPE_TEST_OAUTH_ENDPOINT!,
        },
      };
      const id = await save(host.current(), host.events, profile);
      await reconnect(host.current(), id);
      const before = JSON.parse(await readFile(host.file, "utf8")) as { version: number };
      expect(before.version).toBe(3);
      const a = authentication();
      await save(host.current(), host.events, {
        ...tlsProfile(),
        name: "New SCRAM profile",
        sasl: { mechanism: "SCRAM-SHA-512", username: a.username, password: material(a.password) },
      });
      const after = JSON.parse(await readFile(host.file, "utf8")) as {
        version: number;
        rollbackGeneration: string;
      };
      expect(after.version).toBe(4);
      const backup = JSON.parse(
        await readFile(join(host.file, "..", after.rollbackGeneration), "utf8"),
      ) as { version: number };
      expect(backup.version).toBe(3);
      await host.restart();
      await reconnect(host.current(), id);
    });
  });
  it.each(["wrong passphrase", "mismatched certificate", "undecodable decrypted key"])(
    "rejects %s without disclosing client key material",
    async (scenario) => {
      await ownedHost(async (host) => {
        const wrong = `invalid-${randomBytes(16).toString("hex")}`;
        const undecodableKey = undecodableClientKey(wrong);
        const result = await execute(host.current(), "profiles.test", {
          mode: "create",
          profile: {
            ...tlsProfile(),
            brokers: [authentication().mutualTlsBroker],
            clientIdentity: {
              ...identity(),
              ...(scenario === "wrong passphrase"
                ? { passphrase: material(wrong) }
                : scenario === "mismatched certificate"
                  ? { certificatePem: material(authentication().caPem) }
                  : {
                      privateKeyPem: material(undecodableKey),
                      passphrase: material(wrong),
                    }),
            },
          },
        });
        expect(result).toMatchObject({ ok: false, error: { code: "TLS_TRUST", stage: "tls" } });
        const output = JSON.stringify([result, host.events]);
        expect(output.includes(wrong)).toBe(false);
        expect(output.includes(JSON.stringify(undecodableKey).slice(1, -1))).toBe(false);
        expect(output).toMatch(/key|passphrase|identity/iu);
      });
    },
  );
  it.each(mechanisms)("reports a wrong %s password without disclosing it", async (mechanism) => {
    await ownedHost(async (host) => {
      const wrong = `wrong-${randomBytes(16).toString("hex")}`;
      const profile = {
        ...tlsProfile(),
        sasl: { mechanism, username: authentication().username, password: material(wrong) },
      };
      const result = await execute(host.current(), "profiles.test", { mode: "create", profile });
      expect(result).toMatchObject({ ok: false });
      const output = JSON.stringify([result, host.events]);
      expect(output.includes(wrong)).toBe(false);
      expect(output).toMatch(/authenticat|credentials|SASL/iu);
    });
  });
  it("rejects an untrusted broker and a missing TLS client identity with useful safe diagnostics", async () => {
    await ownedHost(async (host) => {
      const a = authentication();
      const untrusted = {
        ...tlsProfile(),
        sasl: { mechanism: "PLAIN", username: a.username, password: material(a.password) },
        trust: {
          kind: "pem",
          label: "Wrong CA",
          material: material(a.certificatePem),
          password: { mode: "clear" },
        },
      };
      for (const profile of [untrusted, { ...tlsProfile(), brokers: [a.mutualTlsBroker] }]) {
        const result = await execute(host.current(), "profiles.test", { mode: "create", profile });
        expect(result).toMatchObject({ ok: false });
        expect(JSON.stringify([result, host.events])).toMatch(/certificate|TLS|identity/iu);
      }
    });
  });
});

interface ServiceFixture {
  readonly server: Awaited<ReturnType<typeof createHttpsTrustFixture>>;
  readonly secrets: readonly string[];
  readonly resourceRequests: number;
  readonly tokenRequests: number;
  readonly endpoint: ClusterServiceEndpointInput<ProtectedValueCreateInput>;
}
async function serviceFixture(
  mode: "basic" | "oauth-client" | "bearer",
  mutual = false,
): Promise<ServiceFixture> {
  const username = `http-${randomBytes(8).toString("hex")}`;
  const password = randomBytes(24).toString("hex");
  const bearer = randomBytes(32).toString("base64url");
  const expectedBasic = `Basic ${Buffer.from(`${username}:${password}`).toString("base64")}`;
  let resourceRequests = 0;
  let tokenRequests = 0;
  const handle = async (request: IncomingMessage, response: ServerResponse): Promise<void> => {
    response.setHeader("content-type", "application/json");
    if (request.url === "/token" && request.method === "POST") {
      let body = "";
      for await (const chunk of request) body += String(chunk);
      const form = new URLSearchParams(body);
      const valid =
        request.headers.authorization === expectedBasic ||
        (form.get("client_id") === username && form.get("client_secret") === password);
      if (!valid || form.get("grant_type") !== "client_credentials") {
        response
          .writeHead(401)
          .end(JSON.stringify({ error: "invalid_client", reflected: password }));
        return;
      }
      tokenRequests += 1;
      response.end(JSON.stringify({ access_token: bearer, expires_in: 600, token_type: "bearer" }));
      return;
    }
    if (request.headers.authorization !== (mode === "basic" ? expectedBasic : `Bearer ${bearer}`)) {
      response.writeHead(401).end(JSON.stringify({ error: "denied", reflected: password }));
      return;
    }
    resourceRequests += 1;
    response.end(JSON.stringify([]));
  };
  const server = await createHttpsTrustFixture(
    (request, response) => {
      void handle(request, response).catch(() => response.destroy());
    },
    "valid",
    mutual ? authentication().certificatePem : undefined,
  );
  return {
    server,
    secrets: [password, bearer, expectedBasic],
    get resourceRequests(): number {
      return resourceRequests;
    },
    get tokenRequests(): number {
      return tokenRequests;
    },
    endpoint: {
      baseUrl: server.origin,
      authentication: mode,
      ...(mode === "basic"
        ? { basic: { username, password: material(password) } }
        : mode === "bearer"
          ? { bearer: material(bearer) }
          : {
              oauth: {
                clientId: username,
                clientSecret: material(password),
                scope: "service-only",
                tokenEndpoint: `${server.origin}/token`,
              },
            }),
      trust: {
        mode: "custom" as const,
        kind: "pem" as const,
        label: "Separate service CA",
        material: material(server.caPem),
        password: { mode: "clear" as const },
      },
      ...(mutual ? { clientIdentity: identity() } : {}),
    },
  };
}

describe("independent service credentials and trust through real HTTPS", () => {
  it.each([
    { registry: "basic", connect: "oauth-client", mutual: false },
    { registry: "oauth-client", connect: "basic", mutual: true },
    { registry: "bearer", connect: "bearer", mutual: false },
  ] as const)(
    "restores Registry $registry and Connect $connect independently (mTLS=$mutual)",
    async (row) => {
      const registry = await serviceFixture(row.registry, row.mutual);
      let connect: Awaited<ReturnType<typeof serviceFixture>> | undefined;
      try {
        connect = await serviceFixture(row.connect, row.mutual);
        const services = { schemaRegistry: registry.endpoint, connect: connect.endpoint };
        await ownedHost(async (host) => {
          const a = authentication();
          const profile: ProfileCreateInput = {
            ...tlsProfile(),
            sasl: {
              mechanism: "SCRAM-SHA-256",
              username: a.username,
              password: material(a.password),
            },
            services,
          };
          const id = await save(host.current(), host.events, profile);
          for (const restart of [false, true]) {
            if (restart) await host.restart();
            expect(
              await execute(host.current(), "profiles.connect", { profileId: id }),
            ).toMatchObject({ ok: true });
            expect(await execute(host.current(), "schemas.list", {})).toMatchObject({ ok: true });
            expect(await execute(host.current(), "connect.list", {})).toMatchObject({ ok: true });
            const publicOutput = JSON.stringify(host.events);
            const persisted = await readFile(host.file, "utf8");
            for (const secret of [...registry.secrets, ...connect!.secrets]) {
              expect(
                publicOutput.includes(secret),
                "Service credential escaped host projection",
              ).toBe(false);
              expect(persisted.includes(secret), "Service credential persisted in plaintext").toBe(
                false,
              );
            }
          }
        });
        expect(registry.resourceRequests).toBeGreaterThanOrEqual(2);
        expect(connect.resourceRequests).toBeGreaterThanOrEqual(4);
        if (row.registry === "oauth-client")
          expect(registry.tokenRequests).toBeGreaterThanOrEqual(2);
        if (row.connect === "oauth-client") expect(connect.tokenRequests).toBeGreaterThanOrEqual(2);
      } finally {
        try {
          await connect?.server.close();
        } finally {
          await registry.server.close();
        }
      }
    },
    45_000,
  );
  it.each(["basic", "oauth-client", "bearer"] as const)(
    "reports %s service denial and unrelated CA without reflecting secrets or weakening broker TLS",
    async (mode) => {
      const registry = await serviceFixture(mode);
      try {
        await ownedHost(async (host) => {
          const a = authentication();
          const broker = {
            ...tlsProfile(),
            sasl: {
              mechanism: "PLAIN" as const,
              username: a.username,
              password: material(a.password),
            },
          };
          const wrong = `invalid-${randomBytes(16).toString("hex")}`;
          const badAuth: ClusterServiceEndpointInput<ProtectedValueCreateInput> = {
            ...registry.endpoint,
            ...(mode === "basic"
              ? { basic: { username: "wrong-user", password: material(wrong) } }
              : mode === "bearer"
                ? { bearer: material(wrong) }
                : { oauth: { ...registry.endpoint.oauth!, clientSecret: material(wrong) } }),
          };
          const badTrust = {
            ...registry.endpoint,
            trust: {
              mode: "custom" as const,
              kind: "pem" as const,
              label: "Unrelated CA",
              material: material(a.caPem),
              password: { mode: "clear" as const },
            },
          };
          for (const endpoint of [badAuth, badTrust]) {
            expect(
              await execute(host.current(), "profiles.test", {
                mode: "create",
                profile: { ...broker, services: { schemaRegistry: endpoint } },
              }),
            ).toMatchObject({ ok: false });
            const id = await save(host.current(), host.events, {
              ...broker,
              services: { schemaRegistry: endpoint },
            });
            expect(
              await execute(host.current(), "profiles.connect", { profileId: id }),
            ).toMatchObject({ ok: true });
            expect(await execute(host.current(), "schemas.list", {})).toMatchObject({ ok: false });
            expect(await execute(host.current(), "topics.list", {})).toMatchObject({ ok: true });
            const output = JSON.stringify(host.events);
            for (const secret of [...registry.secrets, wrong])
              expect(output.includes(secret)).toBe(false);
            expect(output).toMatch(/authenticat|credentials|certificate|TLS/iu);
            expect(await execute(host.current(), "connection.disconnect", {})).toMatchObject({
              ok: true,
            });
            expect(
              await execute(host.current(), "profiles.delete", { profileId: id }),
            ).toMatchObject({ ok: true });
          }
        });
      } finally {
        await registry.server.close();
      }
    },
  );
});
