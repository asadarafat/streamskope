import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";

import { headers } from "@nats-io/transport-node";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  NATS_PROTOCOL_VERSION,
  parseCorrelatedNatsResponse,
  parseNatsCommand,
  parseNatsEvent,
  type NatsCommand,
  type NatsCommandName,
  type NatsCommandResponse,
  type NatsEvent,
  type NatsProfileSummary,
  type NatsRecord,
} from "../../src/features/nats/contracts";
import { createNatsBackend } from "../../src/platform/node/nats-backend";
import { createNatsProviderEndpoint } from "../../src/platform/node/nats-provider";
import type { ProviderWireEndpoint } from "../../src/platform/node/provider-host";
import { startNatsFixture, type NatsFixture } from "../support/nats-fixture";

function containsSecret(value: unknown, fixture: NatsFixture): boolean {
  const text = JSON.stringify(value);
  return [fixture.token, fixture.caPem, fixture.untrustedCaPem].some((secret) =>
    text.includes(secret),
  );
}

function command<Name extends NatsCommandName>(
  name: Name,
  payload: Extract<NatsCommand, { readonly command: Name }>["payload"],
): Extract<NatsCommand, { readonly command: Name }> {
  // The pure codec validates before this command-specific narrowing.
  return parseNatsCommand({
    version: NATS_PROTOCOL_VERSION,
    id: randomUUID(),
    command: name,
    payload,
  }) as Extract<NatsCommand, { readonly command: Name }>;
}

async function request<Command extends NatsCommand>(
  endpoint: ProviderWireEndpoint,
  submitted: Command,
  fixture: NatsFixture,
): Promise<NatsCommandResponse<Command["command"]>> {
  const wire = await endpoint.dispatch(submitted);
  expect(containsSecret(wire, fixture)).toBe(false);
  return parseCorrelatedNatsResponse(wire, submitted);
}

function records(events: readonly NatsEvent[]): readonly NatsRecord[] {
  return events.flatMap((event) => (event.event === "records.batch" ? event.payload.records : []));
}

async function createProfile(
  endpoint: ProviderWireEndpoint,
  fixture: NatsFixture,
  options: { readonly server?: string; readonly token?: string; readonly caPem?: string } = {},
): Promise<NatsProfileSummary> {
  const response = await request(
    endpoint,
    command("profiles.create", {
      profile: {
        name: "Isolated real NATS",
        servers: [options.server ?? fixture.server],
        authentication:
          fixture.connection.authentication.mode === "none"
            ? { mode: "none" }
            : {
                mode: "token",
                token: { mode: "replace", value: options.token ?? fixture.token },
              },
        tls: { mode: "tls", caPem: { mode: "replace", value: options.caPem ?? fixture.caPem } },
      },
    }),
    fixture,
  );
  expect(response.ok).toBe(true);
  if (!response.ok) throw new Error("Real NATS profile creation failed.");
  expect(response.result.profiles.capability).toMatchObject({
    durability: "session",
    protection: "memory",
    state: "ready",
  });
  const profile = response.result.profiles.profiles.at(-1);
  if (profile === undefined) throw new Error("Real NATS profile was not saved.");
  expect(profile.authentication).toEqual(
    fixture.connection.authentication.mode === "none"
      ? { mode: "none" }
      : { mode: "token", tokenPresent: true },
  );
  expect(profile.tls).toEqual({ mode: "tls", caPresent: true });
  return profile;
}

async function withBackend(
  fixture: NatsFixture,
  run: (endpoint: ProviderWireEndpoint, events: readonly NatsEvent[]) => Promise<void>,
): Promise<void> {
  const endpoint = createNatsProviderEndpoint(createNatsBackend());
  const events: NatsEvent[] = [];
  let leaked = false;
  const unsubscribe = endpoint.subscribe((wire): void => {
    leaked ||= containsSecret(wire, fixture);
    events.push(parseNatsEvent(wire));
  });
  const failures: unknown[] = [];
  try {
    await run(endpoint, events);
  } catch (error) {
    failures.push(error);
  } finally {
    const cleanup = await Promise.allSettled([endpoint.shutdown()]);
    unsubscribe();
    for (const result of cleanup)
      if (result.status === "rejected") failures.push(result.reason as unknown);
  }
  // Boolean-only assertions prevent an accidental leak from being echoed by a test diff.
  if (leaked || containsSecret(events, fixture))
    failures.push(new Error("A real NATS public boundary leaked profile secrets."));
  if (failures.length > 0)
    throw new AggregateError(failures, "Real NATS qualification failed.", { cause: failures[0] });
}

describe("real built-in Core NATS provider", () => {
  let fixture: NatsFixture;
  beforeAll(async (): Promise<void> => {
    fixture = await startNatsFixture();
  }, 180_000);
  afterAll(async (): Promise<void> => {
    await fixture?.dispose();
  }, 60_000);

  it("captures token/TLS wildcard records with fidelity, confirms stop before teardown and reconnects in a new generation", async () => {
    await withBackend(fixture, async (endpoint, events): Promise<void> => {
      const profile = await createProfile(endpoint, fixture);
      const connected = await request(
        endpoint,
        command("profiles.connect", { profileId: profile.id, expectedRevision: profile.revision }),
        fixture,
      );
      expect(connected.ok).toBe(true);
      if (!connected.ok) throw new Error("Real NATS connection was not confirmed.");
      expect(connected.result.connection.state).toBe("connected");
      const started = await request(
        endpoint,
        command("subscription.start", { subject: "qualification.*" }),
        fixture,
      );
      expect(started.ok).toBe(true);
      if (!started.ok) throw new Error("Real NATS subscription interest was not confirmed.");
      expect(started.result.subscription.state).toBe("streaming");
      const firstGeneration = started.result.subscription.generation;
      const publisher = await fixture.publisher();
      const metadata = headers();
      metadata.append("Trace", "one");
      metadata.append("Trace", "two");
      metadata.append("trace", "case-sensitive");
      publisher.publish("qualification.binary", Uint8Array.from([0, 255, 128, 1]), {
        reply: "reply.qualification",
        headers: metadata,
      });
      publisher.publish("qualification.empty", new Uint8Array());
      await publisher.flush();
      await expect.poll(() => records(events).length, { timeout: 10_000, interval: 25 }).toBe(2);
      expect(
        records(events).find((record) => record.subject === "qualification.binary"),
      ).toMatchObject({
        generation: firstGeneration,
        reply: "reply.qualification",
        headers: [
          { name: "Trace", values: ["one", "two"] },
          { name: "trace", values: ["case-sensitive"] },
        ],
        headersTruncated: false,
        payload: { encoding: "base64", data: "AP+AAQ==" },
        payloadBytes: 4,
        timestampProvenance: "host-received",
      });
      expect(
        records(events).find((record) => record.subject === "qualification.empty"),
      ).toMatchObject({ payload: { encoding: "utf8", data: "" }, payloadBytes: 0, headers: [] });

      // This host-only cleanup returns after actual UNSUB+PONG, not local callback disposal.
      await endpoint.stopStream();
      const stopped = await request(endpoint, command("profiles.list", {}), fixture);
      expect(stopped.ok).toBe(true);
      if (!stopped.ok) throw new Error("Real NATS stopped snapshot was unavailable.");
      expect(stopped.result.connection.state).toBe("connected");
      expect(stopped.result.subscription).toMatchObject({
        state: "stopped",
        generation: firstGeneration,
        counters: {
          receivedRecords: 2,
          publishedRecords: 2,
          queuedRecords: 0,
          applicationOmittedRecords: 0,
        },
      });
      const stoppedCount = records(events).length;
      publisher.publish("qualification.sentinel", "must-not-be-captured-after-confirmed-stop");
      await publisher.flush();
      await delay(100);
      expect(records(events).length).toBe(stoppedCount);
      expect(records(events).some((record) => record.subject === "qualification.sentinel")).toBe(
        false,
      );

      const restarted = await request(
        endpoint,
        command("subscription.start", { subject: "qualification.*" }),
        fixture,
      );
      expect(restarted.ok).toBe(true);
      if (!restarted.ok) throw new Error("Real NATS replacement subscription was not confirmed.");
      expect(restarted.result.subscription.generation).not.toBe(firstGeneration);
      publisher.publish("qualification.restarted", "new-generation");
      await publisher.flush();
      await expect
        .poll(
          () => records(events).some((record) => record.subject === "qualification.restarted"),
          { timeout: 10_000, interval: 25 },
        )
        .toBe(true);
      expect(
        records(events).find((record) => record.subject === "qualification.restarted")?.generation,
      ).toBe(restarted.result.subscription.generation);
      for (let count = 0; count < 2; count += 1) {
        const disconnected = await request(endpoint, command("connection.disconnect", {}), fixture);
        expect(disconnected.ok).toBe(true);
        if (!disconnected.ok) throw new Error("Real NATS disconnect was not confirmed.");
        expect(disconnected.result.connection.state).toBe("disconnected");
        expect(disconnected.result.subscription.state).toBe("stopped");
      }
      const reconnected = await request(
        endpoint,
        command("profiles.connect", { profileId: profile.id, expectedRevision: profile.revision }),
        fixture,
      );
      expect(reconnected.ok).toBe(true);
      const resumed = await request(
        endpoint,
        command("subscription.start", { subject: "qualification.*" }),
        fixture,
      );
      expect(resumed.ok).toBe(true);
      if (!resumed.ok)
        throw new Error("Real NATS explicit reconnect subscription was not confirmed.");
      expect(resumed.result.subscription.generation).not.toBe(
        restarted.result.subscription.generation,
      );
      publisher.publish("qualification.reconnected", "explicit-reconnect-generation");
      await publisher.flush();
      await expect
        .poll(
          () => records(events).some((record) => record.subject === "qualification.reconnected"),
          { timeout: 10_000, interval: 25 },
        )
        .toBe(true);
      expect(
        records(events).find((record) => record.subject === "qualification.reconnected")
          ?.generation,
      ).toBe(resumed.result.subscription.generation);
      await endpoint.stopStream();
      await publisher.close();
    });
  }, 120_000);

  it("fails safely for invalid tokens and unrelated CA trust, then verifies a valid IP SAN", async () => {
    for (const scenario of ["token", "ca"] as const) {
      await withBackend(fixture, async (endpoint): Promise<void> => {
        const profile = await createProfile(
          endpoint,
          fixture,
          scenario === "token"
            ? { token: `${fixture.token}-invalid` }
            : { caPem: fixture.untrustedCaPem },
        );
        const response = await request(
          endpoint,
          command("profiles.connect", {
            profileId: profile.id,
            expectedRevision: profile.revision,
          }),
          fixture,
        );
        expect(response.ok).toBe(false);
        if (response.ok) throw new Error("Invalid real NATS authentication or trust was accepted.");
        expect(response.error.code).toBe(scenario === "token" ? "authentication" : "tls");
        expect(response.error).not.toHaveProperty("cause");
        const disconnected = await request(endpoint, command("connection.disconnect", {}), fixture);
        expect(disconnected.ok).toBe(true);
      });
    }
    await withBackend(fixture, async (endpoint): Promise<void> => {
      const profile = await createProfile(endpoint, fixture, { server: fixture.ipServer });
      const response = await request(
        endpoint,
        command("profiles.connect", { profileId: profile.id, expectedRevision: profile.revision }),
        fixture,
      );
      expect(response.ok).toBe(true);
    });
  }, 90_000);

  it("rejects a trusted DNS-only certificate when the profile connects by IP", async () => {
    const dnsOnly = await startNatsFixture({ certificate: "dns-only" });
    const failures: unknown[] = [];
    try {
      await withBackend(dnsOnly, async (endpoint): Promise<void> => {
        const profile = await createProfile(endpoint, dnsOnly, { server: dnsOnly.ipServer });
        const response = await request(
          endpoint,
          command("profiles.connect", {
            profileId: profile.id,
            expectedRevision: profile.revision,
          }),
          dnsOnly,
        );
        expect(response.ok).toBe(false);
        if (response.ok)
          throw new Error("The real NATS IP connection accepted a DNS-only server certificate.");
        expect(response.error.code).toBe("tls");
      });
    } catch (error) {
      failures.push(error);
    } finally {
      const cleanup = await Promise.allSettled([dnsOnly.dispose()]);
      for (const result of cleanup)
        if (result.status === "rejected") failures.push(result.reason as unknown);
    }
    if (failures.length > 0)
      throw new AggregateError(failures, "Real NATS hostname verification failed.", {
        cause: failures[0],
      });
  }, 120_000);

  it("reports a real denied subscription while retaining the connection for allowed interest", async () => {
    // Pinned server no_auth_user registers real user permissions; global token auth does not.
    const restricted = await startNatsFixture({ authentication: "anonymous-restricted" });
    const failures: unknown[] = [];
    try {
      await withBackend(restricted, async (endpoint, events): Promise<void> => {
        const profile = await createProfile(endpoint, restricted);
        const connected = await request(
          endpoint,
          command("profiles.connect", {
            profileId: profile.id,
            expectedRevision: profile.revision,
          }),
          restricted,
        );
        expect(connected.ok).toBe(true);
        const denied = await request(
          endpoint,
          command("subscription.start", { subject: "qualification.denied" }),
          restricted,
        );
        expect(denied.ok).toBe(false);
        if (denied.ok) throw new Error("The real NATS server accepted denied interest.");
        expect(denied.error.code).toBe("permission");
        const snapshot = await request(endpoint, command("profiles.list", {}), restricted);
        expect(snapshot.ok).toBe(true);
        if (!snapshot.ok) throw new Error("The real NATS permission failure hid connection state.");
        expect(snapshot.result.connection.state).toBe("connected");
        expect(records(events)).toHaveLength(0);
        const allowed = await request(
          endpoint,
          command("subscription.start", { subject: "qualification.allowed" }),
          restricted,
        );
        expect(allowed.ok).toBe(true);
        const publisher = await restricted.publisher();
        publisher.publish("qualification.allowed", "after-real-denial");
        await publisher.flush();
        await expect.poll(() => records(events).length, { timeout: 10_000, interval: 25 }).toBe(1);
        expect(records(events)[0]?.payload).toEqual({
          encoding: "utf8",
          data: "after-real-denial",
        });
        await endpoint.stopStream();
        await publisher.close();
      });
    } catch (error) {
      failures.push(error);
    } finally {
      const cleanup = await Promise.allSettled([restricted.dispose()]);
      for (const result of cleanup)
        if (result.status === "rejected") failures.push(result.reason as unknown);
    }
    if (failures.length > 0)
      throw new AggregateError(failures, "Real NATS permission qualification failed.", {
        cause: failures[0],
      });
  }, 120_000);
});
