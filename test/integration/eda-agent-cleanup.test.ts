import { afterEach, expect, it, vi } from "vitest";

import { EdaAgentCapture } from "../../plugins/eda/backend/eda-agent-capture";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";

const closeTunnel = vi.hoisted(() => vi.fn<() => Promise<void>>());
vi.mock("../../plugins/eda/backend/eda-agent-tunnel", () => ({
  EdaAgentTunnel: class {
    static listen(): Promise<{ activate(): void; close: typeof closeTunnel }> {
      return Promise.resolve({ activate(): void {}, close: closeTunnel });
    }
  },
}));

const closeables: { close(): Promise<void> }[] = [];
afterEach(async () => {
  await Promise.all(closeables.splice(0).map((value) => value.close()));
});

it.each([true, false])(
  "cleans up a committed capture whose POST response was lost (cleanup accepted=%s)",
  async (cleanupAccepted) => {
    let sessionId: string | undefined;
    let remoteSessionExists = false;
    let allowRemoval = cleanupAccepted;
    const removals: string[] = [];
    const fixture = await createHttpsTrustFixture((request, response) => {
      const path = request.url ?? "";
      response.setHeader("content-type", "application/json");
      if (path.endsWith("/protocol/openid-connect/token"))
        response.end(JSON.stringify({ access_token: "fixture-token" }));
      else if (path.endsWith("/admin/realms/eda/clients"))
        response.end(JSON.stringify([{ clientId: "eda", id: "fixture-id" }]));
      else if (path.endsWith("/client-secret"))
        response.end(JSON.stringify({ value: "fixture-client-secret" }));
      else if (path === "/core/about/version")
        response.end(JSON.stringify({ eda: { version: "v26.8.2" } }));
      else if (path === "/apps/capture.streamskope.io/v1alpha1") response.end("{}");
      else if (path.endsWith("/streamskope-capture/healthz"))
        response.end(JSON.stringify({ status: "ready", version: "v26.8.2" }));
      else if (path === "/apps/kafka.eda.nokia.com/v1/clusterproducers/interfaces")
        response.end(JSON.stringify({ spec: { exports: [{ topic: "interfaces" }] } }));
      else if (request.method === "POST" && path.endsWith("/streamskope-capture/v1/sessions")) {
        const chunks: Buffer[] = [];
        request.on("data", (chunk: Buffer) => chunks.push(chunk));
        request.on("end", () => {
          const input = JSON.parse(Buffer.concat(chunks).toString("utf8")) as { id: string };
          sessionId = input.id;
          remoteSessionExists = true;
          // EDA has committed the session, but the client never receives a response.
          request.socket.destroy();
        });
      } else if (request.method === "DELETE" && path.endsWith(`/sessions/${sessionId}`)) {
        removals.push(sessionId!);
        if (allowRemoval) remoteSessionExists = false;
        response.writeHead(allowRemoval ? 204 : 503).end();
      } else response.writeHead(404).end("{}");
    });
    closeables.push(fixture);
    closeTunnel.mockResolvedValue();
    const probe = vi.fn(() => Promise.resolve([]));
    const capture = new EdaAgentCapture(probe);
    closeables.push(capture);
    await expect(
      capture.deploy({
        edaApi: {
          baseUrl: fixture.origin,
          username: "operator",
          password: "secret",
          verifyTls: false,
        },
        context: "eda-agent",
        imageDelivery: "configured",
        localPort: 19092,
        source: {
          apiVersion: "kafka.eda.nokia.com/v1",
          kind: "ClusterProducer",
          namespace: "eda-system",
          name: "interfaces",
        },
      }),
    ).rejects.toThrow(/could not be reached securely/u);
    expect(sessionId).toBeDefined();
    expect(removals).toEqual([sessionId]);
    expect(remoteSessionExists).toBe(!cleanupAccepted);
    expect(capture.status()).toMatchObject({
      state: "failed",
      tunnel: "closed",
    });
    if (cleanupAccepted) expect(capture.status().source).toBeUndefined();
    else expect(capture.status().source).toMatchObject({ sessionId });
    expect(capture.status().detail).toContain(
      cleanupAccepted ? "EDA accepted removal" : "cleanup could not be confirmed",
    );
    expect(closeTunnel).toHaveBeenCalledOnce();
    expect(probe).not.toHaveBeenCalled();
    if (!cleanupAccepted) {
      const recoverySource = capture.status().source;
      expect(recoverySource).toBeDefined();
      allowRemoval = true;
      await capture.stop(recoverySource!);
      expect(removals).toEqual([sessionId, sessionId]);
      expect(remoteSessionExists).toBe(false);
      expect(capture.status()).toMatchObject({ state: "stopped", tunnel: "closed" });
    }
  },
);
