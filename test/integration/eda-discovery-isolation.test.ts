import { afterEach, expect, it, vi } from "vitest";

import { EdaAgentCapture } from "../../plugins/eda/backend/eda-agent-capture";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";

afterEach(() => vi.unstubAllEnvs());

it("does not send another deployment's client secret during independent discovery", async () => {
  const bodies: string[] = [];
  const fixture = await createHttpsTrustFixture((request, response) => {
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => {
      bodies.push(Buffer.concat(chunks).toString("utf8"));
      response.setHeader("content-type", "application/json");
      const path = request.url ?? "";
      if (path.endsWith("/protocol/openid-connect/token"))
        response.end(JSON.stringify({ access_token: "fixture-token" }));
      else if (path.endsWith("/admin/realms/eda/clients"))
        response.end(JSON.stringify([{ clientId: "eda", id: "fixture-id" }]));
      else if (path.endsWith("/client-secret"))
        response.end(JSON.stringify({ value: "target-client-secret" }));
      else if (path === "/core/about/version")
        response.end(JSON.stringify({ eda: { version: "v26.8.2" } }));
      else response.end(JSON.stringify({ items: [] }));
    });
  });
  vi.stubEnv("STREAMSKOPE_EDA_API_URL", "https://different.example.test");
  vi.stubEnv("STREAMSKOPE_EDA_API_CLIENT_SECRET", "other-deployment-secret");
  vi.stubEnv("KUBECONFIG", "/not-present/config");
  const capture = new EdaAgentCapture(() => Promise.reject(new Error("No capture expected")));
  try {
    const result = await capture.inspect({
      edaApi: {
        baseUrl: fixture.origin,
        username: "operator",
        password: "fixture",
        verifyTls: false,
      },
    });
    expect(result.sources).toEqual([]);
    expect(bodies.some((body) => body.includes("target-client-secret"))).toBe(true);
    expect(bodies.every((body) => !body.includes("other-deployment-secret"))).toBe(true);
    expect(fixture.requests.every((request) => !request.url.includes("streamskope-capture"))).toBe(
      true,
    );
  } finally {
    await capture.close();
    await fixture.close();
  }
});
