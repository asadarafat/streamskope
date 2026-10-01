import { afterEach, expect, it } from "vitest";

import { requestOAuthToken } from "../../src/features/kafka/engine/oauth";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";

const closes: Array<() => Promise<void>> = [];
afterEach(async () => {
  await Promise.allSettled(closes.splice(0).map((close) => close()));
});

it.each([false, true])(
  "omits an empty OAuth scope in the token request (post fallback: %s)",
  async (fallback) => {
    const bodies: URLSearchParams[] = [];
    const fixture = await createHttpsTrustFixture((request, response) => {
      let body = "";
      request.on("data", (chunk: Buffer) => {
        body += chunk.toString();
      });
      request.on("end", () => {
        bodies.push(new URLSearchParams(body));
        if (fallback && bodies.length === 1) {
          response.writeHead(401);
          response.end();
          return;
        }
        response.end('{"access_token":"fixture-token"}');
      });
    });
    closes.push(() => fixture.close());
    await expect(
      requestOAuthToken({
        tokenEndpoint: fixture.origin,
        caPem: fixture.caPem,
        clientId: "operator",
        clientSecret: "fixture-secret",
        scope: "",
        signal: new AbortController().signal,
      }),
    ).resolves.toMatchObject({ value: "fixture-token" });
    expect(bodies).toHaveLength(fallback ? 2 : 1);
    for (const body of bodies) {
      expect(body.has("scope")).toBe(false);
      expect(body.get("grant_type")).toBe("client_credentials");
    }
  },
);
