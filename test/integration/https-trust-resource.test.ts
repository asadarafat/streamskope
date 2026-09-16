import { expect, it } from "vitest";

import { HTTPS_TRUST_LIMITS } from "../../src/features/kafka/contracts/https-trust-types";
import { NodeHttpsTrustTransport } from "../../src/platform/electron/main/https-trust-transport";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";

it.each([HTTPS_TRUST_LIMITS.materialBytes, HTTPS_TRUST_LIMITS.jsonWireBytes])(
  "bounds %i-byte transfers and closes all request sockets across repeated acquisition cycles",
  async (maximum) => {
    const fixture = await createHttpsTrustFixture((_request, response) =>
      response.end(Buffer.alloc(maximum, 65)),
    );
    const transport = new NodeHttpsTrustTransport();
    const baseline = process.memoryUsage();
    const measurements: { rss: number; external: number; arrayBuffers: number }[] = [];
    try {
      for (let cycle = 0; cycle < 6; cycle += 1) {
        const bytes = await transport.get({
          url: fixture.origin,
          authentication: { mode: "none" },
          tls: { mode: "custom", caPem: fixture.caPem },
          headers: [],
          maximumBytes: maximum,
          signal: new AbortController().signal,
        });
        expect(bytes.byteLength).toBe(maximum);
        expect(bytes[0]).toBe(65);
        expect(bytes[maximum - 1]).toBe(65);
        bytes.fill(0);
        await expect.poll(() => fixture.sockets.size).toBe(0);
        const { rss, external, arrayBuffers } = process.memoryUsage();
        measurements.push({ rss, external, arrayBuffers });
      }
      // Includes the in-process TLS server and V8's non-immediate garbage collection.
      const rssGrowth = Math.max(...measurements.map((entry) => entry.rss)) - baseline.rss;
      expect(rssGrowth).toBeLessThan(192 * 1024 * 1024);
      expect(fixture.requests).toHaveLength(6);
      process.stdout.write(
        JSON.stringify({
          scenario: "https-max-transfer",
          maximumBytes: maximum,
          cycles: 6,
          baseline,
          measurements,
          rssGrowth,
          remainingSockets: fixture.sockets.size,
        }) + "\n",
      );
    } finally {
      await fixture.close();
    }
  },
  30000,
);
