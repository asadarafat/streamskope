import { describe, expect, it } from "vitest";

import { resolveDevelopmentNetwork } from "../../src/platform/dev-host/network";

describe("web development network", () => {
  it("keeps the default launch loopback-only", () => {
    expect(resolveDevelopmentNetwork()).toEqual({
      listenHostname: "127.0.0.1",
      publicHostname: "127.0.0.1",
    });
  });

  it("uses a wildcard listener for an explicit browser-visible hostname", () => {
    expect(
      resolveDevelopmentNetwork({
        publicHostname: "clab.orb.local",
      }),
    ).toEqual({
      listenHostname: "0.0.0.0",
      publicHostname: "clab.orb.local",
    });
  });

  it("retains an explicitly selected listener independently of the public hostname", () => {
    expect(
      resolveDevelopmentNetwork({
        listenHostname: "127.0.0.1",
        publicHostname: "clab.orb.local",
      }),
    ).toEqual({
      listenHostname: "127.0.0.1",
      publicHostname: "clab.orb.local",
    });
  });

  it.each(["0.0.0.0", "[::]"])("rejects unspecified public hostname %s", (publicHostname) => {
    expect(() => resolveDevelopmentNetwork({ publicHostname })).toThrow(
      "Public development hostname must identify a browser-reachable host.",
    );
  });
});
