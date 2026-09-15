import { describe, expect, it } from "vitest";

import { webDevelopmentOptions } from "../../tools/web-development-options";

describe("web development defaults", () => {
  it("uses the OrbStack hostname without environment setup", () => {
    expect(webDevelopmentOptions({}, "/repo")).toEqual({
      hostPort: 4319,
      rendererPort: 5173,
      publicHostname: "clab.orb.local",
      rendererRoot: "/repo",
    });
  });
  it("allows explicit loopback and port overrides", () => {
    expect(
      webDevelopmentOptions(
        {
          STREAMSKOPE_DEV_PUBLIC_HOST: "127.0.0.1",
          STREAMSKOPE_HOST_PORT: "4320",
          STREAMSKOPE_RENDERER_PORT: "5174",
        },
        "/repo",
      ),
    ).toMatchObject({
      hostPort: 4320,
      rendererPort: 5174,
      publicHostname: "127.0.0.1",
    });
  });
  it("rejects invalid endpoints before fixture operations", () => {
    expect(() => webDevelopmentOptions({ STREAMSKOPE_HOST_PORT: "oops" }, "/repo")).toThrow();
    expect(() =>
      webDevelopmentOptions({ STREAMSKOPE_DEV_PUBLIC_HOST: "http://evil/path" }, "/repo"),
    ).toThrow();
  });
});
