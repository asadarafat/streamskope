import { describe, expect, it } from "vitest";

import { webDevelopmentOptions, type WebDevelopmentHostSystem } from "../../tools/dev/launch";

const orbStack = (name: string): WebDevelopmentHostSystem => ({
  hostname: (): string => name,
  isOrbStackGuest: (): boolean => true,
});

describe("web development defaults", () => {
  it("derives the current OrbStack hostname without environment setup", () => {
    expect(webDevelopmentOptions({}, "/repo", orbStack("debian"))).toEqual({
      hostPort: 4319,
      rendererPort: 5173,
      publicHostname: "debian.orb.local",
      rendererRoot: "/repo",
    });
  });

  it("keeps an OrbStack FQDN and falls back to loopback elsewhere", () => {
    expect(webDevelopmentOptions({}, "/repo", orbStack("lab.orb.local"))).toMatchObject({
      publicHostname: "lab.orb.local",
    });
    expect(
      webDevelopmentOptions({}, "/repo", {
        hostname: () => "build-host",
        isOrbStackGuest: () => false,
      }),
    ).toMatchObject({ publicHostname: "127.0.0.1" });
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
