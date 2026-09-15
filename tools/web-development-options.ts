import { resolveDevelopmentNetwork } from "../src/platform/dev-host/network";

import type { WebDevelopmentCommandOptions } from "./web-development-command";

export function webDevelopmentOptions(
  environment: Readonly<Record<string, string | undefined>>,
  rendererRoot: string,
): WebDevelopmentCommandOptions {
  function port(name: string, fallback: number): number {
    const raw = environment[name];
    const value = raw === undefined ? fallback : Number(raw);
    if (!Number.isSafeInteger(value) || value < 1 || value > 65_535) {
      throw new Error(`${name} must be an integer from 1 through 65535.`);
    }
    return value;
  }
  const network = resolveDevelopmentNetwork({
    publicHostname: environment.STREAMSKOPE_DEV_PUBLIC_HOST ?? "clab.orb.local",
  });
  return {
    hostPort: port("STREAMSKOPE_HOST_PORT", 4319),
    rendererPort: port("STREAMSKOPE_RENDERER_PORT", 5173),
    publicHostname: network.publicHostname,
    rendererRoot,
  };
}
