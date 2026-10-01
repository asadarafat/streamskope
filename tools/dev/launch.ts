import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { hostname } from "node:os";

import { resolveDevelopmentNetwork } from "../../src/platform/dev-host/network";

import { WebDevelopmentSessionError } from "./session";
import type { WebDevelopmentCommandOptions } from "./session";

export interface WebDevelopmentHostSystem {
  readonly hostname: () => string;
  readonly isOrbStackGuest: () => boolean;
}

const defaultSystem: WebDevelopmentHostSystem = {
  hostname,
  isOrbStackGuest: () => existsSync("/opt/orbstack-guest"),
};

export function defaultWebDevelopmentHostname(system: WebDevelopmentHostSystem): string {
  if (!system.isOrbStackGuest()) return "127.0.0.1";
  const current = system.hostname().trim().toLowerCase();
  if (current.length === 0 || current === "localhost") return "127.0.0.1";
  return current.endsWith(".orb.local") ? current : `${current}.orb.local`;
}

export function webDevelopmentOptions(
  environment: Readonly<Record<string, string | undefined>>,
  rendererRoot: string,
  system: WebDevelopmentHostSystem = defaultSystem,
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
    publicHostname:
      environment.STREAMSKOPE_DEV_PUBLIC_HOST ?? defaultWebDevelopmentHostname(system),
  });
  return {
    hostPort: port("STREAMSKOPE_HOST_PORT", 4319),
    rendererPort: port("STREAMSKOPE_RENDERER_PORT", 5173),
    publicHostname: network.publicHostname,
    rendererRoot,
  };
}

const BROWSER_OPEN_ACKNOWLEDGEMENT_MS = 1_000;

function browserOpenCommand(browserUrl: string): readonly [string, readonly string[]] {
  const configured = process.env.BROWSER?.trim();
  if (configured !== undefined && configured.length > 0) {
    if (configured.includes("\0")) {
      throw new WebDevelopmentSessionError("BROWSER contains an invalid null character.");
    }
    return [configured, [browserUrl]];
  }
  if (process.platform === "darwin") {
    return ["open", [browserUrl]];
  }
  if (process.platform === "win32") {
    return ["rundll32.exe", ["url.dll,FileProtocolHandler", browserUrl]];
  }
  return ["xdg-open", [browserUrl]];
}

export function openDevelopmentBrowser(browserUrl: string): Promise<void> {
  let parsed: URL;
  try {
    parsed = new URL(browserUrl);
  } catch (error) {
    return Promise.reject(
      new WebDevelopmentSessionError("Browser launch URL must be absolute.", { cause: error }),
    );
  }
  if (
    parsed.protocol !== "http:" ||
    parsed.username.length > 0 ||
    parsed.password.length > 0 ||
    parsed.search.length > 0
  ) {
    return Promise.reject(
      new WebDevelopmentSessionError("Browser launch URL must be a credential-free HTTP URL."),
    );
  }
  let command: string;
  let arguments_: readonly string[];
  try {
    [command, arguments_] = browserOpenCommand(browserUrl);
  } catch (error) {
    return Promise.reject(
      error instanceof Error
        ? error
        : new WebDevelopmentSessionError("Browser opener configuration is invalid."),
    );
  }
  return new Promise<void>((resolve, reject) => {
    const child = spawn(command, [...arguments_], {
      stdio: "ignore",
    });
    let settled = false;
    const finish = (error?: Error): void => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(acknowledgement);
      if (error === undefined) {
        resolve();
      } else {
        reject(error);
      }
    };
    const acknowledgement = setTimeout(() => {
      if (settled) {
        return;
      }
      settled = true;
      child.unref();
      resolve();
    }, BROWSER_OPEN_ACKNOWLEDGEMENT_MS);
    child.once("error", (error) => {
      finish(
        new WebDevelopmentSessionError("Browser opener could not be started.", { cause: error }),
      );
    });
    child.once("exit", (code, signal) => {
      if (code === 0 && signal === null) {
        finish();
        return;
      }
      finish(new WebDevelopmentSessionError("Browser opener did not accept the launch URL."));
    });
  });
}
