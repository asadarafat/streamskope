import { join } from "node:path";

export interface NativePackageLayout {
  readonly archivePath: string;
  readonly executablePath: string;
}

interface MacSigningOptions {
  readonly osxNotarize: {
    readonly appleId: string;
    readonly appleIdPassword: string;
    readonly teamId: string;
  };
  readonly osxSign: {
    readonly identity: string;
  };
}

interface WindowsSigningOptions {
  readonly windowsSign: {
    readonly certificateFile: string;
    readonly certificatePassword: string;
  };
}

export type ProductionSigningOptions =
  Readonly<Record<string, never>> | MacSigningOptions | WindowsSigningOptions;

function required(environment: Readonly<Record<string, string | undefined>>, name: string): string {
  const value = environment[name];
  if (value === undefined || value.length === 0) {
    throw new Error(`${name} is required for a production release.`);
  }
  return value;
}

export function nativePackageLayout(
  bundlePath: string,
  platform: NodeJS.Platform,
): NativePackageLayout {
  switch (platform) {
    case "darwin":
      return {
        archivePath: join(bundlePath, "StreamSkope.app", "Contents", "Resources", "app.asar"),
        executablePath: join(bundlePath, "StreamSkope.app", "Contents", "MacOS", "StreamSkope"),
      };
    case "linux":
      return {
        archivePath: join(bundlePath, "resources", "app.asar"),
        executablePath: join(bundlePath, "StreamSkope"),
      };
    case "win32":
      return {
        archivePath: join(bundlePath, "resources", "app.asar"),
        executablePath: join(bundlePath, "StreamSkope.exe"),
      };
    default:
      throw new Error(`${platform} is not a supported Electron release platform.`);
  }
}

export function productionSigningOptions(
  platform: NodeJS.Platform,
  production: boolean,
  environment: Readonly<Record<string, string | undefined>>,
): ProductionSigningOptions {
  if (!production || platform === "linux") {
    return {};
  }
  if (platform === "darwin") {
    const identity = required(environment, "STREAMSKOPE_MAC_SIGN_IDENTITY");
    const appleId = required(environment, "STREAMSKOPE_APPLE_ID");
    const appleIdPassword = required(environment, "STREAMSKOPE_APPLE_APP_PASSWORD");
    const teamId = required(environment, "STREAMSKOPE_APPLE_TEAM_ID");
    return {
      osxNotarize: {
        appleId,
        appleIdPassword,
        teamId,
      },
      osxSign: {
        identity,
      },
    };
  }
  if (platform === "win32") {
    return {
      windowsSign: {
        certificateFile: required(environment, "WINDOWS_CERTIFICATE_FILE"),
        certificatePassword: required(environment, "WINDOWS_CERTIFICATE_PASSWORD"),
      },
    };
  }
  throw new Error(`${platform} is not a supported Electron release platform.`);
}
