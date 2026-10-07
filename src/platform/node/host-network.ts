const LOOPBACK_DEVELOPMENT_HOSTNAME = "127.0.0.1";
const UNSPECIFIED_HOSTNAMES = new Set(["0.0.0.0", "[::]"]);

export interface DevelopmentNetworkOptions {
  readonly listenHostname?: string;
  readonly publicHostname?: string;
}

export interface DevelopmentNetwork {
  readonly listenHostname: string;
  readonly publicHostname: string;
}

export class DevelopmentNetworkConfigurationError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "DevelopmentNetworkConfigurationError";
  }
}

function parseHostname(value: string, label: string): string {
  let parsed: URL;
  try {
    parsed = new URL(`http://${value}`);
  } catch (error) {
    throw new DevelopmentNetworkConfigurationError(
      `${label} must contain one hostname without a scheme, port, path, query, or fragment.`,
      { cause: error },
    );
  }

  if (
    value.length === 0 ||
    value.trim() !== value ||
    parsed.username.length > 0 ||
    parsed.password.length > 0 ||
    parsed.port.length > 0 ||
    parsed.pathname !== "/" ||
    parsed.search.length > 0 ||
    parsed.hash.length > 0 ||
    parsed.hostname.length === 0
  ) {
    throw new DevelopmentNetworkConfigurationError(
      `${label} must contain one hostname without a scheme, port, path, query, or fragment.`,
    );
  }
  return parsed.hostname;
}

export function resolveDevelopmentNetwork(
  options: DevelopmentNetworkOptions = {},
): DevelopmentNetwork {
  const publicHostname = parseHostname(
    options.publicHostname ?? LOOPBACK_DEVELOPMENT_HOSTNAME,
    "Public development hostname",
  );
  if (UNSPECIFIED_HOSTNAMES.has(publicHostname)) {
    throw new DevelopmentNetworkConfigurationError(
      "Public development hostname must identify a browser-reachable host.",
    );
  }
  return {
    listenHostname: parseHostname(
      options.listenHostname ??
        (publicHostname === LOOPBACK_DEVELOPMENT_HOSTNAME
          ? LOOPBACK_DEVELOPMENT_HOSTNAME
          : "0.0.0.0"),
      "Development listener hostname",
    ),
    publicHostname,
  };
}

export function developmentOrigin(hostname: string, port: number): string {
  return `http://${hostname}:${port}`;
}
