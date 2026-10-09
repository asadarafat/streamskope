type NatsImageFailureReason =
  | "docker-tool-unavailable"
  | "docker-permission-denied"
  | "docker-daemon-unavailable"
  | "registry-rate-limited"
  | "registry-authentication"
  | "image-unavailable"
  | "certificate-validation"
  | "network-unavailable"
  | "command-timeout"
  | "unknown";

export class NatsImageAvailabilityError extends Error {
  constructor(readonly reason: NatsImageFailureReason) {
    super(`Pinned NATS image availability failed (${reason}).`);
    this.name = "NatsImageAvailabilityError";
  }
}

function reason(error: unknown): NatsImageFailureReason {
  if (error === null || typeof error !== "object") return "unknown";
  const detail = error as { code?: unknown; stderr?: unknown; killed?: unknown; signal?: unknown };
  if (detail.code === "ENOENT") return "docker-tool-unavailable";
  if (detail.code === "EACCES" || detail.code === "EPERM") return "docker-permission-denied";
  if (detail.killed === true && detail.signal === "SIGTERM") return "command-timeout";
  // Examine bounded diagnostics locally; only these fixed categories can leave this owner.
  const stderr = typeof detail.stderr === "string" ? detail.stderr.slice(0, 16_384) : "";
  if (/permission denied while trying to connect to the docker daemon/iu.test(stderr))
    return "docker-permission-denied";
  if (/cannot connect to the docker daemon|is the docker daemon running/iu.test(stderr))
    return "docker-daemon-unavailable";
  if (/toomanyrequests|too many requests|pull rate limit/iu.test(stderr))
    return "registry-rate-limited";
  if (
    /unauthorized|authentication required|pull access denied|requested access .* denied/iu.test(
      stderr,
    )
  )
    return "registry-authentication";
  if (/manifest unknown|manifest_unknown|no matching manifest|manifest .* not found/iu.test(stderr))
    return "image-unavailable";
  if (/x509:|certificate verify failed|certificate verification failed/iu.test(stderr))
    return "certificate-validation";
  if (
    /i\/o timeout|tls handshake timeout|dial tcp|no such host|connection refused|connection reset|network is unreachable/iu.test(
      stderr,
    )
  )
    return "network-unavailable";
  return "unknown";
}

/** Image lookup never creates a container; a failed pull is diagnosed without retrying it. */
export async function ensureNatsImage(
  image: string,
  platform: string,
  run: (arguments_: readonly string[], timeoutMs: number) => Promise<unknown>,
): Promise<void> {
  try {
    await run(["image", "inspect", image], 10_000);
  } catch {
    try {
      await run(["pull", "--platform", platform, image], 120_000);
    } catch (error) {
      throw new NatsImageAvailabilityError(reason(error));
    }
  }
}
