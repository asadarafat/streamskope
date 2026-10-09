type ContainerCommandFailureReason =
  | "docker-tool-unavailable"
  | "docker-permission-denied"
  | "docker-daemon-unavailable"
  | "registry-rate-limited"
  | "registry-authentication"
  | "image-unavailable"
  | "registry-server-unavailable"
  | "certificate-validation"
  | "network-unavailable"
  | "local-capacity-exhausted"
  | "port-conflict"
  | "container-name-conflict"
  | "container-runtime-failure"
  | "command-timeout"
  | "unknown";

export class ContainerImageAvailabilityError extends Error {
  constructor(readonly reason: ContainerCommandFailureReason) {
    super(`Pinned container image availability failed (${reason}).`);
    this.name = "ContainerImageAvailabilityError";
  }
}

export function containerCommandFailureReason(error: unknown): ContainerCommandFailureReason {
  if (error === null || typeof error !== "object") return "unknown";
  const detail = error as { code?: unknown; stderr?: unknown; killed?: unknown; signal?: unknown };
  if (detail.code === "ENOENT") return "docker-tool-unavailable";
  if (detail.code === "ENOSPC" || detail.code === "ENOMEM") return "local-capacity-exhausted";
  if (detail.code === "EACCES" || detail.code === "EPERM") return "docker-permission-denied";
  if (detail.killed === true && detail.signal === "SIGTERM") return "command-timeout";
  // Examine bounded diagnostics locally; only these fixed categories can leave this owner.
  const stderr = typeof detail.stderr === "string" ? detail.stderr.slice(0, 16_384) : "";
  if (/permission denied while trying to connect to the docker daemon/iu.test(stderr))
    return "docker-permission-denied";
  if (/cannot connect to the docker daemon|is the docker daemon running/iu.test(stderr))
    return "docker-daemon-unavailable";
  if (/no space left on device|cannot allocate memory|out of memory/iu.test(stderr))
    return "local-capacity-exhausted";
  if (/address already in use|port is already allocated/iu.test(stderr)) return "port-conflict";
  if (/container name .* already in use/iu.test(stderr)) return "container-name-conflict";
  if (/failed to create (?:shim )?task|oci runtime create failed|runc create failed/iu.test(stderr))
    return "container-runtime-failure";
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
  if (
    /500 internal server error|502 bad gateway|503 service unavailable|504 gateway timeout/iu.test(
      stderr,
    )
  )
    return "registry-server-unavailable";
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
export async function ensureContainerImage(
  image: string,
  platform: string | undefined,
  run: (arguments_: readonly string[], timeoutMs: number) => Promise<unknown>,
): Promise<void> {
  try {
    await run(["image", "inspect", image], 10_000);
  } catch {
    try {
      await run(
        ["pull", ...(platform === undefined ? [] : ["--platform", platform]), image],
        120_000,
      );
    } catch (error) {
      throw new ContainerImageAvailabilityError(containerCommandFailureReason(error));
    }
  }
}
