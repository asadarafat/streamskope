import {
  HOST_ERROR_CODES,
  HOST_ERROR_STAGES,
  type HostErrorCode,
  type HostErrorStage,
} from "../../../features/kafka/contracts";

interface StructuredTransportFailure extends Error {
  readonly code: HostErrorCode;
  readonly recovery: string;
  readonly retryable: boolean;
  readonly stage: HostErrorStage;
  readonly target?: string;
}

function isStructuredTransportFailure(error: unknown): error is StructuredTransportFailure {
  if (!(error instanceof Error)) {
    return false;
  }
  const candidate = error as Partial<StructuredTransportFailure>;
  return (
    typeof candidate.code === "string" &&
    HOST_ERROR_CODES.includes(candidate.code) &&
    typeof candidate.recovery === "string" &&
    typeof candidate.retryable === "boolean" &&
    typeof candidate.stage === "string" &&
    HOST_ERROR_STAGES.includes(candidate.stage) &&
    (candidate.target === undefined || typeof candidate.target === "string")
  );
}

export class KafkaRemoteTrustTransportError extends Error {
  constructor(
    readonly code: HostErrorCode,
    readonly stage: HostErrorStage,
    readonly recovery: string,
    readonly retryable: boolean,
    readonly target: string,
    message: string,
  ) {
    super(message);
    this.name = "KafkaRemoteTrustTransportError";
  }
}

export function sshIdentityError(target: string): KafkaRemoteTrustTransportError {
  return new KafkaRemoteTrustTransportError(
    "SSH_IDENTITY",
    "ssh",
    "Do not accept the replacement automatically. Verify the server's current SHA-256 host-key fingerprint with its administrator, then start a new acquisition.",
    false,
    target,
    "The SSH server host key changed after StreamSkope discovered and pinned it.",
  );
}

export function sshAuthenticationError(target: string): KafkaRemoteTrustTransportError {
  return new KafkaRemoteTrustTransportError(
    "SSH_AUTHENTICATION",
    "ssh",
    "Verify the SSH username and selected password, uploaded key/passphrase or available local agent, then retry. No other authentication mode was attempted.",
    false,
    target,
    "The selected SSH authentication mode failed or is unavailable.",
  );
}

export function sshUnavailableError(target: string): KafkaRemoteTrustTransportError {
  return new KafkaRemoteTrustTransportError(
    "SSH_UNREACHABLE",
    "ssh",
    "Verify the SSH host, port, network path, and server availability.",
    true,
    target,
    "The SSH target could not establish a ready session.",
  );
}

export function remoteCommandError(target: string): KafkaRemoteTrustTransportError {
  return new KafkaRemoteTrustTransportError(
    "REMOTE_COMMAND",
    "remote-command",
    "Verify the selected template and remote environment, then try again.",
    false,
    target,
    "The selected remote template did not complete successfully.",
  );
}

export function remoteTransferError(target: string): KafkaRemoteTrustTransportError {
  return new KafkaRemoteTrustTransportError(
    "REMOTE_TRANSFER",
    "remote-transfer",
    "Verify that the template creates one readable bounded regular file.",
    false,
    target,
    "The remote trust file could not be read safely.",
  );
}

export function remoteCleanupError(target: string): KafkaRemoteTrustTransportError {
  return new KafkaRemoteTrustTransportError(
    "REMOTE_CLEANUP",
    "remote-transfer",
    "Inspect the remote temporary-file policy and remove stale StreamSkope files if required.",
    false,
    target,
    "The remote operation completed but temporary-file cleanup was not confirmed.",
  );
}

export function remoteTimeoutError(
  stage: "remote-command" | "remote-transfer" | "ssh",
  target: string,
): KafkaRemoteTrustTransportError {
  return new KafkaRemoteTrustTransportError(
    "TIMEOUT",
    stage,
    "Verify the remote service is responsive, then retry the bounded operation.",
    true,
    target,
    "The bounded remote trust operation timed out.",
  );
}

export function withCleanupRisk(error: unknown, target: string): KafkaRemoteTrustTransportError {
  if (isStructuredTransportFailure(error)) {
    return new KafkaRemoteTrustTransportError(
      error.code,
      error.stage,
      `${error.recovery} Remote temporary-file cleanup could not also be confirmed.`,
      error.retryable,
      error.target ?? target,
      `${error.message} Remote cleanup could not be confirmed.`,
    );
  }
  return new KafkaRemoteTrustTransportError(
    "INTERNAL",
    "remote-transfer",
    "Retry the operation and inspect the remote temporary-file policy.",
    false,
    target,
    "The remote operation and its cleanup both failed safely.",
  );
}
