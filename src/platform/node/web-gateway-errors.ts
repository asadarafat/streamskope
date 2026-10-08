import {
  OperationalDiagnosticError,
  type OperationalDiagnostic,
  type OperationalDiagnosticCode,
} from "../diagnostics";

/** The composition retains its key and ownership when a startup cleanup barrier fails. */
export class WebGatewayCleanupUnconfirmedError extends OperationalDiagnosticError {
  constructor(
    options?: ErrorOptions & { correlationId?: string },
    code: Extract<
      OperationalDiagnosticCode,
      | "CLEANUP_UNCONFIRMED"
      | "KAFKA_CLEANUP_UNCONFIRMED"
      | "NATS_CLEANUP_UNCONFIRMED"
      | "PROVIDER_CLEANUP_UNCONFIRMED"
      | "VAULT_LOCK_FAILED"
    > = "CLEANUP_UNCONFIRMED",
  ) {
    super(code, options);
    this.name = "WebGatewayCleanupUnconfirmedError";
  }
}

export class GatewayProblem extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly recovery?: string,
    readonly diagnostic?: OperationalDiagnostic,
  ) {
    super(message);
  }
}

export function diagnosticProblem(
  status: number,
  code: string,
  diagnostic: OperationalDiagnostic,
): GatewayProblem {
  return new GatewayProblem(status, code, diagnostic.summary, diagnostic.recovery, diagnostic);
}
