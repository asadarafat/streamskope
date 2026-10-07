/** The composition retains its key and ownership when a startup cleanup barrier fails. */
export class WebGatewayCleanupUnconfirmedError extends Error {
  constructor(options?: ErrorOptions) {
    super("Provider cleanup could not be confirmed. Restart the instance.", options);
    this.name = "WebGatewayCleanupUnconfirmedError";
  }
}

export class GatewayProblem extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly recovery?: string,
  ) {
    super(message);
  }
}
