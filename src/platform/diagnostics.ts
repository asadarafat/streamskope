/** Public diagnostics contain catalog text and a correlation ID, never exception text. */
const catalog = {
  RUNTIME_START_FAILED: [
    "browser-runtime",
    "startup",
    "The browser runtime could not start.",
    "Check the installed package and data-directory permissions, then restart the host.",
  ],
  KAFKA_RUNTIME_START_FAILED: [
    "kafka",
    "startup",
    "The Kafka runtime could not start.",
    "Check data-directory permissions and restore the complete package, including its Kafka worker, then restart.",
  ],
  NATS_RUNTIME_START_FAILED: [
    "nats",
    "startup",
    "The NATS runtime could not start.",
    "Restore the complete StreamSkope package and check profile-storage permissions, then restart.",
  ],
  PLUGIN_RUNTIME_START_FAILED: [
    "plugins",
    "startup",
    "The plugin runtime could not start.",
    "Check plugin-storage permissions and installed package integrity, then restart.",
  ],
  PRIVATE_HOST_START_FAILED: [
    "provider-host",
    "startup",
    "The private provider host could not start.",
    "Check the installed package and available local resources, then restart.",
  ],
  DESKTOP_START_FAILED: [
    "desktop",
    "startup",
    "The desktop application could not start.",
    "Restore the complete StreamSkope installation and restart the application.",
  ],
  DESKTOP_SHELL_START_FAILED: [
    "desktop-shell",
    "startup",
    "The desktop window could not start.",
    "Restore the complete installation, including renderer assets, then restart.",
  ],
  PROFILE_PROTECTION_START_FAILED: [
    "profile-protection",
    "startup",
    "Desktop credential protection could not initialize.",
    "Check that the operating-system credential store is available, then restart.",
  ],
  RENDERER_ASSET_UNAVAILABLE: [
    "gateway",
    "assets",
    "A required renderer asset is unavailable.",
    "Restore the complete browser package or image and restart the host.",
  ],
  HOST_FAILURE: [
    "gateway",
    "request",
    "The host could not complete the request.",
    "Inspect the target before retrying a change: the request may have completed without an acknowledgement. Share this diagnostic correlation ID with the maintainer.",
  ],
  BROWSER_START_FAILED: [
    "browser-host",
    "startup",
    "The browser host could not start.",
    "Check the host configuration, port availability and installed package, then retry.",
  ],
  BROWSER_CONFIGURATION_INVALID: [
    "browser-host",
    "configuration",
    "The browser host configuration is invalid.",
    "Check the documented bind address, port, public origin and session settings, then retry.",
  ],
  BROWSER_PORT_IN_USE: [
    "browser-host",
    "listen",
    "The browser host port is already in use.",
    "Stop the conflicting listener or choose an available port and update the public origin, then retry.",
  ],
  BROWSER_LISTEN_DENIED: [
    "browser-host",
    "listen",
    "The browser host cannot bind its configured listener.",
    "Check local bind permissions and choose an allowed port, then retry.",
  ],
  BROWSER_DATA_UNAVAILABLE: [
    "browser-host",
    "storage",
    "The browser host data directory is unavailable.",
    "Preserve the data directory and check its ownership, permissions and available disk space, then retry.",
  ],
  CLEANUP_UNCONFIRMED: [
    "browser-runtime",
    "cleanup",
    "Browser runtime cleanup could not be confirmed.",
    "Preserve recovery records. Stop the existing host and inspect its owned remote capture resources before retrying with the same data directory; process exit does not confirm remote cleanup.",
  ],
  KAFKA_CLEANUP_UNCONFIRMED: [
    "kafka",
    "cleanup",
    "Kafka runtime cleanup could not be confirmed.",
    "Preserve recovery records. Confirm the existing host has exited and inspect its owned remote capture resources before retrying; process exit does not confirm remote cleanup.",
  ],
  NATS_CLEANUP_UNCONFIRMED: [
    "nats",
    "cleanup",
    "NATS runtime cleanup could not be confirmed.",
    "Stop the existing host and confirm it has exited before starting a replacement.",
  ],
  PROVIDER_CLEANUP_UNCONFIRMED: [
    "provider-host",
    "cleanup",
    "Provider cleanup could not be confirmed.",
    "Preserve recovery records. Confirm the existing host has exited and inspect any owned remote capture resources before retrying; process exit does not confirm remote cleanup.",
  ],
  DESKTOP_CLEANUP_UNCONFIRMED: [
    "desktop",
    "cleanup",
    "Desktop cleanup could not be confirmed.",
    "Confirm all StreamSkope processes have exited before restarting the application.",
  ],
  DESKTOP_SHELL_CLEANUP_UNCONFIRMED: [
    "desktop-shell",
    "cleanup",
    "Desktop window cleanup could not be confirmed.",
    "Confirm all StreamSkope processes have exited before restarting the application.",
  ],
  VAULT_LOCK_FAILED: [
    "vault",
    "cleanup",
    "The vault could not finish locking.",
    "Stop the existing host, preserve its data directory and confirm it has exited before restarting.",
  ],
  VAULT_INVALID_PASSPHRASE: [
    "vault",
    "unlock",
    "The passphrase does not meet the vault requirements.",
    "Use a passphrase between 12 and 1024 UTF-8 bytes.",
  ],
  VAULT_ALREADY_EXISTS: [
    "vault",
    "unlock",
    "The data directory already contains a vault or existing data.",
    "Unlock the existing vault. Preserve existing files before selecting another data directory.",
  ],
  VAULT_NOT_CREATED: [
    "vault",
    "unlock",
    "The vault has not been created.",
    "Create a vault in the intended data directory before unlocking it.",
  ],
  VAULT_UNAVAILABLE: [
    "vault",
    "unlock",
    "The vault is unreadable, corrupt or unsupported.",
    "Preserve the data directory and restore its permissions or a known-good backup.",
  ],
  VAULT_IN_USE: [
    "vault",
    "unlock",
    "Another host holds this data directory's lease.",
    "Stop the other StreamSkope host using this data directory, then retry.",
  ],
  VAULT_UNLOCK_FAILED: [
    "vault",
    "unlock",
    "The vault could not be unlocked.",
    "Check the passphrase. Preserve the existing vault and profile files.",
  ],
  VAULT_LOCKED: [
    "vault",
    "unlock",
    "The vault is locked.",
    "Unlock the vault in the browser before continuing.",
  ],
  VAULT_INVALID_VALUE: [
    "vault",
    "unlock",
    "A protected value is corrupt, unsupported or too large.",
    "Preserve the data directory and restore a known-good backup before retrying.",
  ],
} as const;

export type OperationalDiagnosticCode = keyof typeof catalog;
export const OPERATIONAL_DIAGNOSTIC_CODES: readonly OperationalDiagnosticCode[] = Object.freeze(
  Object.keys(catalog) as OperationalDiagnosticCode[],
);
export interface OperationalDiagnostic {
  readonly code: OperationalDiagnosticCode;
  readonly owner: (typeof catalog)[OperationalDiagnosticCode][0];
  readonly stage: (typeof catalog)[OperationalDiagnosticCode][1];
  readonly correlationId: string;
  readonly summary: string;
  readonly recovery: string;
}

const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-[1-8][a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/u;
const keys = ["code", "owner", "stage", "correlationId", "summary", "recovery"] as const;
const ownedErrors = new WeakMap<object, OperationalDiagnostic>();

export function createOperationalDiagnostic(
  code: OperationalDiagnosticCode,
  correlationId?: string,
): OperationalDiagnostic {
  if (!Object.hasOwn(catalog, code)) throw new TypeError("Unknown operational diagnostic code.");
  const [owner, stage, summary, recovery] = catalog[code];
  return Object.freeze({
    code,
    owner,
    stage,
    correlationId:
      correlationId !== undefined && uuid.test(correlationId)
        ? correlationId
        : globalThis.crypto.randomUUID(),
    summary,
    recovery,
  });
}

/** Only an exact catalog-shaped data object is accepted; getters and extra fields are rejected. */
export function parseOperationalDiagnostic(value: unknown): OperationalDiagnostic | null {
  if (value === null || typeof value !== "object") return null;
  try {
    const fields = Object.getOwnPropertyDescriptors(value);
    if (Reflect.ownKeys(fields).length !== keys.length || keys.some((key) => !(key in fields)))
      return null;
    if (keys.some((key) => !Object.hasOwn(fields[key]!, "value"))) return null;
    const code: unknown = fields.code!.value;
    const correlationId: unknown = fields.correlationId!.value;
    if (
      typeof code !== "string" ||
      !Object.hasOwn(catalog, code) ||
      typeof correlationId !== "string" ||
      !uuid.test(correlationId)
    )
      return null;
    const diagnostic = createOperationalDiagnostic(
      code as OperationalDiagnosticCode,
      correlationId,
    );
    return keys.every((key) => fields[key]!.value === diagnostic[key]) ? diagnostic : null;
  } catch {
    return null;
  }
}

/** The cause remains private to host control flow and is never copied into public diagnostics. */
export class OperationalDiagnosticError extends Error {
  readonly diagnostic: OperationalDiagnostic;
  constructor(
    code: OperationalDiagnosticCode,
    options?: { cause?: unknown; correlationId?: string },
  ) {
    const diagnostic = createOperationalDiagnostic(code, options?.correlationId);
    super(diagnostic.summary, { cause: options?.cause });
    this.name = "OperationalDiagnosticError";
    this.diagnostic = diagnostic;
    ownedErrors.set(this, diagnostic);
  }
}

export function operationalDiagnostic(
  error: unknown,
  fallbackCode: OperationalDiagnosticCode,
  correlationId?: string,
): OperationalDiagnostic {
  const owned = error !== null && typeof error === "object" ? ownedErrors.get(error) : undefined;
  return createOperationalDiagnostic(
    owned?.code ?? fallbackCode,
    correlationId !== undefined && uuid.test(correlationId) ? correlationId : owned?.correlationId,
  );
}

/** A single safe JSON record, without a newline or arbitrary fields attached by a caller. */
export function formatOperationalDiagnostic(diagnostic: OperationalDiagnostic): string {
  return JSON.stringify(
    parseOperationalDiagnostic(diagnostic) ?? createOperationalDiagnostic("HOST_FAILURE"),
  );
}
