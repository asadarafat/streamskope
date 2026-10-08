export type NativeFixturePhase =
  | "prepare Kafka distribution"
  | "extract Kafka distribution"
  | "initialize fixture"
  | "initialize OAuth"
  | "generate fixture TLS key"
  | "format broker storage"
  | "start broker"
  | "authenticate with TLS and OAuth";

export interface NativeFixtureDiagnostic {
  readonly phase: NativeFixturePhase;
  readonly exceptionClasses: readonly string[];
  readonly code?: string;
  readonly exitCode?: number;
  readonly cleanup: "passed" | "failed" | "not started";
}

const systemCodes = new Set([
  "ENOENT",
  "EACCES",
  "EPERM",
  "EBUSY",
  "ETIMEDOUT",
  "ENOSPC",
  "EIO",
  "ENOTEMPTY",
]);

/** Only fixed phases, native codes and Java exception identities can leave the fixture. */
export class NativeKafkaFixtureError extends Error {
  readonly diagnostic: NativeFixtureDiagnostic;

  constructor(
    phase: NativeFixturePhase,
    error: unknown,
    logs = "",
    cleanup: NativeFixtureDiagnostic["cleanup"] = "not started",
  ) {
    super(`Native Kafka fixture failed during ${phase}.`);
    this.name = "NativeKafkaFixtureError";
    const code =
      error !== null && typeof error === "object" && "code" in error ? error.code : undefined;
    const webException = error instanceof DOMException;
    this.diagnostic = {
      phase,
      exceptionClasses: [
        ...new Set(
          logs.match(
            /\b(?:java|javax|org\.apache\.kafka|org\.apache\.commons|org\.apache\.logging|kafka|joptsimple)\.(?:[A-Za-z_$][\w$]*\.)*[A-Za-z_$][\w$]*(?:Exception|Error)\b/gu,
          ) ?? [],
        ),
      ].slice(0, 5),
      ...(webException && error.name === "TimeoutError"
        ? { code: "ETIMEDOUT" }
        : typeof code === "string" && systemCodes.has(code)
          ? { code }
          : {}),
      ...(!webException &&
      typeof code === "number" &&
      Number.isInteger(code) &&
      code >= 0 &&
      code <= 255
        ? { exitCode: code }
        : {}),
      cleanup,
    };
  }
}
