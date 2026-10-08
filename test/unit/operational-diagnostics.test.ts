import { describe, expect, it } from "vitest";

import {
  createOperationalDiagnostic,
  formatOperationalDiagnostic,
  operationalDiagnostic,
  OperationalDiagnosticError,
  OPERATIONAL_DIAGNOSTIC_CODES,
  parseOperationalDiagnostic,
  type OperationalDiagnostic,
} from "../../src/platform/diagnostics";

const firstId = "63e8c68b-ae6e-4f60-8b47-493921b33f04";
const requestId = "7138698c-28ef-43b6-9d30-8bce50ce0c19";
const secret = "private-passphrase=https://secret-user:secret-password@private-host/topic";

describe("finite operational diagnostics", () => {
  it("round-trips every catalog entry with fixed actionable text and a host correlation", () => {
    for (const code of OPERATIONAL_DIAGNOSTIC_CODES) {
      const diagnostic = createOperationalDiagnostic(code, firstId);
      expect(
        parseOperationalDiagnostic(JSON.parse(formatOperationalDiagnostic(diagnostic))),
      ).toEqual(diagnostic);
      expect(Object.keys(diagnostic)).toEqual([
        "code",
        "owner",
        "stage",
        "correlationId",
        "summary",
        "recovery",
      ]);
      expect(Object.isFrozen(diagnostic)).toBe(true);
      expect(diagnostic.recovery.length).toBeGreaterThan(20);
    }
  });

  it("replaces malformed correlation input without retaining it in output", () => {
    const diagnostic = createOperationalDiagnostic("HOST_FAILURE", `${firstId}\n${secret}`);
    expect(diagnostic.correlationId).toMatch(
      /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/u,
    );
    expect(formatOperationalDiagnostic(diagnostic)).not.toContain(secret);
    expect(
      parseOperationalDiagnostic({ ...diagnostic, correlationId: firstId.toUpperCase() }),
    ).toBeNull();
  });

  it("rejects forged catalog text, unknown fields, accessors and invalid identities", () => {
    const valid = createOperationalDiagnostic("HOST_FAILURE", firstId);
    for (const forged of [
      { ...valid, code: secret },
      { ...valid, owner: secret },
      { ...valid, stage: secret },
      { ...valid, summary: secret },
      { ...valid, recovery: secret },
      { ...valid, correlationId: secret },
      { ...valid, cause: secret },
      { ...valid, [Symbol("private")]: secret },
    ])
      expect(parseOperationalDiagnostic(forged)).toBeNull();
    let accessed = false;
    const getter = Object.defineProperty({ ...valid }, "summary", {
      get: (): string => {
        accessed = true;
        return secret;
      },
    });
    expect(parseOperationalDiagnostic(getter)).toBeNull();
    expect(accessed).toBe(false);
    expect(
      parseOperationalDiagnostic(
        new Proxy(
          {},
          {
            ownKeys: (): never => {
              throw new Error(secret);
            },
          },
        ),
      ),
    ).toBeNull();
  });

  it("preserves a trusted component failure and private cause while adopting the gateway request ID", () => {
    const cause = Object.assign(new Error(secret), { body: secret, endpoint: secret });
    const error = new OperationalDiagnosticError("NATS_RUNTIME_START_FAILED", {
      cause,
      correlationId: firstId,
    });
    expect(error.cause).toBe(cause);
    expect(operationalDiagnostic(error, "HOST_FAILURE").correlationId).toBe(firstId);
    expect(operationalDiagnostic(error, "HOST_FAILURE", secret).correlationId).toBe(firstId);
    const diagnostic = operationalDiagnostic(error, "HOST_FAILURE", requestId);
    expect(diagnostic).toMatchObject({
      code: "NATS_RUNTIME_START_FAILED",
      owner: "nats",
      stage: "startup",
      correlationId: requestId,
    });
    expect(formatOperationalDiagnostic(diagnostic)).not.toMatch(
      /private|secret-password|endpoint|body|stack/u,
    );
    expect(JSON.stringify(error)).not.toContain(secret);
    Object.defineProperty(error, "diagnostic", { value: { summary: secret } });
    expect(operationalDiagnostic(error, "HOST_FAILURE").code).toBe("NATS_RUNTIME_START_FAILED");
  });

  it("never trusts diagnostic-looking errors or prototype imitations", () => {
    const fake = Object.assign(Object.create(OperationalDiagnosticError.prototype) as object, {
      diagnostic: createOperationalDiagnostic("NATS_RUNTIME_START_FAILED", firstId),
      message: secret,
    });
    for (const error of [fake, { diagnostic: fake, cause: secret }, new Error(secret), secret]) {
      const diagnostic = operationalDiagnostic(error, "HOST_FAILURE", requestId);
      expect(diagnostic).toMatchObject({
        code: "HOST_FAILURE",
        owner: "gateway",
        correlationId: requestId,
      });
      expect(formatOperationalDiagnostic(diagnostic)).not.toContain(secret);
    }
  });

  it("defends the final serialization boundary against an altered diagnostic", () => {
    const unsafe = {
      ...createOperationalDiagnostic("HOST_FAILURE", firstId),
      summary: secret,
    } as OperationalDiagnostic;
    const output = formatOperationalDiagnostic(unsafe);
    expect(output).not.toContain(secret);
    expect(output).not.toContain("\n");
    expect(parseOperationalDiagnostic(JSON.parse(output))).toMatchObject({ code: "HOST_FAILURE" });
  });
});
