import type { HostErrorCode, HostErrorStage } from "../../../src/features/kafka/contracts";

import { NspApiError } from "./api-client";

export function nspProblem(message: string, cleanup = false): Error {
  return Object.assign(new Error(message), {
    code: cleanup ? "REMOTE_CLEANUP" : "VALIDATION",
    stage: cleanup ? "acquisition" : "validation",
    retryable: cleanup,
    recovery: cleanup
      ? "Enter credentials for the same NSP API and run cleanup before retrying or changing the plugin."
      : "Review the NSP connection details and try again.",
  });
}

/** Only bounded, locally authored API messages become user-facing failures. */
export function nspFailure(error: unknown, cancelled = false): unknown {
  if (error instanceof NspApiError) {
    const mapping: Record<NspApiError["code"], readonly [HostErrorCode, HostErrorStage, string]> = {
      VALIDATION: [
        "VALIDATION",
        "validation",
        "Enter an HTTPS NSP API origin and valid credentials.",
      ],
      VERSION: [
        "VALIDATION",
        "validation",
        "Check access to the NSP version API and install a plugin supporting the running NSP release. Pending execution cleanup remains available.",
      ],
      AUTHENTICATION: [
        "HTTPS_AUTHENTICATION",
        "authorization",
        "Check the NSP username and password.",
      ],
      AUTHORIZATION: [
        "HTTPS_AUTHORIZATION",
        "authorization",
        "Use an NSP account permitted to create, publish, execute and delete workflow executions.",
      ],
      NOT_FOUND: [
        "HTTPS_RESPONSE",
        "acquisition",
        "Check that the NSP API provides Workflow Manager.",
      ],
      CONFLICT: [
        "VALIDATION",
        "validation",
        "Inspect the existing StreamSkope helper definition. Unrelated workflows are never overwritten.",
      ],
      NETWORK: [
        "HTTPS_RESPONSE",
        "acquisition",
        "Check NSP connectivity and certificate trust, then retry. Pending execution identifiers are retained.",
      ],
      CANCELLED: ["CANCELLED", "acquisition", "The operation was cancelled. Retry when ready."],
      WORKFLOW: [
        "REMOTE_COMMAND",
        "remote-command",
        "Verify Workflow Manager supports the mounted CA truststore and nsp.python action, then retry.",
      ],
      CLEANUP: [
        "REMOTE_CLEANUP",
        "acquisition",
        "Run NSP cleanup with the same API URL and account. The plugin retains recovery identifiers.",
      ],
    };
    const [code, stage, recovery] = mapping[error.code];
    return Object.assign(new Error(error.message), {
      code,
      stage,
      recovery,
      retryable: ["NETWORK", "CANCELLED", "CLEANUP"].includes(error.code),
    });
  }
  if (cancelled)
    return Object.assign(new Error("NSP capture was cancelled."), {
      code: "CANCELLED",
      stage: "acquisition",
      retryable: true,
      recovery: "Retry when ready.",
    });
  return error;
}
