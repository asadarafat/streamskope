import type { HostError } from "../../../src/features/kafka/contracts";

export const NSP_PLUGIN_ID = "streamskope.nsp";
export const NSP_CAPTURE_METHODS = [
  "nspCapture.connect",
  "nspCapture.cancel",
  "nspCapture.status",
  "nspCapture.cleanup",
] as const;
export type NspMethod = (typeof NSP_CAPTURE_METHODS)[number];

export interface NspCredentials {
  readonly apiUrl: string;
  readonly username: string;
  readonly password: string;
  readonly verifyCertificate: boolean;
}

export interface NspConnectInput extends NspCredentials {
  readonly profileId?: string;
  readonly brokers?: readonly string[];
  readonly authentication?: "auto" | "tls" | "oauth";
}

export interface NspStatus {
  readonly state: "idle" | "running" | "cleanup-required";
  readonly requestId?: string;
  readonly message?: string;
}

export type NspResult =
  | {
      readonly ok: true;
      readonly profileId?: string;
      readonly status?: NspStatus;
      readonly cancelled?: boolean;
    }
  | { readonly ok: false; readonly error: HostError };

export const NSP_CAPTURE_STEPS = [
  "authenticate",
  "workflow",
  "retrieve",
  "cleanup",
  "test",
  "save",
] as const;

export interface NspProgress {
  readonly requestId: string;
  readonly step: (typeof NSP_CAPTURE_STEPS)[number];
  readonly message: string;
}

export interface NspProfileSource {
  readonly apiUrl: string;
  readonly brokers: readonly string[];
  readonly workflowName: string;
  readonly authentication: "tls" | "oauth";
}

export interface NspInputs {
  readonly "nspCapture.connect": NspConnectInput;
  readonly "nspCapture.cancel": { readonly requestId: string };
  readonly "nspCapture.status": Readonly<Record<string, never>>;
  readonly "nspCapture.cleanup": NspCredentials;
}
