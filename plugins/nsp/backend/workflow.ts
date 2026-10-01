import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

// Packaging embeds these exact resource bytes so the installed backend is standalone.
declare const __STREAMSKOPE_PLUGIN_RESOURCES__: Readonly<Record<string, string>> | undefined;

export const NSP_WORKFLOW_NAME = "streamskopeNspCaptureV1";
export const NSP_WORKFLOW_VERSION = "1.0.0";

// Immutable shared helper: changes to its behavior require a new workflow identity.
// It reads CA certificates and the truststore password, never a private key/password.
export const NSP_WORKFLOW_DEFINITION =
  typeof __STREAMSKOPE_PLUGIN_RESOURCES__ === "undefined"
    ? readFileSync(new URL("../resources/nsp-capture.workflow.yaml", import.meta.url), "utf8")
    : __STREAMSKOPE_PLUGIN_RESOURCES__["nsp-capture.workflow.yaml"]!;

export const NSP_WORKFLOW_FINGERPRINT = createHash("sha256")
  .update(NSP_WORKFLOW_DEFINITION)
  .digest("hex");
