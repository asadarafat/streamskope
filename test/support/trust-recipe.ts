import type { TrustAcquisitionRecipeInput } from "../../src/kafka/contracts";

export function trustRecipeInput(): Extract<TrustAcquisitionRecipeInput, { method: "ssh" }> {
  return {
    name: "Certificate file",
    kind: "pem",
    syntax: "named-v1",
    method: "ssh",
    ssh: { source: "file", value: "{{certificate_path}}", password: { source: "none" } },
    parameters: [
      { key: "certificate_path", label: "Certificate path", type: "path", required: true },
    ],
    timeoutSeconds: 30,
  };
}
