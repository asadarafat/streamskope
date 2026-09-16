import { TRUST_RECIPE_LIMITS, type TrustAcquisitionRecipeInput } from "./trust-recipe-types";

export const NSP_KUBECTL_TRUSTSTORE_COMMAND =
  "kubectl exec -n nsp-psa-restricted $(kubectl get pods -n nsp-psa-restricted -o name | grep -m1 nspos-tomcat | awk -F/ '{print $2}') -- cat /opt/nsp/os/ssl/nsp.truststore";

export const NSP_KUBECTL_TRUSTSTORE_PASSWORD_COMMAND =
  "kubectl get secret -o jsonpath='{.data.truststore-pass}' -n nsp-psa-restricted nsp-tls-truststore-pass-nspdeployer | base64 -d; echo";

// Inert starter configuration, not vendor-specific execution behavior.
export const BUILT_IN_TRUST_RECIPES = [
  {
    name: "nsp-26-04",
    kind: "jks",
    syntax: "named-v1",
    method: "ssh",
    ssh: {
      source: "stdout",
      value: NSP_KUBECTL_TRUSTSTORE_COMMAND,
      password: { source: "command", command: NSP_KUBECTL_TRUSTSTORE_PASSWORD_COMMAND },
    },
    parameters: [],
    timeoutSeconds: TRUST_RECIPE_LIMITS.defaultTimeoutSeconds,
    oauth: {
      endpoint: "https://{{host}}/rest-gateway/rest/api/v1/auth/token",
      clientId: "",
      scope: "",
    },
  },
] as const satisfies readonly TrustAcquisitionRecipeInput[];
