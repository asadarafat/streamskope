import {
  NSP_KUBECTL_TRUSTSTORE_COMMAND,
  NSP_KUBECTL_TRUSTSTORE_PASSWORD_COMMAND,
} from "../contracts/trust-recipe-defaults";

import type { KafkaConnectionTemplateDocument } from "./connection-template-types";

export const DEFAULT_CONNECTION_TEMPLATE_DOCUMENT: KafkaConnectionTemplateDocument = {
  catalogs: [
    {
      catalog: "truststore-fetch",
      entries: [
        {
          name: "nsp-25-4",
          template:
            "kubectl cp nsp-psa-restricted/$(kubectl get pods -n nsp-psa-restricted -l app=nsp-tomcat -o name | awk -F/ '{print $2}'):/opt/nsp/os/ssl/nsp.truststore {truststorePath}",
        },
        {
          name: "nsp-25-11",
          template: `${NSP_KUBECTL_TRUSTSTORE_COMMAND} > {truststorePath}`,
        },
      ],
      selectedName: "nsp-25-4",
    },
    {
      catalog: "truststore-password",
      entries: [
        {
          name: "nsp-25-11",
          template: NSP_KUBECTL_TRUSTSTORE_PASSWORD_COMMAND,
        },
      ],
      selectedName: "nsp-25-11",
    },
    {
      catalog: "oauth-endpoint",
      entries: [
        {
          name: "nsp-25-4",
          template: "https://{kafka-cluseter-server}/rest-gateway/rest/api/v1/auth/token",
        },
      ],
      selectedName: "nsp-25-4",
    },
  ],
};
