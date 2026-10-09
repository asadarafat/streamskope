import type { KafkaProfileRecord } from "../../features/kafka/application";
import type { ClusterServiceEndpointInput } from "../../features/kafka/contracts";
import {
  resolveProfileSecurity,
  securityValidationInput,
  type StoredProfileSecurity,
} from "../../features/kafka/application/profile-security";
import {
  parseProfileIdentity,
  parseProfileSasl,
  parseProfileServices,
} from "../../features/kafka/contracts/profile-security-validation";
import {
  boundedText,
  exactKeys,
  record,
  text,
} from "../../features/kafka/contracts/validation-primitives";

export function hasExpandedProfileSecurity(profile: KafkaProfileRecord): boolean {
  return (
    profile.sasl !== undefined ||
    profile.clientIdentity !== undefined ||
    Object.values<ClusterServiceEndpointInput<string>>({ ...profile.services }).some(
      (service) =>
        Object.keys(service).some((key) => key !== "authentication" && key !== "baseUrl") ||
        !["none", "oauth"].includes(service.authentication),
    )
  );
}

export function storedProfileSecurity(profile: KafkaProfileRecord): StoredProfileSecurity {
  return {
    ...(profile.sasl === undefined ? {} : { sasl: profile.sasl }),
    ...(profile.services === undefined ? {} : { services: profile.services }),
    ...(profile.clientIdentity === undefined ? {} : { clientIdentity: profile.clientIdentity }),
  };
}

export function parseStoredProfileSecurity(
  value: unknown,
  plaintext: boolean,
): StoredProfileSecurity {
  const v = record(value, "security");
  exactKeys(
    v,
    plaintext ? ["sasl", "services"] : ["sasl", "services", "clientIdentity"],
    "security",
  );
  const parsed: StoredProfileSecurity = {
    ...(v.sasl === undefined ? {} : { sasl: parseProfileSasl(v.sasl, "security.sasl", text) }),
    ...(v.services === undefined
      ? {}
      : { services: parseProfileServices(v.services, "security.services", boundedText) }),
    ...(v.clientIdentity === undefined
      ? {}
      : {
          clientIdentity: parseProfileIdentity(
            v.clientIdentity,
            "security.clientIdentity",
            boundedText,
          ),
        }),
  };
  const { clientIdentity, ...security } = securityValidationInput(parsed);
  // Resolve through the same protected-value model as manual/plugin drafts, checking missing secrets.
  resolveProfileSecurity(
    plaintext
      ? { ...security, brokers: [], name: "stored", transport: "plaintext" }
      : {
          ...security,
          ...(clientIdentity === undefined ? {} : { clientIdentity }),
          brokers: [],
          name: "stored",
          transport: "tls",
          trust: {
            kind: "pem",
            label: "stored",
            material: { mode: "replace", value: "stored" },
            password: { mode: "clear" },
          },
        },
    undefined,
  );
  return parsed;
}
