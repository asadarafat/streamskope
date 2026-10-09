import type { ProfileSummary, ProfileUpdateInput } from "../../src/features/kafka/contracts";
import {
  retainedClientIdentity,
  retainedServiceEndpoints,
} from "../../src/features/kafka/contracts/profile-retain-input";

export const pluginSecurityProfile: ProfileSummary = {
  id: "plugin-profile",
  revision: 4,
  active: false,
  name: "Managed connection",
  brokers: ["broker.example:9093"],
  transport: "tls",
  createdAt: "2026-10-09",
  updatedAt: "2026-10-09",
  trust: { kind: "pem", label: "broker.pem", materialPresent: true, passwordPresent: false },
  sasl: { mechanism: "SCRAM-SHA-512", username: "broker-user", passwordPresent: true },
  clientIdentity: { certificatePresent: true, privateKeyPresent: true, passphrasePresent: false },
  source: { kind: "plugin", pluginId: "sample.connection", version: 1, data: {} },
  services: {
    schemaRegistry: {
      baseUrl: "https://schema.example",
      authentication: "basic",
      basic: { username: "schema-user", passwordPresent: true },
      trust: {
        mode: "custom",
        kind: "pem",
        label: "schema.pem",
        materialPresent: true,
        passwordPresent: false,
      },
      clientIdentity: {
        certificatePresent: true,
        privateKeyPresent: true,
        passphrasePresent: false,
      },
    },
  },
};

export function retainedPluginProfile(): ProfileUpdateInput {
  const profile = pluginSecurityProfile;
  return {
    name: profile.name,
    brokers: profile.brokers,
    transport: "tls",
    expectedRevision: profile.revision ?? 1,
    trust: {
      kind: "pem",
      label: "broker.pem",
      material: { mode: "retain" },
      password: { mode: "clear" },
    },
    sasl: { mechanism: "SCRAM-SHA-512", username: "broker-user", password: { mode: "retain" } },
    clientIdentity: retainedClientIdentity(profile.clientIdentity!),
    services: retainedServiceEndpoints(profile.services!),
  };
}
