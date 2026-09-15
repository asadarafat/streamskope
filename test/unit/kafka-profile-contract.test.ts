import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  PROFILE_LIMITS,
  HostContractValidationError,
  parseHostCommand,
  parseHostCommandResponse,
  parseHostEvent,
} from "../../src/kafka/contracts";

const replace = (value: string): Readonly<Record<string, string>> => ({
  mode: "replace",
  value,
});

const profileInput = {
  brokers: ["127.0.0.1:19093"],
  name: "Local validation",
  oauth: {
    clientId: "admin",
    clientSecret: replace("fixture-secret"),
    scope: "kafka",
    tokenEndpoint: "http://127.0.0.1:15000/rest-gateway/rest/api/v1/auth/token",
  },
  trust: {
    kind: "pem",
    label: "ca.pem",
    material: replace("-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----"),
    password: { mode: "clear" },
  },
} as const;

const safeProfile = {
  active: false,
  brokers: ["127.0.0.1:19093"],
  createdAt: "2026-07-25T18:00:00.000Z",
  id: "01983770-8f0c-77e2-8edc-51426a9e2450",
  name: "Local validation",
  oauth: {
    clientId: "admin",
    clientSecretPresent: true,
    scope: "kafka",
    tokenEndpoint: "http://127.0.0.1:15000/rest-gateway/rest/api/v1/auth/token",
  },
  trust: {
    kind: "pem",
    label: "ca.pem",
    materialPresent: true,
    passwordPresent: false,
  },
  updatedAt: "2026-07-25T18:00:00.000Z",
} as const;

describe("Kafka profile host contract", () => {
  it("exposes only a validated revision alongside the safe profile summary", () => {
    const event = {
      event: "profiles.changed",
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
      payload: {
        profiles: [{ ...safeProfile, revision: 7 }],
        store: { durability: "session", protection: "memory", state: "ready" },
      },
    };
    expect(parseHostEvent(event)).toMatchObject({ payload: { profiles: [{ revision: 7 }] } });
    expect(() =>
      parseHostEvent({
        ...event,
        payload: { ...event.payload, profiles: [{ ...safeProfile, revision: 0 }] },
      }),
    ).toThrow(HostContractValidationError);
  });

  it("preserves the expected revision on profile updates", () => {
    expect(
      parseHostCommand({
        command: "profiles.update",
        id: "revision-update",
        version: HOST_PROTOCOL_VERSION,
        payload: { profileId: safeProfile.id, profile: { ...profileInput, expectedRevision: 3 } },
      }),
    ).toMatchObject({ payload: { profile: { expectedRevision: 3 } } });
  });

  it.each([0, -1, 1.5, "1", Number.MAX_SAFE_INTEGER + 1])(
    "rejects invalid expected revision %s",
    (expectedRevision) => {
      expect(() =>
        parseHostCommand({
          command: "profiles.update",
          id: "revision-invalid",
          version: HOST_PROTOCOL_VERSION,
          payload: { profileId: safeProfile.id, profile: { ...profileInput, expectedRevision } },
        }),
      ).toThrow(HostContractValidationError);
    },
  );

  it("preserves explicit optional cluster-service endpoints and authentication mode", () => {
    const services = {
      redpandaAdmin: {
        authentication: "oauth",
        baseUrl: "https://redpanda.example.test:9644",
      },
      schemaRegistry: {
        authentication: "none",
        baseUrl: "https://schema.example.test:8081/registry",
      },
    } as const;

    expect(
      parseHostCommand({
        command: "profiles.create",
        id: "profile-create-services",
        payload: { profile: { ...profileInput, services } },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      payload: { profile: { services } },
    });
  });

  it("parses list, create, update, test, delete and connect commands without dropping protected-value intent", () => {
    expect(
      parseHostCommand({
        command: "profiles.list",
        id: "profile-list",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({ command: "profiles.list", payload: {} });

    expect(
      parseHostCommand({
        command: "profiles.create",
        id: "profile-create",
        payload: { profile: profileInput },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      command: "profiles.create",
      payload: {
        profile: {
          oauth: { clientSecret: { mode: "replace", value: "fixture-secret" } },
          trust: {
            material: {
              mode: "replace",
              value: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
            },
          },
        },
      },
    });

    expect(
      parseHostCommand({
        command: "profiles.update",
        id: "profile-update",
        payload: {
          profile: {
            ...profileInput,
            oauth: {
              ...profileInput.oauth,
              clientSecret: { mode: "retain" },
            },
            trust: {
              ...profileInput.trust,
              material: { mode: "retain" },
            },
          },
          profileId: safeProfile.id,
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      command: "profiles.update",
      payload: {
        profile: {
          oauth: { clientSecret: { mode: "retain" } },
          trust: { material: { mode: "retain" } },
        },
        profileId: safeProfile.id,
      },
    });

    expect(
      parseHostCommand({
        command: "profiles.test",
        id: "profile-test-create",
        payload: {
          mode: "create",
          profile: profileInput,
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      command: "profiles.test",
      payload: {
        mode: "create",
        profile: {
          oauth: { clientSecret: { mode: "replace", value: "fixture-secret" } },
          trust: {
            material: {
              mode: "replace",
              value: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
            },
          },
        },
      },
    });

    expect(
      parseHostCommand({
        command: "profiles.test",
        id: "profile-test-update",
        payload: {
          mode: "update",
          profile: {
            ...profileInput,
            oauth: {
              ...profileInput.oauth,
              clientSecret: { mode: "retain" },
            },
            trust: {
              ...profileInput.trust,
              material: { mode: "retain" },
              password: { mode: "retain" },
            },
          },
          profileId: safeProfile.id,
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      command: "profiles.test",
      payload: {
        mode: "update",
        profile: {
          oauth: { clientSecret: { mode: "retain" } },
          trust: {
            material: { mode: "retain" },
            password: { mode: "retain" },
          },
        },
        profileId: safeProfile.id,
      },
    });

    for (const command of ["profiles.delete", "profiles.connect"] as const) {
      expect(
        parseHostCommand({
          command,
          id: command,
          payload: { profileId: safeProfile.id },
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toMatchObject({ command, payload: { profileId: safeProfile.id } });
    }
  });

  it.each([
    {
      label: "create draft with a profile id",
      payload: { mode: "create", profile: profileInput, profileId: safeProfile.id },
    },
    {
      label: "update draft without a profile id",
      payload: { mode: "update", profile: profileInput },
    },
    {
      label: "unknown draft mode",
      payload: { mode: "temporary", profile: profileInput },
    },
    {
      label: "unexpected draft field",
      payload: { mode: "create", profile: profileInput, secret: "leaked" },
    },
  ])("rejects a profile test with $label", ({ payload }) => {
    expect(() =>
      parseHostCommand({
        command: "profiles.test",
        id: "invalid-profile-test",
        payload,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("rejects protected values added to a profile-test response", () => {
    expect(() =>
      parseHostCommandResponse({
        command: "profiles.test",
        id: "profile-test-response",
        ok: true,
        result: {
          correlationId: "profile-test-correlation",
          clientSecret: "must-not-cross",
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("parses a bounded safe profile inventory and explicit store capability", () => {
    expect(
      parseHostEvent({
        event: "profiles.changed",
        payload: {
          profiles: [safeProfile],
          store: {
            durability: "durable",
            protection: "os-protected",
            state: "ready",
          },
        },
        sequence: 8,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toEqual({
      event: "profiles.changed",
      payload: {
        profiles: [safeProfile],
        store: {
          durability: "durable",
          protection: "os-protected",
          state: "ready",
        },
      },
      sequence: 8,
      version: HOST_PROTOCOL_VERSION,
    });

    expect(
      parseHostEvent({
        event: "profiles.changed",
        payload: {
          profiles: [],
          store: {
            durability: "session",
            protection: "memory",
            recovery: "Profiles are cleared when the browser host stops.",
            state: "ready",
          },
        },
        sequence: 9,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      payload: {
        store: { durability: "session", protection: "memory", state: "ready" },
      },
    });
  });

  it.each([
    ["client secret", { clientSecret: "leaked" }],
    ["raw trust material", { material: "leaked" }],
    ["encrypted blob", { ciphertext: "leaked" }],
    ["storage path", { storagePath: "/private/profile.json" }],
  ])("rejects renderer-visible %s", (_label, extra) => {
    expect(() =>
      parseHostEvent({
        event: "profiles.changed",
        payload: {
          profiles: [{ ...safeProfile, ...extra }],
          store: {
            durability: "durable",
            protection: "os-protected",
            state: "ready",
          },
        },
        sequence: 10,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("rejects an inventory beyond its declared bound", () => {
    expect(() =>
      parseHostEvent({
        event: "profiles.changed",
        payload: {
          profiles: Array.from({ length: PROFILE_LIMITS.profiles + 1 }, () => safeProfile),
          store: {
            durability: "session",
            protection: "memory",
            state: "ready",
          },
        },
        sequence: 11,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it.each([
    {
      label: "undeclared create retention",
      profile: {
        ...profileInput,
        trust: { ...profileInput.trust, material: { mode: "retain" } },
      },
    },
    {
      label: "ambiguous protected value",
      profile: {
        ...profileInput,
        oauth: {
          ...profileInput.oauth,
          clientSecret: { mode: "replace" },
        },
      },
    },
    {
      label: "oversized trust material",
      profile: {
        ...profileInput,
        trust: {
          ...profileInput.trust,
          material: replace("x".repeat(PROFILE_LIMITS.trustEncodedCharacters + 1)),
        },
      },
    },
    {
      label: "unexpected profile field",
      profile: { ...profileInput, secretPath: "/tmp/credential" },
    },
  ])("rejects $label", ({ profile }) => {
    expect(() =>
      parseHostCommand({
        command: "profiles.create",
        id: "invalid-create",
        payload: { profile },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("parses an unavailable protected store without inventing profiles", () => {
    expect(
      parseHostEvent({
        event: "profiles.changed",
        payload: {
          profiles: [],
          store: {
            durability: "durable",
            protection: "unavailable",
            recovery: "Unlock the operating-system credential service, then restart StreamSkope.",
            state: "unavailable",
          },
        },
        sequence: 12,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      payload: {
        profiles: [],
        store: {
          durability: "durable",
          protection: "unavailable",
          state: "unavailable",
        },
      },
    });
  });
});
