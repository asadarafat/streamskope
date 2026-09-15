import { describe, expect, it } from "vitest";

import {
  HOST_COMMANDS,
  HOST_ERROR_CODES,
  HOST_ERROR_STAGES,
  HOST_PROTOCOL_VERSION,
  PROFILE_LIMITS,
  REMOTE_TRUST_ACQUISITION_LIMITS,
  HostContractValidationError,
  parseHostCommand,
  parseHostCommandResponse,
} from "../../src/kafka/contracts";

const fingerprint = `SHA256:${"A".repeat(43)}`;
const target = {
  host: "kafka-lab.example.test",
  hostKeyFingerprint: fingerprint,
  password: "ephemeral-ssh-password",
  port: 22,
  username: "operator",
} as const;

describe("SSH capability contract", () => {
  it("accepts only a safe configuration status and no socket or secret fields", () => {
    const command = {
      command: "trustAcquisition.capabilities",
      id: "cap",
      version: HOST_PROTOCOL_VERSION,
      payload: {},
    };
    expect(parseHostCommand(command)).toEqual(command);
    const response = {
      command: command.command,
      id: command.id,
      version: HOST_PROTOCOL_VERSION,
      ok: true,
      result: { correlationId: "cap", sshAgent: "configured" },
    };
    expect(parseHostCommandResponse(response)).toEqual(response);
    expect(() => parseHostCommand({ ...command, payload: { socket: "sentinel" } })).toThrow();
    expect(() =>
      parseHostCommandResponse({ ...response, result: { ...response.result, socket: "sentinel" } }),
    ).toThrow();
    expect(() =>
      parseHostCommandResponse({
        ...response,
        result: { ...response.result, sshAgent: "authenticated" },
      }),
    ).toThrow();
  });
});

describe("Kafka remote-trust host contract", () => {
  it("validates scoped identity challenges and forbids renderer-supplied saved pins", () => {
    const open = {
      command: "trustAcquisition.editor.open",
      id: "editor",
      version: HOST_PROTOCOL_VERSION,
      payload: { profile: { id: "profile", revision: 1 } },
    };
    expect(parseHostCommand(open)).toEqual(open);
    expect(() =>
      parseHostCommand({ ...open, payload: { ...open.payload, fingerprint } }),
    ).toThrow();
    const response = {
      command: "trustAcquisition.hostKey.discover",
      id: "discover",
      version: HOST_PROTOCOL_VERSION,
      ok: true,
      result: {
        correlationId: "discover",
        hostKey: {
          target: { host: target.host, port: 22 },
          fingerprint,
          review: {
            id: "challenge",
            expiresAt: "2099-07-26T13:02:00.000Z",
            confirmationRequired: true,
          },
        },
      },
    };
    expect(parseHostCommandResponse(response)).toEqual(response);
    for (const review of [
      { ...response.result.hostKey.review, password: "sentinel" },
      { ...response.result.hostKey.review, confirmationRequired: "yes" },
      { ...response.result.hostKey.review, id: "" },
    ]) {
      expect(() =>
        parseHostCommandResponse({
          ...response,
          result: { ...response.result, hostKey: { ...response.result.hostKey, review } },
        }),
      ).toThrow();
    }
  });
  it("accepts bounded cancellation identities without credentials or acquisition values", () => {
    const command = {
      command: "trustAcquisition.cancel",
      id: "cancel-request",
      version: HOST_PROTOCOL_VERSION,
      payload: { requestId: "pending-request" },
    };
    expect(parseHostCommand(command)).toEqual(command);
    for (const payload of [
      { requestId: "" },
      { requestId: "pending", password: "secret" },
      { requestId: "x".repeat(257) },
    ]) {
      expect(() => parseHostCommand({ ...command, payload })).toThrow(HostContractValidationError);
    }
  });
  it.each(["ssh\u0000host", "ssh\u0007host"])(
    "rejects control characters in SSH endpoint %#",
    (host) => {
      expect(() =>
        parseHostCommand({
          command: "trustAcquisition.hostKey.discover",
          id: "invalid-host",
          version: HOST_PROTOCOL_VERSION,
          payload: { target: { host, port: 22 } },
        }),
      ).toThrow(HostContractValidationError);
    },
  );
  it.each([
    { mode: "password", password: "ephemeral" },
    { mode: "private-key", privateKey: "uploaded key bytes", passphrase: "ephemeral" },
    { mode: "agent" },
  ])("accepts an explicit ephemeral SSH mode $mode", (authentication) => {
    const command = {
      command: "trustAcquisition.password.fetch",
      id: "auth",
      version: HOST_PROTOCOL_VERSION,
      payload: {
        target: {
          host: target.host,
          port: 22,
          username: "operator",
          hostKeyFingerprint: fingerprint,
          authentication,
        },
      },
    };
    expect(parseHostCommand(command)).toEqual(command);
  });

  it.each([
    { mode: "agent", socketPath: "/local/private/socket" },
    { mode: "private-key", privateKeyPath: "/local/private/key" },
    { mode: "private-key", privateKey: "x".repeat(65_537) },
    { mode: "password", password: "" },
    { mode: "private-key", privateKey: "key", password: "fallback" },
  ])("rejects unauthorized or ambiguous SSH access %#", (authentication) => {
    expect(() =>
      parseHostCommand({
        command: "trustAcquisition.password.fetch",
        id: "auth",
        version: HOST_PROTOCOL_VERSION,
        payload: {
          target: {
            host: target.host,
            port: 22,
            username: "operator",
            hostKeyFingerprint: fingerprint,
            authentication,
          },
        },
      }),
    ).toThrow(HostContractValidationError);
  });

  it("declares the bounded additive protocol surface", () => {
    expect(HOST_PROTOCOL_VERSION).toBe(17);
    expect(HOST_COMMANDS).toEqual(
      expect.arrayContaining([
        "trustAcquisition.hostKey.discover",
        "trustAcquisition.password.fetch",
        "trustAcquisition.material.fetch",
        "trustAcquisition.discard",
      ]),
    );
    expect(HOST_ERROR_CODES).toEqual(
      expect.arrayContaining([
        "SSH_IDENTITY",
        "SSH_AUTHENTICATION",
        "SSH_UNREACHABLE",
        "REMOTE_COMMAND",
        "REMOTE_TRANSFER",
        "REMOTE_CLEANUP",
        "ACQUISITION_NOT_FOUND",
        "ACQUISITION_EXPIRED",
        "ACQUISITION_INCOMPLETE",
        "ACQUISITION_CAPACITY",
      ]),
    );
    expect(HOST_ERROR_STAGES).toEqual(
      expect.arrayContaining(["ssh", "remote-command", "remote-transfer", "acquisition"]),
    );
    expect(REMOTE_TRUST_ACQUISITION_LIMITS).toEqual({
      acquisitions: 8,
      commandOutputBytes: 16_384,
      diagnosticBytes: 8_192,
      fingerprintCharacters: 50,
      hostCharacters: 255,
      materialBytes: PROFILE_LIMITS.trustBinaryBytes,
      operationMs: 60_000,
      passwordCharacters: PROFILE_LIMITS.clientSecretCharacters,
      privateKeyCharacters: 65_536,
      portMaximum: 65_535,
      readyMs: 20_000,
      ttlMs: 10 * 60_000,
      usernameCharacters: 256,
    });
  });

  it("discovers only a bounded SSH endpoint and parses only safe host identity", () => {
    const command = {
      command: "trustAcquisition.hostKey.discover",
      id: "discover-host-key",
      payload: {
        target: {
          host: target.host,
          port: target.port,
        },
      },
      version: HOST_PROTOCOL_VERSION,
    } as const;
    expect(parseHostCommand(command)).toEqual(command);

    for (const unexpected of [
      { hostKeyFingerprint: fingerprint },
      { password: target.password },
      { username: target.username },
      { templateName: "Remote password" },
    ]) {
      expect(() =>
        parseHostCommand({
          ...command,
          payload: {
            target: {
              ...command.payload.target,
              ...unexpected,
            },
          },
        }),
      ).toThrow(HostContractValidationError);
    }
    for (const invalidTarget of [
      { host: "", port: 22 },
      { host: "kafka lab", port: 22 },
      { host: target.host, port: 0 },
      { host: target.host, port: 65_536 },
      { host: target.host, port: 22.5 },
    ]) {
      expect(() =>
        parseHostCommand({
          ...command,
          payload: { target: invalidTarget },
        }),
      ).toThrow(HostContractValidationError);
    }

    const response = {
      command: "trustAcquisition.hostKey.discover",
      id: command.id,
      ok: true,
      result: {
        correlationId: "correlation-discovery",
        hostKey: {
          fingerprint,
          target: command.payload.target,
        },
      },
      version: HOST_PROTOCOL_VERSION,
    } as const;
    expect(parseHostCommandResponse(response)).toEqual(response);

    expect(() =>
      parseHostCommandResponse({
        ...response,
        result: {
          ...response.result,
          password: target.password,
        },
      }),
    ).toThrow(HostContractValidationError);
  });

  it("parses password, material, and discard commands without accepting command text", () => {
    expect(
      parseHostCommand({
        command: "trustAcquisition.password.fetch",
        id: "acquire-password",
        payload: { target },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toEqual({
      command: "trustAcquisition.password.fetch",
      id: "acquire-password",
      payload: { target },
      version: HOST_PROTOCOL_VERSION,
    });

    expect(
      parseHostCommand({
        command: "trustAcquisition.material.fetch",
        id: "acquire-material",
        payload: {
          acquisitionId: "acquisition-1",
          kind: "jks",
          label: "nsp.truststore",
          target,
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      command: "trustAcquisition.material.fetch",
      payload: {
        acquisitionId: "acquisition-1",
        kind: "jks",
        label: "nsp.truststore",
        target,
      },
    });

    expect(
      parseHostCommand({
        command: "trustAcquisition.material.fetch",
        id: "acquire-pem",
        payload: {
          kind: "pem",
          label: "ca.pem",
          target,
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      payload: { kind: "pem", label: "ca.pem" },
    });

    expect(
      parseHostCommand({
        command: "trustAcquisition.discard",
        id: "discard",
        payload: { acquisitionId: "acquisition-1" },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      command: "trustAcquisition.discard",
      payload: { acquisitionId: "acquisition-1" },
    });

    expect(() =>
      parseHostCommand({
        command: "trustAcquisition.password.fetch",
        id: "injected-command",
        payload: {
          commandText: "cat /etc/shadow",
          target,
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it.each([
    ["missing fingerprint", { ...target, hostKeyFingerprint: undefined }],
    ["unprefixed fingerprint", { ...target, hostKeyFingerprint: "A".repeat(43) }],
    ["padded fingerprint", { ...target, hostKeyFingerprint: `${fingerprint}=` }],
    ["empty host", { ...target, host: "" }],
    ["host containing whitespace", { ...target, host: "kafka lab" }],
    ["zero port", { ...target, port: 0 }],
    ["port above range", { ...target, port: 65_536 }],
    ["fractional port", { ...target, port: 22.5 }],
    ["empty username", { ...target, username: "" }],
    ["empty password", { ...target, password: "" }],
    [
      "oversized password",
      { ...target, password: "x".repeat(PROFILE_LIMITS.clientSecretCharacters + 1) },
    ],
  ])("rejects %s before remote work", (_label, invalidTarget) => {
    expect(() =>
      parseHostCommand({
        command: "trustAcquisition.password.fetch",
        id: "invalid-target",
        payload: { target: invalidTarget },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("parses only an opaque aggregate acquisition response", () => {
    const response = {
      command: "trustAcquisition.material.fetch",
      id: "acquire-material",
      ok: true,
      result: {
        acquisition: {
          expiresAt: "2026-07-26T13:10:00.000Z",
          id: "acquisition-1",
          material: {
            byteCount: 4_096,
            kind: "jks",
            label: "nsp.truststore",
            templateName: "NSP 25.11",
          },
          password: {
            present: true,
            templateName: "NSP 25.11",
          },
          target: {
            host: "kafka-lab.example.test",
            hostKeyFingerprint: fingerprint,
            port: 22,
          },
        },
        correlationId: "correlation-1",
      },
      version: HOST_PROTOCOL_VERSION,
    } as const;

    expect(parseHostCommandResponse(response)).toEqual(response);

    expect(() =>
      parseHostCommandResponse({
        ...response,
        result: {
          ...response.result,
          acquisition: {
            ...response.result.acquisition,
            material: {
              ...response.result.acquisition.material,
              expiredCertificates: false,
              notYetValidCertificates: false,
            },
          },
        },
      }),
    ).toThrow(HostContractValidationError);

    for (const leakedField of [
      { commandText: "kubectl get secret" },
      { password: "fetched-password" },
      { stdout: "fetched-password" },
      { truststore: "base64-data" },
      { username: "operator" },
      { hostKeyFingerprint: fingerprint },
      { remotePath: "/tmp/streamskope-secret" },
    ]) {
      expect(() =>
        parseHostCommandResponse({
          ...response,
          result: {
            ...response.result,
            acquisition: {
              ...response.result.acquisition,
              ...leakedField,
            },
          },
        }),
      ).toThrow(HostContractValidationError);
    }
  });

  it("accepts acquisition references without allowing inline trust in the same value", () => {
    expect(
      parseHostCommand({
        command: "connection.test",
        id: "test-acquired",
        payload: {
          brokers: ["127.0.0.1:19093"],
          name: "Local validation",
          tls: {
            acquisitionId: "acquisition-1",
            enabled: true,
            kind: "jks",
          },
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      payload: {
        tls: {
          acquisitionId: "acquisition-1",
          enabled: true,
          kind: "jks",
        },
      },
    });

    expect(() =>
      parseHostCommand({
        command: "connection.test",
        id: "ambiguous-trust",
        payload: {
          brokers: ["127.0.0.1:19093"],
          name: "Local validation",
          tls: {
            acquisitionId: "acquisition-1",
            caPem: "-----BEGIN CERTIFICATE-----",
            enabled: true,
            kind: "jks",
          },
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("accepts acquired trust modes only for profile material and password", () => {
    const acquired = { acquisitionId: "acquisition-1", mode: "acquired" } as const;
    const command = parseHostCommand({
      command: "profiles.create",
      id: "profile-create-acquired",
      payload: {
        profile: {
          brokers: ["127.0.0.1:19093"],
          name: "Remote trust",
          oauth: {
            clientId: "admin",
            clientSecret: { mode: "replace", value: "oauth-secret" },
            scope: "kafka",
            tokenEndpoint: "https://oauth.example.test/token",
          },
          trust: {
            kind: "pkcs12",
            label: "remote.p12",
            material: acquired,
            password: acquired,
          },
        },
      },
      version: HOST_PROTOCOL_VERSION,
    });
    expect(command).toMatchObject({
      payload: {
        profile: {
          trust: {
            material: acquired,
            password: acquired,
          },
        },
      },
    });

    expect(() =>
      parseHostCommand({
        command: "profiles.create",
        id: "oauth-acquired",
        payload: {
          profile: {
            brokers: ["127.0.0.1:19093"],
            name: "Invalid OAuth",
            oauth: {
              clientId: "admin",
              clientSecret: acquired,
              scope: "kafka",
              tokenEndpoint: "https://oauth.example.test/token",
            },
            trust: {
              kind: "pem",
              label: "ca.pem",
              material: acquired,
              password: { mode: "clear" },
            },
          },
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });
});
