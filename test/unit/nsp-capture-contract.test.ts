import { describe, expect, it } from "vitest";

import {
  fromPluginProfileSource,
  normalizeNspApiUrl,
  parseNspConnectInput,
  parseNspProgress,
  parseNspResult,
  toPluginProfileSource,
} from "../../plugins/nsp/contracts";

const credentials = {
  apiUrl: "https://NSP.example.test/",
  username: "operator",
  password: "test-only-password",
  verifyCertificate: true,
};

describe("NSP capture contract", () => {
  it("canonicalizes endpoint identity and preserves passwords exactly", () => {
    expect(
      parseNspConnectInput({
        ...credentials,
        password: " padded password ",
        authentication: "auto",
        brokers: ["broker.example.test:9193", "[::1]:9193"],
      }),
    ).toEqual({
      ...credentials,
      apiUrl: "https://nsp.example.test",
      password: " padded password ",
      authentication: "auto",
      brokers: ["broker.example.test:9193", "[::1]:9193"],
    });
  });

  it.each([
    "http://nsp.example.test",
    "https://user:password@nsp.example.test",
    "https://nsp.example.test/api",
    "https://nsp.example.test/?token=secret",
    "https://nsp.example.test/#secret",
  ])("rejects ambiguous or insecure API endpoint %s", (url) => {
    expect(() => normalizeNspApiUrl(url)).toThrow();
  });

  it.each(
    [[], ["https://broker:9193"], ["broker:0"], ["broker:65536"], ["user@broker:9193"]].map(
      (brokers) => ({ brokers }),
    ),
  )("rejects invalid broker overrides %j", ({ brokers }) => {
    expect(() => parseNspConnectInput({ ...credentials, brokers })).toThrow();
  });

  it("rejects omitted verification and unexpected connection payload fields", () => {
    expect(() => parseNspConnectInput({ ...credentials, verifyCertificate: undefined })).toThrow();
    expect(() => parseNspConnectInput({ ...credentials, workflow: "untrusted-script" })).toThrow();
  });

  it("admits only safe result and progress shapes", () => {
    expect(parseNspResult({ ok: true, profileId: "profile-1" })).toEqual({
      ok: true,
      profileId: "profile-1",
    });
    expect(() =>
      parseNspResult({ ok: true, profileId: "profile-1", truststore: "secret" }),
    ).toThrow();
    expect(() =>
      parseNspResult({ ok: true, status: { state: "idle", password: "secret" } }),
    ).toThrow();
    expect(() =>
      parseNspProgress({
        requestId: "request-1",
        step: "retrieve",
        message: "Retrieved credentials",
        password: "secret",
      }),
    ).toThrow();
    expect(() =>
      parseNspProgress({ requestId: "request-1", step: "eval", message: "Unexpected" }),
    ).toThrow();
  });

  it("retains non-secret ownership metadata and rejects foreign or credential-bearing sources", () => {
    const source = {
      apiUrl: "https://nsp.example.test",
      brokers: ["broker:9193"],
      workflowName: "streamskopeNspCapture",
      authentication: "tls" as const,
    };
    const envelope = toPluginProfileSource(source);
    expect(fromPluginProfileSource(envelope)).toEqual(source);
    expect(fromPluginProfileSource({ ...envelope, pluginId: "streamskope.eda" })).toBeUndefined();
    expect(
      fromPluginProfileSource({ ...envelope, data: { ...envelope.data, password: "secret" } }),
    ).toBeUndefined();
    expect(
      fromPluginProfileSource({
        ...envelope,
        data: { ...envelope.data, authentication: "unknown" },
      }),
    ).toBeUndefined();
  });
});
