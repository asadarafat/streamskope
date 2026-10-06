import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  parseHostCommand,
  parseHostCommandResponse,
} from "../../src/features/kafka/contracts";

const manifest = {
  id: "example.capture",
  name: "Example capture",
  version: "1.0.0",
  apiVersion: 2,
  backend: "backend.cjs",
  renderer: "renderer.js",
};
const reference = { pluginId: manifest.id, version: manifest.version, sha256: "a".repeat(64) };
const publisher = { keyId: "streamskope-publisher", name: "StreamSkope" };
const inspection = {
  candidateId: "host-owned-candidate",
  manifest,
  sha256: reference.sha256,
  source: "file",
  publisher,
  trust: "publisher",
  expiresAt: "2026-10-06T12:05:00.000Z",
  status: "install",
};
const cachedPackage = {
  manifest,
  sha256: reference.sha256,
  cachedAt: "2026-10-06T12:00:00.000Z",
  trust: "official",
};

function command(name: string, payload: unknown): unknown {
  return { command: name, id: "package-request", version: HOST_PROTOCOL_VERSION, payload };
}
function response(name: string, result: Record<string, unknown>): unknown {
  return {
    command: name,
    id: "package-request",
    version: HOST_PROTOCOL_VERSION,
    ok: true,
    result: { correlationId: "package-correlation", ...result },
  };
}

describe("host-owned plugin package delivery contract", () => {
  it("accepts only native selection or a digest-pinned catalog/cache reference", () => {
    for (const payload of [
      { source: "file" },
      { source: "catalog", ...reference },
      { source: "cache", ...reference },
    ]) {
      const input = command("plugins.package.inspect", payload);
      expect(parseHostCommand(input)).toEqual(input);
    }
    for (const payload of [
      { source: "file", path: "/tmp/plugin.skope-plugin" },
      { source: "file", bytes: "untrusted" },
      { source: "file", trust: "publisher" },
      { source: "file", ...reference },
      { source: "catalog", ...reference, url: "https://untrusted.test/plugin" },
      { source: "cache", pluginId: manifest.id, version: manifest.version },
      { source: "catalog", ...reference, sha256: "A".repeat(64) },
      { source: "catalog", ...reference, sha256: `${"a".repeat(64)}\n` },
      { source: "catalog", ...reference, version: "latest" },
      { source: "cache", ...reference, version: "1.0" },
      { source: "cache", ...reference, pluginId: "../plugin" },
      { source: "download", ...reference },
    ]) {
      expect(() => parseHostCommand(command("plugins.package.inspect", payload))).toThrow(
        HostContractValidationError,
      );
    }
  });

  it("bounds opaque candidates and consent without accepting a renderer-chosen installation", () => {
    for (const name of [
      "plugins.package.change.prepare",
      "plugins.package.discard",
      "plugins.package.install",
    ]) {
      const input = command(name, { candidateId: inspection.candidateId });
      expect(parseHostCommand(input)).toEqual(input);
      for (const candidateId of ["", "x".repeat(129), 4]) {
        expect(() => parseHostCommand(command(name, { candidateId }))).toThrow(
          HostContractValidationError,
        );
      }
      expect(() =>
        parseHostCommand(command(name, { candidateId: inspection.candidateId, manifest })),
      ).toThrow(HostContractValidationError);
    }
    const install = command("plugins.package.install", {
      candidateId: inspection.candidateId,
      confirmationToken: "host-consent",
    });
    expect(parseHostCommand(install)).toEqual(install);
    for (const confirmationToken of ["", "x".repeat(129), true]) {
      expect(() =>
        parseHostCommand(
          command("plugins.package.install", {
            candidateId: inspection.candidateId,
            confirmationToken,
          }),
        ),
      ).toThrow(HostContractValidationError);
    }
    const delivery = command("plugins.delivery", {});
    expect(parseHostCommand(delivery)).toEqual(delivery);
    expect(() => parseHostCommand(command("plugins.delivery", { path: "/tmp" }))).toThrow(
      HostContractValidationError,
    );
  });

  it("returns cancellation or bounded inspection metadata with explicit verified provenance", () => {
    for (const pluginPackage of [
      null,
      inspection,
      { ...inspection, source: "cache", installedVersion: "0.9.0", status: "update" },
      { ...inspection, status: "blocked", reason: "This package requires a newer desktop." },
      {
        candidateId: inspection.candidateId,
        manifest,
        sha256: inspection.sha256,
        source: "catalog",
        trust: "development",
        expiresAt: inspection.expiresAt,
        status: "install",
      },
    ]) {
      const input = response("plugins.package.inspect", { pluginPackage });
      expect(parseHostCommandResponse(input)).toEqual(input);
    }
    for (const changed of [
      { path: "/tmp/plugin.skope-plugin" },
      { bytes: "base64-code" },
      { trust: "official", publisher: undefined },
      { publisher: undefined },
      { publisher: { ...publisher, keyId: "publisher\n" } },
      { publisher: { ...publisher, privateKey: "secret" } },
      { expiresAt: "2026-10-06" },
      { expiresAt: "2026-02-31T12:05:00.000Z" },
      { expiresAt: "2026-10-06T14:05:00.000+02:00" },
      { sha256: "wrong" },
      { installedVersion: "latest" },
      { status: "ready" },
      { status: "blocked" },
      { status: "blocked", reason: "" },
      { reason: "x".repeat(4097) },
      { candidateId: "x".repeat(129) },
    ]) {
      expect(() =>
        parseHostCommandResponse(
          response("plugins.package.inspect", { pluginPackage: { ...inspection, ...changed } }),
        ),
      ).toThrow(HostContractValidationError);
    }
  });

  it("bounds the verified package cache independently from the online catalog", () => {
    const pluginDelivery = {
      fileInstallationAvailable: true,
      cachedPackages: [
        cachedPackage,
        {
          ...cachedPackage,
          manifest: { ...manifest, version: "1.1.0" },
          trust: "publisher",
          publisher,
        },
      ],
    };
    const input = response("plugins.delivery", { pluginDelivery });
    expect(parseHostCommandResponse(input)).toEqual(input);
    for (const changed of [
      { fileInstallationAvailable: "true" },
      { cachedPackages: Array.from({ length: 5 }, () => cachedPackage) },
      { cachedPackages: [cachedPackage, cachedPackage] },
      { cachedPackages: [{ ...cachedPackage, sha256: "a".repeat(63) }] },
      { cachedPackages: [{ ...cachedPackage, cachedAt: "yesterday" }] },
      { cachedPackages: [{ ...cachedPackage, trust: "publisher" }] },
      { cachedPackages: [{ ...cachedPackage, publisher }] },
      { cachedPackages: [{ ...cachedPackage, filename: "/tmp/package" }] },
    ]) {
      expect(() =>
        parseHostCommandResponse(
          response("plugins.delivery", { pluginDelivery: { ...pluginDelivery, ...changed } }),
        ),
      ).toThrow(HostContractValidationError);
    }
  });

  it("publishes only distinct digest references for the declared catalog manifests", () => {
    const pluginCatalog = { plugins: [manifest], packages: [reference] };
    const input = response("plugins.catalog", { pluginCatalog });
    expect(parseHostCommandResponse(input)).toEqual(input);
    for (const packages of [
      [reference, reference],
      Array.from({ length: 3 }, () => reference),
      [{ ...reference, version: "1.1.0" }],
      [{ ...reference, pluginId: "unknown.capture" }],
      [{ ...reference, sha256: "X".repeat(64) }],
      [{ ...reference, url: "https://untrusted.test" }],
    ]) {
      expect(() =>
        parseHostCommandResponse(
          response("plugins.catalog", { pluginCatalog: { ...pluginCatalog, packages } }),
        ),
      ).toThrow(HostContractValidationError);
    }
  });

  it("returns lifecycle consent, installed state and a discard acknowledgement", () => {
    const prompt = {
      pluginId: manifest.id,
      token: "one-use-consent",
      title: "Update?",
      message: "Capture is running.",
      detail: "Stop capture before updating.",
      confirmLabel: "Stop and update",
    };
    const replies = [
      response("plugins.package.change.prepare", { pluginChange: null }),
      response("plugins.package.change.prepare", { pluginChange: prompt }),
      response("plugins.package.install", { pluginSnapshot: { revision: 1, plugins: [] } }),
      response("plugins.package.discard", {}),
    ];
    for (const input of replies) expect(parseHostCommandResponse(input)).toEqual(input);
    expect(() =>
      parseHostCommandResponse(response("plugins.package.discard", { pluginPackage: inspection })),
    ).toThrow(HostContractValidationError);
  });
});
