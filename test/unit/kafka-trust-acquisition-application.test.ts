import { describe, expect, it, vi } from "vitest";

import {
  PROFILE_LIMITS,
  REMOTE_TRUST_ACQUISITION_LIMITS,
  type RemoteTrustAcquisitionSummary,
} from "../../src/kafka/contracts";
import { createHarness, deferred, target } from "../support/trust-acquisition-application-fixture";
import { trustRecipeInput } from "../support/trust-recipe";

describe("Kafka trust-acquisition application service", () => {
  it("explains empty command stdout without exposing the command or consuming prior trust", async () => {
    const harness = createHarness();
    const prior = await harness.service.fetchMaterial({ kind: "jks", label: "old", target });
    const catalog = await harness.templates.recipes.create({
      ...trustRecipeInput(),
      kind: "jks",
      parameters: [],
      ssh: {
        source: "stdout",
        value: "cat private-source > {truststorePath}",
        password: { source: "ask" },
      },
    });
    const recipe = catalog.recipes.find((entry) => entry.name === "Certificate file")!;
    harness.remote.materialResult = new Uint8Array();
    const input = {
      kind: "jks" as const,
      label: "new",
      target,
      truststorePassword: "supplied-secret",
      recipe: {
        mode: "replace" as const,
        recipeId: recipe.id,
        recipeRevision: recipe.revision,
        overrides: {},
      },
    };
    await expect(harness.service.fetchMaterial(input)).rejects.toMatchObject({
      code: "TRUST_MATERIAL",
      message:
        "The material command completed but returned no certificate or truststore bytes on stdout.",
      target: "kafka-lab.example.test:22",
      recovery:
        "Return raw certificate or truststore bytes on stdout. Do not redirect output to a file. For an existing remote file, select Remote file.",
    });
    expect(harness.service.resolve(prior.id, "jks").material).toBe("AQID");
    expect(harness.decode).toHaveBeenCalledTimes(1);
  });
  it.each(["jks", "pkcs12"] as const)(
    "uses an ephemeral supplied %s password without a password command",
    async (kind) => {
      const harness = createHarness();
      const catalog = await harness.templates.recipes.create({
        ...trustRecipeInput(),
        kind,
        parameters: [],
        ssh: { source: "stdout", value: "read-trust", password: { source: "ask" } },
      });
      const recipe = catalog.recipes.find((entry) => entry.name === "Certificate file")!;
      const result = await harness.service.fetchMaterial({
        kind,
        label: "trust",
        target,
        truststorePassword: " supplied secret ",
        recipe: {
          mode: "replace",
          recipeId: recipe.id,
          recipeRevision: recipe.revision,
          overrides: {},
        },
      });
      expect(harness.remote.passwordCalls).toEqual([]);
      expect(harness.decode).toHaveBeenCalledWith(
        expect.objectContaining({ password: " supplied secret " }),
        expect.any(AbortSignal),
      );
      expect(JSON.stringify(result)).not.toContain("supplied secret");
      expect(result.password.present).toBe(true);
    },
  );

  it.each([undefined, "", "x".repeat(4097)])(
    "rejects a missing, empty or oversized supplied password before transfer",
    async (password) => {
      const harness = createHarness();
      const catalog = await harness.templates.recipes.create({
        ...trustRecipeInput(),
        kind: "jks",
        parameters: [],
        ssh: { source: "stdout", value: "read-trust", password: { source: "ask" } },
      });
      const recipe = catalog.recipes.find((entry) => entry.name === "Certificate file")!;
      await expect(
        harness.service.fetchMaterial({
          kind: "jks",
          label: "trust",
          target,
          ...(password === undefined ? {} : { truststorePassword: password }),
          recipe: {
            mode: "replace",
            recipeId: recipe.id,
            recipeRevision: recipe.revision,
            overrides: {},
          },
        }),
      ).rejects.toMatchObject({ code: "TRUSTSTORE_PASSWORD" });
      expect(harness.remote.passwordCalls).toEqual([]);
      expect(harness.remote.materialCalls).toEqual([]);
      expect(() => harness.service.resolve("acquisition-1", "jks")).toThrow();
    },
  );

  it("rejects reusing an older candidate as a recipe password source", async () => {
    const harness = createHarness();
    const prior = await harness.service.fetchMaterial({ kind: "jks", label: "old", target });
    const catalog = await harness.templates.recipes.create({
      ...trustRecipeInput(),
      kind: "jks",
      parameters: [],
      ssh: { source: "stdout", value: "read-trust", password: { source: "ask" } },
    });
    const recipe = catalog.recipes.find((entry) => entry.name === "Certificate file")!;
    await expect(
      harness.service.fetchMaterial({
        acquisitionId: prior.id,
        kind: "jks",
        label: "new",
        target,
        truststorePassword: "new",
        recipe: {
          mode: "replace",
          recipeId: recipe.id,
          recipeRevision: recipe.revision,
          overrides: {},
        },
      }),
    ).rejects.toMatchObject({ code: "VALIDATION" });
    expect(harness.remote.materialCalls).toHaveLength(1);
    expect(harness.service.resolve(prior.id, "jks").password).toBe("remote-store-password");
  });
  it("evaluates certificate validity when evidence is returned, not when retrieval started", async () => {
    const harness = createHarness();
    vi.mocked(harness.decode).mockImplementation(async () => {
      harness.advance(1_000);
      await Promise.resolve();
      return {
        kind: "jks",
        caPem: "decoded-ca",
        evidence: {
          count: 1,
          truncated: false,
          certificates: [
            {
              subject: "CN=Expiring",
              issuer: "CN=Fixture",
              validFrom: "2020-01-01T00:00:00.000Z",
              validTo: "2026-07-26T13:00:00.500Z",
              fingerprint: Array(32).fill("AA").join(":"),
              truncated: false,
            },
          ],
        },
      };
    });
    const result = await harness.service.fetchMaterial({
      kind: "jks",
      label: "expiring.jks",
      target,
    });
    expect(result.material?.expiredCertificates).toBe(true);
  });
  it("executes a converted legacy recipe without changing its temporary-file placeholders", async () => {
    const harness = createHarness();
    const catalog = await harness.templates.recipes.create({
      ...trustRecipeInput(),
      kind: "jks",
      syntax: "legacy-v1",
      parameters: [],
      ssh: {
        source: "legacy-tempfile",
        value: "copy --password {storepass} --directory {destDir} source {truststorePath}",
        password: { source: "command", command: "fetch-password --quiet" },
      },
    });
    const recipe = catalog.recipes[0];
    if (recipe === undefined) throw new Error("Missing fixture recipe");
    const result = await harness.service.fetchMaterial({
      kind: "jks",
      label: "legacy.jks",
      target,
      recipe: {
        mode: "replace",
        recipeId: recipe.id,
        recipeRevision: recipe.revision,
        overrides: {},
      },
    });
    expect(harness.remote.materialCalls).toEqual([
      {
        command:
          "copy --password 'remote-store-password' --directory '/tmp' source '/tmp/streamskope-2.trust'",
        maximumBytes: 8 * 1_048_576,
        remotePath: "/tmp/streamskope-2.trust",
        target,
      },
    ]);
    expect(result.password.present).toBe(true);
    expect(JSON.stringify(result)).not.toContain("remote-store-password");
  });
  it.each(["missing", "unknown", "undeclared"] as const)(
    "rejects %s recipe inputs before remote work",
    async (failure) => {
      const harness = createHarness();
      const catalog = await harness.templates.recipes.create(trustRecipeInput());
      const recipe = catalog.recipes[0];
      if (recipe === undefined) throw new Error("Missing fixture recipe");
      await expect(
        harness.service.fetchMaterial({
          kind: "pem",
          label: "ca.pem",
          target,
          recipe: {
            mode: "replace",
            recipeId: failure === "unknown" ? "missing-recipe" : recipe.id,
            recipeRevision: recipe.revision,
            overrides:
              failure === "undeclared" ? { certificate_path: "/ca.pem", injected: "value" } : {},
          },
        }),
      ).rejects.toThrow();
      expect(harness.remote.discoveryCalls).toEqual([]);
      expect(harness.remote.passwordCalls).toEqual([]);
      expect(harness.remote.materialCalls).toEqual([]);
      expect(harness.decode).not.toHaveBeenCalled();
    },
  );
  it("reports whole-bundle expired and future dates even when those entries are not displayed", async () => {
    const harness = createHarness();
    const certificates = Array.from({ length: 16 }, () => ({
      subject: "Visible",
      issuer: "Fixture",
      validFrom: "2026-01-01T00:00:00.000Z",
      validTo: "2027-01-01T00:00:00.000Z",
      fingerprint: Array(32).fill("AA").join(":"),
      truncated: false,
    }));
    vi.mocked(harness.decode).mockResolvedValue({
      kind: "jks",
      caPem: "decoded-ca",
      evidence: {
        count: 18,
        truncated: true,
        certificates,
        validity: {
          earliestExpiry: "2020-05-30T15:10:39.000Z",
          latestStart: "2099-01-01T00:00:00.000Z",
        },
      },
    });
    const result = await harness.service.fetchMaterial({
      kind: "jks",
      label: "bundle.jks",
      target,
    });
    expect(result.material).toMatchObject({
      expiredCertificates: true,
      notYetValidCertificates: true,
    });
  });
  it("cancels discovery by request identity before an acquisition result exists", async () => {
    const harness = createHarness();
    const entered = deferred<AbortSignal | undefined>();
    const release = deferred<string>();
    vi.spyOn(harness.remote, "discoverHostKey").mockImplementation((_request, signal) => {
      entered.resolve(signal);
      return release.promise;
    });
    const work = harness.service.discoverHostKey(
      { target: { host: target.host, port: 22 } },
      undefined,
      "discovery-request",
    );
    const signal = await entered.promise;
    harness.service.cancel("discovery-request");
    expect(signal?.aborted).toBe(true);
    release.resolve(target.hostKeyFingerprint);
    await expect(work).rejects.toMatchObject({ code: "CANCELLED" });
    expect(harness.remote.passwordCalls).toEqual([]);
    expect(harness.remote.materialCalls).toEqual([]);
    expect(() => harness.service.cancel("discovery-request")).not.toThrow();
  });

  it("cancels only the pending request and preserves previously acquired trust", async () => {
    const harness = createHarness();
    const previous = await harness.service.fetchMaterial({ kind: "jks", label: "old.jks", target });
    const entered = deferred<AbortSignal | undefined>();
    const release = deferred<Uint8Array>();
    vi.spyOn(harness.remote, "fetchMaterial").mockImplementation((_request, signal) => {
      entered.resolve(signal);
      return release.promise;
    });
    const work = harness.service.fetchMaterial(
      { kind: "jks", label: "new.jks", target },
      undefined,
      "replacement-request",
    );
    const signal = await entered.promise;
    harness.service.cancel("unknown-request");
    expect(signal?.aborted).toBe(false);
    await expect(
      harness.service.fetchMaterial(
        { kind: "jks", label: "duplicate.jks", target },
        undefined,
        "replacement-request",
      ),
    ).rejects.toThrow("already");
    harness.service.cancel("replacement-request");
    release.resolve(new Uint8Array([4, 5, 6]));
    await expect(work).rejects.toMatchObject({ code: "CANCELLED" });
    expect(harness.service.resolve(previous.id, "jks").material).toBe("AQID");
    expect(harness.decode).toHaveBeenCalledTimes(1);
  });

  it("shares one recipe deadline across password and transfer and clears its timer", async () => {
    vi.useFakeTimers();
    try {
      const harness = createHarness();
      const catalog = await harness.templates.recipes.create({
        ...trustRecipeInput(),
        kind: "jks",
        timeoutSeconds: 1,
        ssh: {
          source: "file",
          value: "{{certificate_path}}",
          password: { source: "command", command: "password-command" },
        },
      });
      const recipe = catalog.recipes[0];
      if (recipe === undefined) throw new Error("Missing recipe");
      const started = deferred<void>();
      vi.spyOn(harness.remote, "fetchPassword").mockImplementation(async () => {
        started.resolve();
        await new Promise<void>((resolve) => {
          setTimeout(resolve, 600);
        });
        return "password\n";
      });
      vi.spyOn(harness.remote, "fetchMaterial").mockImplementation(
        (_request, signal) =>
          new Promise((_resolve, reject) => {
            signal?.addEventListener(
              "abort",
              () =>
                reject(
                  signal.reason instanceof Error ? signal.reason : new Error("Acquisition aborted"),
                ),
              { once: true },
            );
          }),
      );
      const result = harness.service.fetchMaterial({
        kind: "jks",
        label: "remote.jks",
        target,
        recipe: {
          mode: "replace",
          recipeId: recipe.id,
          recipeRevision: recipe.revision,
          overrides: { certificate_path: "/ca" },
        },
      });
      const rejected = expect(result).rejects.toMatchObject({ code: "TIMEOUT" });
      await started.promise;
      await vi.advanceTimersByTimeAsync(1_000);
      await rejected;
      expect(harness.decode).not.toHaveBeenCalled();
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });
  it("executes the exact generic recipe with a supplied binary password in one operation", async () => {
    const harness = createHarness();
    const catalog = await harness.templates.recipes.create({
      ...trustRecipeInput(),
      kind: "jks",
      ssh: { source: "file", value: "{{certificate_path}}", password: { source: "ask" } },
    });
    const recipe = catalog.recipes[0];
    if (recipe === undefined) throw new Error("Missing recipe");
    const result = await harness.service.fetchMaterial({
      kind: "jks",
      label: "remote.jks",
      target,
      recipe: {
        mode: "replace",
        recipeId: recipe.id,
        recipeRevision: recipe.revision,
        overrides: { certificate_path: "/private/source.jks" },
      },
      truststorePassword: " supplied password ",
    });
    expect(harness.remote.passwordCalls).toEqual([]);
    expect(harness.remote.materialCalls).toEqual([
      {
        source: "file",
        remotePath: "/private/source.jks",
        maximumBytes: PROFILE_LIMITS.trustBinaryBytes,
        target,
      },
    ]);
    expect(harness.decode).toHaveBeenCalledWith(
      { kind: "jks", material: "AQID", password: " supplied password " },
      expect.any(AbortSignal),
    );
    expect(harness.service.resolve(result.id, "jks").password).toBe(" supplied password ");
    expect(JSON.stringify(result)).not.toContain(" supplied password ");
  });

  it("rejects a stale generic recipe before authenticating instead of using legacy selections", async () => {
    const harness = createHarness();
    const catalog = await harness.templates.recipes.create(trustRecipeInput());
    const recipe = catalog.recipes[0];
    if (recipe === undefined) throw new Error("Missing recipe");
    await harness.templates.recipes.update(recipe.id, recipe.revision, {
      ...trustRecipeInput(),
      name: "Changed",
    });
    await expect(
      harness.service.fetchMaterial({
        kind: "pem",
        label: "remote.pem",
        target,
        recipe: {
          mode: "replace",
          recipeId: recipe.id,
          recipeRevision: recipe.revision,
          overrides: { certificate_path: "/ca" },
        },
      }),
    ).rejects.toMatchObject({ code: "VALIDATION" });
    expect(harness.remote.materialCalls).toEqual([]);
    expect(harness.remote.passwordCalls).toEqual([]);
  });
  it("counts pending work toward capacity before starting a ninth remote request", async () => {
    const { remote, service } = createHarness();
    const output = deferred<string>();
    remote.fetchPassword = (request): Promise<string> => {
      remote.passwordCalls.push(request);
      return output.promise;
    };
    const pending = Array.from({ length: 8 }, () => service.fetchPassword({ target }));
    try {
      await expect(service.fetchPassword({ target })).rejects.toMatchObject({
        code: "ACQUISITION_CAPACITY",
      });
    } finally {
      output.resolve("store-password");
      await Promise.all(pending);
    }
    expect(remote.passwordCalls).toHaveLength(8);
  });

  it("runs independent work without waiting for a blocked request and propagates cancellation", async () => {
    const { remote, service } = createHarness();
    const started = deferred<void>();
    const controller = new AbortController();
    remote.fetchPassword = (request, signal): Promise<string> => {
      if (request.target.host === "second.example.test") return Promise.resolve("second-password");
      return new Promise((_resolve, reject) => {
        signal?.addEventListener(
          "abort",
          () => reject(new DOMException("Cancelled", "AbortError")),
          { once: true },
        );
        started.resolve();
      });
    };
    const first = service.fetchPassword({ target }, controller.signal);
    const rejected = expect(first).rejects.toMatchObject({ name: "AbortError" });
    await started.promise;
    try {
      await expect(
        service.fetchPassword({ target: { ...target, host: "second.example.test" } }),
      ).resolves.toMatchObject({ password: { present: true } });
    } finally {
      controller.abort();
      await rejected;
    }
  });

  it("rejects concurrent replacement of one candidate without changing its retained trust", async () => {
    const { remote, service } = createHarness();
    const candidate = await service.fetchMaterial({ kind: "pem", label: "original.pem", target });
    const started = deferred<void>();
    const output = deferred<Uint8Array>();
    remote.fetchMaterial = (): Promise<Uint8Array> => {
      started.resolve();
      return output.promise;
    };
    const input = {
      acquisitionId: candidate.id,
      kind: "pem",
      label: "replacement.pem",
      target,
    } as const;
    const first = service.fetchMaterial(input);
    await started.promise;
    try {
      await expect(service.fetchMaterial(input)).rejects.toMatchObject({ code: "VALIDATION" });
      expect(service.resolve(candidate.id, "pem")).toMatchObject({ label: "original.pem" });
    } finally {
      output.resolve(new Uint8Array([65]));
      await first;
    }
  });

  it("does not publish remote work completed after shutdown clears the service", async () => {
    const { remote, service } = createHarness();
    const started = deferred<void>();
    const output = deferred<string>();
    remote.fetchPassword = (): Promise<string> => {
      started.resolve();
      return output.promise;
    };
    const pending = service.fetchPassword({ target });
    await started.promise;
    service.clear();
    output.resolve("late-secret");
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(() => service.resolve("acquisition-1", "jks")).toThrow();
  });

  it("releases failed reservations and permits retry without evicting retained candidates", async () => {
    const { remote, service } = createHarness();
    const retained = await service.fetchMaterial({ kind: "pem", label: "retained.pem", target });
    remote.passwordRejection = new Error("fixture failure");
    for (let index = 0; index < 9; index += 1) {
      await expect(service.fetchPassword({ target })).rejects.toThrow("fixture failure");
    }
    remote.passwordRejection = undefined;
    await expect(service.fetchPassword({ target })).resolves.toMatchObject({
      password: { present: true },
    });
    expect(service.resolve(retained.id, "pem")).toMatchObject({ label: "retained.pem" });
  });

  it("keeps prior complete trust when a replacement is cancelled", async () => {
    const { remote, service } = createHarness();
    const retained = await service.fetchMaterial({ kind: "pem", label: "retained.pem", target });
    const started = deferred<void>();
    const output = deferred<Uint8Array>();
    const controller = new AbortController();
    remote.fetchMaterial = (): Promise<Uint8Array> => {
      started.resolve();
      return output.promise;
    };
    const pending = service.fetchMaterial(
      { acquisitionId: retained.id, kind: "pem", label: "replacement.pem", target },
      controller.signal,
    );
    await started.promise;
    controller.abort();
    output.resolve(new Uint8Array([65]));
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(service.resolve(retained.id, "pem")).toMatchObject({ label: "retained.pem" });
  });

  it("does not resurrect a discarded candidate after a late replacement", async () => {
    const { remote, service } = createHarness();
    const candidate = await service.fetchPassword({ target });
    const started = deferred<void>();
    const output = deferred<Uint8Array>();
    remote.fetchMaterial = (): Promise<Uint8Array> => {
      started.resolve();
      return output.promise;
    };
    const pending = service.fetchMaterial({
      acquisitionId: candidate.id,
      kind: "jks",
      label: "late.jks",
      target,
    });
    await started.promise;
    service.discard(candidate.id);
    output.resolve(new Uint8Array([1, 2, 3]));
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(() => service.resolve(candidate.id, "jks")).toThrow();
  });

  it("starts candidate lifetime at reservation rather than successful transfer", async () => {
    const { advance, remote, service } = createHarness();
    remote.fetchPassword = (): Promise<string> => {
      advance(REMOTE_TRUST_ACQUISITION_LIMITS.ttlMs + 1);
      return Promise.resolve("late-secret");
    };
    await expect(service.fetchPassword({ target })).rejects.toMatchObject({
      code: "ACQUISITION_EXPIRED",
    });
  });

  it("allows repeated discard without affecting another candidate", async () => {
    const { service } = createHarness();
    const first = await service.fetchMaterial({ kind: "pem", label: "first.pem", target });
    const second = await service.fetchMaterial({ kind: "pem", label: "second.pem", target });
    service.discard(first.id);
    expect(() => service.discard(first.id)).not.toThrow();
    expect(service.resolve(second.id, "pem")).toMatchObject({ label: "second.pem" });
  });

  it("returns only the automatically discovered endpoint identity", async () => {
    const { remote, service } = createHarness();

    await expect(
      service.discoverHostKey({
        target: {
          host: target.host,
          port: target.port,
        },
      }),
    ).resolves.toEqual({
      fingerprint: target.hostKeyFingerprint,
      target: {
        host: target.host,
        port: target.port,
      },
    });
    expect(remote.discoveryCalls).toEqual([
      {
        target: {
          host: target.host,
          port: target.port,
        },
      },
    ]);
  });

  it("snapshots only the selected password template and returns aggregate metadata", async () => {
    const { remote, service } = createHarness();

    const summary = await service.fetchPassword({ target });

    expect(remote.passwordCalls).toEqual([
      {
        command: "fetch-password --quiet",
        target,
      },
    ]);
    expect(summary).toEqual({
      createdAt: "2026-07-26T13:00:00.000Z",
      expiresAt: "2026-07-26T13:10:00.000Z",
      id: "acquisition-1",
      material: null,
      password: {
        present: true,
        templateName: "Remote password",
      },
      target: {
        host: "kafka-lab.example.test",
        hostKeyFingerprint: target.hostKeyFingerprint,
        port: 22,
      },
    });
    expect(JSON.stringify(summary)).not.toContain("remote-store-password");
    expect(JSON.stringify(summary)).not.toContain("ssh-password");
    expect(JSON.stringify(summary)).not.toContain("fetch-password");
  });

  it("acquires bounded binary material and its host-owned password in one direct operation", async () => {
    const { decode, remote, service } = createHarness();

    const summary = await service.fetchMaterial({
      kind: "jks",
      label: "nsp.truststore",
      target,
    });

    expect(remote.passwordCalls).toEqual([
      {
        command: "fetch-password --quiet",
        target,
      },
    ]);
    expect(remote.materialCalls).toHaveLength(1);
    expect(remote.materialCalls[0]).toMatchObject({
      maximumBytes: PROFILE_LIMITS.trustBinaryBytes,
      remotePath: "/tmp/streamskope-2.trust",
      target,
    });
    expect(remote.materialCalls[0]?.command).toBe(
      "copy --password 'remote-store-password' --directory '/tmp' source '/tmp/streamskope-2.trust'",
    );
    expect(decode).toHaveBeenCalledWith(
      {
        kind: "jks",
        material: "AQID",
        password: "remote-store-password",
      },
      expect.objectContaining({ aborted: false }),
    );
    expect(summary.material).toEqual({
      byteCount: 3,
      kind: "jks",
      label: "nsp.truststore",
      templateName: "Remote trust",
    });
    expect(summary.password).toEqual({
      present: true,
      templateName: "Remote password",
    });

    expect(service.resolve(summary.id, "jks")).toEqual({
      caPem: "decoded-jks-ca",
      id: "acquisition-1",
      kind: "jks",
      label: "nsp.truststore",
      material: "AQID",
      password: "remote-store-password",
    });
    expect(service.resolve(summary.id, "jks")).toBeDefined();
    service.consume(summary.id);
    expect(() => service.resolve(summary.id, "jks")).toThrow(
      expect.objectContaining({ code: "ACQUISITION_NOT_FOUND", stage: "acquisition" }),
    );
  });

  it("does not start material retrieval when direct binary password acquisition fails", async () => {
    const { remote, service } = createHarness();
    remote.passwordResult = "\r\n";

    await expect(
      service.fetchMaterial({
        kind: "jks",
        label: "remote.jks",
        target,
      }),
    ).rejects.toMatchObject({
      code: "TRUSTSTORE_PASSWORD",
      stage: "trust",
    });

    expect(remote.passwordCalls).toHaveLength(1);
    expect(remote.materialCalls).toHaveLength(0);
  });

  it("removes a newly acquired password when direct binary material retrieval fails", async () => {
    const { remote, service } = createHarness();
    remote.materialRejection = Object.assign(new Error("raw remote output"), {
      code: "REMOTE_COMMAND",
      recovery: "Check the selected template and remote environment.",
      retryable: false,
      stage: "remote-command",
      target: "kafka-lab.example.test:22",
    });

    await expect(
      service.fetchMaterial({
        kind: "jks",
        label: "remote.jks",
        target,
      }),
    ).rejects.toMatchObject({ code: "REMOTE_COMMAND" });

    expect(remote.passwordCalls).toHaveLength(1);
    expect(remote.materialCalls).toHaveLength(1);
    expect(() => service.resolve("acquisition-1", "jks")).toThrow(
      expect.objectContaining({ code: "ACQUISITION_NOT_FOUND" }),
    );
  });

  it("supports direct PEM acquisition without inventing a password", async () => {
    const { decode, remote, service } = createHarness();
    remote.materialResult = new TextEncoder().encode(
      "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
    );

    const summary = await service.fetchMaterial({
      kind: "pem",
      label: "ca.pem",
      target,
    });

    expect(remote.passwordCalls).toHaveLength(0);
    expect(summary.password).toEqual({ present: false, templateName: null });
    expect(decode).toHaveBeenCalledWith(
      {
        kind: "pem",
        material: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
      },
      expect.objectContaining({ aborted: false }),
    );
  });

  it("rejects capacity and expiry before starting additional remote work", async () => {
    const { advance, remote, service } = createHarness();
    const summaries: RemoteTrustAcquisitionSummary[] = [];
    for (let index = 0; index < REMOTE_TRUST_ACQUISITION_LIMITS.acquisitions; index += 1) {
      summaries.push(await service.fetchPassword({ target }));
    }
    await expect(service.fetchPassword({ target })).rejects.toMatchObject({
      code: "ACQUISITION_CAPACITY",
      stage: "acquisition",
    });
    expect(remote.passwordCalls).toHaveLength(REMOTE_TRUST_ACQUISITION_LIMITS.acquisitions);

    advance(REMOTE_TRUST_ACQUISITION_LIMITS.ttlMs + 1);
    expect(() => service.resolve(summaries[0]!.id, "jks")).toThrow(
      expect.objectContaining({ code: "ACQUISITION_EXPIRED", stage: "acquisition" }),
    );
    await expect(service.fetchPassword({ target })).resolves.toMatchObject({
      id: "acquisition-9",
    });
  });

  it("does not replace a complete acquisition after remote or validation failure", async () => {
    const { remote, service } = createHarness();
    const password = await service.fetchPassword({ target });
    await service.fetchMaterial({
      acquisitionId: password.id,
      kind: "jks",
      label: "original.jks",
      target,
    });
    remote.rejection = Object.assign(new Error("raw remote output"), {
      code: "REMOTE_COMMAND",
      recovery: "Check the selected template and remote environment.",
      retryable: false,
      stage: "remote-command",
      target: "kafka-lab.example.test:22",
    });

    await expect(
      service.fetchMaterial({
        acquisitionId: password.id,
        kind: "jks",
        label: "replacement.jks",
        target,
      }),
    ).rejects.toMatchObject({ code: "REMOTE_COMMAND" });
    expect(service.resolve(password.id, "jks")).toMatchObject({
      label: "original.jks",
      material: "AQID",
    });
  });

  it("rejects empty/oversized password output and target changes", async () => {
    const { remote, service } = createHarness();
    remote.passwordResult = "\r\n";
    await expect(service.fetchPassword({ target })).rejects.toMatchObject({
      code: "TRUSTSTORE_PASSWORD",
      stage: "trust",
    });
    remote.passwordResult = "x".repeat(PROFILE_LIMITS.clientSecretCharacters + 1);
    await expect(service.fetchPassword({ target })).rejects.toMatchObject({
      code: "TRUSTSTORE_PASSWORD",
      stage: "trust",
    });
    remote.passwordResult = "valid";
    const password = await service.fetchPassword({ target });

    await expect(
      service.fetchMaterial({
        acquisitionId: password.id,
        kind: "jks",
        label: "remote.jks",
        target: { ...target, host: "different.example.test" },
      }),
    ).rejects.toMatchObject({ code: "VALIDATION", stage: "validation" });
    await expect(
      service.fetchMaterial({
        acquisitionId: password.id,
        kind: "jks",
        label: "remote.jks",
        target: {
          ...target,
          hostKeyFingerprint: `SHA256:${"B".repeat(43)}`,
        },
      }),
    ).rejects.toMatchObject({ code: "VALIDATION", stage: "validation" });
    expect(remote.materialCalls).toHaveLength(0);
  });

  it("accepts a bounded multibyte password and preserves meaningful surrounding spaces", async () => {
    const { remote, service } = createHarness();
    remote.passwordResult = `\r\n ${"密".repeat(3000)} \r\n`;
    const acquisition = await service.fetchMaterial({ kind: "jks", label: "remote.jks", target });
    expect(service.resolve(acquisition.id, "jks").password).toBe(` ${"密".repeat(3000)} `);
    expect(JSON.stringify(acquisition)).not.toContain("密");
  });

  it("discards explicitly and clears every acquisition on shutdown", async () => {
    const { service } = createHarness();
    const first = await service.fetchPassword({ target });
    const second = await service.fetchPassword({ target });

    service.discard(first.id);
    expect(() => service.resolve(first.id, "jks")).toThrow(
      expect.objectContaining({ code: "ACQUISITION_NOT_FOUND" }),
    );
    service.clear();
    expect(() => service.resolve(second.id, "jks")).toThrow(
      expect.objectContaining({ code: "ACQUISITION_NOT_FOUND" }),
    );
  });
});
