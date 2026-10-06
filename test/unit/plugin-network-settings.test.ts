import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import { lstat, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import type {
  PluginNetworkConfiguration,
  PluginNetworkUpdateInput,
} from "../../src/plugins/contracts";
import type { ProfileProtector } from "../../src/platform/node/profile-protector";
import { PluginNetworkSettings } from "../../src/platform/node/plugins/network-settings";
import type {
  PluginNetworkTransport,
  PluginNetworkTransportConfiguration,
} from "../../src/platform/node/plugins/network-transport";

const roots: string[] = [];
const custom: PluginNetworkConfiguration = {
  mode: "custom",
  proxyUrl: "http://proxy.example:3128",
  offline: false,
};
const system: PluginNetworkConfiguration = { mode: "system", proxyUrl: null, offline: false };
const replace: PluginNetworkUpdateInput = {
  configuration: custom,
  credentials: { action: "replace", username: "private-user", password: "private-password" },
};
function protector(): ProfileProtector {
  const key = randomBytes(32);
  return {
    protect: (plaintext): Promise<Buffer> => {
      const iv = randomBytes(12);
      const cipher = createCipheriv("aes-256-gcm", key, iv);
      const encrypted = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()]);
      return Promise.resolve(Buffer.concat([iv, cipher.getAuthTag(), encrypted]));
    },
    unprotect: (value): ReturnType<ProfileProtector["unprotect"]> => {
      const decipher = createDecipheriv("aes-256-gcm", key, value.subarray(0, 12));
      decipher.setAuthTag(value.subarray(12, 28));
      return Promise.resolve({
        plaintext: Buffer.concat([decipher.update(value.subarray(28)), decipher.final()]).toString(
          "utf8",
        ),
        shouldReEncrypt: false,
      });
    },
  };
}
function transport(
  configure = vi.fn((_configuration: PluginNetworkTransportConfiguration): Promise<void> =>
    Promise.resolve(),
  ),
): PluginNetworkTransport {
  return {
    nativeAvailable: true,
    supportedProxyProtocols: ["http", "https"],
    fetch: vi.fn<typeof fetch>(() => Promise.reject(new Error("Unexpected network request"))),
    configure,
    close: (): Promise<void> => Promise.resolve(),
  };
}
async function setup(
  options: {
    readonly protection?: ProfileProtector;
    readonly native?: PluginNetworkTransport;
  } = {},
): Promise<{
  root: string;
  path: string;
  settings: PluginNetworkSettings;
  changing: ReturnType<typeof vi.fn>;
}> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-plugin-network-"));
  roots.push(root);
  const path = join(root, "private", "network.json");
  const changing = vi.fn();
  return {
    root,
    path,
    changing,
    settings: new PluginNetworkSettings({
      path,
      changing,
      ...(options.protection === undefined ? {} : { protector: options.protection }),
      ...(options.native === undefined ? {} : { transport: options.native }),
    }),
  };
}
afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("private plugin download settings", () => {
  it("protects credentials at rest and keeps them out of snapshots while restoring the dedicated transport", async () => {
    const protection = protector();
    const native = transport();
    const { path, settings } = await setup({ native, protection });
    const saved = await settings.update(replace);
    expect(saved).toMatchObject({
      configuration: custom,
      credentialsConfigured: true,
      credentialStorage: "encrypted",
    });
    const stored = await readFile(path, "utf8");
    expect(stored).not.toContain("private-user");
    expect(stored).not.toContain("private-password");
    expect(JSON.stringify(saved)).not.toContain("private-user");
    expect(JSON.stringify(saved)).not.toContain("private-password");
    expect((await lstat(path)).mode & 0o777).toBe(0o600);
    expect((await lstat(join(path, ".."))).mode & 0o777).toBe(0o700);
    const configured = vi.fn((_configuration: PluginNetworkTransportConfiguration): Promise<void> =>
      Promise.resolve(),
    );
    const reopened = new PluginNetworkSettings({
      path,
      transport: transport(configured),
      protector: protection,
      changing: (): void => undefined,
    });
    expect(await reopened.snapshot()).toEqual(saved);
    expect(configured).toHaveBeenLastCalledWith({
      mode: "custom",
      proxyUrl: custom.proxyUrl,
      credentials: { username: "private-user", password: "private-password" },
    });
  });
  it("keeps unsupported secure-storage credentials only in this session, including an encryption failure", async () => {
    const { path, settings } = await setup({ native: transport() });
    expect((await settings.update(replace)).credentialStorage).toBe("session");
    const stored = await readFile(path, "utf8");
    expect(stored).not.toContain("private-user");
    expect(stored).not.toContain("private-password");
    expect(stored).not.toContain("protectedCredentials");
    const reopened = new PluginNetworkSettings({
      path,
      transport: transport(),
      changing: (): void => undefined,
    });
    expect(await reopened.snapshot()).toMatchObject({
      configuration: custom,
      credentialsConfigured: false,
      credentialStorage: "session",
    });
    const failed = await setup({
      native: transport(),
      protection: {
        protect: (): Promise<never> => Promise.reject(new Error("Keyring unavailable")),
        unprotect: (): Promise<never> => Promise.reject(new Error("Unused")),
      },
    });
    expect((await failed.settings.update(replace)).credentialStorage).toBe("session");
    expect(await readFile(failed.path, "utf8")).not.toContain("private-password");
  });
  it("binds protected credentials to the remembered origin even if public metadata is altered", async () => {
    const protection = protector();
    const { path, settings } = await setup({ native: transport(), protection });
    await settings.update(replace);
    const stored: unknown = JSON.parse(await readFile(path, "utf8"));
    if (stored === null || typeof stored !== "object") throw new Error("Expected saved settings");
    await writeFile(
      path,
      JSON.stringify({
        ...stored,
        configuration: { ...custom, proxyUrl: "http://different.example:3128" },
      }),
    );
    const configure = vi.fn((_configuration: PluginNetworkTransportConfiguration): Promise<void> =>
      Promise.resolve(),
    );
    const reopened = new PluginNetworkSettings({
      path,
      transport: transport(configure),
      protector: protection,
      changing: (): void => undefined,
    });
    expect(await reopened.snapshot()).toMatchObject({
      configuration: null,
      credentialsConfigured: false,
    });
    await expect(reopened.remote()).rejects.toThrow(/need attention/u);
    expect(configure).not.toHaveBeenCalled();
  });
  it("remembers credentials through system mode without forwarding them and rejects implicit endpoint reuse", async () => {
    const configure = vi.fn((_configuration: PluginNetworkTransportConfiguration): Promise<void> =>
      Promise.resolve(),
    );
    const { settings } = await setup({ native: transport(configure) });
    await settings.update(replace);
    expect(
      (
        await settings.update({
          configuration: { ...custom, mode: "system" },
          credentials: { action: "unchanged" },
        })
      ).credentialsConfigured,
    ).toBe(true);
    expect(configure).toHaveBeenLastCalledWith({ mode: "system" });
    await expect(
      settings.update({
        configuration: { ...custom, proxyUrl: "https://another.example:443" },
        credentials: { action: "unchanged" },
      }),
    ).rejects.toThrow(/different endpoint/u);
    expect((await settings.snapshot()).configuration?.mode).toBe("system");
    await settings.update({ configuration: custom, credentials: { action: "unchanged" } });
    expect(configure).toHaveBeenLastCalledWith({
      mode: "custom",
      proxyUrl: custom.proxyUrl,
      credentials: { username: "private-user", password: "private-password" },
    });
  });
  it("fails closed on corrupt private metadata and permits an explicit reset", async () => {
    const { path, settings } = await setup({ native: transport() });
    await settings.update({ configuration: system, credentials: { action: "clear" } });
    await writeFile(path, "truncated metadata with private-password");
    const reopened = new PluginNetworkSettings({
      path,
      transport: transport(),
      changing: (): void => undefined,
    });
    expect(await reopened.snapshot()).toMatchObject({
      configuration: null,
      credentialsConfigured: false,
      error: expect.any(String) as unknown,
    });
    await expect(reopened.remote()).rejects.toThrow(/need attention/u);
    expect(JSON.stringify(await reopened.snapshot())).not.toContain("private-password");
    expect(
      (await reopened.update({ configuration: system, credentials: { action: "clear" } }))
        .configuration,
    ).toEqual(system);
    expect(await reopened.remote()).toBe(1);
  });
  it("rolls back failed apply or private-file writes without changing the previously working settings", async () => {
    const configure = vi.fn((_configuration: PluginNetworkTransportConfiguration): Promise<void> =>
      Promise.resolve(),
    );
    const { root, path, settings } = await setup({ native: transport(configure) });
    const previous = await settings.update({
      configuration: custom,
      credentials: { action: "clear" },
    });
    configure.mockRejectedValueOnce(new Error("Raw error containing private-password"));
    await expect(
      settings.update({
        configuration: { ...custom, offline: true },
        credentials: { action: "unchanged" },
      }),
    ).rejects.toThrow(/could not be saved/u);
    expect(await settings.snapshot()).toEqual(previous);
    const target = join(root, "outside");
    await writeFile(target, "untouched");
    await rm(path);
    await symlink(target, path);
    await expect(
      settings.update({ configuration: system, credentials: { action: "clear" } }),
    ).rejects.toThrow(/could not be saved/u);
    expect(await settings.snapshot()).toEqual(previous);
    expect(await readFile(target, "utf8")).toBe("untouched");
    expect(configure).toHaveBeenLastCalledWith({ mode: "custom", proxyUrl: custom.proxyUrl });
  });
  it("keeps browser offline policy in memory and blocks unsupported proxy authentication", async () => {
    const { settings, path } = await setup();
    expect(await settings.snapshot()).toMatchObject({
      nativeAvailable: false,
      credentialStorage: "unavailable",
    });
    await settings.update({
      configuration: { ...system, offline: true },
      credentials: { action: "clear" },
    });
    await expect(settings.remote()).rejects.toThrow(/offline/u);
    await expect(settings.update(replace)).rejects.toThrow(/desktop/u);
    await expect(readFile(path)).rejects.toMatchObject({ code: "ENOENT" });
  });
  it("does not restore credentials or transport authority when shutdown overtakes protection", async () => {
    let release!: (value: Buffer) => void;
    let entered!: () => void;
    const ready = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const protecting = new Promise<Buffer>((resolve) => {
      release = resolve;
    });
    const configure = vi.fn((_configuration: PluginNetworkTransportConfiguration): Promise<void> =>
      Promise.resolve(),
    );
    const { settings, path } = await setup({
      native: transport(configure),
      protection: {
        protect: (): Promise<Buffer> => {
          entered();
          return protecting;
        },
        unprotect: (): Promise<never> => Promise.reject(new Error("Unused")),
      },
    });
    const updating = settings.update(replace);
    await ready;
    settings.close();
    release(Buffer.from("protected fixture"));
    await expect(updating).rejects.toThrow(/closing/u);
    expect(configure).toHaveBeenCalledOnce();
    expect(configure).toHaveBeenLastCalledWith({ mode: "system" });
    await expect(readFile(path)).rejects.toMatchObject({ code: "ENOENT" });
  });
  it("does not publish or forward decrypted credentials when shutdown overtakes loading", async () => {
    const protection = protector();
    const { settings, path } = await setup({ native: transport(), protection });
    await settings.update(replace);
    let release!: (value: Awaited<ReturnType<ProfileProtector["unprotect"]>>) => void;
    let entered!: () => void;
    const ready = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const decrypting = new Promise<Awaited<ReturnType<ProfileProtector["unprotect"]>>>(
      (resolve) => {
        release = resolve;
      },
    );
    const configure = vi.fn((_configuration: PluginNetworkTransportConfiguration): Promise<void> =>
      Promise.resolve(),
    );
    const reopened = new PluginNetworkSettings({
      path,
      transport: transport(configure),
      changing: (): void => undefined,
      protector: {
        protect: protection.protect.bind(protection),
        unprotect: (): ReturnType<ProfileProtector["unprotect"]> => {
          entered();
          return decrypting;
        },
      },
    });
    const loading = reopened.snapshot();
    await ready;
    reopened.close();
    release({
      plaintext: JSON.stringify({
        proxyUrl: custom.proxyUrl,
        username: "private-user",
        password: "private-password",
      }),
      shouldReEncrypt: false,
    });
    await expect(loading).rejects.toThrow(/closing/u);
    expect(configure).not.toHaveBeenCalled();
  });
});
