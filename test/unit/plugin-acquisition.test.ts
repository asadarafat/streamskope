import { afterEach, describe, expect, it, vi } from "vitest";

import type { PluginAcquisitionProgress } from "../../src/plugins/contracts";
import {
  PluginAcquisitions,
  type PluginAcquisitionContext,
} from "../../src/platform/node/plugins/acquisition";
import { pluginNetworkProblem } from "../../src/platform/node/plugins/network-errors";
import { pluginProblem } from "../../src/platform/node/plugins/problem";

afterEach(() => {
  vi.useRealTimers();
});
describe("owned plugin acquisitions", () => {
  it("does not let matching error codes bypass host redaction while retaining deliberately safe diagnostics", () => {
    const forged = Object.assign(new Error("private-password /private/path"), {
      code: "BACKEND_UNAVAILABLE",
      recovery: "private-password /private/path",
    });
    const safe = pluginNetworkProblem(forged);
    expect(safe).not.toBe(forged);
    expect(safe.message).not.toContain("private-password");
    expect(JSON.stringify(safe)).not.toContain("/private/path");
    const deliberate = pluginProblem(
      "Plugin networking is offline.",
      "Enable downloads or select a signed file.",
    );
    expect(pluginNetworkProblem(deliberate)).toBe(deliberate);
  });
  it("cancels only its owner promptly and disposes an ignored late result", async () => {
    const owners = new PluginAcquisitions();
    let release!: (value: string) => void;
    const pending = new Promise<string>((resolve) => {
      release = resolve;
    });
    const discard = vi.fn();
    const cancelled = owners.run("cancelled", "inspect", true, () => pending, discard);
    const healthy = owners.run("healthy", "catalog", true, (): Promise<string> =>
      Promise.resolve("working"),
    );
    owners.cancel("cancelled");
    await expect(cancelled).rejects.toThrow(/cancelled/u);
    expect(await healthy).toBe("working");
    release("late candidate");
    await Promise.resolve();
    await Promise.resolve();
    expect(discard).toHaveBeenCalledOnce();
    expect(discard).toHaveBeenCalledWith("late candidate");
    owners.cancel("cancelled");
    owners.cancel("missing");
    owners.close();
  });
  it("bounds IDs and concurrent owners and cancels remote work without stopping local selection", async () => {
    const owners = new PluginAcquisitions();
    let release!: () => void;
    const local = owners.run(
      "local",
      "inspect",
      false,
      () =>
        new Promise<void>((resolve) => {
          release = resolve;
        }),
    );
    const remotes = Array.from({ length: 15 }, (_, index) =>
      owners.run(
        `remote-${index}`,
        "catalog",
        true,
        (): Promise<never> => new Promise(() => undefined),
      ),
    );
    const outcomes = Promise.allSettled(remotes);
    await expect(
      owners.run("extra", "catalog", true, (): Promise<null> => Promise.resolve(null)),
    ).rejects.toThrow(/Too many/u);
    await expect(
      owners.run("local", "inspect", false, (): Promise<null> => Promise.resolve(null)),
    ).rejects.toThrow(/already in use/u);
    owners.cancelRemote();
    await outcomes;
    release();
    await local;
    owners.close();
    await expect(
      owners.run("closed", "inspect", false, (): Promise<null> => Promise.resolve(null)),
    ).rejects.toThrow(/closing/u);
  });
  it("throttles byte updates and emits bounded truthful counts and one final state", async () => {
    vi.useFakeTimers();
    const owners = new PluginAcquisitions();
    const progress: PluginAcquisitionProgress[] = [];
    owners.subscribe((value): void => {
      progress.push(value);
    });
    let context!: PluginAcquisitionContext;
    let release!: () => void;
    let entered!: () => void;
    const ready = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const running = owners.run("progress", "inspect", true, (value) => {
      context = value;
      entered();
      return new Promise<void>((resolve) => {
        release = resolve;
      });
    });
    await ready;
    context.progress("download", 0, 100);
    for (let byte = 1; byte <= 50; byte += 1) context.progress("download", byte, 100);
    expect(progress).toHaveLength(2);
    vi.advanceTimersByTime(251);
    context.progress("download", 80, 40);
    expect(progress.at(-1)).toMatchObject({ receivedBytes: 80 });
    expect(progress.at(-1)?.totalBytes).toBeUndefined();
    context.progress("verify", 90, 90);
    release();
    await running;
    expect(progress.at(-1)?.state).toBe("succeeded");
    expect(progress.filter((value) => value.state === "succeeded")).toHaveLength(1);
    owners.close();
  });
  it.each([
    ["HTTP 407 secret", /proxy authentication/u],
    ["CERT_AUTHORITY_INVALID secret", /certificate validation/u],
    ["TimeoutError secret", /timed out/u],
    ["HTTP 429 secret", /limited/u],
    ["PROXY_CONNECTION_FAILED secret", /proxy could not connect/u],
  ])("classifies %s without reflecting secrets or paths", (message, expected) => {
    const error = pluginNetworkProblem(
      new Error(`${message} https://private-user:private-password@proxy.example /private/path`),
    );
    expect(error.message).toMatch(expected);
    expect(error.message).not.toContain("private-password");
    expect(error.message).not.toContain("/private/path");
    expect(error).toMatchObject({ code: "BACKEND_UNAVAILABLE", stage: "backend" });
  });
});
