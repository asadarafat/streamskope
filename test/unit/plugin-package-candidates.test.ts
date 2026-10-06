import { afterEach, describe, expect, it, vi } from "vitest";

import type { PluginPackageInspection } from "../../src/plugins/contracts";
import {
  PluginPackageCandidates,
  PLUGIN_PACKAGE_REVIEW_TTL_MS,
} from "../../src/platform/node/plugins/package-candidates";

const review: Omit<PluginPackageInspection, "candidateId" | "expiresAt"> = {
  manifest: {
    id: "streamskope.eda",
    name: "Capture",
    version: "1.0.0",
    apiVersion: 2,
    backend: "backend.cjs",
    renderer: "renderer.js",
  },
  sha256: "a".repeat(64),
  source: "catalog",
  trust: "official",
  status: "install",
};
afterEach(() => {
  vi.useRealTimers();
});
describe("opaque exact-byte plugin reviews", () => {
  it("expires through its actual timer, releasing the archive pin exactly once", () => {
    vi.useFakeTimers();
    const pool = new PluginPackageCandidates();
    const release = vi.fn();
    const value = pool.create(review, release);
    expect(pool.get(value.candidateId)).toEqual(value);
    vi.advanceTimersByTime(PLUGIN_PACKAGE_REVIEW_TTL_MS);
    expect(release).toHaveBeenCalledOnce();
    expect(() => pool.get(value.candidateId)).toThrow(/expired/u);
    pool.close();
    expect(release).toHaveBeenCalledOnce();
  });
  it("bounds concurrent reviews, prevents double installation and retains failed consent for retry", () => {
    const pool = new PluginPackageCandidates();
    const releases = Array.from({ length: 8 }, () => vi.fn());
    const values = releases.map((release) => pool.create(review, release));
    expect(() => pool.create(review, vi.fn())).toThrow(/Too many/u);
    const value = values[0]!;
    expect(pool.begin(value.candidateId)).toEqual(value);
    expect(() => pool.begin(value.candidateId)).toThrow(/already/u);
    pool.failed(value.candidateId);
    expect(pool.begin(value.candidateId)).toEqual(value);
    pool.discard(value.candidateId);
    pool.discard(value.candidateId);
    expect(releases[0]).not.toHaveBeenCalled();
    pool.succeeded(value.candidateId);
    expect(() => pool.get(value.candidateId)).toThrow(/closed/u);
    pool.close();
    releases.forEach((release) => expect(release).toHaveBeenCalledOnce());
  });
  it("transfers the pin from idle review expiry to an admitted installation until it settles", () => {
    vi.useFakeTimers();
    const pool = new PluginPackageCandidates();
    const release = vi.fn();
    const value = pool.create(review, release);
    vi.advanceTimersByTime(PLUGIN_PACKAGE_REVIEW_TTL_MS - 10_000);
    pool.begin(value.candidateId);
    vi.advanceTimersByTime(20_000);
    pool.discard(value.candidateId);
    expect(pool.get(value.candidateId)).toEqual(value);
    expect(release).not.toHaveBeenCalled();
    pool.succeeded(value.candidateId);
    expect(release).toHaveBeenCalledOnce();
    pool.close();
  });
});
