import { expect, it, vi } from "vitest";

import { NATS_SERVER_IMAGES } from "../../tools/dev/nats-fixture/definition";
import {
  ensureContainerImage,
  ContainerImageAvailabilityError,
} from "../../tools/dev/container-image";

const image = NATS_SERVER_IMAGES.x64;
type ImageCommand = Parameters<typeof ensureContainerImage>[2];

it("uses a cached pinned image without pulling or creating resources", async () => {
  const run = vi.fn<ImageCommand>().mockResolvedValue({ stdout: "cached", stderr: "" });
  await ensureContainerImage(image, "linux/amd64", run);
  expect(run.mock.calls).toEqual([[["image", "inspect", image], 10_000]]);
});

it("pulls the same architecture and immutable reference exactly once when uncached", async () => {
  const run = vi
    .fn<ImageCommand>()
    .mockRejectedValueOnce(new Error("not cached"))
    .mockResolvedValue({});
  await ensureContainerImage(image, "linux/amd64", run);
  expect(run.mock.calls).toEqual([
    [["image", "inspect", image], 10_000],
    [["pull", "--platform", "linux/amd64", image], 120_000],
  ]);
});

it.each([
  { reason: "docker-tool-unavailable", detail: { code: "ENOENT" } },
  { reason: "docker-permission-denied", detail: { code: "EACCES" } },
  {
    reason: "docker-daemon-unavailable",
    detail: { stderr: "Cannot connect to the Docker daemon at unix:///private/docker.sock" },
  },
  {
    reason: "registry-rate-limited",
    detail: {
      stderr: "Error response from daemon: toomanyrequests: You have reached your pull rate limit.",
    },
  },
  {
    reason: "registry-authentication",
    detail: { stderr: "unauthorized: authentication required; manifest unknown" },
  },
  {
    reason: "image-unavailable",
    detail: { stderr: "manifest unknown: manifest unknown" },
  },
  {
    reason: "registry-server-unavailable",
    detail: { stderr: "received unexpected HTTP status: 503 Service Unavailable" },
  },
  {
    reason: "certificate-validation",
    detail: { stderr: "x509: certificate signed by unknown authority" },
  },
  {
    reason: "network-unavailable",
    detail: { stderr: "dial tcp: lookup private-registry: no such host" },
  },
  { reason: "command-timeout", detail: { killed: true, signal: "SIGTERM" } },
  { reason: "unknown", detail: { stderr: "An unrecognized registry failure" } },
])(
  "reports only $reason after a failed pull, without retry or raw details",
  async ({ reason, detail }) => {
    const failure = Object.assign(new Error("raw private command and credentials"), detail, {
      stdout: "private-fixture-token",
      stderr: `${"stderr" in detail ? detail.stderr : ""}\nhttps://user:private-password@registry.invalid/token`,
    });
    const run = vi
      .fn<ImageCommand>()
      .mockRejectedValueOnce(new Error("not cached"))
      .mockRejectedValue(failure);
    const caught: unknown = await ensureContainerImage(image, "linux/amd64", run).catch(
      (error: unknown) => error,
    );
    expect(caught).toBeInstanceOf(ContainerImageAvailabilityError);
    expect((caught as Error).message).toBe(
      `Pinned container image availability failed (${reason}).`,
    );
    expect(JSON.stringify(caught)).not.toMatch(/private-|user:|registry\.invalid|raw private/);
    expect(caught).not.toHaveProperty("cause");
    expect(run).toHaveBeenCalledTimes(2);
    expect(run.mock.calls.map((call) => call[0][0])).toEqual(["image", "pull"]);
  },
);
