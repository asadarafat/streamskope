import { Admin } from "@platformatic/kafka";
import { expect, it, vi, type MockInstance } from "vitest";

import { startAuthorizationFixture } from "../../aio-kafka/fixtures/authorization";

type Runtime = NonNullable<Parameters<typeof startAuthorizationFixture>[0]>;
const secret = "private-fixture-password";

function runtime(): Runtime & {
  docker: ReturnType<typeof vi.fn<Runtime["docker"]>>;
  admin: Admin;
  listTopics: MockInstance<Admin["listTopics"]>;
  close: MockInstance<Admin["close"]>;
  pause: ReturnType<typeof vi.fn<Runtime["pause"]>>;
} {
  const admin = new Admin({
    clientId: "fixture-diagnostic-unit",
    bootstrapBrokers: ["127.0.0.1:1"],
  });
  const listTopics = vi.spyOn(admin, "listTopics").mockResolvedValue([]);
  const close = vi.spyOn(admin, "close").mockResolvedValue(undefined);
  return {
    admin,
    listTopics,
    close,
    createAdmin: () => admin,
    docker: vi.fn<Runtime["docker"]>().mockImplementation(fixtureDocker()),
    pause: vi.fn<Runtime["pause"]>().mockResolvedValue(undefined),
  };
}

function fixtureDocker(): Runtime["docker"] {
  let token = "";
  let present = false;
  const id = "a".repeat(64);
  return (args): Promise<{ stdout: string }> => {
    if (args[0] === "create") {
      token = args[args.indexOf("--label") + 1]!.split("=")[1]!;
      present = true;
      return Promise.resolve({ stdout: id });
    }
    if (args[0] === "rm") present = false;
    return Promise.resolve({
      stdout: args[0] === "inspect" ? `${id} ${token}` : args[0] === "ps" && present ? id : "",
    });
  };
}

async function safeFailure(pending: Promise<unknown>, message: string): Promise<void> {
  const failure: unknown = await pending.catch((error: unknown) => error);
  expect(failure).toBeInstanceOf(Error);
  expect((failure as Error).message).toContain(message);
  expect((failure as Error).message).not.toContain(secret);
  expect(JSON.stringify(failure)).not.toContain(secret);
  expect(failure).not.toHaveProperty("cause");
}

it("checks its pinned image before creating the owned broker and keeps cleanup scoped to its name", async () => {
  const f = runtime();
  const fixture = await startAuthorizationFixture(f);
  expect(f.docker.mock.calls.map(([args]) => args[0])).toEqual(["image", "create", "start"]);
  const image = f.docker.mock.calls[0]![0][2];
  expect(image).toMatch(/^apache\/kafka:.*@sha256:[a-f0-9]{64}$/u);
  const creation = f.docker.mock.calls[1]![0];
  expect(creation.at(-1)).toBe(image);
  const name = creation[creation.indexOf("--name") + 1];
  expect(name).toMatch(/^streamskope-acl-qualification-[a-f0-9-]{36}$/u);
  await fixture.dispose();
  expect(f.docker.mock.calls).toContainEqual([
    ["rm", "--force", "--volumes", "a".repeat(64)],
    30_000,
  ]);
  expect(f.close).toHaveBeenCalledOnce();
});

it("distinguishes a failed image pull without creating or removing any broker", async () => {
  const f = runtime();
  f.docker.mockRejectedValueOnce(new Error("uncached")).mockRejectedValue(
    Object.assign(new Error(`Docker command ${secret}`), {
      stderr: `toomanyrequests: pull rate limit ${secret}`,
    }),
  );
  await safeFailure(
    startAuthorizationFixture(f),
    "pinned image availability (registry-rate-limited); owned cleanup was confirmed",
  );
  expect(f.docker.mock.calls.map(([args]) => args[0])).toEqual(["image", "pull"]);
  expect(f.docker.mock.calls[1]![0][1]).toMatch(/^apache\/kafka:/u);
  expect(f.listTopics).not.toHaveBeenCalled();
  expect(f.close).toHaveBeenCalledOnce();
});

it.each([
  { reason: "docker-daemon-unavailable", stderr: "Cannot connect to the Docker daemon." },
  { reason: "local-capacity-exhausted", stderr: "no space left on device" },
  { reason: "port-conflict", stderr: "Bind for 127.0.0.1:9092 failed: port is already allocated" },
  {
    reason: "container-name-conflict",
    stderr: 'Conflict. The container name "/private-name" is already in use.',
  },
  {
    reason: "container-runtime-failure",
    stderr: "failed to create task for container: OCI runtime create failed",
  },
])(
  "classifies $reason during credential-bearing creation and confirms owned cleanup",
  async ({ reason, stderr }) => {
    const f = runtime();
    const successful = fixtureDocker();
    f.docker.mockImplementation((args) =>
      args[0] === "create"
        ? Promise.reject(
            Object.assign(new Error(`--env ${secret}`), {
              stderr: `${stderr} ${secret}`,
            }),
          )
        : successful(args, 30_000),
    );
    await safeFailure(
      startAuthorizationFixture(f),
      `owned container creation (${reason}); owned cleanup was confirmed`,
    );
    expect(f.docker.mock.calls.map(([args]) => args[0])).toEqual(["image", "create", "ps"]);
    expect(f.listTopics).not.toHaveBeenCalled();
    expect(f.close).toHaveBeenCalledOnce();
  },
);

it("reports broker readiness separately after the unchanged bounded polling window", async () => {
  const f = runtime();
  f.listTopics.mockRejectedValue(new Error(`SASL authentication ${secret}`));
  await safeFailure(
    startAuthorizationFixture(f),
    "broker readiness (broker-not-ready); owned cleanup was confirmed",
  );
  expect(f.listTopics).toHaveBeenCalledTimes(60);
  expect(f.pause).toHaveBeenCalledTimes(60);
  expect(f.docker.mock.calls.map(([args]) => args[0])).toEqual([
    "image",
    "create",
    "start",
    "ps",
    "inspect",
    "rm",
    "ps",
  ]);
});

it.each(["admin", "container"] as const)(
  "does not claim confirmed cleanup when %s cleanup fails after a startup failure",
  async (owner) => {
    const f = runtime();
    const successful = fixtureDocker();
    if (owner === "admin") f.close.mockRejectedValue(new Error(secret));
    f.docker.mockImplementation((args) =>
      args[0] === "start" || (owner === "container" && args[0] === "rm")
        ? Promise.reject(Object.assign(new Error(secret), { stderr: secret }))
        : successful(args, 30_000),
    );
    await safeFailure(startAuthorizationFixture(f), "owned cleanup could not be confirmed");
    expect(f.docker.mock.calls.map(([args]) => args[0])).toContain("rm");
    expect(f.close).toHaveBeenCalledOnce();
  },
);

it("keeps disposal errors safe after a successful startup while still closing both resources", async () => {
  const f = runtime();
  const fixture = await startAuthorizationFixture(f);
  f.close.mockRejectedValue(new Error(secret));
  f.docker.mockRejectedValue(
    Object.assign(new Error(secret), {
      stderr: `Cannot connect to the Docker daemon. ${secret}`,
    }),
  );
  await safeFailure(
    fixture.dispose(),
    "cleanup could not be confirmed (admin close, container removal: docker-daemon-unavailable)",
  );
  expect(f.close).toHaveBeenCalledOnce();
  expect(f.docker.mock.calls.at(-1)?.[0][0]).toBe("ps");
});
