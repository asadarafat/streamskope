import { expect, it, vi } from "vitest";

import {
  FixtureContainerOwner,
  type DockerCommand,
} from "../../aio-kafka/fixtures/container-owner";

const id = "a".repeat(64);

it("recovers a timed-out creation by ownership label without deleting a foreign name", async () => {
  let present = true;
  const command = vi.fn<DockerCommand>((args) => {
    if (args[0] === "create") return Promise.reject(new Error("private command credentials"));
    if (args[0] === "ps") {
      if (args.includes("name=^/owned$")) {
        expect(args).toContain(`label=streamskope.fixture=${owner.token}`);
      }
      return Promise.resolve({ stdout: present ? id : "" });
    }
    if (args[0] === "inspect") return Promise.resolve({ stdout: `${id} ${owner.token}` });
    if (args[0] === "rm") {
      expect(args).toContain(id);
      present = false;
    }
    return Promise.resolve({ stdout: "" });
  });
  const owner: FixtureContainerOwner = new FixtureContainerOwner(command);
  await expect(owner.create("owned", ["image"])).rejects.toThrow();
  await owner.dispose();
  expect(command.mock.calls.filter(([args]) => args[0] === "rm")).toHaveLength(1);
  await owner.dispose();
  expect(command.mock.calls.filter(([args]) => args[0] === "rm")).toHaveLength(1);
  await expect(owner.create("later", ["image"])).rejects.toThrow("revoked");
});

it("does not remove a foreign container when creation fails with a name conflict", async () => {
  const command = vi.fn<DockerCommand>((args) =>
    args[0] === "create"
      ? Promise.reject(new Error("name conflict"))
      : Promise.resolve({ stdout: "" }),
  );
  const owner: FixtureContainerOwner = new FixtureContainerOwner(command);
  await expect(owner.create("foreign", ["image"])).rejects.toThrow();
  await owner.dispose();
  expect(command.mock.calls.some(([args]) => args[0] === "rm")).toBe(false);
});

it("retains an unresolved identity for retry when its label differs", async () => {
  let labelMatches = false,
    present = true;
  const command = vi.fn<DockerCommand>((args) => {
    if (args[0] === "create") return Promise.resolve({ stdout: id });
    if (args[0] === "ps") return Promise.resolve({ stdout: present ? id : "" });
    if (args[0] === "inspect")
      return Promise.resolve({ stdout: `${id} ${labelMatches ? owner.token : "foreign"}` });
    if (args[0] === "rm") present = false;
    return Promise.resolve({ stdout: "" });
  });
  const owner: FixtureContainerOwner = new FixtureContainerOwner(command);
  await owner.create("owned", ["image"]);
  await expect(owner.dispose()).rejects.toThrow("cleanup could not be confirmed");
  expect(command.mock.calls.some(([args]) => args[0] === "rm")).toBe(false);
  labelMatches = true;
  await owner.dispose();
  expect(present).toBe(false);
});

it("waits for an admitted creation before confirming cleanup and rejects its late start", async () => {
  let finish: ((value: { stdout: string }) => void) | undefined;
  let present = false;
  const command = vi.fn<DockerCommand>((args) => {
    if (args[0] === "create")
      return new Promise((resolve) => {
        finish = resolve;
      });
    if (args[0] === "ps") return Promise.resolve({ stdout: present ? id : "" });
    if (args[0] === "inspect") return Promise.resolve({ stdout: `${id} ${owner.token}` });
    if (args[0] === "rm") present = false;
    return Promise.resolve({ stdout: "" });
  });
  const owner: FixtureContainerOwner = new FixtureContainerOwner(command);
  const creation = owner.create("owned", ["image"]);
  let cleaned = false;
  const cleanup = owner.dispose().then(() => {
    cleaned = true;
  });
  await Promise.resolve();
  expect(cleaned).toBe(false);
  present = true;
  finish!({ stdout: id });
  await creation;
  await cleanup;
  expect(present).toBe(false);
  await expect(owner.start(id)).rejects.toThrow("active owner");
});
