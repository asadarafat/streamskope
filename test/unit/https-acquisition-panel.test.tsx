// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { RemoteTrustAcquisitionPanel } from "../../src/features/kafka/ui/RemoteTrustAcquisitionPanel";
import { trustRecipeInput } from "../support/trust-recipe";
import type { ProfileTrustRecipeSelection } from "../../src/features/kafka/ui/ProfileTrustRecipeSelector";

afterEach(cleanup);
it("acquires HTTPS directly and uses the shared candidate review without SSH controls", async () => {
  const commands: HostCommand[] = [];
  const host: StreamSkopeHost = {
    execute: (command) => {
      commands.push(command);
      const base = {
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true as const,
      };
      if (command.command === "trustAcquisition.capabilities")
        return Promise.resolve({
          ...base,
          result: {
            correlationId: "capabilities",
            sshAgent: "unavailable",
            methods: ["ssh", "https"],
          },
        });
      if (command.command === "trustAcquisition.editor.open")
        return Promise.resolve({
          ...base,
          result: { correlationId: "editor", editor: { id: "editor", generation: 1 } },
        });
      if (command.command === "trustAcquisition.https.fetch")
        return Promise.resolve({
          ...base,
          result: {
            correlationId: "acquire",
            acquisition: {
              id: "candidate",
              editor: command.payload.editor,
              expiresAt: "2099-01-01T00:00:00.000Z",
              recipe: { id: "api", revision: 1, source: "https" },
              password: { present: false, templateName: null },
              target: { host: "api.example.test", port: 443, origin: "https://api.example.test" },
              material: { kind: "pem", label: "CA", templateName: "API", byteCount: 100 },
            },
          },
        });
      return Promise.resolve({ ...base, result: { correlationId: "accepted" } });
    },
    subscribe: () => () => undefined,
    openExternalUrl: () =>
      Promise.reject(new Error("External navigation is not part of acquisition")),
  };
  const selection: ProfileTrustRecipeSelection = {
    recipe: {
      id: "api",
      revision: 1,
      name: "API",
      method: "https",
      syntax: "named-v1",
      kind: "pem",
      timeoutSeconds: 30,
      parameters: [],
      https: {
        authentication: "bearer",
        material: {
          url: "https://api.example.test/ca",
          headers: [],
          query: [],
          extraction: { mode: "raw" },
        },
        password: { source: "none" },
      },
    },
    reference: { mode: "replace", recipeId: "api", recipeRevision: 1, overrides: {} },
  };
  const changed = vi.fn();
  const rendered = render(
    <RemoteTrustAcquisitionPanel
      host={host}
      recipeSelection={selection}
      acquisition={null}
      kind="pem"
      label="CA"
      onAcquisitionChange={changed}
      onOpenActivity={vi.fn()}
      templateSnapshot={null}
    />,
  );
  const user = userEvent.setup();
  await user.type(await screen.findByLabelText(/API bearer token/u), "ephemeral-secret");
  const sshSelection: ProfileTrustRecipeSelection = {
    recipe: { ...trustRecipeInput(), id: "ssh", revision: 1 },
    reference: { mode: "replace", recipeId: "ssh", recipeRevision: 1, overrides: {} },
  };
  const panel = (recipeSelection: ProfileTrustRecipeSelection): React.JSX.Element => (
    <RemoteTrustAcquisitionPanel
      host={host}
      recipeSelection={recipeSelection}
      acquisition={null}
      kind="pem"
      label="CA"
      onAcquisitionChange={changed}
      onOpenActivity={vi.fn()}
      templateSnapshot={null}
    />
  );
  rendered.rerender(panel(sshSelection));
  expect(screen.queryByLabelText(/API bearer token/u)).not.toBeInTheDocument();
  rendered.rerender(panel(selection));
  expect(screen.getByLabelText(/API bearer token/u)).toHaveValue("");
  await user.type(screen.getByLabelText(/API bearer token/u), "ephemeral-secret");
  expect(screen.queryByLabelText("SSH host")).not.toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Retrieve" }));
  await screen.findByRole("button", { name: "Apply to connection" });
  expect(changed).not.toHaveBeenCalled();
  expect(
    commands.find((command) => command.command === "trustAcquisition.https.fetch"),
  ).toMatchObject({
    payload: {
      api: {
        authentication: { mode: "bearer", token: "ephemeral-secret" },
        tls: { mode: "system" },
      },
    },
  });
  expect(commands.some((command) => command.command === "trustAcquisition.hostKey.discover")).toBe(
    false,
  );
  await user.click(screen.getByRole("button", { name: "Apply to connection" }));
  expect(changed).toHaveBeenCalledWith(expect.objectContaining({ id: "candidate" }));
});
