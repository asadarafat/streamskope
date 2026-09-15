// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { useState } from "react";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { pasteText } from "../support/paste-text";
import {
  HOST_PROTOCOL_VERSION,
  type ConnectionTemplateSnapshot,
  type HostCommand,
  type HostCommandResponse,
  type HostEventListener,
  type RemoteTrustAcquisitionSummary,
  type StreamSkopeHost,
} from "../../src/kafka/contracts";
import { RemoteTrustAcquisitionPanel } from "../../src/kafka/ui/RemoteTrustAcquisitionPanel";
import { trustRecipeInput } from "../support/trust-recipe";

const templates: ConnectionTemplateSnapshot = {
  catalogs: [
    {
      catalog: "truststore-fetch",
      entries: [
        {
          name: "Remote trust",
          template: "copy source {truststorePath} using {storepass}",
        },
      ],
      selectedName: "Remote trust",
    },
    {
      catalog: "truststore-password",
      entries: [{ name: "Remote password", template: "read remote password" }],
      selectedName: "Remote password",
    },
    {
      catalog: "oauth-endpoint",
      entries: [],
      selectedName: null,
    },
  ],
  store: { durability: "session", state: "ready" },
};

const passwordSummary: RemoteTrustAcquisitionSummary = {
  expiresAt: "2099-07-26T13:10:00.000Z",
  id: "acquisition-1",
  material: null,
  password: { present: true, templateName: "Remote password" },
  target: {
    host: "kafka-lab.example.test",
    hostKeyFingerprint: `SHA256:${"A".repeat(43)}`,
    port: 22,
  },
};

const materialSummary: RemoteTrustAcquisitionSummary = {
  ...passwordSummary,
  editor: { id: "editor", generation: 2 },
  material: {
    byteCount: 4_096,
    kind: "jks",
    label: "remote.truststore.jks",
    templateName: "Remote trust",
  },
};

class FakeHost implements StreamSkopeHost {
  firstUse = false;
  readonly commands: HostCommand[] = [];
  discoveryResponse: Promise<HostCommandResponse> | undefined;
  materialResponse: Promise<HostCommandResponse> | undefined;
  failure: HostCommandResponse | undefined;

  execute(command: HostCommand): Promise<HostCommandResponse> {
    if (
      command.command.startsWith("trustAcquisition.editor.") ||
      command.command === "trustAcquisition.apply"
    ) {
      this.lifecycle.push(command);
      return Promise.resolve({
        command: command.command,
        id: command.id,
        ok: true,
        version: HOST_PROTOCOL_VERSION,
        result:
          command.command === "trustAcquisition.editor.open"
            ? { correlationId: "editor", editor: { id: "editor", generation: 1 } }
            : { correlationId: "editor" },
      });
    }
    if (command.command === "trustAcquisition.capabilities") {
      this.capabilityCalls += 1;
      return Promise.resolve({
        command: command.command,
        id: command.id,
        ok: true,
        version: HOST_PROTOCOL_VERSION,
        result: { correlationId: "capability", sshAgent: this.agentStatus },
      });
    }
    this.commands.push(command);
    if (this.failure !== undefined && this.failure.command === command.command) {
      const response = this.failure;
      this.failure = undefined;
      return Promise.resolve(response);
    }
    if ((command as { readonly command: string }).command === "trustAcquisition.hostKey.discover") {
      if (this.discoveryResponse !== undefined) {
        return this.discoveryResponse;
      }
      return Promise.resolve({
        command: "trustAcquisition.hostKey.discover",
        id: command.id,
        ok: true,
        result: {
          correlationId: "correlation-discovery",
          hostKey: {
            review: {
              id: "identity-review",
              expiresAt: "2099-07-26T13:02:00.000Z",
              confirmationRequired: this.firstUse,
            },
            fingerprint: `SHA256:${"A".repeat(43)}`,
            target: {
              host: "kafka-lab.example.test",
              port: 22,
            },
          },
        },
        version: HOST_PROTOCOL_VERSION,
      } as unknown as HostCommandResponse);
    }
    if (command.command === "trustAcquisition.password.fetch") {
      return Promise.resolve({
        command: command.command,
        id: command.id,
        ok: true,
        result: {
          acquisition: passwordSummary,
          correlationId: "correlation-password",
        },
        version: HOST_PROTOCOL_VERSION,
      });
    }
    if (command.command === "trustAcquisition.material.fetch") {
      if (this.materialResponse !== undefined) return this.materialResponse;
      return Promise.resolve({
        command: command.command,
        id: command.id,
        ok: true,
        result: {
          acquisition: materialSummary,
          correlationId: "correlation-material",
        },
        version: HOST_PROTOCOL_VERSION,
      });
    }
    return Promise.resolve({
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: "correlation-discard" },
      version: HOST_PROTOCOL_VERSION,
    });
  }

  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("External URL action was not expected."));
  }

  capabilityCalls = 0;
  lifecycle: HostCommand[] = [];
  agentStatus: "configured" | "unavailable" = "configured";

  subscribe(_listener: HostEventListener): () => void {
    return (): void => undefined;
  }
}

function Harness({
  host,
  onOpenActivity = (): void => undefined,
}: {
  readonly host: StreamSkopeHost;
  readonly onOpenActivity?: () => void;
}): React.JSX.Element {
  const [acquisition, setAcquisition] = useState<RemoteTrustAcquisitionSummary | null>(null);
  return (
    <RemoteTrustAcquisitionPanel
      acquisition={acquisition}
      host={host}
      kind="jks"
      label="remote.truststore.jks"
      onAcquisitionChange={setAcquisition}
      onOpenActivity={onOpenActivity}
      templateSnapshot={templates}
    />
  );
}

async function fillTarget(user: ReturnType<typeof userEvent.setup>): Promise<void> {
  await waitFor(() => expect(screen.getByRole("textbox", { name: "SSH host" })).toBeEnabled());
  await pasteText(
    user,
    screen.getByRole("textbox", { name: "SSH host" }),
    "kafka-lab.example.test",
  );
  await user.clear(screen.getByRole("spinbutton", { name: "SSH port" }));
  await user.type(screen.getByRole("spinbutton", { name: "SSH port" }), "22");
  await pasteText(user, screen.getByRole("textbox", { name: "SSH username" }), "operator");
  await pasteText(user, screen.getByLabelText("SSH password"), "ssh-password");
}

afterEach(() => {
  cleanup();
});

describe("Material UI remote-trust acquisition panel", () => {
  it("requires first-use confirmation and cancels review without sending credentials", async () => {
    const host = new FakeHost();
    host.firstUse = true;
    const user = userEvent.setup();
    render(<Harness host={host} />);
    await fillTarget(user);
    await user.click(screen.getByRole("button", { name: "Retrieve" }));
    await screen.findByRole("button", { name: "Accept identity and acquire" });
    expect(host.commands).toHaveLength(1);
    await user.click(screen.getByRole("button", { name: "Cancel acquisition" }));
    expect(
      host.commands.some((command) => command.command === "trustAcquisition.material.fetch"),
    ).toBe(false);
    await user.click(screen.getByRole("button", { name: "Retrieve" }));
    await user.click(await screen.findByRole("button", { name: "Accept identity and acquire" }));
    await screen.findByRole("button", { name: "Apply to connection" });
    expect(
      host.commands.find((command) => command.command === "trustAcquisition.material.fetch"),
    ).toMatchObject({ payload: { identityId: "identity-review", acceptIdentity: true } });
  });
  it("applies only opted-in OAuth suggestions after Use acquired trust is confirmed", async () => {
    const host = new FakeHost();
    host.materialResponse = Promise.resolve({
      command: "trustAcquisition.material.fetch",
      id: "suggestion",
      version: HOST_PROTOCOL_VERSION,
      ok: true,
      result: {
        correlationId: "suggestion",
        acquisition: {
          ...materialSummary,
          oauth: { endpoint: "https://new.example/token", clientId: "new-client", scope: "read" },
        },
      },
    });
    const applied = vi.fn();
    const user = userEvent.setup();
    render(
      <RemoteTrustAcquisitionPanel
        host={host}
        kind="jks"
        label="trust.jks"
        acquisition={null}
        onAcquisitionChange={applied}
        onOpenActivity={() => undefined}
        templateSnapshot={templates}
        currentOAuth={{
          endpoint: "https://old.example/token",
          clientId: "old-client",
          scope: "write",
        }}
      />,
    );
    await fillTarget(user);
    await user.click(screen.getByRole("button", { name: "Retrieve" }));
    const endpoint = await screen.findByRole("checkbox", {
      name: "Use suggested OAuth token endpoint",
    });
    expect(endpoint).not.toBeChecked();
    expect(applied).not.toHaveBeenCalled();
    await user.click(endpoint);
    await user.click(screen.getByRole("button", { name: "Apply to connection" }));
    await waitFor(() =>
      expect(applied).toHaveBeenCalledWith(expect.objectContaining({ id: materialSummary.id }), {
        endpoint: "https://new.example/token",
      }),
    );
  });
  it("executes the selected pinned recipe without a legacy catalog selection", async () => {
    const host = new FakeHost();
    const recipe = { ...trustRecipeInput(), id: "generic-ca", revision: 2 };
    const reference = {
      mode: "replace" as const,
      recipeId: recipe.id,
      recipeRevision: 2,
      overrides: { certificate_path: "/etc/tls/ca.pem" },
    };
    const user = userEvent.setup();
    render(
      <RemoteTrustAcquisitionPanel
        host={host}
        kind="pem"
        label="trust.pem"
        acquisition={null}
        onAcquisitionChange={() => undefined}
        onOpenActivity={() => undefined}
        templateSnapshot={null}
        recipeSelection={{ recipe, reference }}
      />,
    );
    await fillTarget(user);
    await user.click(screen.getByRole("button", { name: "Retrieve" }));
    await waitFor(() =>
      expect(
        host.commands.some((command) => command.command === "trustAcquisition.material.fetch"),
      ).toBe(true),
    );
    expect(
      host.commands.find((command) => command.command === "trustAcquisition.material.fetch"),
    ).toMatchObject({ payload: { kind: "pem", recipe: reference } });
  });
  it("acquires within a host editor and asks the host before applying a candidate", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    const { unmount } = render(<Harness host={host} />);
    await fillTarget(user);
    await user.click(screen.getByRole("button", { name: "Retrieve" }));
    await screen.findByRole("button", { name: "Apply to connection" });
    expect(
      host.commands.find((command) => command.command === "trustAcquisition.material.fetch"),
    ).toMatchObject({ payload: { editor: { id: "editor", generation: 2 } } });
    await user.click(screen.getByRole("button", { name: "Apply to connection" }));
    expect(host.lifecycle).toContainEqual(
      expect.objectContaining({
        command: "trustAcquisition.apply",
        payload: { editorId: "editor", acquisitionId: "acquisition-1" },
      }),
    );
    unmount();
    expect(host.lifecycle).toContainEqual(
      expect.objectContaining({ command: "trustAcquisition.editor.close" }),
    );
  });
  it("explains an unavailable host agent and prevents acquisition without fallback", async () => {
    const host = new FakeHost();
    host.agentStatus = "unavailable";
    const user = userEvent.setup();
    render(<Harness host={host} />);
    await fillTarget(user);
    await user.click(screen.getByRole("combobox", { name: "SSH authentication" }));
    await user.click(screen.getByRole("option", { name: "Local SSH agent" }));
    expect(await screen.findByText(/No SSH agent is configured/u)).toBeVisible();
    expect(screen.getByRole("button", { name: "Retrieve" })).toBeDisabled();
    expect(host.capabilityCalls).toBe(1);
    expect(host.commands).toEqual([]);
  });
  it("cancels pending discovery and ignores its late success", async () => {
    const host = new FakeHost();
    let resolveDiscovery!: (response: HostCommandResponse) => void;
    host.discoveryResponse = new Promise((resolve) => {
      resolveDiscovery = resolve;
    });
    const user = userEvent.setup();
    render(<Harness host={host} />);
    await fillTarget(user);
    await user.click(screen.getByRole("button", { name: "Retrieve" }));
    await user.click(screen.getByRole("button", { name: "Cancel acquisition" }));
    expect(host.commands[1]).toMatchObject({
      command: "trustAcquisition.cancel",
      payload: { requestId: host.commands[0]?.id },
    });
    expect(await screen.findByText(/Cancellation requested/u)).toBeVisible();
    await act(async () => {
      resolveDiscovery({
        command: "trustAcquisition.hostKey.discover",
        id: "late-discovery",
        ok: true,
        version: HOST_PROTOCOL_VERSION,
        result: {
          correlationId: "late",
          hostKey: {
            fingerprint: passwordSummary.target.hostKeyFingerprint!,
            target: { host: passwordSummary.target.host, port: 22 },
          },
        },
      });
      await host.discoveryResponse;
    });
    expect(
      host.commands.some((command) => command.command === "trustAcquisition.material.fetch"),
    ).toBe(false);
    expect(screen.queryByRole("button", { name: "Apply to connection" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retrieve" })).toBeEnabled();
  });

  it("cancels pending host work when the editor closes", async () => {
    const host = new FakeHost();
    host.discoveryResponse = new Promise(() => undefined);
    const user = userEvent.setup();
    const view = render(<Harness host={host} />);
    await fillTarget(user);
    await user.click(screen.getByRole("button", { name: "Retrieve" }));
    view.unmount();
    expect(host.commands[1]).toMatchObject({
      command: "trustAcquisition.cancel",
      payload: { requestId: host.commands[0]?.id },
    });
  });
  it("rejects oversized uploaded keys without a discovery or authentication request", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<Harness host={host} />);
    await fillTarget(user);
    await user.click(screen.getByRole("combobox", { name: "SSH authentication" }));
    await user.click(screen.getByRole("option", { name: "Private key" }));
    await user.upload(
      screen.getByLabelText("SSH private key file"),
      new File(["x".repeat(65_537)], "oversized.key"),
    );
    expect(await screen.findByText(/no larger than 64 KiB/u)).toBeVisible();
    expect(screen.getByRole("button", { name: "Retrieve" })).toBeDisabled();
    expect(host.commands).toEqual([]);
  });
  it("uses only an explicitly selected local agent and clears the previous password", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<Harness host={host} />);
    await fillTarget(user);
    await user.click(screen.getByRole("combobox", { name: "SSH authentication" }));
    await user.click(screen.getByRole("option", { name: "Local SSH agent" }));
    expect(screen.queryByLabelText("SSH password")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Retrieve" }));
    await screen.findByRole("button", { name: "Apply to connection" });
    expect(host.commands[1]).toMatchObject({
      payload: { target: { authentication: { mode: "agent" } } },
    });
    expect(JSON.stringify(host.commands)).not.toContain("ssh-password");
    await user.click(screen.getByRole("button", { name: "Discard acquired trust" }));
    await user.click(screen.getByRole("combobox", { name: "SSH authentication" }));
    await user.click(screen.getByRole("option", { name: "Password" }));
    expect(screen.getByLabelText("SSH password")).toHaveValue("");
  });

  it("uploads an ephemeral SSH key without authorizing a local path", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<Harness host={host} />);
    await fillTarget(user);
    await user.click(screen.getByRole("combobox", { name: "SSH authentication" }));
    await user.click(screen.getByRole("option", { name: "Private key" }));
    expect(screen.getByRole("button", { name: "Retrieve" })).toBeDisabled();
    await user.upload(
      screen.getByLabelText("SSH private key file"),
      new File(["fixture-key-bytes"], "id_ed25519"),
    );
    await screen.findByText("Private key loaded for this editor only.");
    await user.type(screen.getByLabelText("SSH key passphrase"), "key-passphrase");
    await user.click(screen.getByRole("button", { name: "Retrieve" }));
    await screen.findByRole("button", { name: "Apply to connection" });
    expect(host.commands[1]).toMatchObject({
      payload: {
        target: {
          authentication: {
            mode: "private-key",
            privateKey: "fixture-key-bytes",
            passphrase: "key-passphrase",
          },
        },
      },
    });
    expect(JSON.stringify(host.commands)).not.toMatch(/ssh-password|id_ed25519/u);
    expect(document.body).not.toHaveTextContent("fixture-key-bytes");
    await user.click(screen.getByRole("button", { name: "Discard acquired trust" }));
    await user.clear(screen.getByLabelText("SSH key passphrase"));
    await user.click(screen.getByRole("button", { name: "Retrieve" }));
    await screen.findByRole("button", { name: "Apply to connection" });
    expect(host.commands.at(-1)).toMatchObject({
      command: "trustAcquisition.material.fetch",
      payload: {
        target: { authentication: { mode: "private-key", privateKey: "fixture-key-bytes" } },
      },
    });
    const last = host.commands.at(-1);
    if (last?.command !== "trustAcquisition.material.fetch")
      throw new Error("Missing key acquisition");
    expect(last.payload.target.authentication).not.toHaveProperty("passphrase");
  });
  it("preserves applied trust when replacement acquisition fails", async () => {
    const host = new FakeHost();
    host.failure = {
      command: "trustAcquisition.material.fetch",
      id: "replacement",
      ok: false,
      error: {
        activeStateChanged: false,
        code: "SSH_AUTHENTICATION",
        correlationId: "replacement",
        recovery: "Check SSH credentials.",
        retryable: false,
        stage: "ssh",
        summary: "Authentication failed.",
      },
      version: HOST_PROTOCOL_VERSION,
    };
    const changed = vi.fn();
    render(
      <RemoteTrustAcquisitionPanel
        acquisition={materialSummary}
        host={host}
        kind="jks"
        label="remote trust"
        onAcquisitionChange={changed}
        onOpenActivity={vi.fn()}
        templateSnapshot={templates}
      />,
    );
    const user = userEvent.setup();
    await fillTarget(user);
    await user.click(screen.getByRole("button", { name: "Retrieve" }));
    await screen.findByText(/Authentication failed/u);
    expect(changed).not.toHaveBeenCalled();
    expect(
      screen.getByRole("status", { name: "Remote trust acquisition status" }),
    ).toHaveTextContent("JKS trust material acquired");
    expect(host.commands.some((command) => command.command === "trustAcquisition.discard")).toBe(
      false,
    );
  });
  it("presents certificate evidence and validity warnings without implying Kafka health", () => {
    render(
      <RemoteTrustAcquisitionPanel
        acquisition={{
          ...materialSummary,
          material: {
            byteCount: 4096,
            kind: "jks",
            label: "remote trust",
            templateName: "Remote trust",
            evidence: {
              count: 17,
              truncated: true,
              validity: {
                earliestExpiry: "2021-01-01T00:00:00.000Z",
                latestStart: "2020-01-01T00:00:00.000Z",
              },
              certificates: Array.from({ length: 16 }, (_, index) => ({
                subject: `CN=Fixture CA ${index}`,
                issuer: `CN=Fixture issuer ${index}`,
                validFrom: "2020-01-01T00:00:00.000Z",
                validTo: "2021-01-01T00:00:00.000Z",
                fingerprint: Array(32).fill("AA").join(":"),
                truncated: false,
              })),
            },
            expiredCertificates: true,
            notYetValidCertificates: false,
          },
        }}
        host={new FakeHost()}
        kind="jks"
        label="remote trust"
        onAcquisitionChange={vi.fn()}
        onOpenActivity={vi.fn()}
        templateSnapshot={templates}
      />,
    );
    expect(screen.getByText("CN=Fixture CA 0")).toBeVisible();
    expect(screen.getByText("CN=Fixture issuer 0")).toBeVisible();
    expect(screen.getByText(/Expired certificates/u)).toBeVisible();
    expect(screen.getByText(/Showing 16 of 17/u)).toBeVisible();
    expect(screen.getByText(/Validity warnings include every certificate/u)).toBeVisible();
    expect(screen.getByText(/Kafka connectivity has not been tested/u)).toBeVisible();
    expect(screen.queryByText("Healthy")).not.toBeInTheDocument();
  });
  it("locks candidate inputs until the candidate is used or discarded", async () => {
    const user = userEvent.setup();
    render(<Harness host={new FakeHost()} />);
    await fillTarget(user);
    await user.click(screen.getByRole("button", { name: "Retrieve" }));
    await screen.findByRole("button", { name: "Apply to connection" });
    expect(screen.getByRole("textbox", { name: "SSH host" })).toBeDisabled();
    expect(screen.getByLabelText("SSH password")).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Discard acquired trust" }));
    expect(screen.getByRole("textbox", { name: "SSH host" })).toBeEnabled();
  });

  it("discards a late candidate after the trust format changes without applying it", async () => {
    const host = new FakeHost();
    let complete: (response: HostCommandResponse) => void = () => {
      throw new Error("Missing fixture resolver");
    };
    const pending = new Promise<HostCommandResponse>((resolve) => {
      complete = resolve;
    });
    host.materialResponse = pending;
    const changed = vi.fn();
    const props = {
      acquisition: null,
      host,
      label: "remote trust",
      onAcquisitionChange: changed,
      onOpenActivity: vi.fn(),
      templateSnapshot: templates,
    };
    const view = render(<RemoteTrustAcquisitionPanel {...props} kind="jks" />);
    const user = userEvent.setup();
    await fillTarget(user);
    await user.click(screen.getByRole("button", { name: "Retrieve" }));
    await waitFor(() => expect(host.commands).toHaveLength(2));
    view.rerender(<RemoteTrustAcquisitionPanel {...props} kind="pem" />);
    await act(async () => {
      complete({
        command: "trustAcquisition.material.fetch",
        id: "late",
        ok: true,
        result: { acquisition: materialSummary, correlationId: "late" },
        version: HOST_PROTOCOL_VERSION,
      });
      await pending;
    });
    expect(changed).not.toHaveBeenCalled();
    expect(screen.queryByRole("button", { name: "Apply to connection" })).not.toBeInTheDocument();
    expect(host.commands.at(-1)).toMatchObject({
      command: "trustAcquisition.discard",
      payload: { acquisitionId: materialSummary.id },
    });
  });
  it("keeps a candidate separate until Use acquired trust is pressed", async () => {
    const host = new FakeHost();
    const changed = vi.fn();
    const user = userEvent.setup();
    render(
      <RemoteTrustAcquisitionPanel
        acquisition={null}
        host={host}
        kind="jks"
        label="remote.truststore.jks"
        onAcquisitionChange={changed}
        onOpenActivity={vi.fn()}
        templateSnapshot={templates}
      />,
    );
    await fillTarget(user);
    await user.click(screen.getByRole("button", { name: "Retrieve" }));
    await screen.findByText(/JKS trust material acquired/u);
    expect(changed).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Apply to connection" }));
    expect(changed).toHaveBeenCalledWith(materialSummary);
    expect(host.commands.map((command) => command.command)).toEqual([
      "trustAcquisition.hostKey.discover",
      "trustAcquisition.material.fetch",
    ]);
  });
  it("reveals ephemeral input, presents the exact effect, and acquires complete trust with one press", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<Harness host={host} />);
    await fillTarget(user);
    expect(
      screen.queryByRole("textbox", { name: "SSH host-key fingerprint" }),
    ).not.toBeInTheDocument();

    const password = screen.getByLabelText("SSH password");
    const reveal = screen.getByRole("button", { name: "Show SSH password" });
    expect(password).toHaveAttribute("type", "password");
    expect(reveal).toHaveAttribute("aria-pressed", "false");
    await user.click(reveal);
    expect(password).toHaveAttribute("type", "text");
    expect(screen.getByRole("button", { name: "Hide SSH password" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    await user.click(screen.getByRole("button", { name: "Hide SSH password" }));

    expect(screen.queryByRole("button", { name: "Acquire password" })).not.toBeInTheDocument();
    const acquireTrust = screen.getByRole("button", { name: "Retrieve" });
    expect(acquireTrust).toBeEnabled();
    expect(screen.getByText("kafka-lab.example.test:22 as operator")).toBeVisible();
    expect(screen.getByText(/removes only that owned file/u)).toBeVisible();
    expect(screen.queryByLabelText("Remote trust template preview")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Advanced acquisition details" }));
    expect(screen.getByLabelText("Remote password template preview")).toHaveTextContent(
      "read remote password",
    );
    expect(screen.getByLabelText("Remote trust template preview")).toHaveTextContent(
      "copy source <truststore-path> using <masked-password>",
    );
    expect(document.body).not.toHaveTextContent("ssh-password");

    await user.click(acquireTrust);
    await waitFor(() => {
      expect(
        screen.getByRole("status", { name: "Remote trust acquisition status" }),
      ).toHaveTextContent("JKS trust material acquired");
    });
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(host.commands).toHaveLength(2);
    expect(host.commands[0]).toMatchObject({
      command: "trustAcquisition.hostKey.discover",
      payload: {
        target: {
          host: "kafka-lab.example.test",
          port: 22,
        },
      },
    });
    expect(host.commands[1]).toMatchObject({
      command: "trustAcquisition.material.fetch",
      payload: {
        kind: "jks",
        label: "remote.truststore.jks",
        target: {
          host: "kafka-lab.example.test",
          hostKeyFingerprint: `SHA256:${"A".repeat(43)}`,
          password: "ssh-password",
          port: 22,
          username: "operator",
        },
      },
      version: HOST_PROTOCOL_VERSION,
    });
    expect(host.commands[1]?.payload).not.toHaveProperty("acquisitionId");
    expect(
      host.commands.filter((command) => command.command === "trustAcquisition.password.fetch"),
    ).toHaveLength(0);
    expect(
      host.commands.filter((command) => command.command === "trustAcquisition.hostKey.discover"),
    ).toHaveLength(1);
    expect(screen.getByText(`Pinned identity SHA256:${"A".repeat(43)}`)).toBeVisible();
    expect(JSON.stringify(document.body.textContent)).not.toMatch(/ssh-password|AQID/);
    expect(acquireTrust).toBeDisabled();

    await user.click(screen.getByRole("button", { name: "Discard acquired trust" }));
    expect(host.commands[2]).toMatchObject({
      command: "trustAcquisition.discard",
      payload: { acquisitionId: "acquisition-1" },
    });
    await waitFor(() => {
      expect(
        screen.getByRole("status", { name: "Remote trust acquisition status" }),
      ).toHaveTextContent("No remote trust acquired");
    });
  });

  it("keeps acquisition unavailable and sends no host command while required input is incomplete", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<Harness host={host} />);

    const acquireTrust = screen.getByRole("button", { name: "Retrieve" });
    expect(acquireTrust).toBeDisabled();
    expect(host.commands).toHaveLength(0);

    await pasteText(
      user,
      screen.getByRole("textbox", { name: "SSH host" }),
      "kafka-lab.example.test",
    );
    await pasteText(user, screen.getByRole("textbox", { name: "SSH username" }), "operator");
    expect(acquireTrust).toBeDisabled();
    expect(
      screen.getByText("Complete the SSH target and credentials to acquire trust."),
    ).toBeVisible();
    expect(host.commands).toHaveLength(0);
  });

  it("announces bounded discovery during direct acquisition", async () => {
    const host = new FakeHost();
    host.discoveryResponse = new Promise<HostCommandResponse>(() => undefined);
    const user = userEvent.setup();
    render(<Harness host={host} />);
    await fillTarget(user);

    await user.click(screen.getByRole("button", { name: "Retrieve" }));

    expect(screen.getByRole("status", { name: "Remote trust operation status" })).toHaveTextContent(
      "Discovering SSH host identity. No credentials or remote template have been sent.",
    );
    expect(screen.getByRole("button", { name: "Discovering host identity…" })).toBeDisabled();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(host.commands).toHaveLength(1);
  });

  it("keeps inputs editable and shows exact recovery when discovery fails", async () => {
    const host = new FakeHost();
    host.failure = {
      command: "trustAcquisition.hostKey.discover",
      error: {
        activeStateChanged: false,
        code: "SSH_UNREACHABLE",
        correlationId: "correlation-discovery-failed",
        recovery: "Verify the SSH host, port, network path, and server availability.",
        retryable: true,
        stage: "ssh",
        summary: "The SSH target could not establish a handshake.",
        target: "kafka-lab.example.test:22",
      },
      id: "discovery-failed",
      ok: false,
      version: HOST_PROTOCOL_VERSION,
    };
    const user = userEvent.setup();
    const onOpenActivity = vi.fn();
    render(<Harness host={host} onOpenActivity={onOpenActivity} />);
    await fillTarget(user);

    await user.click(screen.getByRole("button", { name: "Retrieve" }));

    expect(
      await screen.findByText(/The SSH target could not establish a handshake\./u),
    ).toHaveTextContent("Verify the SSH host, port, network path, and server availability.");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "SSH host" })).toBeEnabled();
    expect(screen.getByLabelText("SSH password")).toHaveValue("ssh-password");
    expect(host.commands).toHaveLength(1);
    await user.click(screen.getByRole("button", { name: "Open activity log" }));
    expect(onOpenActivity).toHaveBeenCalledOnce();
    expect(onOpenActivity).toHaveBeenCalledWith("correlation-discovery-failed");
    expect(screen.getByRole("textbox", { name: "SSH host" })).toHaveValue("kafka-lab.example.test");
  });

  it("shows precise host recovery and keeps the submitted fields available for retry", async () => {
    const host = new FakeHost();
    host.failure = {
      command: "trustAcquisition.material.fetch",
      error: {
        activeStateChanged: false,
        code: "SSH_AUTHENTICATION",
        correlationId: "correlation-failed",
        recovery: "Verify the ephemeral SSH username and password, then try again.",
        retryable: false,
        stage: "ssh",
        summary: "The SSH server rejected password authentication.",
        target: "kafka-lab.example.test:22",
      },
      id: "failed",
      ok: false,
      version: HOST_PROTOCOL_VERSION,
    };
    const user = userEvent.setup();
    render(<Harness host={host} />);
    await fillTarget(user);

    await user.click(screen.getByRole("button", { name: "Retrieve" }));

    expect(
      await screen.findByText(/The SSH server rejected password authentication\./u),
    ).toBeVisible();
    expect(
      screen.getByText(/The SSH server rejected password authentication\./u),
    ).toHaveTextContent("Verify the ephemeral SSH username and password");
    expect(screen.getByLabelText("SSH password")).toHaveValue("ssh-password");
  });
});
