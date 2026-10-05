// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { NatsProfileSummary } from "../../src/features/nats/contracts";
import {
  ProfileEditor,
  type ProfileEditorProperties,
} from "../../src/features/nats/ui/ProfileEditor";

const pem = "-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n";
const profile: NatsProfileSummary = {
  id: "profile-saved",
  revision: 7,
  name: "Saved NATS",
  servers: ["tls://nats.example:4222"],
  authentication: { mode: "token", tokenPresent: true },
  tls: { mode: "tls", caPresent: true },
  createdAt: "2026-10-05T10:00:00.000Z",
  updatedAt: "2026-10-05T10:00:00.000Z",
};

function properties(overrides: Partial<ProfileEditorProperties> = {}): ProfileEditorProperties {
  return {
    profile: null,
    available: true,
    isInteractive: () => true,
    failure: null,
    onCreate: vi.fn<ProfileEditorProperties["onCreate"]>().mockResolvedValue(false),
    onUpdate: vi.fn<ProfileEditorProperties["onUpdate"]>().mockResolvedValue(false),
    onClose: vi.fn(),
    ...overrides,
  };
}

async function choose(label: string, option: string): Promise<void> {
  const user = userEvent.setup();
  await user.click(screen.getByRole("combobox", { name: label }));
  await user.click(screen.getByRole("option", { name: option }));
}

function fillCreate(): void {
  fireEvent.change(screen.getByRole("textbox", { name: "Profile name" }), {
    target: { value: "New NATS" },
  });
  fireEvent.change(screen.getByRole("textbox", { name: "NATS servers" }), {
    target: { value: "tls://nats.example\ntls://backup.example:4223" },
  });
}

afterEach(cleanup);

describe("NATS profile editor secret and revision semantics", () => {
  it("focuses the profile name and creates explicit replacement secrets with verified TLS", async () => {
    const props = properties();
    render(<ProfileEditor {...props} />);
    await waitFor(() =>
      expect(screen.getByRole("textbox", { name: "Profile name" })).toHaveFocus(),
    );
    fillCreate();
    await choose("Authentication", "Token");
    fireEvent.change(screen.getByLabelText("Token", { exact: true }), {
      target: { value: "unit-test-token" },
    });
    fireEvent.change(screen.getByRole("textbox", { name: "CA certificate PEM" }), {
      target: { value: pem },
    });
    await userEvent.setup().click(screen.getByRole("button", { name: "Save profile" }));
    expect(props.onCreate).toHaveBeenCalledWith({
      name: "New NATS",
      servers: ["tls://nats.example:4222", "tls://backup.example:4223"],
      authentication: { mode: "token", token: { mode: "replace", value: "unit-test-token" } },
      tls: { mode: "tls", caPem: { mode: "replace", value: pem } },
    });
    expect(props.onUpdate).not.toHaveBeenCalled();
  });

  it("retains blank host-held secrets and the revision captured when editing began", async () => {
    const props = properties({ profile });
    const view = render(<ProfileEditor {...props} />);
    expect(screen.getByLabelText("Token", { exact: true })).toHaveValue("");
    expect(screen.getByRole("textbox", { name: "CA certificate PEM" })).toHaveValue("");
    view.rerender(
      <ProfileEditor {...props} profile={{ ...profile, revision: 8, name: "Concurrent update" }} />,
    );
    await userEvent.setup().click(screen.getByRole("button", { name: "Save profile" }));
    expect(props.onUpdate).toHaveBeenCalledWith(profile, {
      name: "Saved NATS",
      servers: profile.servers,
      authentication: { mode: "token", token: { mode: "retain" } },
      tls: { mode: "tls", caPem: { mode: "retain" } },
    });
  });

  it("requires a token when no saved token exists instead of inventing a retain operation", async () => {
    const props = properties({
      profile: { ...profile, authentication: { mode: "token", tokenPresent: false } },
    });
    render(<ProfileEditor {...props} />);
    await userEvent.setup().click(screen.getByRole("button", { name: "Save profile" }));
    expect(props.onUpdate).not.toHaveBeenCalled();
    expect(
      screen.getByText("Enter a token of at most 16 KiB without control characters."),
    ).toBeVisible();
  });

  it("clears authentication with None and clears a saved CA with verified system trust", async () => {
    const props = properties({ profile });
    render(<ProfileEditor {...props} />);
    await choose("Authentication", "None");
    await choose("CA handling", "Use system trust");
    await userEvent.setup().click(screen.getByRole("button", { name: "Save profile" }));
    expect(props.onUpdate).toHaveBeenCalledWith(profile, {
      name: profile.name,
      servers: profile.servers,
      authentication: { mode: "none" },
      tls: { mode: "tls", caPem: { mode: "clear" } },
    });
  });

  it("clears secret buffers on dismissal even before its parent removes the dialog", async () => {
    const props = properties({ profile });
    render(<ProfileEditor {...props} />);
    fireEvent.change(screen.getByLabelText("Token", { exact: true }), {
      target: { value: "unit-test-replacement" },
    });
    fireEvent.change(screen.getByRole("textbox", { name: "CA certificate PEM" }), {
      target: { value: pem },
    });
    await userEvent.setup().click(screen.getByRole("button", { name: "Cancel" }));
    expect(props.onClose).toHaveBeenCalledOnce();
    expect(screen.getByLabelText("Token", { exact: true })).toHaveValue("");
    expect(screen.getByRole("textbox", { name: "CA certificate PEM" })).toHaveValue("");
    expect(props.onUpdate).not.toHaveBeenCalled();
  });

  it("checks the live activation getter when a portalled form callback is admitted", async () => {
    let interactive = true;
    const props = properties({ profile, isInteractive: () => interactive });
    render(<ProfileEditor {...props} />);
    interactive = false;
    await userEvent.setup().click(screen.getByRole("button", { name: "Save profile" }));
    expect(props.onUpdate).not.toHaveBeenCalled();
    interactive = true;
    await userEvent.setup().click(screen.getByRole("button", { name: "Save profile" }));
    expect(props.onUpdate).toHaveBeenCalledOnce();
  });

  it("honors an already admitted successful write after retirement and clears secret buffers", async () => {
    let interactive = true;
    let complete: ((accepted: boolean) => void) | undefined;
    const props = properties({
      profile,
      isInteractive: () => interactive,
      onUpdate: vi.fn(
        () =>
          new Promise<boolean>((resolve) => {
            complete = resolve;
          }),
      ),
    });
    render(<ProfileEditor {...props} />);
    fireEvent.change(screen.getByLabelText("Token", { exact: true }), {
      target: { value: "unit-test-replacement" },
    });
    fireEvent.change(screen.getByRole("textbox", { name: "CA certificate PEM" }), {
      target: { value: pem },
    });
    await userEvent.setup().click(screen.getByRole("button", { name: "Save profile" }));
    expect(props.onUpdate).toHaveBeenCalledOnce();
    interactive = false;
    await act(async () => {
      complete?.(true);
      await Promise.resolve();
    });
    expect(props.onClose).toHaveBeenCalledOnce();
    expect(screen.getByLabelText("Token", { exact: true })).toHaveValue("");
    expect(screen.getByRole("textbox", { name: "CA certificate PEM" })).toHaveValue("");
  });
});
