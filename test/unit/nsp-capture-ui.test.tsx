// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { NspCaptureDialog } from "../../plugins/nsp/ui/NspCaptureDialog";
import type { NspUiHost } from "../../plugins/nsp/ui/host";
import type {
  NspInputs,
  NspMethod,
  NspProgress,
  NspResult,
  NspStatus,
} from "../../plugins/nsp/contracts";

afterEach(cleanup);

class NspHost implements NspUiHost {
  readonly calls: { method: NspMethod; input: NspInputs[NspMethod]; requestId?: string }[] = [];
  readonly listeners = new Set<(progress: NspProgress) => void>();
  status: NspStatus = { state: "idle" };
  next: Promise<NspResult> = Promise.resolve({ ok: true, profileId: "saved-nsp" });
  execute<Method extends NspMethod>(
    method: Method,
    input: NspInputs[Method],
    requestId?: string,
  ): Promise<NspResult> {
    this.calls.push({ method, input, ...(requestId === undefined ? {} : { requestId }) });
    if (method === "nspCapture.status") return Promise.resolve({ ok: true, status: this.status });
    if (method === "nspCapture.cancel") return Promise.resolve({ ok: true, cancelled: true });
    if (method === "nspCapture.cleanup") {
      this.status = { state: "idle" };
      return Promise.resolve({ ok: true, status: this.status });
    }
    return this.next;
  }
  subscribe(listener: (progress: NspProgress) => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }
}

function pendingResult(): { promise: Promise<NspResult>; resolve: (value: NspResult) => void } {
  let complete: ((value: NspResult) => void) | undefined;
  const promise = new Promise<NspResult>((resolve) => {
    complete = resolve;
  });
  return { promise, resolve: (value) => complete!(value) };
}

async function fillCredentials(
  user: ReturnType<typeof userEvent.setup>,
  apiUrl = true,
): Promise<void> {
  if (apiUrl) await user.type(screen.getByLabelText("NSP API URL"), "https://nsp.example.test");
  await user.type(screen.getByLabelText("NSP username"), "operator");
  await user.type(screen.getByLabelText("NSP password"), "test-password");
}

describe("NSP API connection view", () => {
  it("defaults to verified API TLS and delegates secret retrieval to the backend", async () => {
    const host = new NspHost();
    const onReady = vi.fn();
    const user = userEvent.setup();
    render(<NspCaptureDialog host={host} onClose={() => undefined} onProfileReady={onReady} />);
    expect(screen.getByRole("checkbox", { name: "Verify NSP API certificate" })).toBeChecked();
    await fillCredentials(user);
    await user.click(screen.getByRole("button", { name: "Create connection profile" }));
    await waitFor(() => expect(onReady).toHaveBeenCalledWith("saved-nsp"));
    expect(host.calls.find((call) => call.method === "nspCapture.connect")?.input).toEqual({
      apiUrl: "https://nsp.example.test",
      username: "operator",
      password: "test-password",
      verifyCertificate: true,
      authentication: "auto",
    });
    expect(screen.getByLabelText("NSP password")).toHaveValue("");
  });

  it("warns explicitly when verification is disabled and preserves inputs after failure", async () => {
    const host = new NspHost();
    host.next = Promise.resolve({
      ok: false,
      error: {
        activeStateChanged: false,
        code: "HTTPS_AUTHENTICATION",
        correlationId: "nsp-test",
        recovery: "Check NSP credentials and retry.",
        retryable: true,
        stage: "backend",
        summary: "NSP sign-in failed.",
      },
    });
    const user = userEvent.setup();
    render(
      <NspCaptureDialog host={host} onClose={() => undefined} onProfileReady={() => undefined} />,
    );
    await fillCredentials(user);
    await user.click(screen.getByRole("checkbox", { name: "Verify NSP API certificate" }));
    expect(screen.getByText(/Certificate verification is disabled/)).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Create connection profile" }));
    expect(await screen.findByText(/NSP sign-in failed/)).toBeVisible();
    expect(screen.getByLabelText("NSP password")).toHaveValue("test-password");
    expect(screen.getByRole("button", { name: "Create connection profile" })).toBeEnabled();
  });

  it("prevents duplicate submissions and waits for cleanup after cancellation", async () => {
    const host = new NspHost();
    const pending = pendingResult();
    host.next = pending.promise;
    const user = userEvent.setup();
    const ready = vi.fn();
    render(<NspCaptureDialog host={host} onClose={() => undefined} onProfileReady={ready} />);
    await fillCredentials(user);
    await user.dblClick(screen.getByRole("button", { name: "Create connection profile" }));
    const calls = host.calls.filter((call) => call.method === "nspCapture.connect");
    expect(calls).toHaveLength(1);
    await user.click(screen.getByRole("button", { name: "Cancel operation" }));
    expect(host.calls.find((call) => call.method === "nspCapture.cancel")?.input).toEqual({
      requestId: calls[0]?.requestId,
    });
    expect(screen.getByText(/Waiting for remote cleanup/)).toBeVisible();
    expect(screen.getByRole("button", { name: "Create connection profile" })).toBeDisabled();
    await act(async () => {
      pending.resolve({ ok: true, cancelled: true });
      await pending.promise;
    });
    expect(await screen.findByText("Operation cancelled. No new profile was saved.")).toBeVisible();
    expect(ready).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Create connection profile" })).toBeEnabled();
  });

  it("offers recovery cleanup and retains the API inputs afterward", async () => {
    const host = new NspHost();
    host.status = { state: "cleanup-required", message: "Previous workflow output needs removal." };
    const user = userEvent.setup();
    render(
      <NspCaptureDialog host={host} onClose={() => undefined} onProfileReady={() => undefined} />,
    );
    await screen.findByRole("button", { name: "Retry cleanup" });
    await fillCredentials(user);
    await user.click(screen.getByRole("button", { name: "Retry cleanup" }));
    expect(await screen.findByText("Remote cleanup confirmed.")).toBeVisible();
    expect(host.calls.find((call) => call.method === "nspCapture.cleanup")?.input).toEqual({
      apiUrl: "https://nsp.example.test",
      username: "operator",
      password: "test-password",
      verifyCertificate: true,
    });
    expect(screen.getByLabelText("NSP password")).toHaveValue("test-password");
    expect(screen.getByRole("button", { name: "Create connection profile" })).toBeEnabled();
  });

  it("refreshes the same recorded profile and excludes unrelated progress", async () => {
    const host = new NspHost();
    const pending = pendingResult();
    host.next = pending.promise;
    const user = userEvent.setup();
    render(
      <NspCaptureDialog
        host={host}
        profileId="existing-nsp"
        source={{
          apiUrl: "https://nsp.example.test",
          brokers: ["broker:9193"],
          workflowName: "owned-workflow",
          authentication: "oauth",
        }}
        onClose={() => undefined}
        onProfileReady={() => undefined}
      />,
    );
    expect(screen.getByLabelText("NSP API URL")).toBeDisabled();
    await fillCredentials(user, false);
    await user.click(screen.getByRole("button", { name: "Refresh credentials" }));
    const call = host.calls.find((entry) => entry.method === "nspCapture.connect");
    expect(call?.input).toMatchObject({
      profileId: "existing-nsp",
      authentication: "oauth",
      brokers: ["broker:9193"],
    });
    act(() => {
      for (const listener of host.listeners)
        listener({ requestId: "foreign-request", step: "retrieve", message: "Unrelated progress" });
    });
    expect(screen.queryByText("Unrelated progress")).not.toBeInTheDocument();
    act(() => {
      for (const listener of host.listeners)
        listener({
          requestId: call!.requestId!,
          step: "test",
          message: "Testing broker connectivity",
        });
    });
    expect(screen.getByText("Testing broker connectivity")).toBeVisible();
    await act(async () => {
      pending.resolve({ ok: true, profileId: "existing-nsp" });
      await pending.promise;
    });
  });
});
