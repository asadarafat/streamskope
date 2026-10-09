// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import { ProfileProtectedFileField } from "../../src/features/kafka/ui/ProfileProtectedFields";

class DeferredReader extends EventTarget {
  static current: DeferredReader | undefined;
  result: string | null = null;
  aborted = false;
  readAsText(): void {
    DeferredReader.current = this;
  }
  abort(): void {
    this.aborted = true;
    this.dispatchEvent(new ProgressEvent("abort"));
    this.dispatchEvent(new ProgressEvent("loadend"));
  }
  complete(): void {
    this.result = "private-key-content";
    this.dispatchEvent(new ProgressEvent("load"));
    this.dispatchEvent(new ProgressEvent("loadend"));
  }
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  DeferredReader.current = undefined;
});

it("uses the latest form callback when a protected file completes after another field changes", async () => {
  vi.stubGlobal("FileReader", DeferredReader);
  const stale = vi.fn();
  const current = vi.fn();
  const initial = { value: "", retain: false };
  const view = render(
    <ProfileProtectedFileField
      disabled={false}
      label="Client private key"
      value={initial}
      onChange={stale}
    />,
  );
  await userEvent.upload(
    screen.getByLabelText("Client private key file"),
    new File(["pending"], "key.pem"),
  );
  view.rerender(
    <ProfileProtectedFileField
      disabled={false}
      label="Client private key"
      value={initial}
      onChange={current}
    />,
  );
  await act(async () => {
    DeferredReader.current?.complete();
    await Promise.resolve();
  });
  expect(stale).not.toHaveBeenCalled();
  expect(current).toHaveBeenCalledExactlyOnceWith(
    { value: "private-key-content", retain: false },
    "key.pem",
  );
  expect(screen.queryByText("private-key-content")).not.toBeInTheDocument();
});

it.each(["disabled", "unmounted"] as const)(
  "aborts a protected file read when its editor is %s",
  async (state) => {
    vi.stubGlobal("FileReader", DeferredReader);
    const change = vi.fn();
    const field = { value: "", retain: true };
    const view = render(
      <ProfileProtectedFileField
        disabled={false}
        label="Client private key"
        value={field}
        onChange={change}
      />,
    );
    await userEvent.upload(
      screen.getByLabelText("Client private key file"),
      new File(["pending"], "key.pem"),
    );
    const reader = DeferredReader.current;
    if (state === "disabled")
      view.rerender(
        <ProfileProtectedFileField
          disabled
          label="Client private key"
          value={field}
          onChange={change}
        />,
      );
    else view.unmount();
    await act(async () => {
      reader?.complete();
      await Promise.resolve();
    });
    expect(reader?.aborted).toBe(true);
    expect(change).not.toHaveBeenCalled();
  },
);
