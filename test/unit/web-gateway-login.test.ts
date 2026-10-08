// @vitest-environment jsdom
import { runInThisContext } from "node:vm";

import { fireEvent, waitFor } from "@testing-library/dom";
import { afterEach, expect, it, vi } from "vitest";

import { createOperationalDiagnostic } from "../../src/platform/diagnostics";
import {
  WEB_GATEWAY_LOGIN_SCRIPT,
  webGatewayLoginPage,
} from "../../src/platform/node/web-gateway-assets";

afterEach(() => {
  vi.unstubAllGlobals();
  document.body.innerHTML = "";
});
function login(response: unknown): HTMLFormElement {
  document.body.innerHTML = webGatewayLoginPage("unlock");
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({ ok: false, status: 503, json: () => Promise.resolve(response) }),
  );
  // Exercise the exact script served before the authenticated renderer is available.
  runInThisContext(`{\n${WEB_GATEWAY_LOGIN_SCRIPT}\n}`);
  const form = document.querySelector("form")!;
  (form.querySelector("input") as HTMLInputElement).value = "private-vault-passphrase";
  return form;
}
it("displays actionable correlated failure and downloads only catalog-owned diagnostic fields", async () => {
  const diagnostic = createOperationalDiagnostic("KAFKA_RUNTIME_START_FAILED");
  const form = login({
    error: {
      summary: "private injected upstream error",
      diagnostic: { ...diagnostic, token: "private-token" },
    },
  });
  const downloads: string[] = [];
  vi.stubGlobal(
    "Blob",
    class {
      constructor(parts: string[]) {
        downloads.push(parts.join(""));
      }
    },
  );
  const release = vi.fn();
  vi.stubGlobal(
    "URL",
    class extends URL {
      static override createObjectURL = (): string => "blob:fixture";
      static override revokeObjectURL = release;
    },
  );
  const click = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => undefined);
  fireEvent.submit(form);
  await waitFor(() =>
    expect(form.querySelector('[role="alert"]')!.textContent).toContain(diagnostic.correlationId),
  );
  expect(form.textContent).toContain(diagnostic.recovery);
  const button = form.querySelector("#diagnostic") as HTMLButtonElement;
  expect(button.hidden).toBe(false);
  fireEvent.click(button);
  expect(click).toHaveBeenCalledOnce();
  expect(release).toHaveBeenCalledWith("blob:fixture");
  expect(JSON.parse(downloads[0]!)).toEqual(diagnostic);
  expect(downloads[0]).not.toMatch(/private|token|passphrase/u);
});
it("refuses mismatched diagnostic text instead of displaying or downloading upstream detail", async () => {
  const form = login({
    error: {
      summary: "private password detail",
      diagnostic: { ...createOperationalDiagnostic("HOST_FAILURE"), recovery: "private endpoint" },
    },
  });
  fireEvent.submit(form);
  await waitFor(() =>
    expect(form.querySelector('[role="alert"]')!.textContent).toContain("could not be unlocked"),
  );
  expect(form.textContent).not.toMatch(/private|endpoint|password detail/u);
  expect((form.querySelector("#diagnostic") as HTMLButtonElement).hidden).toBe(true);
});
