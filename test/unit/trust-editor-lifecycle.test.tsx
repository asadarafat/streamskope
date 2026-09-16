// @vitest-environment jsdom
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { createKafkaBackend } from "../../src/platform/electron/main";
import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../../src/features/kafka/contracts";
import { useTrustAcquisitionEditor } from "../../src/features/kafka/ui/use-trust-acquisition-editor";

afterEach(cleanup);

describe("host-owned trust editor lifecycle", () => {
  it("opens a host token, advances its generation and closes it on unmount", async () => {
    const backend = createKafkaBackend();
    const host: StreamSkopeHost = {
      execute: backend.execute.bind(backend),
      subscribe: backend.subscribe.bind(backend),
      openExternalUrl: () => Promise.reject(new Error("Not expected")),
    };
    try {
      const { result, unmount } = renderHook(() => useTrustAcquisitionEditor(host));
      await waitFor(() => expect(result.current.editor?.generation).toBe(1));
      const original = result.current.editor!;
      await act(async () => {
        await result.current.advance();
      });
      expect(result.current.editor).toEqual({ id: original.id, generation: 2 });
      unmount();
      await waitFor(async () => {
        const response = await host.execute({
          command: "trustAcquisition.editor.advance",
          id: "closed",
          version: HOST_PROTOCOL_VERSION,
          payload: { id: original.id, generation: 3 },
        });
        expect(response.ok).toBe(false);
      });
    } finally {
      await backend.shutdown();
    }
  });
});
