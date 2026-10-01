import { useEffect, useRef, useState } from "react";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import type { TrustAcquisitionEditor } from "../contracts/remote-trust-types";

export function useTrustAcquisitionEditor(
  host: StreamSkopeHost,
  profile?: { readonly id: string; readonly revision: number },
): {
  editor: TrustAcquisitionEditor | null;
  error: string | undefined;
  advance: () => Promise<TrustAcquisitionEditor>;
} {
  const [editor, setEditor] = useState<TrustAcquisitionEditor | null>(null);
  const [error, setError] = useState<string>();
  const current = useRef<TrustAcquisitionEditor | null>(null);
  const profileId = profile?.id;
  const profileRevision = profile?.revision;
  useEffect(() => {
    let active = true;
    setError(undefined);
    setEditor(null);
    const close = (editorId: string): void => {
      void host
        .execute({
          command: "trustAcquisition.editor.close",
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: { editorId },
        })
        .catch(() => undefined);
    };
    void host
      .execute({
        command: "trustAcquisition.editor.open",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload:
          profileId === undefined || profileRevision === undefined
            ? {}
            : { profile: { id: profileId, revision: profileRevision } },
      })
      .then((response) => {
        if (response.ok && "editor" in response.result) {
          if (!active) {
            close(response.result.editor.id);
            return;
          }
          current.current = response.result.editor;
          setEditor(response.result.editor);
        } else if (active)
          setError(
            response.ok
              ? "The host returned no trust editor. Reopen the profile to retry."
              : `${response.error.summary} ${response.error.recovery}`,
          );
      })
      .catch(() => {
        if (active) setError("The trust editor could not be opened. Reopen the profile to retry.");
      });
    return (): void => {
      active = false;
      const previous = current.current;
      current.current = null;
      if (previous !== null) close(previous.id);
    };
  }, [host, profileId, profileRevision]);

  async function advance(): Promise<TrustAcquisitionEditor> {
    const previous = current.current;
    if (previous === null)
      throw new Error("The trust editor is not ready. Reopen the profile to retry.");
    const next = { id: previous.id, generation: previous.generation + 1 };
    const response = await host.execute({
      command: "trustAcquisition.editor.advance",
      id: crypto.randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: next,
    });
    if (current.current !== previous)
      throw new Error("The trust editor changed before acquisition began.");
    if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
    current.current = next;
    setEditor(next);
    return next;
  }

  return { editor, error, advance };
}
