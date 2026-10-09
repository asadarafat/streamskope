import { useCallback, useEffect, useRef, useState } from "react";

import { HOST_PROTOCOL_VERSION, type HostCommand, type StreamSkopeHost } from "../contracts";
import type { KafkaQueryLibrarySnapshot } from "../contracts/query-library";

type LibraryCommand = Extract<
  HostCommand,
  { readonly command: "queries.list" | "queries.put" | "queries.delete" }
>;
export interface InvestigationLibraryController {
  readonly snapshot: KafkaQueryLibrarySnapshot | undefined;
  readonly busy: boolean;
  readonly error: string | undefined;
  execute(this: void, command: LibraryCommand): Promise<boolean>;
  refresh(this: void): Promise<boolean>;
}

/** Dialog-scoped command owner; late replies cannot mutate another host's library. */
export function useInvestigationLibrary(host: StreamSkopeHost): InvestigationLibraryController {
  const [snapshot, setSnapshot] = useState<KafkaQueryLibrarySnapshot>();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const generation = useRef(0);
  const pending = useRef(false);
  const execute = useCallback(
    async (command: LibraryCommand): Promise<boolean> => {
      if (pending.current) return false;
      const current = generation.current;
      pending.current = true;
      setBusy(true);
      setError(undefined);
      try {
        const response = await host.execute(command);
        if (current !== generation.current) return false;
        if (!response.ok) {
          setError(`${response.error.summary} ${response.error.recovery}`);
          return false;
        }
        setSnapshot(response.result.snapshot);
        return true;
      } catch {
        if (current === generation.current)
          setError("The host did not confirm the saved-view operation. Refresh before retrying.");
        return false;
      } finally {
        if (current === generation.current) {
          pending.current = false;
          setBusy(false);
        }
      }
    },
    [host],
  );
  const refresh = useCallback(
    (): Promise<boolean> =>
      execute({
        command: "queries.list",
        id: crypto.randomUUID(),
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    [execute],
  );
  useEffect(() => {
    generation.current++;
    pending.current = false;
    setSnapshot(undefined);
    void refresh();
    return (): void => {
      generation.current++;
      pending.current = false;
    };
  }, [refresh]);
  return { snapshot, busy, error, execute, refresh };
}
