import { useState, type Dispatch, type SetStateAction } from "react";

import type { HostCommand, StreamSkopeHost } from "../contracts";

interface HostCommandRunner {
  readonly busy: boolean;
  readonly requestError: string | undefined;
  readonly setRequestError: Dispatch<SetStateAction<string | undefined>>;
  readonly run: (command: HostCommand) => Promise<boolean>;
}

export function useHostCommand(host: StreamSkopeHost, rejectionMessage: string): HostCommandRunner {
  const [requestError, setRequestError] = useState<string>();
  const [busy, setBusy] = useState(false);
  const run = async (command: HostCommand): Promise<boolean> => {
    setBusy(true);
    setRequestError(undefined);
    try {
      const response = await host.execute(command);
      if (!response.ok) {
        setRequestError(`${response.error.summary} ${response.error.recovery}`);
        return false;
      }
      return true;
    } catch {
      setRequestError(rejectionMessage);
      return false;
    } finally {
      setBusy(false);
    }
  };
  return { busy, requestError, setRequestError, run };
}
