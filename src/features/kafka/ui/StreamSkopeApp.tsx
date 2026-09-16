import type { StreamSkopeDesktop } from "../../../platform/desktop";
import type { StreamSkopeHost } from "../contracts";
import { StreamSkopeThemeProvider } from "../../../platform/ui/StreamSkopeThemeProvider";

import { StreamSkopeWorkbench, type StreamSkopeWorkbenchProperties } from "./StreamSkopeWorkbench";
import "./application.css";

export interface StreamSkopeAppProperties {
  readonly desktop?: StreamSkopeDesktop | undefined;
  readonly host: StreamSkopeHost;
  readonly streamMonitorObserver?: StreamSkopeWorkbenchProperties["streamMonitorObserver"];
}

/**
 * Product composition root shared by browser development and Electron.
 * Host-specific adapters stop here; Kafka features own behavior and shared UI
 * owns presentation contracts below this boundary.
 */
export function StreamSkopeApp({
  desktop,
  host,
  streamMonitorObserver,
}: StreamSkopeAppProperties): React.JSX.Element {
  return (
    <StreamSkopeThemeProvider>
      <StreamSkopeWorkbench
        desktop={desktop}
        host={host}
        {...(streamMonitorObserver === undefined ? {} : { streamMonitorObserver })}
      />
    </StreamSkopeThemeProvider>
  );
}
