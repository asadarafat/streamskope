import type { StreamSkopeDesktop } from "../../../platform/desktop";
import type { StreamSkopeHost } from "../contracts";
import { StreamSkopeThemeProvider } from "../../../platform/ui/StreamSkopeThemeProvider";

import { StreamSkopeWorkbench, type StreamSkopeWorkbenchProperties } from "./StreamSkopeWorkbench";
import "./application.css";
import { PluginsProvider, type PluginRendererImporter } from "./PluginsProvider";

export interface StreamSkopeAppProperties {
  readonly desktop?: StreamSkopeDesktop | undefined;
  readonly host: StreamSkopeHost;
  readonly initialQueryImport?: string | undefined;
  readonly pluginImporter?: PluginRendererImporter | undefined;
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
  pluginImporter,
  streamMonitorObserver,
  initialQueryImport,
}: StreamSkopeAppProperties): React.JSX.Element {
  return (
    <StreamSkopeThemeProvider>
      <PluginsProvider host={host} importer={pluginImporter}>
        <StreamSkopeWorkbench
          desktop={desktop}
          host={host}
          initialQueryImport={initialQueryImport}
          {...(streamMonitorObserver === undefined ? {} : { streamMonitorObserver })}
        />
      </PluginsProvider>
    </StreamSkopeThemeProvider>
  );
}
