import type { StreamSkopeDesktop } from "../../../platform/desktop";
import type { StreamSkopeHost } from "../contracts";
import { StreamSkopeThemeProvider } from "../../../platform/ui/StreamSkopeThemeProvider";

import { StreamSkopeWorkbench, type StreamSkopeWorkbenchProperties } from "./StreamSkopeWorkbench";
import "../../../platform/ui/application.css";
import { PluginsProvider, type PluginRendererImporter } from "./PluginsProvider";

export interface StreamSkopeAppProperties {
  readonly desktop?: StreamSkopeDesktop | undefined;
  readonly host: StreamSkopeHost;
  readonly providerControl?: React.ReactNode;
  readonly isInteractive?: (() => boolean) | undefined;
  readonly initialQueryImport?: string | undefined;
  readonly pluginImporter?: PluginRendererImporter | undefined;
  readonly streamMonitorObserver?: StreamSkopeWorkbenchProperties["streamMonitorObserver"];
}

/** Unthemed Kafka workspace; connection plugins remain local to this provider. */
export function KafkaWorkspace({
  desktop,
  host,
  pluginImporter,
  streamMonitorObserver,
  initialQueryImport,
  providerControl,
  isInteractive,
}: StreamSkopeAppProperties): React.JSX.Element {
  return (
    <PluginsProvider host={host} importer={pluginImporter}>
      <StreamSkopeWorkbench
        desktop={desktop}
        host={host}
        initialQueryImport={initialQueryImport}
        providerControl={providerControl}
        isInteractive={isInteractive}
        {...(streamMonitorObserver === undefined ? {} : { streamMonitorObserver })}
      />
    </PluginsProvider>
  );
}

/** Direct Kafka embedding retains its existing theme boundary and public props. */
export function StreamSkopeApp(properties: StreamSkopeAppProperties): React.JSX.Element {
  return (
    <StreamSkopeThemeProvider>
      <KafkaWorkspace {...properties} />
    </StreamSkopeThemeProvider>
  );
}
