import type { StreamSkopeDesktop } from "../../../platform/desktop";
import type { HostEvent, StreamSkopeHost } from "../contracts";
import { StreamSkopeThemeProvider } from "../../../platform/ui/StreamSkopeThemeProvider";

import type { KafkaViewSettings } from "./investigation-view-settings";
import { StreamSkopeWorkbench, type StreamSkopeWorkbenchProperties } from "./StreamSkopeWorkbench";
import "../../../platform/ui/application.css";
import { PluginsProvider, type PluginRendererImporter } from "./PluginsProvider";

export interface StreamSkopeAppProperties {
  readonly desktop?: StreamSkopeDesktop | undefined;
  readonly host: StreamSkopeHost;
  readonly providerControl?: React.ReactNode;
  readonly profilesPage?: React.ReactNode;
  readonly initialConnectionEvent?:
    Extract<HostEvent, { readonly event: "connection.state" }> | undefined;
  readonly isInteractive?: (() => boolean) | undefined;
  readonly initialQueryImport?: string | undefined;
  readonly initialRestoredView?: KafkaViewSettings | undefined;
  readonly onPendingViewConnection?:
    ((query: KafkaViewSettings, profileId: string | undefined) => void) | undefined;
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
  initialRestoredView,
  onPendingViewConnection,
  providerControl,
  profilesPage,
  initialConnectionEvent,
  isInteractive,
}: StreamSkopeAppProperties): React.JSX.Element {
  return (
    <PluginsProvider host={host} importer={pluginImporter}>
      <StreamSkopeWorkbench
        desktop={desktop}
        host={host}
        initialQueryImport={initialQueryImport}
        initialRestoredView={initialRestoredView}
        onPendingViewConnection={onPendingViewConnection}
        providerControl={providerControl}
        profilesPage={profilesPage}
        initialConnectionEvent={initialConnectionEvent}
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
