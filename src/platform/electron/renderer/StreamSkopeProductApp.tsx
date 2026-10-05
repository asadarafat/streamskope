import { useMemo } from "react";

import type { StreamSkopeAppProperties } from "../../../features/kafka/ui/StreamSkopeApp";
import { createKafkaWorkspaceRegistration } from "../../../features/kafka/ui/provider-workspace";
import { ProviderApplication } from "../../ui/ProviderApplication";
import { StreamSkopeThemeProvider } from "../../ui/StreamSkopeThemeProvider";
import "../../ui/application.css";

export interface StreamSkopeProductAppProperties {
  readonly kafka: Omit<StreamSkopeAppProperties, "providerControl" | "isInteractive">;
}

/** Product composition; each feature owns its typed workspace and cleanup. */
export function StreamSkopeProductApp({
  kafka,
}: StreamSkopeProductAppProperties): React.JSX.Element {
  const workspaces = useMemo(
    () => [createKafkaWorkspaceRegistration(kafka)],
    [
      kafka.host,
      kafka.desktop,
      kafka.initialQueryImport,
      kafka.pluginImporter,
      kafka.streamMonitorObserver,
    ],
  );
  return (
    <StreamSkopeThemeProvider>
      <ProviderApplication workspaces={workspaces} />
    </StreamSkopeThemeProvider>
  );
}
