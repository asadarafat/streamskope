import { useMemo } from "react";

import type { StreamSkopeAppProperties } from "../../../features/kafka/ui/StreamSkopeApp";
import { createKafkaWorkspaceRegistration } from "../../../features/kafka/ui/provider-workspace";
import {
  createNatsWorkspaceRegistration,
  type NatsWorkspaceRegistrationProperties,
} from "../../../features/nats/ui/provider-workspace";
import { ProviderApplication } from "../../ui/ProviderApplication";
import { StreamSkopeThemeProvider } from "../../ui/StreamSkopeThemeProvider";
import "../../ui/application.css";

export interface StreamSkopeProductAppProperties {
  readonly kafka: Omit<StreamSkopeAppProperties, "providerControl" | "isInteractive">;
  readonly nats: NatsWorkspaceRegistrationProperties;
}

/** Product composition; each feature owns its typed workspace and cleanup. */
export function StreamSkopeProductApp({
  kafka,
  nats,
}: StreamSkopeProductAppProperties): React.JSX.Element {
  const workspaces = useMemo(
    () => [createKafkaWorkspaceRegistration(kafka), createNatsWorkspaceRegistration(nats)],
    [
      kafka.host,
      kafka.desktop,
      kafka.initialQueryImport,
      kafka.pluginImporter,
      kafka.streamMonitorObserver,
      nats.resolveSource,
    ],
  );
  return (
    <StreamSkopeThemeProvider>
      <ProviderApplication workspaces={workspaces} />
    </StreamSkopeThemeProvider>
  );
}
