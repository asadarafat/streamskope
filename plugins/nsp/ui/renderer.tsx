import { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import { ThemeProvider, useColorScheme } from "@mui/material/styles";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";

import type { PluginRenderer, PluginViewContext } from "../../../src/plugins/renderer-api";
import { streamSkopeTheme } from "../../../src/platform/ui/createStreamSkopeTheme";
import { StudioButton } from "../../../src/platform/ui/controls";
import { NSP_PLUGIN_ID, fromPluginProfileSource } from "../contracts";

import { NspCaptureDialog } from "./NspCaptureDialog";
import { createNspUiHost } from "./host";

function ThemeMode({ mode }: { readonly mode: PluginViewContext["themeMode"] }): null {
  const { setMode } = useColorScheme();
  useEffect(() => {
    setMode(mode ?? "system");
  }, [mode, setMode]);
  return null;
}

function NspPluginView({
  context,
}: {
  readonly context: PluginViewContext;
}): React.JSX.Element | null {
  const host = useMemo(() => createNspUiHost(context.host), [context.host]);
  const [refreshOpen, setRefreshOpen] = useState(false);
  if (context.view === "connection")
    return (
      <NspCaptureDialog
        host={host}
        onClose={context.onClose}
        onProfileReady={context.onProfileReady}
      />
    );
  const source = fromPluginProfileSource(context.profile.source);
  if (source === undefined) return null;
  return (
    <Stack spacing={1}>
      <Typography variant="subtitle2">NSP connection</Typography>
      <Typography variant="body2">{source.apiUrl}</Typography>
      <Typography variant="body2">
        Refresh the saved Kafka credentials after NSP rotates its certificates or password.
      </Typography>
      <StudioButton onClick={() => setRefreshOpen(true)}>Refresh NSP credentials</StudioButton>
      {refreshOpen ? (
        <NspCaptureDialog
          key={context.profile.id}
          host={host}
          source={source}
          profileId={context.profile.id}
          onClose={() => setRefreshOpen(false)}
          onProfileReady={() => setRefreshOpen(false)}
        />
      ) : null}
    </Stack>
  );
}

const renderer: PluginRenderer = {
  apiVersion: 4,
  id: NSP_PLUGIN_ID,
  connectionActions: [{ id: "capture", label: "Connect to NSP" }],
  profileLabel: (source) =>
    fromPluginProfileSource(source) === undefined ? undefined : "NSP connection profile",
  profileSummary: (source) => {
    const nsp = fromPluginProfileSource(source);
    return nsp === undefined ? undefined : `NSP Kafka · ${nsp.apiUrl}`;
  },
  mount: (element, initialContext) => {
    const root = createRoot(element);
    function update(context: PluginViewContext): void {
      root.render(
        <ThemeProvider
          defaultMode={context.themeMode ?? "system"}
          noSsr
          storageManager={null}
          theme={streamSkopeTheme}
        >
          <ThemeMode mode={context.themeMode} />
          <NspPluginView context={context} />
        </ThemeProvider>,
      );
    }
    update(initialContext);
    return { update, dispose: () => root.unmount() };
  },
};

export default renderer;
