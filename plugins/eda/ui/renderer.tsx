import { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import { ThemeProvider, useColorScheme } from "@mui/material/styles";

import type { PluginRenderer, PluginViewContext } from "../../../src/plugins/renderer-api";
import { streamSkopeTheme } from "../../../src/platform/ui/createStreamSkopeTheme";
import { EDA_PLUGIN_ID, fromPluginProfileSource } from "../contracts";

import { EdaCaptureDialog } from "./EdaCaptureDialog";
import { EdaCaptureStatusPanel } from "./EdaCaptureStatusPanel";
import { createEdaUiHost } from "./host";

function PluginThemeMode({ mode }: { readonly mode: PluginViewContext["themeMode"] }): null {
  const { setMode } = useColorScheme();
  useEffect(() => {
    setMode(mode ?? "system");
  }, [mode, setMode]);
  return null;
}

function EdaPluginView({
  context,
}: {
  readonly context: PluginViewContext;
}): React.JSX.Element | null {
  const host = useMemo(() => createEdaUiHost(context.host), [context.host]);
  const [resumeOpen, setResumeOpen] = useState(false);
  if (context.view === "connection") {
    return (
      <EdaCaptureDialog
        host={host}
        open
        profiles={context.profiles}
        onClose={context.onClose}
        onProfileReady={context.onProfileReady}
        onExistingDestination={context.onExistingDestination}
      />
    );
  }
  const source = fromPluginProfileSource(context.profile.source);
  if (source === undefined) return null;
  return (
    <>
      <EdaCaptureStatusPanel
        host={host}
        source={source}
        connected={context.profile.active}
        onResume={() => setResumeOpen(true)}
      />
      {resumeOpen ? (
        <EdaCaptureDialog
          host={host}
          open
          profiles={context.profiles}
          resume={source}
          resumeProfileId={context.profile.id}
          onClose={() => setResumeOpen(false)}
          onProfileReady={() => setResumeOpen(false)}
        />
      ) : null}
    </>
  );
}

const renderer: PluginRenderer = {
  apiVersion: 4,
  id: EDA_PLUGIN_ID,
  connectionActions: [{ id: "capture", label: "Connect via EDA" }],
  profileLabel: (source) =>
    fromPluginProfileSource(source) === undefined ? undefined : "EDA capture profile",
  profileSummary: (source) => {
    const capture = fromPluginProfileSource(source);
    return capture === undefined ? undefined : `EDA API capture · ${capture.source.name}`;
  },
  mount: (element, initialContext) => {
    const root = createRoot(element, {
      identifierPrefix: `streamskope-eda-${globalThis.crypto.randomUUID()}-`,
    });
    function update(context: PluginViewContext): void {
      root.render(
        <ThemeProvider
          defaultMode={context.themeMode ?? "system"}
          noSsr
          storageManager={null}
          theme={streamSkopeTheme}
        >
          <PluginThemeMode mode={context.themeMode} />
          <EdaPluginView context={context} />
        </ThemeProvider>,
      );
    }
    update(initialContext);
    return { update, dispose: () => root.unmount() };
  },
};

export default renderer;
