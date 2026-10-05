import type { ReactNode } from "react";
import Box from "@mui/material/Box";
import Drawer from "@mui/material/Drawer";

import { streamSkopeLayout, streamSkopeSpacing } from "./createStreamSkopeTheme";
import { ProductApplicationBar } from "./ProductApplicationBar";
import { ProductResourceSidebar, type ProductResourceGroup } from "./ProductResourceSidebar";
import type { ProductNavigator } from "./use-product-navigator";

export interface ProviderWorkbenchShellProperties<Destination extends string> {
  readonly navigator: ProductNavigator;
  readonly resources: readonly ProductResourceGroup<Destination>[];
  readonly navigation: Destination;
  readonly resourceLabel: string;
  readonly onNavigate: (destination: Destination) => void;
  readonly onOpenCommandPalette?: (() => void) | undefined;
  readonly onOpenPreferences?: (() => void) | undefined;
  readonly providerControl?: ReactNode;
  readonly headerActions?: ReactNode;
  readonly breadcrumbs: ReactNode;
  readonly activity: ReactNode;
  readonly activityHeight: number;
  readonly status: ReactNode;
  readonly overlays?: ReactNode;
  readonly children: ReactNode;
}

/** One product frame; provider-owned slots describe available workflows and their evidence. */
export function ProviderWorkbenchShell<Destination extends string>({
  navigator,
  resources,
  navigation,
  resourceLabel,
  onNavigate,
  onOpenCommandPalette,
  onOpenPreferences,
  providerControl,
  headerActions,
  breadcrumbs,
  activity,
  activityHeight,
  status,
  overlays,
  children,
}: ProviderWorkbenchShellProperties<Destination>): React.JSX.Element {
  const sidebar = (
    <ProductResourceSidebar groups={resources} navigation={navigation} onChange={onNavigate} />
  );
  return (
    <Box
      sx={{
        bgcolor: "background.default",
        color: "text.primary",
        display: "grid",
        gridTemplateAreas: navigator.temporary
          ? '"application" "workspace" "status"'
          : '"application application" "sidebar workspace" "status status"',
        gridTemplateColumns: navigator.temporary
          ? "minmax(0, 1fr)"
          : `${String(streamSkopeLayout.resourceDefaultWidth)}px minmax(0, 1fr)`,
        gridTemplateRows: `${String(streamSkopeLayout.headerHeight)}px minmax(0, 1fr) ${String(streamSkopeLayout.statusBarHeight)}px`,
        height: "100dvh",
        minHeight: 480,
        minWidth: 0,
        overflow: "clip",
      }}
    >
      <Box sx={{ gridArea: "application", minWidth: 0 }}>
        <ProductApplicationBar
          navigatorOpen={navigator.open}
          navigatorTemporary={navigator.temporary}
          onToggleNavigator={navigator.toggle}
          resourceLabel={resourceLabel}
          providerControl={providerControl}
          actions={headerActions}
          onOpenCommandPalette={onOpenCommandPalette}
          onOpenPreferences={onOpenPreferences}
        />
      </Box>
      {navigator.temporary ? (
        <Drawer
          anchor="left"
          onClose={navigator.close}
          open={navigator.open}
          slotProps={{
            paper: {
              "aria-label": "StreamSkope resources drawer",
              sx: {
                bottom: `${String(streamSkopeLayout.statusBarHeight)}px`,
                height: "auto",
                maxWidth: "88vw",
                top: `${String(streamSkopeLayout.headerHeight)}px`,
                width: streamSkopeLayout.resourceDefaultWidth,
              },
            },
          }}
          variant="temporary"
        >
          {sidebar}
        </Drawer>
      ) : (
        <Box component="aside" sx={{ gridArea: "sidebar", minHeight: 0 }}>
          {sidebar}
        </Box>
      )}
      <Box
        sx={{
          display: "grid",
          gridArea: "workspace",
          gridTemplateRows: `${String(streamSkopeLayout.breadcrumbBarHeight)}px minmax(0, 1fr) ${String(activityHeight)}px`,
          minHeight: 0,
          minWidth: 0,
          overflow: "hidden",
        }}
      >
        {breadcrumbs}
        <Box
          id="streamskope-active-page"
          sx={{ display: "grid", minHeight: 0, minWidth: 0, overflow: "hidden" }}
        >
          {children}
        </Box>
        {activity}
      </Box>
      <Box
        component="footer"
        sx={{
          gridArea: "status",
          alignItems: "center",
          bgcolor: "background.paper",
          borderTop: 1,
          borderColor: "divider",
          display: "flex",
          gap: `${String(streamSkopeSpacing.scale.space12)}px`,
          height: streamSkopeLayout.statusBarHeight,
          minWidth: 0,
          overflow: "hidden",
          px: `${String(streamSkopeSpacing.scale.space12)}px`,
        }}
      >
        {status}
      </Box>
      {overlays}
    </Box>
  );
}
