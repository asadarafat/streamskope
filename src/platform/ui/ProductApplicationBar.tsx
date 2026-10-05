import { useState, type ReactNode } from "react";
import AppBar from "@mui/material/AppBar";
import Box from "@mui/material/Box";
import Toolbar from "@mui/material/Toolbar";
import Typography from "@mui/material/Typography";
import { useColorScheme } from "@mui/material/styles";
import NightlightRoundIcon from "@mui/icons-material/NightlightRound";
import SearchIcon from "@mui/icons-material/Search";
import SettingsIcon from "@mui/icons-material/Settings";
import ViewSidebarIcon from "@mui/icons-material/ViewSidebar";

import { streamSkopeGeometry } from "./studioTokens";
import { StreamSkopeAppIcon } from "./StreamSkopeAppIcon";
import {
  StudioButton as Button,
  StudioIconButton as IconButton,
  StudioMenu as Menu,
  StudioMenuItem as MenuItem,
  StudioTooltip as Tooltip,
} from "./controls";

export interface ProductApplicationBarProperties {
  readonly navigatorOpen: boolean;
  readonly navigatorTemporary: boolean;
  readonly actions?: ReactNode;
  readonly providerControl?: ReactNode;
  readonly resourceLabel: string;
  readonly onOpenCommandPalette?: (() => void) | undefined;
  readonly onOpenPreferences?: (() => void) | undefined;
  readonly onToggleNavigator: () => void;
}

export function ProductApplicationBar({
  navigatorOpen,
  navigatorTemporary,
  onOpenCommandPalette,
  actions,
  providerControl,
  resourceLabel,
  onOpenPreferences,
  onToggleNavigator,
}: ProductApplicationBarProperties): React.JSX.Element {
  const { mode, setMode } = useColorScheme();
  const [themeAnchor, setThemeAnchor] = useState<HTMLElement | null>(null);
  const navigatorAction = `${navigatorOpen ? "Close" : "Open"} ${resourceLabel}`;

  return (
    <AppBar
      aria-label="StreamSkope application bar"
      component="header"
      enableColorOnDark
      position="static"
      sx={{
        bgcolor: "var(--streamskope-nav-background)",
        borderBottom: 1,
        borderColor: "divider",
        color: "text.primary",
        height: streamSkopeGeometry.commandBarHeight,
      }}
    >
      <Toolbar
        disableGutters
        variant="dense"
        sx={{
          gap: 1,
          height: streamSkopeGeometry.commandBarHeight,
          minHeight: `${String(streamSkopeGeometry.commandBarHeight)}px !important`,
          px: 1.5,
        }}
      >
        {navigatorTemporary ? (
          <Tooltip title={navigatorAction}>
            <IconButton
              aria-expanded={navigatorOpen}
              aria-label={navigatorAction}
              onClick={onToggleNavigator}
            >
              <ViewSidebarIcon />
            </IconButton>
          </Tooltip>
        ) : null}

        <Box sx={{ alignItems: "center", display: "flex", gap: 1, minWidth: 148 }}>
          <Box
            data-testid="streamskope-brand-mark"
            sx={{
              alignItems: "center",
              display: "flex",
              height: 30,
              justifyContent: "center",
              width: 30,
            }}
          >
            <StreamSkopeAppIcon size={30} />
          </Box>
          <Typography component="h1" noWrap variant="h6">
            StreamSkope
          </Typography>
        </Box>

        {providerControl}
        <Box sx={{ flex: 1 }} />
        {actions}

        {onOpenCommandPalette === undefined ? null : navigatorTemporary ? (
          <Tooltip title="Search and commands">
            <IconButton aria-label="Search and commands" onClick={onOpenCommandPalette}>
              <SearchIcon />
            </IconButton>
          </Tooltip>
        ) : (
          <Button
            aria-label="Search and commands"
            onClick={onOpenCommandPalette}
            startIcon={<SearchIcon />}
            sx={{
              borderColor: "divider",
              color: "text.secondary",
              justifyContent: "space-between",
              minWidth: 184,
            }}
            variant="outlined"
          >
            <Box component="span" sx={{ mr: 2 }}>
              Search
            </Box>
            <Box component="kbd" sx={{ color: "text.secondary", font: "inherit" }}>
              Ctrl K
            </Box>
          </Button>
        )}

        <Tooltip title="Theme">
          <IconButton
            aria-description={`Current theme: ${mode ?? "system"}`}
            aria-controls={themeAnchor === null ? undefined : "streamskope-theme-menu"}
            aria-haspopup="menu"
            aria-label="Theme"
            onClick={(event) => setThemeAnchor(event.currentTarget)}
          >
            <NightlightRoundIcon />
          </IconButton>
        </Tooltip>
        <Menu
          anchorEl={themeAnchor}
          id="streamskope-theme-menu"
          onClose={() => setThemeAnchor(null)}
          open={themeAnchor !== null}
        >
          {(["light", "dark", "system"] as const).map((candidate) => (
            <MenuItem
              key={candidate}
              onClick={() => {
                setMode(candidate);
                setThemeAnchor(null);
              }}
              selected={mode === candidate}
            >
              {candidate.slice(0, 1).toUpperCase() + candidate.slice(1)}
            </MenuItem>
          ))}
        </Menu>

        {onOpenPreferences === undefined ? null : (
          <Tooltip title="Preferences">
            <IconButton aria-label="Preferences" onClick={onOpenPreferences}>
              <SettingsIcon />
            </IconButton>
          </Tooltip>
        )}
      </Toolbar>
    </AppBar>
  );
}
