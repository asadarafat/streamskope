import Box from "@mui/material/Box";
import ButtonBase from "@mui/material/ButtonBase";
import Typography from "@mui/material/Typography";

import {
  isNavigationAvailable,
  WORKBENCH_RESOURCE_GROUPS,
  type NavigationView,
} from "./workbench-navigation";

interface WorkbenchSidebarProperties {
  readonly connected: boolean;
  readonly navigation: NavigationView;
  readonly onChange: (navigation: NavigationView) => void;
}

function SidebarButton({
  active,
  available,
  label,
  onClick,
}: {
  readonly active: boolean;
  readonly available: boolean;
  readonly label: string;
  readonly onClick: () => void;
}): React.JSX.Element {
  return (
    <ButtonBase
      aria-current={active ? "page" : undefined}
      aria-disabled={!available || undefined}
      aria-label={label}
      disabled={!available}
      onClick={available ? onClick : undefined}
      sx={{
        borderRadius: 0.75,
        color: active ? "var(--streamskope-nav-text)" : "var(--streamskope-nav-muted)",
        display: "flex",
        justifyContent: "flex-start",
        minHeight: 34,
        mx: 0.75,
        px: 1.25,
        textAlign: "left",
        width: "calc(100% - 12px)",
        "&.Mui-disabled": {
          color: "var(--streamskope-nav-muted)",
          cursor: "not-allowed",
          opacity: 0.48,
        },
        ...(active ? { bgcolor: "var(--streamskope-nav-selected)" } : {}),
        "&:hover": {
          bgcolor: active ? "var(--streamskope-nav-selected)" : "action.hover",
          color: "var(--streamskope-nav-text)",
        },
      }}
    >
      <Typography component="span" noWrap sx={{ fontWeight: active ? 600 : 400 }} variant="body2">
        {label}
      </Typography>
    </ButtonBase>
  );
}

export function WorkbenchSidebar({
  connected,
  navigation,
  onChange,
}: WorkbenchSidebarProperties): React.JSX.Element {
  return (
    <Box
      aria-label="StreamSkope resources"
      component="nav"
      data-active-resource={navigation}
      sx={{
        bgcolor: "var(--streamskope-nav-background)",
        borderRight: 1,
        borderColor: "divider",
        color: "var(--streamskope-nav-text)",
        display: "flex",
        flexDirection: "column",
        height: "100%",
        minHeight: 0,
        overflow: "hidden",
        width: "100%",
      }}
    >
      <Box sx={{ flex: 1, minHeight: 0, overflowY: "auto", py: 0.75 }}>
        {WORKBENCH_RESOURCE_GROUPS.map((group, groupIndex) => (
          <Box key={group.label} sx={{ mt: groupIndex === 0 ? 0 : 1.5 }}>
            <Typography
              component="h2"
              sx={{ color: "var(--streamskope-nav-muted)", px: 2, py: 0.5 }}
              variant="overline"
            >
              {group.label}
            </Typography>
            {group.items.map((item) => (
              <SidebarButton
                active={navigation === item.value}
                available={isNavigationAvailable(item.value, connected)}
                key={item.value}
                label={item.label}
                onClick={() => onChange(item.value)}
              />
            ))}
          </Box>
        ))}
      </Box>
    </Box>
  );
}
