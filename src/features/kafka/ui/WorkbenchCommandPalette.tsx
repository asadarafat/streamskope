import { useEffect, useMemo, useState } from "react";
import Box from "@mui/material/Box";
import InputAdornment from "@mui/material/InputAdornment";
import List from "@mui/material/List";
import Typography from "@mui/material/Typography";

import type { ProfileSummary } from "../contracts";
import {
  StudioDialog as Dialog,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioListItemButton as ListItemButton,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";
import { streamSkopeMuiMonospaceTypography } from "../../../platform/ui/createStreamSkopeTheme";

import {
  isNavigationAvailable,
  WORKBENCH_RESOURCE_GROUPS,
  type NavigationView,
} from "./workbench-navigation";
import { WorkbenchIcon } from "./WorkbenchIcons";

const resources = WORKBENCH_RESOURCE_GROUPS.flatMap((group) => group.items);

export interface WorkbenchCommandPaletteProperties {
  readonly connected: boolean;
  readonly onClose: () => void;
  readonly onOpenResource: (navigation: NavigationView) => void;
  readonly onOpenTopic: (topic: string) => void;
  readonly onSelectProfile: (profileId: string) => void;
  readonly open: boolean;
  readonly profiles: readonly ProfileSummary[];
  readonly topics: readonly string[];
}

export function WorkbenchCommandPalette({
  connected,
  onClose,
  onOpenResource,
  onOpenTopic,
  onSelectProfile,
  open,
  profiles,
  topics,
}: WorkbenchCommandPaletteProperties): React.JSX.Element {
  const [query, setQuery] = useState("");
  useEffect(() => {
    if (!open) setQuery("");
  }, [open]);
  const normalizedQuery = query.trim().toLocaleLowerCase();
  const matchingResources = useMemo(
    () =>
      resources.filter(
        (resource) =>
          normalizedQuery.length === 0 ||
          resource.label.toLocaleLowerCase().includes(normalizedQuery),
      ),
    [normalizedQuery],
  );
  const matchingTopics = useMemo(
    () =>
      connected
        ? topics.filter(
            (topic) =>
              normalizedQuery.length > 0 && topic.toLocaleLowerCase().includes(normalizedQuery),
          )
        : [],
    [connected, normalizedQuery, topics],
  );
  const matchingProfiles = useMemo(
    () =>
      profiles.filter(
        (profile) =>
          normalizedQuery.length > 0 &&
          [profile.name, ...profile.brokers].some((value) =>
            value.toLocaleLowerCase().includes(normalizedQuery),
          ),
      ),
    [normalizedQuery, profiles],
  );

  return (
    <Dialog
      aria-labelledby="streamskope-command-palette-title"
      maxWidth="sm"
      onClose={onClose}
      open={open}
    >
      <DialogTitle id="streamskope-command-palette-title">Search and commands</DialogTitle>
      <DialogContent dividers sx={{ p: 0 }}>
        <Box sx={{ borderBottom: 1, borderColor: "divider", p: 1.25 }}>
          <TextField
            autoFocus
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Find a profile, workspace or topic"
            slotProps={{
              htmlInput: { "aria-label": "Search profiles, resources and topics", type: "search" },
              input: {
                startAdornment: (
                  <InputAdornment position="start">
                    <WorkbenchIcon name="search" />
                  </InputAdornment>
                ),
              },
            }}
            value={query}
          />
        </Box>
        <Box sx={{ maxHeight: "min(520px, 65vh)", overflowY: "auto", py: 0.75 }}>
          {matchingProfiles.length > 0 ? (
            <>
              <Typography
                color="text.secondary"
                component="h2"
                sx={{ px: 2, py: 0.5 }}
                variant="overline"
              >
                Connection profiles · selects without connecting
              </Typography>
              <List disablePadding>
                {matchingProfiles.map((profile) => (
                  <ListItemButton
                    key={profile.id}
                    aria-label={`Select profile ${profile.name}`}
                    onClick={() => {
                      onSelectProfile(profile.id);
                      onClose();
                    }}
                    sx={{ mx: 0.75, px: 1.25 }}
                  >
                    <Box sx={{ minWidth: 0 }}>
                      <Typography variant="body2">{profile.name}</Typography>
                      <Typography
                        color="text.secondary"
                        noWrap
                        sx={streamSkopeMuiMonospaceTypography}
                      >
                        {profile.brokers.join(", ")}
                      </Typography>
                    </Box>
                  </ListItemButton>
                ))}
              </List>
            </>
          ) : null}
          {matchingResources.length > 0 ? (
            <>
              <Typography
                color="text.secondary"
                component="h2"
                sx={{ px: 2, py: 0.5 }}
                variant="overline"
              >
                Workspaces
              </Typography>
              <List disablePadding>
                {matchingResources.map((resource) => (
                  <ListItemButton
                    aria-disabled={!isNavigationAvailable(resource.value, connected) || undefined}
                    disabled={!isNavigationAvailable(resource.value, connected)}
                    key={resource.value}
                    onClick={() => {
                      if (!isNavigationAvailable(resource.value, connected)) return;
                      onOpenResource(resource.value);
                      onClose();
                    }}
                    sx={{ mx: 0.75, px: 1.25 }}
                  >
                    <Typography variant="body2">{resource.label}</Typography>
                  </ListItemButton>
                ))}
              </List>
            </>
          ) : null}
          {matchingTopics.length > 0 ? (
            <>
              <Typography
                color="text.secondary"
                component="h2"
                sx={{ px: 2, pb: 0.5, pt: 1.5 }}
                variant="overline"
              >
                Topics · opens Messages
              </Typography>
              <List disablePadding>
                {matchingTopics.map((topic) => (
                  <ListItemButton
                    aria-label={`Open and read topic ${topic}`}
                    key={topic}
                    onClick={() => {
                      onOpenTopic(topic);
                      onClose();
                    }}
                    sx={{ mx: 0.75, px: 1.25 }}
                  >
                    <Typography sx={streamSkopeMuiMonospaceTypography}>{topic}</Typography>
                  </ListItemButton>
                ))}
              </List>
            </>
          ) : null}
          {matchingResources.length === 0 &&
          matchingTopics.length === 0 &&
          matchingProfiles.length === 0 ? (
            <Box role="status" sx={{ px: 2, py: 4, textAlign: "center" }}>
              <Typography color="text.secondary" variant="body2">
                No profile, resource or topic matches “{query}”.
              </Typography>
            </Box>
          ) : null}
        </Box>
      </DialogContent>
    </Dialog>
  );
}
