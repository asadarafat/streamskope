import Box from "@mui/material/Box";
import Breadcrumbs from "@mui/material/Breadcrumbs";
import ButtonBase from "@mui/material/ButtonBase";
import Typography from "@mui/material/Typography";

import { streamSkopeLayout } from "../../../platform/ui/createStreamSkopeTheme";
import { StudioTechnicalText } from "../../../platform/ui/StudioCodeBlock";

import { navigationGroupLabel, navigationLabel, type NavigationView } from "./workbench-navigation";

export interface WorkbenchBreadcrumbsProperties {
  readonly navigation: NavigationView;
  readonly onNavigate: (navigation: NavigationView) => void;
  readonly selectedConsumerGroupId: string | null;
  readonly selectedTopic: string | null;
}

export function WorkbenchBreadcrumbs({
  navigation,
  onNavigate,
  selectedConsumerGroupId,
  selectedTopic,
}: WorkbenchBreadcrumbsProperties): React.JSX.Element {
  const selectedObject =
    navigation === "topics"
      ? selectedTopic
      : navigation === "consumer-groups"
        ? selectedConsumerGroupId
        : null;
  const resource = navigationLabel(navigation);

  return (
    <Box
      sx={{
        alignItems: "center",
        bgcolor: "var(--streamskope-nav-background)",
        borderBottom: 1,
        borderColor: "divider",
        display: "flex",
        height: streamSkopeLayout.breadcrumbBarHeight,
        minHeight: streamSkopeLayout.breadcrumbBarHeight,
        minWidth: 0,
        px: 1.5,
      }}
    >
      <Breadcrumbs aria-label="Breadcrumb" separator="/" sx={{ minWidth: 0 }}>
        <Typography color="text.secondary" variant="body2">
          {navigationGroupLabel(navigation)}
        </Typography>
        {selectedObject === null ? (
          <Typography aria-current="page" color="text.primary" noWrap variant="body2">
            {resource}
          </Typography>
        ) : (
          <ButtonBase
            aria-label={resource}
            onClick={() => onNavigate(navigation)}
            sx={{ borderRadius: 0.75, color: "text.secondary", px: 0.5, py: 0.25 }}
          >
            <Typography variant="body2">{resource}</Typography>
          </ButtonBase>
        )}
        {selectedObject === null ? null : (
          <StudioTechnicalText
            aria-current="page"
            sx={{
              display: "block",
              maxWidth: "min(52vw, 560px)",
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
          >
            {selectedObject}
          </StudioTechnicalText>
        )}
      </Breadcrumbs>
    </Box>
  );
}
