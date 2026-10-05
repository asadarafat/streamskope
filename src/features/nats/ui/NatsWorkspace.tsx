import { useState, type ReactNode } from "react";
import Box from "@mui/material/Box";
import Breadcrumbs from "@mui/material/Breadcrumbs";
import Typography from "@mui/material/Typography";

import { ProviderWorkbenchShell } from "../../../platform/ui/ProviderWorkbenchShell";
import type { ProductResourceGroup } from "../../../platform/ui/ProductResourceSidebar";
import { useProductNavigator } from "../../../platform/ui/use-product-navigator";
import { StudioAlert as Alert, StudioButton as Button } from "../../../platform/ui/controls";

import { ProfileWorkspace } from "./ProfileWorkspace";
import { SubscriptionWorkspace } from "./SubscriptionWorkspace";
import { useNatsWorkspace } from "./use-nats-workspace";
import type { NatsWorkspaceSource } from "./workspace-types";

type Resource = "profiles" | "subscription";
const resources: readonly ProductResourceGroup<Resource>[] = [
  {
    label: "Core NATS",
    items: [
      { label: "Connection Profiles", value: "profiles", available: true },
      { label: "Live Subscription", value: "subscription", available: true },
    ],
  },
];
const alwaysInteractive = (): boolean => true;
const label = (state: string): string => state.charAt(0).toUpperCase() + state.slice(1);

export interface NatsWorkspaceProperties {
  readonly source: NatsWorkspaceSource;
  readonly providerControl?: ReactNode;
  readonly isInteractive?: () => boolean;
}

export function NatsWorkspace({
  source,
  providerControl,
  isInteractive = alwaysInteractive,
}: NatsWorkspaceProperties): React.JSX.Element {
  const navigator = useProductNavigator();
  const [navigation, setNavigation] = useState<Resource>("profiles");
  const controller = useNatsWorkspace({
    host: source.state === "ready" ? source.host : null,
    isInteractive,
    ...(source.state === "unavailable" ? { unavailableRecovery: source.recovery } : {}),
  });
  const unavailable = !controller.available;
  const displayedFailure =
    controller.failure ?? controller.subscription.failure ?? controller.connection.failure ?? null;
  const connectionLabel = unavailable ? "Host unavailable" : label(controller.connection.state);
  const subscriptionLabel = unavailable ? "Host unavailable" : label(controller.subscription.state);
  const connectionColor =
    unavailable || controller.connection.state === "failed"
      ? "error.main"
      : controller.connection.state === "connected"
        ? "success.main"
        : "text.secondary";
  const subscriptionColor =
    unavailable || controller.subscription.state === "failed"
      ? "error.main"
      : controller.subscription.state === "streaming"
        ? "success.main"
        : "text.secondary";

  return (
    <ProviderWorkbenchShell
      activity={null}
      activityHeight={0}
      breadcrumbs={
        <Box
          sx={{
            alignItems: "center",
            bgcolor: "var(--streamskope-nav-background)",
            borderBottom: 1,
            borderColor: "divider",
            display: "flex",
            minWidth: 0,
            px: 1.5,
          }}
        >
          <Breadcrumbs aria-label="Breadcrumb" separator="/">
            <Typography color="text.secondary" variant="body2">
              Core NATS
            </Typography>
            <Typography aria-current="page" variant="body2">
              {navigation === "profiles" ? "Connection Profiles" : "Live Subscription"}
            </Typography>
          </Breadcrumbs>
        </Box>
      }
      navigation={navigation}
      navigator={navigator}
      onNavigate={(destination) => {
        if (!isInteractive()) return;
        setNavigation(destination);
        navigator.close();
      }}
      providerControl={providerControl}
      resourceLabel="NATS resources"
      resources={resources}
      status={
        <>
          <Typography
            aria-label="Connection status"
            aria-live="polite"
            color={connectionColor}
            noWrap
            sx={{ flexShrink: 0 }}
            variant="caption"
          >
            Connection: {connectionLabel}
          </Typography>
          <Typography
            aria-label="Subscription status"
            aria-live="polite"
            color={subscriptionColor}
            noWrap
            sx={{ flexShrink: 0 }}
            variant="caption"
          >
            Subscription: {subscriptionLabel}
          </Typography>
          <Typography color="text.secondary" noWrap sx={{ flex: 1, minWidth: 0 }} variant="caption">
            {unavailable
              ? "Connection and subscription health cannot be verified"
              : "Core NATS · live delivery"}
          </Typography>
        </>
      }
    >
      <Box
        sx={{
          display: "grid",
          gridTemplateRows: displayedFailure === null ? "minmax(0, 1fr)" : "auto minmax(0, 1fr)",
          minHeight: 0,
          minWidth: 0,
          overflow: "hidden",
        }}
      >
        {displayedFailure === null ? null : (
          <Alert
            action={
              unavailable || controller.failure === null ? undefined : (
                <Button
                  color="inherit"
                  onClick={() => {
                    if (isInteractive()) controller.clearFailure();
                  }}
                  variant="text"
                >
                  Dismiss
                </Button>
              )
            }
            severity={unavailable ? "warning" : "error"}
            sx={{ borderRadius: 0, minWidth: 0, overflowWrap: "anywhere" }}
          >
            {displayedFailure.summary} {displayedFailure.recovery}
          </Alert>
        )}
        {navigation === "profiles" ? (
          <ProfileWorkspace controller={controller} isInteractive={isInteractive} />
        ) : (
          <SubscriptionWorkspace controller={controller} isInteractive={isInteractive} />
        )}
      </Box>
    </ProviderWorkbenchShell>
  );
}
