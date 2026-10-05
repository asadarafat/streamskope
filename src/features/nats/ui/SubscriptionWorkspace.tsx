import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import Box from "@mui/material/Box";
import Drawer from "@mui/material/Drawer";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import useMediaQuery from "@mui/material/useMediaQuery";

import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";
import { streamSkopeMuiMonospaceTypography } from "../../../platform/ui/createStreamSkopeTheme";

import { RecordDataGrid } from "./RecordDataGrid";
import { RecordInspector } from "./RecordInspector";
import type { NatsWorkspaceController } from "./use-nats-workspace";

export function SubscriptionWorkspace({
  controller,
  isInteractive,
}: {
  readonly controller: NatsWorkspaceController;
  readonly isInteractive: () => boolean;
}): React.JSX.Element {
  const [subject, setSubject] = useState(controller.subscription.subject ?? "");
  const compact = useMediaQuery("(max-width:899.95px)");
  const grid = useRef<HTMLDivElement>(null);
  const returnFocus = useRef<HTMLElement | null>(null);
  const focusFrame = useRef<number | null>(null);
  const cancelFocusRestoration = useCallback((): void => {
    const frame = focusFrame.current;
    focusFrame.current = null;
    if (frame !== null) globalThis.cancelAnimationFrame(frame);
  }, []);
  useEffect(() => cancelFocusRestoration, [cancelFocusRestoration]);
  useLayoutEffect(() => {
    if (controller.selectedRecord !== null) cancelFocusRestoration();
  }, [cancelFocusRestoration, controller.selectedRecord]);
  const busy = controller.pending.length > 0;
  const disabled = !controller.available || busy || !isInteractive();
  const subscriptionActive = ["loading", "streaming", "stopping"].includes(
    controller.subscription.state,
  );
  const counters = controller.subscription.counters;
  const stopped = controller.subscription.state === "stopped";

  function select(id: string): void {
    if (!isInteractive()) return;
    cancelFocusRestoration();
    if (
      document.activeElement instanceof HTMLElement &&
      grid.current?.contains(document.activeElement)
    )
      returnFocus.current = document.activeElement;
    controller.selectRecord(id);
  }

  function closeInspector(): void {
    if (!isInteractive()) return;
    cancelFocusRestoration();
    const opener = returnFocus.current;
    const origin = grid.current;
    controller.selectRecord(null);
    const frame = globalThis.requestAnimationFrame((): void => {
      if (focusFrame.current !== frame) return;
      focusFrame.current = null;
      if (!isInteractive() || origin?.isConnected !== true || grid.current !== origin) return;
      if (opener?.isConnected === true && origin.contains(opener)) opener.focus();
      else {
        const focusTarget =
          origin.querySelector<HTMLElement>('[role="gridcell"][tabindex="0"]') ??
          origin.querySelector<HTMLElement>('[role="gridcell"], [role="columnheader"]');
        focusTarget?.focus();
      }
    });
    focusFrame.current = frame;
  }

  function start(): void {
    if (isInteractive() && !disabled && subject.trim().length > 0)
      void controller.startSubscription(subject.trim());
  }

  const inspector =
    controller.selectedRecord === null ? null : (
      <RecordInspector onClose={closeInspector} record={controller.selectedRecord} />
    );

  return (
    <Box
      aria-label="Live Subscription"
      component="main"
      onKeyDown={(event) => {
        if (event.key === "Escape" && !compact && controller.selectedRecord !== null) {
          event.stopPropagation();
          closeInspector();
        }
      }}
      sx={{
        display: "grid",
        gridTemplateRows: "auto auto minmax(0, 1fr)",
        minHeight: 0,
        minWidth: 0,
        overflow: "hidden",
      }}
    >
      <Box sx={{ bgcolor: "background.paper", borderBottom: 1, borderColor: "divider", p: 2 }}>
        <Typography component="h2" variant="h5">
          Live Subscription
        </Typography>
        <Typography color="text.secondary" sx={{ mt: 0.5 }} variant="body2">
          Read live Core NATS deliveries using an exact subject or the * and &gt; wildcards.
        </Typography>
        <Box
          component="form"
          onSubmit={(event) => {
            event.preventDefault();
            start();
          }}
          sx={{
            alignItems: "flex-start",
            display: "flex",
            flexWrap: "wrap",
            gap: 1,
            minWidth: 0,
            mt: 1.5,
          }}
        >
          <TextField
            disabled={disabled || subscriptionActive}
            label="Subject filter"
            onChange={(event) => {
              if (isInteractive()) setSubject(event.target.value);
            }}
            placeholder="events.*"
            slotProps={{ input: { sx: streamSkopeMuiMonospaceTypography } }}
            sx={{ flex: "1 1 220px", minWidth: 0, width: "auto" }}
            value={subject}
          />
          <Button
            disabled={
              disabled ||
              subscriptionActive ||
              controller.connection.state !== "connected" ||
              subject.trim().length === 0
            }
            type="submit"
            variant="contained"
          >
            {controller.subscription.state === "loading"
              ? "Starting subscription…"
              : "Start subscription"}
          </Button>
          <Button
            disabled={
              disabled || !subscriptionActive || controller.subscription.state === "stopping"
            }
            onClick={() => {
              if (isInteractive()) void controller.stopSubscription();
            }}
            variant="outlined"
          >
            {controller.subscription.state === "stopping"
              ? "Stopping subscription…"
              : "Stop subscription"}
          </Button>
          <Button
            disabled={disabled || controller.connection.state === "disconnected"}
            onClick={() => {
              if (isInteractive()) void controller.disconnect();
            }}
            variant="outlined"
          >
            Disconnect
          </Button>
        </Box>
      </Box>
      <Box sx={{ borderBottom: 1, borderColor: "divider", minWidth: 0, px: 1.5, py: 1 }}>
        {controller.connection.state !== "connected" && !subscriptionActive && !stopped ? (
          <Typography color="text.secondary" variant="body2">
            Connect a profile from Connection Profiles to begin.
          </Typography>
        ) : null}
        {stopped ? (
          <Typography color="text.secondary" role="status" variant="body2">
            Stopped subscription · retained records from the last confirmed generation.
          </Typography>
        ) : null}
        {!controller.available && controller.records.length > 0 ? (
          <Typography color="warning.main" variant="body2">
            Retained record evidence is stale while the NATS host is unavailable.
          </Typography>
        ) : null}
        <Stack direction="row" spacing={1.5} sx={{ flexWrap: "wrap", minWidth: 0 }}>
          <Typography color="text.secondary" variant="caption">
            Viewer: {controller.records.length.toLocaleString()} records
          </Typography>
          <Typography color="text.secondary" variant="caption">
            Host received: {counters.receivedRecords.toLocaleString()}
          </Typography>
          <Typography color="text.secondary" variant="caption">
            Host emitted: {counters.publishedRecords.toLocaleString()}
          </Typography>
        </Stack>
        {controller.viewerOmittedRecords +
          counters.applicationOmittedRecords +
          counters.transportOmittedRecords >
        0 ? (
          <Typography color="warning.main" role="status" variant="body2">
            Viewer evicted: {controller.viewerOmittedRecords.toLocaleString()} · application
            omitted: {counters.applicationOmittedRecords.toLocaleString()} · transport omitted:{" "}
            {counters.transportOmittedRecords.toLocaleString()}. Core NATS has no replay history in
            this workspace.
          </Typography>
        ) : null}
        {controller.selectionNotice === null ? null : (
          <Alert severity="info" sx={{ mt: 0.5 }}>
            {controller.selectionNotice}
          </Alert>
        )}
      </Box>
      <Box
        sx={{
          display: "grid",
          gridTemplateColumns:
            !compact && inspector !== null ? "minmax(0, 1fr) 380px" : "minmax(0, 1fr)",
          minHeight: 0,
          minWidth: 0,
          overflow: "hidden",
        }}
      >
        <Box ref={grid} sx={{ display: "grid", minHeight: 0, minWidth: 0, overflow: "hidden" }}>
          <RecordDataGrid
            onSelect={select}
            records={controller.records}
            selectedId={controller.selectedRecord?.id ?? null}
          />
        </Box>
        {!compact ? inspector : null}
      </Box>
      {compact ? (
        <Drawer
          anchor="right"
          onClose={closeInspector}
          open={inspector !== null}
          slotProps={{
            paper: {
              "aria-label": "NATS record inspector drawer",
              sx: {
                bottom: "26px",
                height: "auto",
                maxWidth: "90vw",
                minWidth: 0,
                top: "52px",
                width: 420,
              },
            },
          }}
        >
          {inspector}
        </Drawer>
      ) : null}
    </Box>
  );
}
