import { useCallback, useEffect, useRef, useState } from "react";

import type { StreamSkopeDesktop } from "../../../platform/desktop";
import { streamSkopeLayout } from "../../../platform/ui/createStreamSkopeTheme";

import type { KafkaUiState } from "./state";
import { useAutoOpenActivityOnError, useDesktopActions } from "./workbench-runtime-effects";

interface WorkbenchActivityController {
  readonly activityOpen: boolean;
  readonly activityHeight: number;
  readonly activityQuery: string;
  readonly commandPaletteOpen: boolean;
  readonly preferenceDialogMounted: boolean;
  readonly preferenceDialogOpen: boolean;
  readonly closeActivity: () => void;
  readonly openActivity: () => void;
  readonly openProfileActivity: (correlationId?: string) => void;
  readonly openPreferences: () => void;
  readonly setActivityHeight: (value: number) => void;
  readonly setCommandPaletteOpen: (value: boolean) => void;
  readonly setPreferenceDialogOpen: (value: boolean) => void;
}

export function useWorkbenchActivity(
  desktop: StreamSkopeDesktop | undefined,
  activities: KafkaUiState["activities"],
): WorkbenchActivityController {
  const [activityOpen, setActivityOpen] = useState(false);
  const [activityHeight, setActivityHeight] = useState<number>(streamSkopeLayout.activityHeight);
  const [commandPaletteOpen, setCommandPaletteOpen] = useState(false);
  const [preferenceDialogMounted, setPreferenceDialogMounted] = useState(false);
  const [preferenceDialogOpen, setPreferenceDialogOpen] = useState(false);
  const [activityQuery, setActivityQuery] = useState("");
  const activityReturnFocusRef = useRef<HTMLElement | null>(null);
  const restoreActivityFocusRef = useRef(false);
  const openActivity = useCallback(() => {
    setActivityQuery("");
    const activeElement = globalThis.document.activeElement;
    activityReturnFocusRef.current =
      activeElement instanceof globalThis.HTMLElement ? activeElement : null;
    setActivityOpen(true);
  }, []);
  const openProfileActivity = (correlationId?: string): void => {
    openActivity();
    setActivityQuery(correlationId ?? "");
  };

  const closeActivity = useCallback(() => {
    restoreActivityFocusRef.current = true;
    setActivityOpen(false);
  }, []);

  useDesktopActions(desktop, openActivity, setPreferenceDialogMounted, setPreferenceDialogOpen);

  useEffect(() => {
    const openCommands = (event: KeyboardEvent): void => {
      if ((event.ctrlKey || event.metaKey) && event.key.toLocaleLowerCase() === "k") {
        event.preventDefault();
        setCommandPaletteOpen(true);
      }
    };
    globalThis.addEventListener("keydown", openCommands);
    return (): void => globalThis.removeEventListener("keydown", openCommands);
  }, []);

  useEffect(() => {
    if (!activityOpen && restoreActivityFocusRef.current) {
      restoreActivityFocusRef.current = false;
      activityReturnFocusRef.current?.focus();
    }
  }, [activityOpen]);

  useAutoOpenActivityOnError(activities, activityOpen, openActivity);
  const openPreferences = (): void => {
    setPreferenceDialogMounted(true);
    setPreferenceDialogOpen(true);
  };
  return {
    activityOpen,
    activityHeight,
    activityQuery,
    commandPaletteOpen,
    preferenceDialogMounted,
    preferenceDialogOpen,
    closeActivity,
    openActivity,
    openProfileActivity,
    openPreferences,
    setActivityHeight,
    setCommandPaletteOpen,
    setPreferenceDialogOpen,
  };
}
