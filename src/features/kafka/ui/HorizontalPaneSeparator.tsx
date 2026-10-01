import { useCallback, useRef, useState } from "react";
import { Box } from "@mui/material";

type PaneSide = "after" | "before";

interface HorizontalPaneSeparatorProperties {
  readonly label: string;
  readonly maximum: number;
  readonly minimum: number;
  readonly onChange: (value: number) => void;
  readonly paneSide: PaneSide;
  readonly value: number;
}

interface PointerResize {
  readonly id: number;
  readonly startValue: number;
  readonly startX: number;
}

function clampPaneWidth(value: number, minimum: number, maximum: number): number {
  return Math.min(maximum, Math.max(minimum, Math.round(value)));
}

function availableStorage(): Storage | null {
  try {
    return globalThis.localStorage;
  } catch {
    return null;
  }
}

export function usePersistentPaneWidth(
  storageKey: string,
  defaultValue: number,
  minimum: number,
  maximum: number,
): readonly [number, (value: number) => void] {
  const [value, setValue] = useState(() => {
    const stored = availableStorage()?.getItem(storageKey);
    if (stored === null || stored === undefined) {
      return defaultValue;
    }
    const parsed = Number(stored);
    return Number.isFinite(parsed) && parsed >= minimum && parsed <= maximum
      ? Math.round(parsed)
      : defaultValue;
  });

  const update = useCallback(
    (nextValue: number): void => {
      const clamped = clampPaneWidth(nextValue, minimum, maximum);
      setValue(clamped);
      try {
        availableStorage()?.setItem(storageKey, String(clamped));
      } catch {
        // Local presentation preferences are optional when browser storage is unavailable.
      }
    },
    [maximum, minimum, storageKey],
  );

  return [value, update] as const;
}

export function HorizontalPaneSeparator({
  label,
  maximum,
  minimum,
  onChange,
  paneSide,
  value,
}: HorizontalPaneSeparatorProperties): React.JSX.Element {
  const pointerResize = useRef<PointerResize | null>(null);

  function changeFromSeparatorMovement(deltaX: number): void {
    onChange(value + (paneSide === "before" ? deltaX : -deltaX));
  }

  return (
    <Box
      aria-label={label}
      aria-orientation="vertical"
      aria-valuemax={maximum}
      aria-valuemin={minimum}
      aria-valuenow={value}
      onKeyDown={(event) => {
        const step = event.shiftKey ? 48 : 16;
        if (event.key === "ArrowLeft") {
          event.preventDefault();
          changeFromSeparatorMovement(-step);
        } else if (event.key === "ArrowRight") {
          event.preventDefault();
          changeFromSeparatorMovement(step);
        } else if (event.key === "Home") {
          event.preventDefault();
          onChange(minimum);
        } else if (event.key === "End") {
          event.preventDefault();
          onChange(maximum);
        }
      }}
      onPointerDown={(event) => {
        pointerResize.current = {
          id: event.pointerId,
          startValue: value,
          startX: event.clientX,
        };
        event.currentTarget.setPointerCapture?.(event.pointerId);
      }}
      onPointerMove={(event) => {
        const active = pointerResize.current;
        if (active === null || active.id !== event.pointerId) {
          return;
        }
        const deltaX = event.clientX - active.startX;
        onChange(active.startValue + (paneSide === "before" ? deltaX : -deltaX));
      }}
      onPointerUp={(event) => {
        if (pointerResize.current?.id !== event.pointerId) {
          return;
        }
        pointerResize.current = null;
        event.currentTarget.releasePointerCapture?.(event.pointerId);
      }}
      role="separator"
      sx={{
        bgcolor: "background.default",
        cursor: "col-resize",
        height: "100%",
        minWidth: 5,
        position: "relative",
        touchAction: "none",
        width: 5,
        "&::after": {
          bgcolor: "var(--streamskope-divider-strong)",
          content: '""',
          insetBlock: 0,
          left: 2,
          position: "absolute",
          width: 1,
        },
        "&:hover::after, &:focus-visible::after": {
          bgcolor: "primary.main",
          width: 2,
        },
      }}
      tabIndex={0}
    />
  );
}
