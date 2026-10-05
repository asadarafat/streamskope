import { useCallback, useState } from "react";
import useMediaQuery from "@mui/material/useMediaQuery";

import { streamSkopeLayout } from "./createStreamSkopeTheme";

export interface ProductNavigator {
  readonly open: boolean;
  readonly temporary: boolean;
  readonly close: () => void;
  readonly toggle: () => void;
}

/** Responsive navigation and its explicit closure stay independent of feature destinations. */
export function useProductNavigator(): ProductNavigator {
  const temporary = useMediaQuery(
    `(max-width:${String(streamSkopeLayout.fullDesktopMinimumWidth - 0.05)}px)`,
  );
  const [open, setOpen] = useState(false);
  const close = useCallback((): void => setOpen(false), []);
  const toggle = useCallback((): void => setOpen((current) => !current), []);
  return { open, temporary, close, toggle };
}
