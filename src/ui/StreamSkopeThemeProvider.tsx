import CssBaseline from "@mui/material/CssBaseline";
import { ThemeProvider } from "@mui/material/styles";
import type { PropsWithChildren } from "react";

import { streamSkopeTheme } from "./createStreamSkopeTheme";

/** One theme boundary shared by browser and Electron renderers. */
export function StreamSkopeThemeProvider({ children }: PropsWithChildren): React.JSX.Element {
  return (
    <ThemeProvider
      defaultMode="system"
      disableTransitionOnChange
      noSsr
      storageManager={null}
      theme={streamSkopeTheme}
    >
      <CssBaseline enableColorScheme />
      {children}
    </ThemeProvider>
  );
}
