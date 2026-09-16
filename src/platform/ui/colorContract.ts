/**
 * Canonical StreamSkope chrome colors. Feature presenters consume semantic
 * Material UI palette tokens and never own application color literals.
 */
export interface StreamSkopeColorScheme {
  readonly action: {
    readonly focus: string;
    readonly hover: string;
    readonly selected: string;
  };
  readonly background: {
    readonly default: string;
    readonly paper: string;
  };
  readonly divider: string;
  readonly error: { readonly main: string };
  readonly info: { readonly main: string };
  readonly navigation: {
    readonly background: string;
    readonly border: string;
    readonly muted: string;
    readonly selected: string;
    readonly text: string;
  };
  readonly primary: {
    readonly contrastText: string;
    readonly dark: string;
    readonly light: string;
    readonly main: string;
  };
  readonly success: { readonly main: string };
  readonly text: {
    readonly disabled: string;
    readonly primary: string;
    readonly secondary: string;
  };
  readonly warning: { readonly main: string };
}

export const streamSkopeColors: Readonly<{
  readonly dark: StreamSkopeColorScheme;
  readonly light: StreamSkopeColorScheme;
}> = Object.freeze({
  dark: {
    action: {
      focus: "rgba(110, 139, 251, 0.28)",
      hover: "rgba(255, 255, 255, 0.05)",
      selected: "rgba(255, 255, 255, 0.09)",
    },
    background: { default: "#1b1b1f", paper: "#232328" },
    divider: "rgba(255, 255, 255, 0.08)",
    error: { main: "#f87171" },
    info: { main: "#60a5fa" },
    navigation: {
      background: "#151518",
      border: "rgba(255, 255, 255, 0.08)",
      muted: "#9d9da7",
      selected: "rgba(255, 255, 255, 0.09)",
      text: "#e6e6ea",
    },
    primary: {
      contrastText: "#1b1b1f",
      dark: "#6682ee",
      light: "#a8b9ff",
      main: "#6e8bfb",
    },
    success: { main: "#4ade80" },
    text: { disabled: "#9d9da7", primary: "#e6e6ea", secondary: "#9d9da7" },
    warning: { main: "#e7b341" },
  },
  light: {
    action: {
      focus: "rgba(54, 95, 232, 0.22)",
      hover: "rgba(0, 0, 0, 0.04)",
      selected: "rgba(0, 0, 0, 0.07)",
    },
    background: { default: "#fafafa", paper: "#ffffff" },
    divider: "rgba(0, 0, 0, 0.08)",
    error: { main: "#b91c1c" },
    info: { main: "#1d4ed8" },
    navigation: {
      background: "#f4f4f5",
      border: "rgba(0, 0, 0, 0.08)",
      muted: "#6e6e78",
      selected: "rgba(0, 0, 0, 0.07)",
      text: "#1c1c21",
    },
    primary: {
      contrastText: "#ffffff",
      dark: "#294cc7",
      light: "#dce4ff",
      main: "#365fe8",
    },
    success: { main: "#15803d" },
    text: { disabled: "#6e6e78", primary: "#1c1c21", secondary: "#6e6e78" },
    warning: { main: "#8f6209" },
  },
});
