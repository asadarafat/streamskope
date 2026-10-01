import SvgIcon from "@mui/material/SvgIcon";
import type { SvgIconProps } from "@mui/material/SvgIcon";

import { streamSkopeGeometry } from "../../../platform/ui/createStreamSkopeTheme";

type WorkbenchIconName =
  | "add"
  | "activity"
  | "acls"
  | "collapse"
  | "close"
  | "expand"
  | "more"
  | "overview"
  | "play"
  | "refresh"
  | "resources"
  | "schemas"
  | "search"
  | "settings"
  | "topics"
  | "groups"
  | "transforms"
  | "stop";

const iconPaths: Readonly<Record<WorkbenchIconName, string>> = Object.freeze({
  add: "M11 5h2v6h6v2h-6v6h-2v-6H5v-2h6V5z",
  activity: "M4 4h16v2H4V4zm0 5h16v2H4V9zm0 5h10v2H4v-2zm0 5h13v2H4v-2z",
  acls: "M12 2 4 5v6c0 5.1 3.4 9.8 8 11 4.6-1.2 8-5.9 8-11V5l-8-3zm0 3.2 5 1.9V11c0 3.7-2.2 7.1-5 8-2.8-.9-5-4.3-5-8V7.1l5-1.9zm-1 3.3h2v3h3v2h-3v3h-2v-3H8v-2h3v-3z",
  collapse: "m7.41 15.41 4.59-4.58 4.59 4.58L18 14l-6-6-6 6 1.41 1.41z",
  close:
    "M18.3 5.71 16.89 4.3 12 9.17 7.11 4.3 5.7 5.71 10.59 10.6 5.7 15.49l1.41 1.41L12 12.01l4.89 4.89 1.41-1.41-4.89-4.89 4.89-4.89z",
  expand: "m7.41 8.59 4.59 4.58 4.59-4.58L18 10l-6 6-6-6 1.41-1.41z",
  more: "M6 10a2 2 0 1 0 0 4 2 2 0 0 0 0-4zm6 0a2 2 0 1 0 0 4 2 2 0 0 0 0-4zm6 0a2 2 0 1 0 0 4 2 2 0 0 0 0-4z",
  overview: "M4 4h7v7H4V4zm9 0h7v4h-7V4zm0 6h7v10h-7V10zM4 13h7v7H4v-7z",
  play: "M8 5v14l11-7L8 5z",
  refresh:
    "M17.65 6.35A7.95 7.95 0 0 0 12 4a8 8 0 1 0 7.75 10h-2.1A6 6 0 1 1 12 6c1.66 0 3.14.69 4.22 1.78L13 11h8V3l-3.35 3.35z",
  resources: "M3 4h18v16H3V4zm2 2v12h5V6H5zm7 0v12h7V6h-7z",
  schemas: "M5 3h10l4 4v14H5V3zm2 2v14h10V8h-3V5H7zm2 6h6v2H9v-2zm0 4h6v2H9v-2z",
  search:
    "M9.5 3a6.5 6.5 0 1 0 3.98 11.64L19.85 21 21 19.85l-6.36-6.37A6.5 6.5 0 0 0 9.5 3zm0 2a4.5 4.5 0 1 1 0 9 4.5 4.5 0 0 1 0-9z",
  settings:
    "M19.14 12.94c.04-.3.06-.61.06-.94 0-.32-.02-.64-.07-.94l2.03-1.58c.18-.14.23-.41.12-.61l-1.92-3.32c-.12-.22-.37-.29-.59-.22l-2.39.96c-.5-.38-1.03-.7-1.62-.94l-.36-2.54c-.04-.24-.24-.41-.48-.41h-3.84c-.24 0-.43.17-.47.41l-.36 2.54c-.59.24-1.13.57-1.62.94l-2.39-.96c-.22-.08-.47 0-.59.22L2.74 8.87c-.12.21-.08.47.12.61l2.03 1.58c-.05.3-.09.63-.09.94s.02.64.07.94l-2.03 1.58c-.18.14-.23.41-.12.61l1.92 3.32c.12.22.37.29.59.22l2.39-.96c.5.38 1.03.7 1.62.94l.36 2.54c.05.24.24.41.48.41h3.84c.24 0 .44-.17.47-.41l.36-2.54c.59-.24 1.13-.56 1.62-.94l2.39.96c.22.08.47 0 .59-.22l1.92-3.32c.12-.22.07-.47-.12-.61zM12 15.6c-1.98 0-3.6-1.62-3.6-3.6s1.62-3.6 3.6-3.6 3.6 1.62 3.6 3.6-1.62 3.6-3.6 3.6",
  topics:
    "M4 5c0-1.1 3.58-2 8-2s8 .9 8 2-3.58 2-8 2-8-.9-8-2zm0 3.5c1.74.99 4.76 1.5 8 1.5s6.26-.51 8-1.5V12c0 1.1-3.58 2-8 2s-8-.9-8-2V8.5zm0 7c1.74.99 4.76 1.5 8 1.5s6.26-.51 8-1.5V19c0 1.1-3.58 2-8 2s-8-.9-8-2v-3.5z",
  groups:
    "M16 11c1.66 0 2.99-1.34 2.99-3S17.66 5 16 5s-3 1.34-3 3 1.34 3 3 3zM8 11c1.66 0 2.99-1.34 2.99-3S9.66 5 8 5 5 6.34 5 8s1.34 3 3 3zm0 2c-2.33 0-7 1.17-7 3.5V19h14v-2.5C15 14.17 10.33 13 8 13zm8 0c-.29 0-.62.02-.97.05 1.16.84 1.97 1.97 1.97 3.45V19h6v-2.5c0-2.33-4.67-3.5-7-3.5z",
  transforms:
    "M4 4h6v6H4V4zm10 0h6v6h-6V4zM4 14h6v6H4v-6zm10 1h2v-3h2v3h3v2h-3v3h-2v-3h-2v-2zM10 7h4v2h-4V7zm-3 3h2v4H7v-4z",
  stop: "M6 6h12v12H6V6z",
});

const kafkaGlyphPath =
  "M201.816 230.216c-16.186 0-30.697 7.171-40.634 18.461l-25.463-18.026c2.703-7.442 4.255-15.433 4.255-23.797 0-8.219-1.498-16.076-4.112-23.408l25.406-17.835c9.936 11.233 24.409 18.365 40.548 18.365 29.875 0 54.184-24.305 54.184-54.184 0-29.879-24.309-54.184-54.184-54.184-29.875 0-54.184 24.305-54.184 54.184 0 5.348.808 10.505 2.258 15.389l-25.423 17.844c-10.62-13.175-25.911-22.374-43.333-25.182v-30.64c24.544-5.155 43.037-26.962 43.037-53.019C124.171 24.305 99.862 0 69.987 0 40.112 0 15.803 24.305 15.803 54.184c0 25.708 18.014 47.246 42.067 52.769v31.038C25.044 143.753 0 172.401 0 206.854c0 34.621 25.292 63.374 58.355 68.94v32.774c-24.299 5.341-42.552 27.011-42.552 52.894 0 29.879 24.309 54.184 54.184 54.184 29.875 0 54.184-24.305 54.184-54.184 0-25.883-18.253-47.553-42.552-52.894v-32.775a69.965 69.965 0 0 0 42.6-24.776l25.633 18.143c-1.423 4.84-2.22 9.946-2.22 15.24 0 29.879 24.309 54.184 54.184 54.184 29.875 0 54.184-24.305 54.184-54.184 0-29.879-24.309-54.184-54.184-54.184zm0-126.695c14.487 0 26.27 11.788 26.27 26.271s-11.783 26.27-26.27 26.27-26.27-11.787-26.27-26.27c0-14.483 11.783-26.271 26.27-26.271zm-158.1-49.337c0-14.483 11.784-26.27 26.271-26.27s26.27 11.787 26.27 26.27c0 14.483-11.783 26.27-26.27 26.27s-26.271-11.787-26.271-26.27zm52.541 307.278c0 14.483-11.783 26.27-26.27 26.27s-26.271-11.787-26.271-26.27c0-14.483 11.784-26.27 26.271-26.27s26.27 11.787 26.27 26.27zm-26.272-117.97c-20.205 0-36.642-16.434-36.642-36.638 0-20.205 16.437-36.642 36.642-36.642 20.204 0 36.641 16.437 36.641 36.642 0 20.204-16.437 36.638-36.641 36.638zm131.831 67.179c-14.487 0-26.27-11.788-26.27-26.271s11.783-26.27 26.27-26.27 26.27 11.787 26.27 26.27c0 14.483-11.783 26.271-26.27 26.271z";

export interface WorkbenchIconProperties extends Omit<SvgIconProps, "children"> {
  readonly name: WorkbenchIconName;
}

export function WorkbenchIcon({ name, ...properties }: WorkbenchIconProperties): React.JSX.Element {
  return (
    <SvgIcon aria-hidden="true" {...properties} viewBox="0 0 24 24">
      <path d={iconPaths[name]} />
    </SvgIcon>
  );
}

export interface KafkaResourceIconProperties extends Omit<SvgIconProps, "children" | "sx"> {
  readonly connected: boolean;
  readonly streaming: boolean;
}

export function KafkaResourceIcon({
  connected,
  streaming,
  ...properties
}: KafkaResourceIconProperties): React.JSX.Element {
  return (
    <SvgIcon
      aria-hidden="true"
      data-streaming={connected && streaming ? "true" : "false"}
      data-testid="kafka-resource-icon"
      {...properties}
      sx={{
        color: connected ? "success.main" : "text.disabled",
        filter: connected && streaming ? "drop-shadow(0 0 2px currentColor)" : "none",
        height: streamSkopeGeometry.resourceIconSize,
        opacity: connected ? 1 : 0.72,
        width: streamSkopeGeometry.resourceIconSize,
      }}
      viewBox="0 0 256 256"
    >
      <g transform="translate(60, 22) scale(0.50)">
        <path d={kafkaGlyphPath} fill="currentColor" />
      </g>
    </SvgIcon>
  );
}
