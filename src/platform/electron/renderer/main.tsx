import { useMemo } from "react";
import { createRoot } from "react-dom/client";

import { StreamSkopeApp } from "../../../features/kafka/ui/StreamSkopeApp";
import type { StreamSkopeDesktop } from "../../desktop";

import { installRendererRandomUuid } from "./crypto-compatibility";
import { resolveStreamSkopeHost } from "./host";

declare global {
  interface Window {
    streamSkopeDesktop?: StreamSkopeDesktop;
  }
}

installRendererRandomUuid(globalThis.crypto);

function StreamSkopeApplication(): React.JSX.Element {
  const host = useMemo(() => resolveStreamSkopeHost(window), []);
  return <StreamSkopeApp desktop={window.streamSkopeDesktop} host={host} />;
}

const rootElement = document.querySelector("#root");
if (rootElement === null) {
  throw new Error("StreamSkope renderer root was not found.");
}

createRoot(rootElement).render(<StreamSkopeApplication />);
