import { useMemo } from "react";
import { createRoot } from "react-dom/client";

import type { StreamSkopeDesktop } from "../../desktop";

import { installRendererRandomUuid } from "./crypto-compatibility";
import { resolveStreamSkopeHost } from "./host";
import { takeInitialQueryImport } from "./query-entry";
import { StreamSkopeProductApp } from "./StreamSkopeProductApp";

declare global {
  interface Window {
    streamSkopeDesktop?: StreamSkopeDesktop;
  }
}

installRendererRandomUuid(globalThis.crypto);
const initialQueryImport = takeInitialQueryImport(window);

function StreamSkopeApplication(): React.JSX.Element {
  const host = useMemo(() => resolveStreamSkopeHost(window), []);
  return (
    <StreamSkopeProductApp
      kafka={{ desktop: window.streamSkopeDesktop, host, initialQueryImport }}
    />
  );
}

const rootElement = document.querySelector("#root");
if (rootElement === null) {
  throw new Error("StreamSkope renderer root was not found.");
}

// Downloaded plugins may bundle their own React runtime and ID counter.
createRoot(rootElement, { identifierPrefix: "streamskope-core-" }).render(
  <StreamSkopeApplication />,
);
