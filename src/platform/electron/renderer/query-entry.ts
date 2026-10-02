import { KAFKA_QUERY_TRANSFER_LIMITS } from "../../../features/kafka/contracts";

/** Read only the portable query fragment, then remove it from the visible URL/history entry. */
export function takeInitialQueryImport(browserWindow: Window): string | undefined {
  const hash = browserWindow.location.hash;
  if (!hash.startsWith("#query=")) return undefined;
  browserWindow.history.replaceState(
    {},
    browserWindow.document.title,
    browserWindow.location.pathname,
  );
  // Retain one excess character so the parser rejects oversized input rather than accepting truncation.
  return hash.slice(0, KAFKA_QUERY_TRANSFER_LIMITS.linkCharacters + 1);
}
