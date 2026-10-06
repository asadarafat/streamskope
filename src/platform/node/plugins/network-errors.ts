import { isPluginProblem, pluginProblem } from "./problem";

/** Diagnose known network failures without reflecting URLs, local paths or proxy secrets. */
export function pluginNetworkProblem(error: unknown, signal?: AbortSignal): Error {
  if (isPluginProblem(error)) return error;
  let value = error;
  let diagnostic = "";
  for (let depth = 0; depth < 3 && value !== null && typeof value === "object"; depth += 1) {
    if (value instanceof Error) diagnostic += ` ${value.name} ${value.message}`;
    if ("code" in value && typeof value.code === "string") diagnostic += ` ${value.code}`;
    if ("status" in value && typeof value.status === "number")
      diagnostic += ` HTTP ${value.status}`;
    value = "cause" in value ? value.cause : undefined;
  }
  if (signal?.aborted && !(signal.reason instanceof Error && signal.reason.name === "TimeoutError"))
    return pluginProblem(
      "Plugin acquisition was cancelled.",
      "Retry when ready, or install a signed file or cached package.",
    );
  if (/407|PROXY_AUTH|proxy authentication/iu.test(diagnostic))
    return pluginProblem(
      "Plugin proxy authentication failed.",
      "Check the proxy username and password in Preferences > Plugins > Plugin download settings.",
    );
  if (/CERT_|certificate|trusted CA/iu.test(diagnostic))
    return pluginProblem(
      "Plugin download certificate validation failed.",
      "Install your organization's trusted CA in the operating system. Certificate verification remains enabled.",
    );
  if (/SSL_|TLS negotiation/iu.test(diagnostic))
    return pluginProblem(
      "Plugin TLS negotiation failed.",
      "Check the proxy's TLS protocol and tunnel policy, then retry. Certificate verification remains enabled.",
    );
  if (/TimeoutError|TIMED_OUT|timed out/iu.test(diagnostic))
    return pluginProblem(
      "Plugin acquisition timed out.",
      "Check the proxy and network, retry, or install a signed file or cached package.",
    );
  if (/HTTP (403|429)|rate.limit/iu.test(diagnostic))
    return pluginProblem(
      "GitHub temporarily limited plugin requests.",
      "Wait before retrying, or install a signed file or cached package.",
    );
  if (/PROXY_CONNECTION|TUNNEL_CONNECTION|proxy could not connect/iu.test(diagnostic))
    return pluginProblem(
      "The plugin proxy could not connect.",
      "Check its address, port and tunnel policy, or install a signed file or cached package.",
    );
  if (/SHA256|signature|publisher|manifest|size limit|conflicting package/iu.test(diagnostic))
    return pluginProblem(
      "The official plugin package or metadata could not be verified.",
      "Refresh the official catalog after the publisher corrects the release, or select a verified signed portable package.",
    );
  return pluginProblem(
    "Plugin download connection failed.",
    "Check system or custom proxy settings, retry, or install a signed file or cached package.",
  );
}
