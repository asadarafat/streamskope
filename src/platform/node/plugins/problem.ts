export function pluginProblem(
  summary: string,
  recovery = "Open Preferences > Plugins to install or repair the plugin.",
): Error {
  return Object.assign(new Error(summary), {
    code: "BACKEND_UNAVAILABLE",
    stage: "backend",
    retryable: false,
    recovery,
  });
}
