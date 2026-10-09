const trustedProblems = new WeakSet<Error>();

/** Only diagnostics deliberately created at the host boundary bypass sanitization. */
export function isPluginProblem(error: unknown): error is Error {
  return error instanceof Error && trustedProblems.has(error);
}

export function pluginProblem(
  summary: string,
  recovery = "Open Preferences > Plugins to install or repair the plugin.",
): Error {
  const error = Object.assign(new Error(summary), {
    code: "BACKEND_UNAVAILABLE",
    stage: "backend",
    retryable: false,
    recovery,
  });
  trustedProblems.add(error);
  return error;
}

export function pluginErrorSummary(error: unknown): string {
  return error instanceof Error ? error.message.slice(0, 1_024) : "The plugin could not be loaded.";
}
