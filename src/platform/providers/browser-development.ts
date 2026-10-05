export const BROWSER_DEVELOPMENT_GATEWAY_PATH = "/__streamskope_host";
export const BROWSER_DEVELOPMENT_SESSION_COOKIE = "streamskope_dev_session";

export function browserDevelopmentSessionCookie(rendererOrigin: string): string {
  return `${BROWSER_DEVELOPMENT_SESSION_COOKIE}_${new URL(rendererOrigin).port || "80"}`;
}
