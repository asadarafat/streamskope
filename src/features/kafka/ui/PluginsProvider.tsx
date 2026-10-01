import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type PropsWithChildren,
} from "react";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import type { PluginRenderer } from "../../../plugins/renderer-api";
import type {
  PluginInstallation,
  PluginManifest,
  PluginProfileSource,
  PluginSnapshot,
} from "../../../plugins/contracts";

export type PluginRendererImporter = (url: string) => Promise<{ readonly default: PluginRenderer }>;

export interface LoadedPluginRenderer {
  readonly manifest: PluginManifest;
  readonly renderer: PluginRenderer;
  readonly activationId: string;
  readonly lifetime: AbortSignal;
}

interface PluginRenderers {
  readonly plugins: readonly LoadedPluginRenderer[];
  readonly errors: Readonly<Record<string, string>>;
  readonly loading: boolean;
  readonly refresh: () => Promise<void>;
}

const PluginContext = createContext<PluginRenderers>({
  plugins: [],
  errors: {},
  loading: false,
  refresh: () => Promise.resolve(),
});

/** Optional presentation hooks must not make saved profiles unusable when a plugin fails. */
export function pluginProfileText(
  renderer: PluginRenderer | undefined,
  contribution: "profileLabel" | "profileSummary",
  source: PluginProfileSource,
): string | undefined {
  try {
    const text = renderer?.[contribution]?.(source);
    return typeof text === "string" ? text : undefined;
  } catch {
    return undefined;
  }
}

function importRenderer(url: string): Promise<{ readonly default: PluginRenderer }> {
  return import(/* @vite-ignore */ url) as Promise<{ readonly default: PluginRenderer }>;
}

/** Installed modules share the application origin; remote scripts are never imported. */
export function pluginAssetUrl(value: string, extension: "js" | "css"): string {
  const current = new URL(globalThis.location.href);
  const url = new URL(value, current);
  if (
    url.protocol !== current.protocol ||
    url.host !== current.host ||
    url.username.length > 0 ||
    url.password.length > 0 ||
    url.search.length > 0 ||
    url.hash.length > 0 ||
    !url.pathname.startsWith("/plugins/") ||
    !url.pathname.endsWith(`/renderer.${extension}`)
  )
    throw new Error("The plugin renderer must be an installed application asset.");
  return url.href;
}

export function PluginsProvider({
  children,
  host,
  importer = importRenderer,
}: PropsWithChildren<{
  readonly host: StreamSkopeHost;
  readonly importer?: PluginRendererImporter | undefined;
}>): React.JSX.Element {
  const refresh = useRef<() => Promise<void>>(() => Promise.resolve());
  const refreshPlugins = useCallback(() => refresh.current(), []);
  const [state, setState] = useState<Omit<PluginRenderers, "refresh">>({
    plugins: [],
    errors: {},
    loading: true,
  });

  useEffect(() => {
    let active = true;
    let revision = -1;
    let operation = 0;
    const loaded = new Map<
      string,
      {
        plugin: LoadedPluginRenderer;
        lifetime: AbortController;
        stylesheet?: HTMLLinkElement;
      }
    >();
    const rejected = new Map<string, string>();
    const retire = (id: string): void => {
      const entry = loaded.get(id);
      entry?.lifetime.abort();
      entry?.stylesheet?.remove();
      loaded.delete(id);
    };
    const generation = (entry: PluginInstallation): string | undefined => entry.activationId;

    async function reconcile(snapshot: PluginSnapshot, force = false): Promise<void> {
      if (!active || snapshot.revision < revision || (!force && snapshot.revision === revision))
        return;
      revision = snapshot.revision;
      const currentOperation = ++operation;
      const installations = snapshot.plugins.filter(
        (entry) =>
          entry.active !== undefined &&
          entry.rendererUrl !== undefined &&
          generation(entry) !== undefined,
      );
      // Revocation is synchronous: detached renderers cannot dispatch while React catches up.
      for (const [id, entry] of loaded) {
        if (
          !installations.some(
            (installation) =>
              installation.id === id && generation(installation) === entry.plugin.activationId,
          )
        )
          retire(id);
      }
      const errors: Record<string, string> = {};
      if (snapshot.error !== undefined) errors.host = snapshot.error;
      for (const installation of snapshot.plugins) {
        if (installation.error !== undefined) errors[installation.id] = installation.error;
      }
      setState({
        plugins: [...loaded.values()].map((entry) => entry.plugin),
        errors,
        loading: true,
      });
      for (const installation of installations) {
        if (!active || currentOperation !== operation) return;
        if (loaded.has(installation.id)) continue;
        const activationId = generation(installation);
        if (
          activationId === undefined ||
          installation.active === undefined ||
          installation.rendererUrl === undefined
        )
          continue;
        const activationKey = `${installation.id}:${activationId}`;
        const rejectedMessage = rejected.get(activationKey);
        if (!force && rejectedMessage !== undefined) {
          errors[installation.id] = rejectedMessage;
          continue;
        }
        try {
          const module = await importer(pluginAssetUrl(installation.rendererUrl, "js"));
          if (!active || currentOperation !== operation) return;
          const renderer = module.default;
          if (
            renderer.apiVersion !== installation.active.apiVersion ||
            renderer.id !== installation.id ||
            typeof renderer.mount !== "function" ||
            (renderer.profileLabel !== undefined && typeof renderer.profileLabel !== "function") ||
            (renderer.profileSummary !== undefined &&
              typeof renderer.profileSummary !== "function") ||
            !Array.isArray(renderer.connectionActions) ||
            renderer.connectionActions.some(
              (action: unknown) =>
                action === null ||
                typeof action !== "object" ||
                !("id" in action) ||
                typeof action.id !== "string" ||
                !("label" in action) ||
                typeof action.label !== "string",
            )
          )
            throw new Error("The installed plugin renderer is incompatible.");
          const lifetime = new AbortController();
          let stylesheet: HTMLLinkElement | undefined;
          if (installation.stylesUrl !== undefined) {
            stylesheet = document.createElement("link");
            stylesheet.rel = "stylesheet";
            stylesheet.href = pluginAssetUrl(installation.stylesUrl, "css");
            document.head.append(stylesheet);
          }
          loaded.set(installation.id, {
            plugin: {
              manifest: installation.active,
              renderer,
              activationId,
              lifetime: lifetime.signal,
            },
            lifetime,
            ...(stylesheet === undefined ? {} : { stylesheet }),
          });
        } catch (error) {
          if (!active || currentOperation !== operation) return;
          const message = error instanceof Error ? error.message : "Plugin UI failed to load.";
          errors[installation.id] = message;
          rejected.set(activationKey, message);
          try {
            const response = await host.execute({
              command: "plugins.renderer.failed",
              id: globalThis.crypto.randomUUID(),
              payload: { pluginId: installation.id, activationId, error: message },
              version: HOST_PROTOCOL_VERSION,
            });
            if (response.ok) await reconcile(response.result.pluginSnapshot);
          } catch {
            // Keep the actionable renderer error even if the host is unavailable.
          }
        }
      }
      if (active && currentOperation === operation) {
        setState({
          plugins: [...loaded.values()].map((entry) => entry.plugin),
          errors,
          loading: false,
        });
      }
    }

    async function load(force = false): Promise<void> {
      try {
        const response = await host.execute({
          command: "plugins.list",
          id: globalThis.crypto.randomUUID(),
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        });
        if (!response.ok) throw new Error(response.error.summary);
        await reconcile(response.result.pluginSnapshot, force);
      } catch (error) {
        if (active)
          setState((current) => ({
            ...current,
            loading: false,
            errors: {
              ...current.errors,
              host: error instanceof Error ? error.message : "Plugins could not be loaded.",
            },
          }));
      }
    }
    refresh.current = (): Promise<void> => load(true);
    const unsubscribe = host.subscribe((event) => {
      if (event.event === "plugins.changed") void reconcile(event.payload);
    });
    void load();
    return (): void => {
      active = false;
      operation += 1;
      unsubscribe();
      for (const id of loaded.keys()) retire(id);
      refresh.current = (): Promise<void> => Promise.resolve();
    };
  }, [host, importer]);

  return (
    <PluginContext.Provider value={{ ...state, refresh: refreshPlugins }}>
      {children}
    </PluginContext.Provider>
  );
}

export function usePlugins(): PluginRenderers {
  return useContext(PluginContext);
}
