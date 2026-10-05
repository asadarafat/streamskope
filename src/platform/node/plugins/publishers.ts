export interface TrustedPluginPublisher {
  readonly keyId: string;
  readonly name: string;
  readonly publicKey: string;
  readonly pluginIds: readonly string[];
}

/** Trust is shipped with the desktop; package contents cannot add publishers or permissions. */
export const TRUSTED_PLUGIN_PUBLISHERS: readonly TrustedPluginPublisher[] = Object.freeze([
  Object.freeze({
    keyId: "streamskope-d2840096e865aa64",
    name: "StreamSkope",
    publicKey:
      "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAAFxlkqSrTyxyOymez4Vmns3YFBF8YUKEYwG37gXFbUs=\n-----END PUBLIC KEY-----\n",
    pluginIds: Object.freeze(["streamskope.eda", "streamskope.nsp"]),
  }),
]);
