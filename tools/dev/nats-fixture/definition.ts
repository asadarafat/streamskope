/** One pinned server owner shared by persistent AIO and isolated qualification. */
export const NATS_SERVER_IMAGES = {
  arm64:
    "nats:2.15.0-alpine@sha256:d5091b05d2033732bf4b282301e4d588f49e8fa9bb58ea0387a0ecea08277bd3",
  x64: "nats:2.15.0-alpine@sha256:eda962d67930eda338222072d9a9f3818855d922ad224c399b0b01d251e9b91b",
} as const;
