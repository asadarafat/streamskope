# syntax=docker/dockerfile:1
ARG NODE_IMAGE=node:24.21.0-bookworm-slim@sha256:d6aa754f16b3197301076f047b5def2f02ea1dbbc2ca920407d46d7ec7f87b20
FROM ${NODE_IMAGE} AS build
WORKDIR /build
ENV ELECTRON_SKIP_BINARY_DOWNLOAD=1 NODE_OPTIONS=--max-old-space-size=2048
COPY package.json package-lock.json LICENSE ./
RUN npm ci --ignore-scripts
ARG STREAMSKOPE_VERSION=0.0.0-dev
ARG STREAMSKOPE_SOURCE_SHA
RUN node -e "const actual=require('./package.json').version;if(actual!==process.env.STREAMSKOPE_VERSION)throw new Error('Image version must match the source manifest');if(!/^[0-9a-f]{40}$/.test(process.env.STREAMSKOPE_SOURCE_SHA||''))throw new Error('Image source revision must be a 40-character commit SHA');"
COPY config ./config
COPY tools ./tools
COPY src ./src
COPY plugins ./plugins
COPY test ./test
COPY index.html ./
RUN npm run build -- web
RUN node --import tsx tools/package/container-notices.ts
RUN npm prune --omit=dev --ignore-scripts

FROM ${NODE_IMAGE} AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates util-linux \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/streamskope
ARG STREAMSKOPE_VERSION=0.0.0-dev
ARG STREAMSKOPE_SOURCE_SHA
LABEL org.opencontainers.image.title="StreamSkope browser" \
      org.opencontainers.image.source="https://github.com/asadarafat/streamskope" \
      org.opencontainers.image.version="${STREAMSKOPE_VERSION}" \
      org.opencontainers.image.revision="${STREAMSKOPE_SOURCE_SHA}" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /build/package.json ./package.json
COPY --from=build /build/LICENSE ./LICENSE
COPY --from=build /build/dist/web/THIRD_PARTY_LICENSES.json ./THIRD_PARTY_LICENSES.json
COPY --from=build /build/node_modules ./node_modules
COPY --from=build /build/dist/renderer ./dist/renderer
COPY --from=build /build/dist/web/server.cjs ./dist/web/server.cjs
COPY --from=build /build/dist/web/data-preflight.cjs ./dist/web/data-preflight.cjs
COPY --from=build /build/dist/web/record-codec-worker.cjs ./dist/web/record-codec-worker.cjs
COPY --from=build /build/dist/web/trust-material-worker.cjs ./dist/web/trust-material-worker.cjs
ENV NODE_ENV=production \
    NODE_OPTIONS=--max-old-space-size=512 \
    STREAMSKOPE_DATA_DIR=/data \
    STREAMSKOPE_LISTEN_HOST=0.0.0.0 \
    STREAMSKOPE_PORT=8080 \
    STREAMSKOPE_PUBLIC_ORIGIN=http://127.0.0.1:8080
USER 1000:1000
EXPOSE 8080
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD node -e "const http=require('node:http');const origin=new URL(process.env.STREAMSKOPE_PUBLIC_ORIGIN);const req=http.get({host:'127.0.0.1',port:process.env.STREAMSKOPE_PORT,path:'/health',headers:{host:origin.host}},res=>{res.resume();process.exit(res.statusCode===200?0:1);});req.on('error',()=>process.exit(1));req.setTimeout(2000,()=>{req.destroy();process.exit(1);});"
ENTRYPOINT ["node", "dist/web/server.cjs"]
