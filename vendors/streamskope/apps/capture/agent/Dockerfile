FROM --platform=$BUILDPLATFORM golang:1.24.0-alpine3.21@sha256:2d40d4fc278dad38be0777d5e2a88a2c6dee51b0b29c97a764fc6c6a11ca893c AS build
WORKDIR /source
COPY capture/agent/go.mod ./
COPY capture/agent/*.go ./
COPY capture/agent/cmd ./cmd
RUN go test ./...
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/streamskope-capture-agent ./cmd/agent

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/streamskope-capture-agent /streamskope-capture-agent
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/streamskope-capture-agent"]
