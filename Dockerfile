# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETOS TARGETARCH TARGETVARIANT
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOARM=${TARGETVARIANT#v} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/loqed-mqtt ./cmd/loqed-mqtt \
 && mkdir -p /out/data

# Home Assistant add-on image: runs as root because the Supervisor mounts
# /data owned by root. The Supervisor watchdog replaces HEALTHCHECK.
FROM gcr.io/distroless/static-debian12 AS addon
LABEL org.opencontainers.image.source=https://github.com/t3hk0d3/go-loqed
COPY --from=build /out/loqed-mqtt /loqed-mqtt
ENTRYPOINT ["/loqed-mqtt"]

# Standalone image (default target): distroless nonroot. /data is owned by
# uid 65532, and Docker copies that ownership into new named volumes.
FROM gcr.io/distroless/static-debian12:nonroot AS standalone
LABEL org.opencontainers.image.source=https://github.com/t3hk0d3/go-loqed
COPY --from=build /out/loqed-mqtt /loqed-mqtt
COPY --from=build --chown=65532:65532 /out/data /data
EXPOSE 8099
VOLUME /data
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s CMD ["/loqed-mqtt", "healthcheck"]
ENTRYPOINT ["/loqed-mqtt"]
