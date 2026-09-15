# Runs the plugin attached to a RepeaterTastic elsewhere:
#   docker run -d --restart unless-stopped -e RT_PLUGIN_ADDR=repeatertastic:4450 -e RT_PLUGIN_TOKEN=rtp_… ghcr.io/scotmesh/repeatertastic-meshflow
# Most people install the zip bundle in RepeaterTastic instead; this image is for attached setups.
FROM --platform=$BUILDPLATFORM golang:1.25-bookworm AS build
ARG VERSION=dev
ARG TARGETOS TARGETARCH TARGETVARIANT
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Cross-compile natively (no emulation); arm/v6 and arm/v7 both come from GOARM.
RUN GOARM=${TARGETVARIANT#v} CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/repeatertastic-meshflow ./cmd/repeatertastic-meshflow

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/repeatertastic-meshflow /usr/local/bin/repeatertastic-meshflow
ENV RT_PLUGIN_ID=meshflow
ENTRYPOINT ["/usr/local/bin/repeatertastic-meshflow"]
