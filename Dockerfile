# Runs the plugin attached to a RepeaterTastic elsewhere:
#   docker run -e RT_PLUGIN_ADDR=repeatertastic:4450 -e RT_PLUGIN_TOKEN=rtp_… ghcr.io/scotmesh/repeatertastic-meshflow
# Most people install the zip bundle in RepeaterTastic instead; this image is for attached setups.
FROM golang:1.25-bookworm AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/repeatertastic-meshflow ./cmd/repeatertastic-meshflow

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/repeatertastic-meshflow /usr/local/bin/repeatertastic-meshflow
ENV RT_PLUGIN_ID=meshflow
ENTRYPOINT ["/usr/local/bin/repeatertastic-meshflow"]
