# AGENTS.md

Guidance for coding agents (and humans) working on repeatertastic-meshflow.

## What this is

A RepeaterTastic plugin (Plugin API v1, `github.com/A13xB0/RepeaterTastic/pluginsdk`) that makes
each radio's relay persona a Meshflow feeder. It stands in for meshflow-bot, so the wire format
must match what meshflow-bot sends: meshflow-api is the judge.

## Layout

```
cmd/repeatertastic-meshflow   entry point and plugin.yaml (embedded; copied into the bundle)
internal/meshflow             meshflow-api client: packet JSON (MessageToDict shape), node upsert v3, WebSocket commands
internal/feeder               one reporter per radio: filtering, queue, retries, node change tracking, status
assets/logo.png               Meshflow logo (used with permission)
scripts/bundle.sh             the installable zip
```

## Commands

- `make test`: vet plus race tests. `gofmt -l .` must be empty.
- `make bundle`: the plugin zip. `make image`: the attached-mode container.

## Rules

- **Wire compatibility first.** Packet bodies copy the Meshtastic Python library's dict (camelCase,
  enum names, base64 bytes, `fromId`/`toId`, `decoded.<name>`) after meshflow-bot's sanitising.
  Node bodies use v3 `meshtastic_*` fields. Check changes against
  `meshflow-api/Meshflow/packets/serializers.py` and add a test.
- **Report only what the relay persona would hear.** Upload a packet only when
  `relay_channel_index >= 0` and it's addressed to broadcast or to the relay persona. Build nodes
  only from those packets (and the relay's own node), never from `ListNodes`/`NodeEvent`: the
  host's node database is shared by every identity.
- **Don't flood.** Traceroutes go through RepeaterTastic's budget. Don't add retries that transmit.
- **Never log the API key.** It travels in the WebSocket query; use `redact`.
- Commits end with the attribution lines the session asks for. `go.mod` pins RepeaterTastic to a
  commit on its `plugins` branch until that is merged and tagged: then pin the tag.
