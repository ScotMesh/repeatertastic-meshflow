![ScotMesh Meshtastic](https://raw.githubusercontent.com/ScotMesh/branding/main/networks/meshtastic/readme-header.png)

<p align="center">
  <img src="assets/logo.png" width="96" alt="Meshflow">
</p>

# Meshflow for RepeaterTastic

A [RepeaterTastic](https://github.com/A13xB0/RepeaterTastic) plugin that feeds
[Meshflow](https://github.com/pskillen/meshflow-api). Each of the site's radios becomes a Meshflow
feeder and reports as its **relay persona**, the node that radio already is on the mesh. It does
what [meshflow-bot](https://github.com/pskillen/meshflow-bot) does for a Meshtastic node, without
a separate radio or bot.

- **Packets:** text messages, positions, node info, telemetry and traceroutes that the relay persona
  hears on its channels, plus DMs to it. Other identities' private messages are never sent.
- **Nodes:** each radio's node list is uploaded at start, and nodes are updated as they change.
- **Traceroutes:** Meshflow's traceroute requests arrive over the command WebSocket and are sent
  from the relay persona. They stay within RepeaterTastic's plugin budget and duty cycle.
- **Status** on the plugin's card in RepeaterTastic, per radio: packets, nodes, traceroutes, and
  anything Meshflow refused.

## Install

1. In Meshflow, add each radio's relay persona as a **managed node** and create a **node API key**
   linked to it (one key can cover several radios).
   - To claim the node, send the claim key as a DM from the relay persona to a Meshflow feeder:
     **Chat → Speaking as** the relay persona in RepeaterTastic.
2. Download `repeatertastic-meshflow-<version>.zip` from the
   [releases](https://github.com/ScotMesh/repeatertastic-meshflow/releases).
3. In RepeaterTastic, go to **Plugins → Install plugin** and drop the zip, or paste its release URL.
4. Open **Meshflow → Settings** and set the **Meshflow API URL** and **Node API key**.
5. Turn it on and review the permissions: `packets.read`, `nodes.read` and `traceroute.send`.

Without the GUI:

```bash
sudo -u repeatertastic repeatertastic plugin install https://github.com/ScotMesh/repeatertastic-meshflow/releases/download/v0.1.0/repeatertastic-meshflow-v0.1.0.zip
```

Or pin it in `repeatertastic.yaml` and keep the key in the environment:

```yaml
plugins:
    entries:
        - id: meshflow
          enabled: true
          permissions: [packets.read, nodes.read, traceroute.send]
          settings:
              api_url: https://meshflow.example.org
              api_key: ${MESHFLOW_API_KEY}
```

## Settings

| Setting | meshflow-bot equivalent | |
| --- | --- | --- |
| Meshflow API URL | `STORAGE_API_ROOT` | The API server; a trailing `/api` is fine |
| Node API key | `STORAGE_API_TOKEN` | Linked in Meshflow to every relay persona you feed |
| Command WebSocket URL | `MESHFLOW_WS_URL` | Empty = derived from the API URL |
| Radios | | Tick the radios to feed; none ticked = every radio |
| Upload packets / nodes | | Both on by default |
| Run Meshflow's traceroutes | | On by default. Raise `plugins.traceroutes_per_hour` in RepeaterTastic if Meshflow asks for more than 12 an hour |
| Don't upload | `IGNORE_PORTNUMS` | Tick packet types to keep out of Meshflow |

Uses Meshflow's feeder API v3: `POST /api/v3/packets/{node}/ingest/` and `/nodes/`,
`PUT …/bot-version/`, and `ws/nodes/?feeder_node_id=…`. The node is the relay persona's node
number. The WebSocket carries `feeder_node_id`, so one key can serve several radios.

## Run it attached

To run it next to RepeaterTastic instead of inside it:

1. Set `plugins.listen` in RepeaterTastic.
2. **Plugins → Attach** with id `meshflow`, then copy the token.
3. Start the container:

```bash
docker run -d --name meshflow -e RT_PLUGIN_ADDR=<repeatertastic-host>:4450 -e RT_PLUGIN_TOKEN=rtp_… ghcr.io/scotmesh/repeatertastic-meshflow
```

## Build

```bash
make test
make bundle        # dist/repeatertastic-meshflow-<version>.zip for arm64, arm (Pi) and amd64
make image
```

## Licence

GPL-3.0-or-later. A Go port of meshflow-bot's Meshtastic feeder (MIT); see [NOTICE.md](NOTICE.md).
The Meshflow logo is used with the Meshflow maintainer's permission.
