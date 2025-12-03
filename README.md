RoomCast – multiroom audio skeleton
===================================

This repository is a starting point for a Sonos‑style multiroom audio system. The goal is a central controller with a web UI, synchronized playback across hardware nodes, and per‑speaker volume/EQ. The stack leans on battle‑tested pieces so we only build the glue:

- Snapcast for perfectly synchronized PCM streaming and per‑client volume.
- Librespot to appear as a Spotify Connect endpoint and feed PCM into Snapcast.
- A FastAPI controller that exposes REST/WebSocket control and serves the web UI.
- Lightweight node agents (one per mini‑computer) to run `snapclient` and handle local volume/EQ hooks.

What exists here
----------------
- `docker-compose.yml` to spin up Snapserver, Librespot, and the controller (web UI).
- `server/` FastAPI + static web UI; all config (Spotify creds, node registration, per-node EQ/volume) is via the UI.
- `node-agent/` minimal agent to run on each speaker node (volume hook + health).
- Controller UI shows per-node agent versions and can trigger remote self-updates so you always know which Raspberry Pis run the latest bits.
- Built-in Spotify search palette (click the magnifying glass beside playlists) that queries albums, songs, artists, and playlists without leaving the app — results play instantly on the active RoomCast session.
- Built-in WebRTC “web nodes” reachable via the UI; the controller relays Snapcast audio through an internal snapclient + WebRTC bridge so any browser can join in sync without extra installs.
- Documentation for wiring Spotify and registering nodes; EQ support is stubbed but the hook is there so you can plug in ALSA LADSPA/pipewire effects.

High-level architecture
-----------------------
1) **Controller (FastAPI)**  
   - Tracks rooms/nodes, proxies basic Snapcast operations (list clients, set volumes, group mute).  
   - Serves the web UI and emits WebSocket events for live updates.  
   - Talks to Librespot via its `--name` (appears as “RoomCast”)—users pick it in Spotify and audio is piped into Snapserver.

2) **Snapserver + Librespot**  
   - Librespot writes PCM into a FIFO mounted into Snapserver (`/var/lib/snapserver/snapfifo`).  
   - Snapserver broadcasts that stream to all connected `snapclient` instances with tight sync.

3) **Speaker nodes**  
   - Run `snapclient` pointed at the controller’s Snapserver address.  
   - A small agent exposes HTTP endpoints so the controller can request volume/EQ changes; volume is applied via `amixer` (replace with your mixer/pipewire commands). EQ endpoint is a placeholder for your preferred DSP chain (e.g., `alsaequal`, `ladspa`, `loudgain`, `pipewire` filter graph).

Quick start
-----------
Prereqs: Docker, Docker Compose, Spotify Premium (Librespot needs it), open TCP 1704/1780 to nodes.

1) Local dev with auto-reload: `make dev` (uses `docker-compose.dev.yml` to mount code and reload FastAPI). Prod-ish run: `docker compose up -d`. The compose stack runs the controller, Snapserver, and Librespot in **host networking mode** so the controller automatically sees the same LAN interfaces as the host—no CIDR configuration, no hardcoded ranges, discovery “just works.” Run on a Linux host and make sure ports 8000/1704/1705/1780 are free.
   - Snapshot config: copy the sanitized example before starting containers so Docker has a host file to mount: `cp snapserver.example.json snapserver.json`. The runtime file is `.gitignore`d so your environment-specific client list never leaves your machine.
2) Open the web UI at `http://<controller-host>:8000`. The landing view lists nodes with per-node volume/mute/EQ; open the ⚙︎ settings menu to enter Spotify credentials + device name (Librespot reloads automatically), add nodes, and view Snapcast clients.  
3) On each hardware node (e.g. Raspberry Pi Zero 2), install `snapclient` and run the `node-agent` service (see `node-agent/README.md`). Native install via systemd is recommended on Pi Zero 2 for the lightest footprint; Docker is optional and supported with the provided Dockerfile. Register nodes via the web UI (name + agent URL).
4) To create an ad-hoc browser speaker, click **New web node** in the UI. A new tab will open, negotiate WebRTC against the controller, and the controller’s snapclient relay will keep that browser in lockstep with the rest of the Snapcast group.
5) Use the UI to set per-node EQ (bands JSON) and volume; adjust Snapcast client volumes in the “Snapcast clients” section.
Optional: if your host participates in multiple VLANs/subnets and you want to scan additional ranges beyond whatever interfaces the OS exposes, set `DISCOVERY_CIDR` to a comma/semicolon separated list (e.g. `192.168.1.0/24;10.10.0.0/24`). Those ranges will be scanned in addition to the interfaces that host networking already reveals.

Deploy from GHCR (Dockge/Portainer friendly)
-------------------------------------------
The repository ships a GitHub Actions workflow (`.github/workflows/publish-controller.yml`) that builds and pushes two images to GHCR whenever `main` changes:

- `ghcr.io/<owner>/roomcast-controller` – FastAPI web UI + API server with static assets baked in.
- `ghcr.io/<owner>/roomcast-librespot` – Librespot wrapper with the helper script used in development.

Once the workflow succeeds you can deploy on any host (or via Dockge/Portainer) with the production compose file:

1) Copy `docker-compose.deploy.yml` to the server (or paste it into Dockge). No Spotify secrets are required in advance—the controller stores them under `/config` and the UI prompts for them on first launch.
2) Optionally create a `.env` next to the compose file to override images (`ROOMCAST_CONTROLLER_IMAGE`, `ROOMCAST_LIBRESPOT_IMAGE`), device name (`LIBRESPOT_NAME`), or Snapserver host/port. Defaults point at GHCR `latest` tags.
3) Run `docker compose -f docker-compose.deploy.yml up -d` (Dockge will do the equivalent). The controller immediately serves the UI on port 8000; open it and use the ⚙︎ panel to enter Spotify Client ID/secret, device name, and other runtime settings. Credentials are persisted inside the mounted `config` volume, so containers can be rebuilt without re-entering secrets.

Because the controller image already contains the compiled UI, no bind mounts or build arguments are necessary—the compose stack works with only Docker + an internet connection to fetch the GHCR images.

Caveats / next steps
--------------------
- EQ: implement your chosen pipeline on the nodes (PipeWire filter chain or ALSA LADSPA) and update `node-agent/agent.py` in `set_eq`.  
- Auth: Librespot needs a real Spotify username/password or `--credentials` blob. Add auth to the controller UI if exposed on untrusted networks.  
- Persistence: the controller currently tracks rooms in memory; wire it to Redis/Postgres for durability.  
- Hardening: supervise `snapclient` on nodes with systemd; add TLS and auth between controller and agents; add Prometheus metrics.  
- Sources beyond Spotify: add an HTTP radio/line-in GStreamer sender into Snapserver as another stream.
