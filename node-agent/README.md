RoomCast node agent
===================

Lightweight FastAPI service that sits on each speaker node. It exposes:
- `POST /volume` – set local mixer volume (default ALSA `Master` via `amixer`).
- `POST /eq` – stub to receive EQ settings; wire this to your DSP chain.
- `GET /health` – health and last EQ state.

Running
-------
### Raspberry Pi Zero 2 (recommended: native + systemd)
1) Flash Raspberry Pi OS Lite or DietPi (32-bit armhf is fine) and update packages.  
2) Install snapclient:
   ```
   sudo apt-get update
   sudo apt-get install -y snapclient
   ```
3) Install Python dependencies:
   ```
   sudo apt-get install -y python3 python3-pip alsa-utils
   cd /home/pi/roomcast/node-agent
   pip3 install --break-system-packages -r requirements.txt
   ```
4) Enable services (example systemd units provided in this folder):
   ```
   sudo cp systemd-snapclient.service /etc/systemd/system/snapclient.service
   sudo cp systemd-roomcast-agent.service /etc/systemd/system/roomcast-agent.service
   sudo systemctl daemon-reload
   sudo systemctl enable --now snapclient.service roomcast-agent.service
   ```
   Edit the unit files to point at your controller host and secrets.

### Docker on the node (optional)
The `node-agent/Dockerfile` builds for arm/v7 and arm64. If you prefer containerized deployment:
```
docker buildx build --platform linux/arm/v7,linux/arm64 -t yourrepo/roomcast-agent .
docker run -d --net=host --env AGENT_SHARED_SECRET=changeme --env MIXER_CONTROL=Master yourrepo/roomcast-agent
```
`--net=host` keeps latency low. You still need `snapclient` on the host or in its own container on the same network namespace.

Securing
--------
- Keep the agent on a trusted LAN and set `AGENT_SHARED_SECRET` to a strong value; the controller sends it as `X-Agent-Secret`.  
- Restrict firewall rules so only the controller can reach port 9700.

EQ hookup ideas
---------------
- ALSA: use the `alsaequal` or `ladspa` plugin and replace `_amixer_set` with commands that write to that control.  
- PipeWire: build a filter-chain JSON and reload it per request, or expose a `pw-link` preset and call it here.  
- DSP outboard: if using a miniDSP/DAW, swap the EQ endpoint to call its API instead of ALSA.
