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
2) Install snapclient (binary only; the agent now launches it when configured):
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
   sudo cp systemd-roomcast-agent.service /etc/systemd/system/roomcast-agent.service
   sudo systemctl daemon-reload
   sudo systemctl enable --now roomcast-agent.service
   ```
   The agent manages `snapclient` internally once a controller pushes configuration, so no separate snapclient systemd unit is required.

### One-command Raspberry Pi setup (recommended)
You can bootstrap a Pi in one step by piping the installer script from GitHub:

```
curl -fsSL https://raw.githubusercontent.com/Malnes/RoomCast/main/node-agent/install.sh \
   | sudo bash
```

Flags you might care about:

- `--user` – system user that should own the repo/services (defaults to `pi`)
- `--mixer` – ALSA mixer control to drive (`Master` by default)
- `--snap-port` – snapserver PCM port if you changed it (defaults to `1704`)

The script installs `snapclient`, pulls this repo into `/opt/roomcast`, sets up a Python venv, writes the CamillaDSP + node-agent services, and enables them so they persist across reboots. Newly installed nodes start in an unpaired state until you register them in the RoomCast dashboard, after which the controller pushes snapserver settings and secrets automatically. The node agent supervises `snapclient` directly once a controller configures it.

Pairing workflow
----------------
- When you register the node from the RoomCast dashboard (via **Discover nodes** or manual entry), the controller automatically calls the agent's `/pair` endpoint to mint a per-controller secret stored in `/var/lib/roomcast/agent-secret`.  
- Moving a node between controllers?
   - If you still have the old controller, click **Pair node** on the server UI. The controller proves knowledge of the existing node secret and rotates it safely.
   - If the old controller is gone and the node is already paired, use the **recovery code** flow below.
- You can rotate the key at any time; the controller will push the new secret to the agent and update its own record automatically.

Recovery code (takeover without reinstall)
----------------------------------------

If a node is already paired to a controller that is no longer reachable, the agent can open a short takeover window on boot:

- Condition: node is paired **and** it has a configured snapserver host, but that snapserver is unreachable at boot.
- Behavior: for ~10 minutes (configurable), the node will blink a 6-digit recovery code on its status LED (best-effort; hardware/permissions vary).

On the new RoomCast server:
- Discover/register the node as usual.
- Click **Pair node**. If takeover is required, the UI will prompt for the 6-digit code.

Notes:
- Digit encoding: each digit blinks that many times; `0` is encoded as 10 blinks.
- LED control relies on `/sys/class/leds` and may require permissions depending on distro/kernel.
- You can override LED selection with `RECOVERY_LED_PATH=/sys/class/leds/<name>` or disable it with `RECOVERY_LED_ENABLED=0`.

### Updating nodes
- Each agent exposes its version via `/health`, and the controller UI displays that version next to the node entry.  
- The installer now drops `/usr/local/bin/roomcast-updater` plus a sudoers rule so the controller can trigger updates remotely. Click **Update node** in the UI to download and install the latest `main` build on that Pi; the agent restarts automatically when the update completes.

### Docker on the node (optional)
The `node-agent/Dockerfile` builds for arm/v7 and arm64. If you prefer containerized deployment:
```
docker buildx build --platform linux/arm/v7,linux/arm64 -t yourrepo/roomcast-agent .
docker run -d --net=host -v /var/lib/roomcast:/var/lib/roomcast \
   -e AGENT_SECRET_PATH=/var/lib/roomcast/agent-secret \
   -e MIXER_CONTROL=Master \
   yourrepo/roomcast-agent
```
`--net=host` keeps latency low. Mount a persistent directory for `/var/lib/roomcast` so pairing secrets survive restarts, and make sure the `snapclient` binary is available inside the container image (the agent launches it automatically once configured).

Securing
--------
- Keep the agent on a trusted LAN; pairing secrets are issued per controller when you register the node and stored locally under `/var/lib/roomcast`. Re-registering from another RoomCast server automatically rotates the secret.  
- Restrict firewall rules so only trusted controllers can reach port 9700.

EQ hookup ideas
---------------
- ALSA: use the `alsaequal` or `ladspa` plugin and replace `_amixer_set` with commands that write to that control.  
- PipeWire: build a filter-chain JSON and reload it per request, or expose a `pw-link` preset and call it here.  
- DSP outboard: if using a miniDSP/DAW, swap the EQ endpoint to call its API instead of ALSA.
