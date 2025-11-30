# RoomCast Raspberry Pi Node (Quick Setup)

Follow these steps on a fresh Raspberry Pi OS **Lite** (or any Debian-based) install.

1. **Update the Pi**
   ```bash
   sudo apt update && sudo apt full-upgrade -y
   ```

2. **Run the one-command installer**
      Nodes now boot in an "unassigned" state with no knowledge of any controller. Fetch the installer directly from GitHub and run it with sudo:
      ```bash
      curl -fsSL https://raw.githubusercontent.com/Malnes/RoomCast/main/node-agent/install.sh \
         | sudo bash
      ```

3. **Wait for services to start**
   The script installs everything, writes the systemd units, and enables them. The node agent now starts independently, even if you skip CamillaDSP. You can confirm with:
   ```bash
   sudo systemctl status roomcast-agent roomcast-camilla
   ```

4. **Register and pair the node in RoomCast**
   Open the RoomCast dashboard (usually `http://localhost:8000` on the controller), click **Discover nodes**, and add the Pi (or add it manually with `http://<pi-ip>:9700`). The controller will automatically pair with the agent, push its snapserver host/port, and mint a per-controller key. If you move this Pi to a different RoomCast controller later, just click **Pair node** and **Configure audio** in the dashboard (or rerun the install script) to rotate the key and point the node at the new snapserver.
   Once registered you'll see the node's agent version plus an **Update node** button in the UI, which runs the installer again on that Pi so keeping nodes current is a single click.

That’s it. No manual package installs, unit edits, or extra steps per Pi. Re-run the installer later to pull updates.

   ## Audio mixer control notes

   Most Pi images expose a `Master` simple mixer control, but some HDMI or USB DACs only provide controls like `PCM`, `Digital`, or `Speaker`. The agent now auto-detects the first available control when `Master` is missing so mute/volume actions keep working out of the box. If you want to force a specific control, set the `MIXER_CONTROL` environment variable in the `roomcast-agent` systemd unit (for example run `sudo systemctl edit roomcast-agent`, add `Environment=MIXER_CONTROL=PCM`, then `sudo systemctl restart roomcast-agent`).
