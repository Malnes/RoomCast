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

## Recovering a node when the old controller is gone

If a node is still paired to a controller that no longer exists, you do **not** need to reinstall from scratch.

- On boot, if the node is paired and its configured snapserver host is unreachable, it will blink a 6-digit recovery code for ~10 minutes.
- In the new RoomCast UI, click **Pair node** and enter the recovery code when prompted.

If you don't have a controllable LED on that board (or permissions prevent access), you can still recover by temporarily re-enabling SSH and using the pairing endpoint manually.

## First-boot install (no SSH)

If you want to provision nodes without SSHing into each Pi, you can inject a one-time systemd unit into the SD card **before** first boot.
On first boot, the Pi will download and run the installer, then disable itself.

Prereq: you still need to preconfigure Wi‑Fi/Ethernet and (optionally) SSH credentials using Raspberry Pi Imager.

### On a Linux workstation

1) Flash Raspberry Pi OS Lite as usual.
2) Insert the SD card and mount the root partition (usually partition 2):

```bash
sudo mount /dev/sdX2 /mnt/pi-root
```

3) From a clone of this repo, run:

```bash
cd node-agent/firstboot
sudo ./prepare-sd-linux.sh --root /mnt/pi-root --ref main --user pi
sudo umount /mnt/pi-root
```

4) Boot the Pi.

Progress/diagnostics on the Pi:
- Logs: `/var/log/roomcast-firstboot.log`
- Marker file after success: `/var/lib/roomcast/firstboot.done`

After the first boot finishes, register the node in the RoomCast dashboard as usual.

   ## Audio mixer control notes

   Most Pi images expose a `Master` simple mixer control, but some HDMI or USB DACs only provide controls like `PCM`, `Digital`, or `Speaker`. The agent now auto-detects the first available control when `Master` is missing so mute/volume actions keep working out of the box. If you want to force a specific control, set the `MIXER_CONTROL` environment variable in the `roomcast-agent` systemd unit (for example run `sudo systemctl edit roomcast-agent`, add `Environment=MIXER_CONTROL=PCM`, then `sudo systemctl restart roomcast-agent`).
