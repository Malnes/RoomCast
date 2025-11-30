# RoomCast Raspberry Pi Node (Quick Setup)

Follow these steps on a fresh Raspberry Pi OS **Lite** (or any Debian-based) install.

1. **Update the Pi**
   ```bash
   sudo apt update && sudo apt full-upgrade -y
   ```

2. **Run the one-command installer**
      Nodes now boot in an "unassigned" state with no knowledge of any controller. Pick whichever option fits how you access the repository today:

      **2a. Private repo (recommended): copy the local script**
      ```bash
      scp /home/<your-user>/repos/RoomCast/node-agent/install.sh pi@rcn.local:/tmp/roomcast-install.sh
      ssh pi@rcn.local
      sudo bash /tmp/roomcast-install.sh
      ```

      **2b. Public repo: curl directly from GitHub**
      ```bash
      curl -fsSL https://raw.githubusercontent.com/Malnes/RoomCast/main/node-agent/install.sh \
         | sudo bash
      ```

3. **Wait for services to start**
   The script installs everything, writes the systemd units, and enables them. You can confirm with:
   ```bash
   sudo systemctl status roomcast-agent roomcast-camilla
   ```

4. **Register and pair the node in RoomCast**
   Open the RoomCast dashboard (usually `http://localhost:8000` on the controller), click **Discover nodes**, and add the Pi (or add it manually with `http://<pi-ip>:9700`). The controller will automatically pair with the agent, push its snapserver host/port, and mint a per-controller key. If you move this Pi to a different RoomCast controller later, just click **Pair node** and **Configure audio** in the dashboard (or rerun the install script) to rotate the key and point the node at the new snapserver.

That’s it. No manual package installs, unit edits, or extra steps per Pi. Re-run the installer later to pull updates.
