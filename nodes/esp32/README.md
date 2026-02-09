# RoomCast ESP32 node (ESP-IDF)

Target: ESP32-S3-WROOM1 (16MB flash, 8MB PSRAM).

## Features (WIP)
- Captive-portal Wi-Fi setup (SoftAP + local web form).
- RoomCast agent API endpoints (see [nodes/common/agent_api.md](../common/agent_api.md)).
- EQ support with max active band limit (default 5).
- Snapclient binary protocol (PCM) with I2S output (buffered, time-synced).
- OTA endpoint (HTTPS) with stored firmware URL.

## Build
```bash
idf.py set-target esp32s3
idf.py menuconfig
idf.py build
```

For ESP32-S3 N16R8 boards (16MB flash + 8MB PSRAM), defaults are provided in
`sdkconfig.defaults`. If your existing `sdkconfig` was created for a different board,
regenerate once:

```bash
rm -f sdkconfig
idf.py reconfigure
```

## Flash
```bash
idf.py -p /dev/ttyUSB0 flash monitor
```

## Wiring (PCM5102A I2S DAC)
- BCK  -> ESP32 I2S BCLK
- LCK  -> ESP32 I2S LRCLK
- DIN  -> ESP32 I2S DATA
- GND  -> GND
- VIN  -> 3.3V or 5V (check DAC board)

Default pins are set in [nodes/esp32/main/roomcast_config.h](main/roomcast_config.h) and can be adjusted there.

## OTA
The agent expects a stored OTA URL (NVS key `ota_url`).
- Send `POST /update` with `{ "url": "https://.../firmware.bin" }` once to store it.
- Subsequent update triggers can call `POST /update` without a body.
