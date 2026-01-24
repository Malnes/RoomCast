# RoomCast node agent API (shared)

This document defines the HTTP API used by the RoomCast controller for all hardware nodes.

## Auth
- All endpoints except `/health` and `/pair` require the `X-Agent-Secret` header.

## Endpoints

### `GET /health`
Returns node status and capabilities.

Response (example):
```json
{
  "status": "ok",
  "paired": true,
  "configured": true,
  "version": "0.3.23",
  "updating": false,
  "playback_device": "i2s",
  "outputs": {"selected": "i2s", "options": [{"id": "i2s", "label": "I2S DAC"}]},
  "fingerprint": "<unique-node-id>",
  "max_volume_percent": 100,
  "wifi": {"percent": 76, "signal_dbm": -57, "interface": "wifi"},
  "eq": {"preset": "peq31", "band_count": 31, "bands": []},
  "eq_max_bands": 5,
  "eq_active_bands": 2
}
```

### `GET /pair`
Returns pairing state.
```json
{"paired": true}
```

### `POST /pair`
Request a new controller secret.

Request:
```json
{"force": true, "recovery_code": "123456"}
```

Response:
```json
{"secret": "<token>"}
```

### `GET /config/snapclient`
Get audio config / snapclient status.

Response:
```json
{"config": {"snapserver_host": "192.168.1.10", "snapserver_port": 1704}, "configured": true, "running": true}
```

### `POST /config/snapclient`
Set snapserver host/port.

Request:
```json
{"snapserver_host": "192.168.1.10", "snapserver_port": 1704}
```

### `POST /volume`
Request volume (0–100).

### `POST /mute`
Request mute toggle.

### `POST /eq`
Update EQ bands.

Request:
```json
{
  "preset": "peq31",
  "band_count": 31,
  "bands": [{"freq": 1000, "gain": -2.5, "q": 1.0}]
}
```

### `POST /stereo`
Set stereo mode (`both`, `left`, `right`).

### `POST /outputs`
Select output device.

### `POST /update`
Trigger OTA update.

### `POST /restart`
Restart the node.
