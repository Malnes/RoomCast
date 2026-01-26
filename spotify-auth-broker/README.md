# Spotify Auth Broker (RoomCast)

Small OAuth callback service used to avoid local HTTPS requirements on devices. This is intended to run on a public HTTPS domain (e.g. `https://auth.jepp.xyz`).

## What it does
- Receives Spotify OAuth callback (`/callback`).
- Exchanges the authorization code for tokens.
- Forwards the token payload to a RoomCast device callback URL (provided in a signed `state` payload).

## Required environment variables
- `SPOTIFY_CLIENT_ID` – Spotify app client ID.
- `SPOTIFY_CLIENT_SECRET` – Spotify app client secret.
- `ROOMCAST_SHARED_SECRET` – HMAC secret shared with RoomCast for signing the `state` payload.
- `PUBLIC_BASE_URL` – Public base URL of this service, e.g. `https://auth.jepp.xyz`.
- `ALLOWED_CALLBACK_HOSTS` – Comma-separated allowlist of RoomCast callback hosts (e.g. `roomcast.local,192.168.1.10`).

Optional:
- `STATE_TTL_SECONDS` – Default 600 seconds.

## Docker

Build:
```
docker build -t roomcast-spotify-auth ./spotify-auth-broker
```

Run:
```
docker run -p 18088:18088 \
  -e SPOTIFY_CLIENT_ID=... \
  -e SPOTIFY_CLIENT_SECRET=... \
  -e ROOMCAST_SHARED_SECRET=... \
  -e PUBLIC_BASE_URL=https://auth.jepp.xyz \
  -e ALLOWED_CALLBACK_HOSTS=roomcast.local,192.168.1.10 \
  roomcast-spotify-auth
```

## State payload format
The broker expects `state` formatted as:
```
base64url(payload).base64url(hmac_sha256(payload_b64, ROOMCAST_SHARED_SECRET))
```

Example payload JSON:
```
{
  "iat": 1730000000,
  "callback_url": "http://roomcast.local:8000/api/spotify/broker",
  "device_id": "node-abc",
  "source_id": "spotify:a"
}
```

RoomCast should generate and sign the `state` and pass it to Spotify `/authorize`.
The broker will POST the token to the `callback_url`.
