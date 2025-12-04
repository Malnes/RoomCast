# Dual Radio Channels Plan

## Goals
- Add two configurable internet radio channels (channels 3 and 4) alongside existing Spotify channels 1 and 2.
- Keep the enable/disable behavior consistent across all channels so the carousel only shows active ones.
- Provide discovery, search, and favorites workflows powered by the Radio Browser API without exposing raw endpoints to the browser.
- Stream selected stations into Snapcast so every RoomCast node can play them with the same latency properties as Spotify streams.

## Architecture Overview
```
+----------------------------+       +-----------------------------+
|  Server (FastAPI)         |       |  Radio Worker (new service) |
|  - Channel API updates    | <---> |  - Station tuner (ffmpeg)   |
|  - Radio discovery proxy  |       |  - Snap FIFO writer         |
|  - State persistence      |       |  - Metadata broadcaster     |
+----------------------------+       +-----------------------------+
            |                                         |
            | writes channels.json                    | writes PCM/OGG to
            v                                         v
        channels.json                          snapserver FIFOs (CH3/CH4)
```

### Key components
1. **Channel metadata**: add `source` (`spotify` or `radio`) plus `radio_state` (station id/url, metadata timestamp). Default config ships with:
   - CH1/CH2: `source=spotify` (existing behavior)
   - CH3/CH4: `source=radio`, disabled until configured
2. **Radio Browser proxy routes** (FastAPI):
   - `GET /api/radio/genres`
   - `GET /api/radio/top`
   - `GET /api/radio/countries`
   - `GET /api/radio/search?q=...`
   All call `https://api.radio-browser.info/json/...`, with short-lived caching (e.g., 60s) to avoid rate limits.
3. **Station tune endpoint**: `POST /api/radio/{channel_id}/station` takes station payload (id, stream URL, display text). Persists to `channels.json`, signals worker to retune.
4. **Radio worker**: new container (Python) started via docker-compose, subscribed to `channels.json` updates (or API) and responsible for:
   - Managing two independent pipelines (one per radio channel) that fetch the configured station URL.
   - Using `ffmpeg`/`gst-launch` to convert whatever codec (mp3/aac/ogg) into PCM/OGG and write into the correct Snapcast FIFO path.
   - Handling retries/failover when a stream dies.
   - Emitting lightweight status (connected, buffering, last metadata) that the controller exposes via `/api/radio/status`.
5. **Front-end updates**:
   - Channel cards: display `Source: Spotify/Radio`, show station info when `source=radio`.
   - Player panel: when active channel is radio:
     - Disable shuffle/repeat/seek buttons.
     - Repurpose playlist button → “Discover stations” modal with tabs (Genres, Top, Country) using the new proxy APIs.
     - Repurpose search button → station search dialog.
     - Show station title/subtitle instead of Spotify track info when metadata is absent.
   - Discovery modal: tabs list stations; selecting one shows preview + "Tune channel" action.
   - Remember last tuned station per radio channel so switching back resumes instantly.

## Data Model Changes
```jsonc
{
  "id": "channel-3",
  "name": "Radio 1",
  "order": 3,
  "source": "radio",
  "snap_stream": "Radio_CH1",
  "enabled": false,
  "radio_state": {
    "station_id": null,
    "station_name": null,
    "stream_url": null,
    "last_metadata": null
  }
}
```
Notes:
- `snap_stream` stays because Snapcast still needs a FIFO per channel.
- `radio_state.station_id` ties back to Radio Browser API so we can refresh metadata.

## API Additions
1. `GET /api/radio/genres`
   - Returns `{ genres: [ { name, stationcount } ] }`
2. `GET /api/radio/top`
   - Query params `type=votes|clicks`, `limit`
3. `GET /api/radio/countries`
   - Returns `{ countries: [...] }`
4. `GET /api/radio/search?query=&country=&tag=`
5. `POST /api/radio/{channel_id}/station`
   - Validates `channel_id` is radio-enabled channel.
   - Persists `radio_state` and notifies worker.
6. `GET /api/radio/status/{channel_id}`
   - Returns worker heartbeat (connecting, playing, error, now playing, bitrate).

## Worker Implementation Notes
- Language: Python (keeps stack consistent) running in new `radio` container.
- Dependencies: `aiohttp` for control plane, `ffmpeg` binary for stream handling.
- Flow per channel:
  1. Long-poll `/api/radio/assignments` (or websockets) to receive new station config.
  2. When assigned, spawn `ffmpeg -i <url> -f ogg -acodec libvorbis -ac 2 -ar 48000 fifo_path`.
  3. Monitor process; restart on failure with exponential backoff.
  4. Parse ICY metadata (when available) and POST back to `/api/radio/status`.
- Security: Worker authenticates to controller using shared token or existing auth since it runs inside the same docker network.

## Front-End UX
### Player Panel (radio channel active)
- Title row: `station_name` (fallback to channel name) + optional `station_description`/country.
- Playlist button → Discover modal with tabs:
  - **Genres**: grid of top tags, tap to list stations.
  - **Top lists**: toggles for `Most Clicked` / `Most Voted`.
  - **Countries**: region selector + list.
- Search button → search field (debounced) hitting `/api/radio/search`.
- Each station card offers `Preview` (plays short sample locally?) or simply `Tune Channel`.
- Show "Currently tuned" badge for the channel’s active station.

### Settings Panel
- Channel cards show source badge (Spotify/Radio).
- Radio channels display station summary and "Change station" shortcut linking to Discover modal pre-filtered.

## Deployment Steps
1. Add new `radio` service to `docker-compose*.yml` with access to Snapcast FIFOs.
2. Update controller image build to include Radio Browser proxy dependencies.
3. Provide migration script to append two radio channel entries if missing.
4. Ensure Snapserver config defines FIFOs `Radio_CH1` and `Radio_CH2`.

## Milestones
1. **Data layer & defaults**: extend `channels.json`, migrations, API schemas.
2. **Radio worker**: container scaffolding, tune endpoint, FIFO streaming, status reporting.
3. **Controller endpoints**: discovery proxies, station assignment, status read.
4. **Frontend UX**: radio-aware player controls, discovery modal, settings updates.
5. **QA**: integration tests for station tuning, fallback when station offline, UI smoke tests.

This plan keeps Spotify and radio paths isolated while sharing the existing channel infrastructure, so adding more sources later (e.g., local library) stays straightforward.
