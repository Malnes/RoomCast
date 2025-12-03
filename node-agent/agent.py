import asyncio
import json
import logging
import os
import re
import secrets
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Callable, List, Optional

try:  # Camilla v3 exposes control API over WebSocket
    import websockets
    from websockets import exceptions as ws_exceptions
except ImportError:  # pragma: no cover - optional dependency
    websockets = None  # type: ignore
    ws_exceptions = None  # type: ignore

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field


AGENT_VERSION = os.getenv("AGENT_VERSION", "0.3.21")
MIXER_CONTROL = os.getenv("MIXER_CONTROL", "Master")
MIXER_FALLBACKS = [
    MIXER_CONTROL,
    "Master",
    "PCM",
    "Digital",
    "Speaker",
    "Headphone",
    "Playback",
]
PLAYBACK_DEVICE = os.getenv("PLAYBACK_DEVICE", "hw:Loopback,0,0")
DRY_RUN = os.getenv("AGENT_DRY_RUN", "").lower() in ("1", "true", "yes")
CAMILLA_ENABLED = os.getenv("CAMILLA_ENABLED", "1").lower() not in {"0", "false", "no"}

CAMILLA_HOST = os.getenv("CAMILLA_HOST", "127.0.0.1")
CAMILLA_PORT = int(os.getenv("CAMILLA_PORT", "1234"))
CAMILLA_FILTER_PATH = os.getenv("CAMILLA_FILTER_PATH", "filters.peq_stack_{slot:02d}")
CAMILLA_MAX_BANDS = int(os.getenv("CAMILLA_MAX_BANDS", "31"))
CAMILLA_RETRY_INTERVAL = int(os.getenv("CAMILLA_RETRY_INTERVAL", "5"))
CAMILLA_CONFIG_PATH = Path(os.getenv("CAMILLA_CONFIG_PATH", "/etc/roomcast/camilladsp.yml"))
CAMILLA_TEMPLATE_PATH = Path(os.getenv("CAMILLA_TEMPLATE_PATH", Path(__file__).resolve().parent / "camilladsp-config.yml"))
PACKAGED_CAMILLA_TEMPLATE_PATH = Path(__file__).resolve().parent / "camilladsp-config.yml"
CAMILLA_SERVICE_NAME = os.getenv("CAMILLA_SERVICE_NAME", "roomcast-camilla.service")
SYSTEMCTL_BIN = shutil.which("systemctl") or "/bin/systemctl"
AGENT_SECRET_PATH = Path(os.getenv("AGENT_SECRET_PATH", "/var/lib/roomcast/agent-secret"))
AGENT_CONFIG_PATH = Path(os.getenv("AGENT_CONFIG_PATH", "/var/lib/roomcast/agent-config.json"))
NODE_UID_PATH = Path(os.getenv("NODE_UID_PATH", "/var/lib/roomcast/node-uid"))
SNAPCLIENT_BIN = os.getenv("SNAPCLIENT_BIN", "snapclient")
SNAPCLIENT_DEFAULT_PORT = int(os.getenv("SNAPCLIENT_PORT", "1704"))
UPDATE_COMMAND = os.getenv("ROOMCAST_UPDATE_COMMAND", "sudo /usr/local/bin/roomcast-updater")
UPDATE_COMMAND_ARGS = shlex.split(UPDATE_COMMAND) if UPDATE_COMMAND else []
RESTART_COMMAND = os.getenv("ROOMCAST_RESTART_COMMAND", "sudo /sbin/reboot")
RESTART_COMMAND_ARGS = shlex.split(RESTART_COMMAND) if RESTART_COMMAND else []

app = FastAPI(title="RoomCast Node Agent", version=AGENT_VERSION)

update_lock = asyncio.Lock()
update_task: asyncio.Task | None = None

eq_state: dict = {"preset": "peq15", "band_count": 15, "bands": []}
muted_state: bool = False
pre_mute_volume: Optional[int] = None
log = logging.getLogger("roomcast-agent")
agent_secret: str | None = None
agent_config: dict = {}
snapclient_process: Optional[asyncio.subprocess.Process] = None
snapclient_lock = asyncio.Lock()


def _read_text_safely(path: Path) -> str:
    try:
        return path.read_text()
    except FileNotFoundError:
        return ""
    except OSError as exc:  # pragma: no cover - filesystem edge
        log.warning("Failed to read %s: %s", path, exc)
        return ""


def _needs_camilla_schema_migration(content: str) -> bool:
    stripped = (content or "").strip()
    if not stripped:
        return True
    markers = ["peq_stack_00", "names:", "- type: Filter"]
    if any(marker not in stripped for marker in markers):
        return True
    if "control:" in stripped:
        return True
    return False


def _load_packaged_camilla_template() -> str:
    try:
        return PACKAGED_CAMILLA_TEMPLATE_PATH.read_text()
    except OSError as exc:  # pragma: no cover - deployment issue
        raise RuntimeError(f"Packaged Camilla template missing: {exc}") from exc


def _ensure_camilla_template_latest() -> str:
    packaged = _load_packaged_camilla_template()
    on_disk = _read_text_safely(CAMILLA_TEMPLATE_PATH)
    if _needs_camilla_schema_migration(on_disk):
        try:
            CAMILLA_TEMPLATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            CAMILLA_TEMPLATE_PATH.write_text(packaged)
            try:
                os.chmod(CAMILLA_TEMPLATE_PATH, 0o640)
            except OSError:
                pass
            log.info("Updated Camilla template at %s", CAMILLA_TEMPLATE_PATH)
        except OSError as exc:  # pragma: no cover - filesystem edge
            log.warning("Failed to update Camilla template %s: %s", CAMILLA_TEMPLATE_PATH, exc)
            return packaged
        return packaged
    return on_disk or packaged


def _camilla_config_requires_migration() -> bool:
    return _needs_camilla_schema_migration(_read_text_safely(CAMILLA_CONFIG_PATH))


def _load_agent_secret() -> str | None:
    try:
        raw = AGENT_SECRET_PATH.read_text().strip()
        return raw or None
    except FileNotFoundError:
        return None
    except OSError as exc:  # pragma: no cover - filesystem edge
        log.warning("Failed to read agent secret: %s", exc)
        return None


def _persist_agent_secret(value: str) -> None:
    AGENT_SECRET_PATH.parent.mkdir(parents=True, exist_ok=True)
    AGENT_SECRET_PATH.write_text(value)
    try:
        os.chmod(AGENT_SECRET_PATH, 0o600)
    except OSError:
        pass


agent_secret = _load_agent_secret()
def _load_node_uid() -> str:
    try:
        raw = NODE_UID_PATH.read_text().strip()
        if raw:
            return raw
    except FileNotFoundError:
        pass
    except OSError as exc:  # pragma: no cover - filesystem edge
        log.warning("Failed to read node uid: %s", exc)
    value = secrets.token_hex(16)
    NODE_UID_PATH.parent.mkdir(parents=True, exist_ok=True)
    NODE_UID_PATH.write_text(value)
    try:
        os.chmod(NODE_UID_PATH, 0o600)
    except OSError:
        pass
    return value

node_uid = _load_node_uid()
DEFAULT_AGENT_CONFIG = {
    "snapserver_host": None,
    "snapserver_port": SNAPCLIENT_DEFAULT_PORT,
    "playback_device": PLAYBACK_DEVICE,
    "max_volume_percent": 100,
}


def _load_agent_config() -> dict:
    try:
        raw = AGENT_CONFIG_PATH.read_text()
        data = json.loads(raw)
        if not isinstance(data, dict):
            return dict(DEFAULT_AGENT_CONFIG)
        merged = dict(DEFAULT_AGENT_CONFIG)
        merged.update(data)
        return merged
    except FileNotFoundError:
        return dict(DEFAULT_AGENT_CONFIG)
    except (OSError, json.JSONDecodeError) as exc:  # pragma: no cover - filesystem edge
        log.warning("Failed to read agent config: %s", exc)
        return dict(DEFAULT_AGENT_CONFIG)


def _persist_agent_config(data: dict) -> None:
    AGENT_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged = dict(DEFAULT_AGENT_CONFIG)
    merged.update(data)
    AGENT_CONFIG_PATH.write_text(json.dumps(merged))
    try:
        os.chmod(AGENT_CONFIG_PATH, 0o600)
    except OSError:
        pass


agent_config = _load_agent_config()
snapclient_process = None
_resolved_control: Optional[str] = None
last_requested_volume: int = 75


def _max_volume_percent() -> int:
    raw = agent_config.get("max_volume_percent", 100)
    try:
        percent = int(raw)
    except (TypeError, ValueError):
        return 100
    return max(0, min(100, percent))


def _effective_volume(percent: int) -> int:
    try:
        requested = int(percent)
    except (TypeError, ValueError):
        requested = 0
    requested = max(0, min(100, requested))
    return (requested * _max_volume_percent()) // 100


def _current_playback_device() -> str:
    device = (agent_config.get("playback_device") or PLAYBACK_DEVICE or "").strip()
    return device or "plughw:0,0"


def _current_playback_card() -> Optional[str]:
    device = _current_playback_device()
    if ":" not in device:
        return None
    _, suffix = device.split(":", 1)
    suffix = suffix.strip()
    if not suffix:
        return None
    primary = suffix.split(",", 1)[0].strip()
    if not primary:
        return None
    if "=" in primary:
        key, _, value = primary.partition("=")
        if key.strip().lower() == "card":
            primary = value.strip()
    if primary.lower().startswith("hw:"):
        primary = primary.split(":", 1)[1].strip() or primary
    return primary or None


def _list_playback_devices() -> list[dict]:
    try:
        proc = subprocess.run(
            ["aplay", "-l"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError:
        log.warning("aplay not installed; unable to list hardware outputs")
        return []
    except subprocess.CalledProcessError as exc:
        log.warning("Failed to list playback devices: %s", exc.stderr.strip() or exc)
        return []

    pattern = re.compile(r"card (\d+): ([^\[]+)\[([^\]]+)\], device (\d+): ([^\[]+)\[([^\]]+)\]")
    options: list[dict] = []
    seen: set[str] = set()
    for line in proc.stdout.splitlines():
        line = line.strip()
        match = pattern.match(line)
        if not match:
            continue
        card_idx, _card_id, card_desc, dev_idx, _dev_id, dev_desc = match.groups()
        card_desc = card_desc.strip()
        dev_desc = dev_desc.strip()
        device_id = f"plughw:{card_idx},{dev_idx}"
        if device_id in seen:
            continue
        label = f"{card_desc.strip()} – {dev_desc.strip()} (hw:{card_idx},{dev_idx})"
        options.append({"id": device_id, "label": label})
        seen.add(device_id)
    return options


def _outputs_snapshot() -> dict:
    options = _list_playback_devices()
    selected = _current_playback_device()
    if selected and selected not in {opt["id"] for opt in options}:
        options.insert(0, {"id": selected, "label": f"{selected} (current)"})
    return {"selected": selected, "options": options}


def _wifi_signal_snapshot() -> dict | None:
    path = Path("/proc/net/wireless")
    try:
        lines = path.read_text().strip().splitlines()
    except FileNotFoundError:
        return None
    except OSError:
        return None
    if len(lines) <= 2:
        return None
    snapshots: list[dict] = []
    for line in lines[2:]:
        if ":" not in line:
            continue
        iface_part, metrics = line.split(":", 1)
        iface = iface_part.strip()
        if not iface:
            continue
        fields = metrics.split()
        if len(fields) < 3:
            continue
        try:
            quality = float(fields[1])
        except ValueError:
            continue
        try:
            signal_dbm = float(fields[2])
        except ValueError:
            signal_dbm = None
        percent = max(0, min(100, int(round((quality / 70.0) * 100))))
        entry = {"interface": iface, "percent": percent}
        if signal_dbm is not None:
            entry["signal_dbm"] = signal_dbm
        snapshots.append(entry)
    if not snapshots:
        return None
    snapshots.sort(key=lambda item: item["percent"], reverse=True)
    return snapshots[0]


def _render_camilla_config(playback_device: str) -> str:
    template = _ensure_camilla_template_latest()
    if not template:
        raise RuntimeError("CamillaDSP template not found")
    rendered = template.replace("__PLAYBACK_DEVICE__", playback_device)
    return rendered


def _write_camilla_config(playback_device: str) -> None:
    rendered = _render_camilla_config(playback_device)
    CAMILLA_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CAMILLA_CONFIG_PATH.write_text(rendered)


def _restart_camilla_service_sync() -> None:
    cmd = ["sudo", SYSTEMCTL_BIN, "restart", CAMILLA_SERVICE_NAME]
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


async def _restart_camilla_service() -> None:
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, _restart_camilla_service_sync)
    except subprocess.CalledProcessError as exc:
        raise HTTPException(status_code=500, detail=exc.stderr.decode() or str(exc))


async def _set_playback_device(device: str) -> dict:
    normalized = (device or "").strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="Device must be provided")
    agent_config["playback_device"] = normalized
    _persist_agent_config(agent_config)
    try:
        await asyncio.get_running_loop().run_in_executor(None, _write_camilla_config, normalized)
    except Exception as exc:
        log.exception("Failed to write Camilla config")
        raise HTTPException(status_code=500, detail=f"Failed to write Camilla config: {exc}")
    await _restart_camilla_service()
    global camilla_pending_eq
    try:
        await _apply_camilla_eq()
        camilla_pending_eq = False
    except Exception as exc:  # pragma: no cover - hardware path
        if CAMILLA_ENABLED and _is_connection_error(exc):
            camilla_pending_eq = True
            _schedule_camilla_retry()
        else:
            log.exception("Failed to reapply EQ after output change")
            raise HTTPException(status_code=500, detail=f"Failed to reapply EQ: {exc}")
    return _outputs_snapshot()


def _list_mixer_controls() -> list[str]:
    if DRY_RUN:
        return [MIXER_CONTROL]
    card = _current_playback_card()
    try:
        args = ["amixer"]
        if card:
            args.extend(["-c", card])
        args.append("scontrols")
        proc = subprocess.run(
            args,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        log.warning("amixer not installed; unable to auto-detect mixer controls")
        return []
    except subprocess.CalledProcessError as exc:
        log.warning(
            "Failed to list ALSA simple controls: %s",
            exc.stderr.decode(errors="ignore") or str(exc),
        )
        return []
    names: list[str] = []
    for line in proc.stdout.decode(errors="ignore").splitlines():
        if "'" not in line:
            continue
        start = line.find("'") + 1
        end = line.find("'", start)
        if start >= 0 and end > start:
            names.append(line[start:end])
    return names


def _mixer_candidate_order() -> list[str]:
    candidates: list[str] = []

    def add(name: Optional[str]) -> None:
        normalized = (name or "").strip()
        if normalized and normalized not in candidates:
            candidates.append(normalized)

    if _resolved_control:
        add(_resolved_control)

    preferred = (MIXER_CONTROL or "Master").strip() or "Master"
    add(preferred)
    for fallback in MIXER_FALLBACKS:
        add(fallback)

    for detected in _list_mixer_controls():
        add(detected)

    if not candidates:
        candidates.append(preferred)
    return candidates


def _try_mixer_command(builder: Callable[[str], list[str]]) -> None:
    last_error: Optional[str] = None
    card = _current_playback_card()
    for control in _mixer_candidate_order():
        args = ["amixer"]
        if card:
            args.extend(["-c", card])
        args.extend(builder(control))
        try:
            subprocess.run(
                args,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except FileNotFoundError:
            raise HTTPException(status_code=500, detail="amixer not installed")
        except subprocess.CalledProcessError as exc:
            detail = exc.stderr.decode(errors="ignore") or str(exc)
            log.warning("Mixer control '%s' failed via amixer: %s", control, detail.strip() or detail)
            last_error = detail
            continue
        global _resolved_control
        _resolved_control = control
        return

    raise HTTPException(
        status_code=500,
        detail=last_error or "Unable to control mixer. Set MIXER_CONTROL to a valid ALSA control.",
    )


def _snapclient_args(config: dict) -> list[str]:
    host = config.get("snapserver_host")
    port = int(config.get("snapserver_port", SNAPCLIENT_DEFAULT_PORT))
    if not host:
        return []
    base = [
        SNAPCLIENT_BIN,
        "-h",
        str(host),
        "-p",
        str(port),
        "--player",
        f"alsa:device={PLAYBACK_DEVICE}",
        "--sampleformat",
        "48000:32:*",
        "--hostID",
        node_uid,
    ]
    return base


async def _stop_snapclient() -> None:
    global snapclient_process
    if not snapclient_process:
        return
    proc = snapclient_process
    snapclient_process = None
    if proc.returncode is None:
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()


async def _monitor_snapclient(proc: asyncio.subprocess.Process) -> None:
    try:
        await proc.wait()
    finally:
        global snapclient_process
        if snapclient_process is proc:
            snapclient_process = None
        if agent_config.get("snapserver_host"):
            await asyncio.sleep(2)
            await _reconcile_snapclient()


async def _start_snapclient() -> None:
    global snapclient_process
    args = _snapclient_args(agent_config)
    if not args:
        return
    await _stop_snapclient()
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
    except FileNotFoundError:
        log.error("snapclient binary not found; please install snapclient")
        return
    snapclient_process = proc
    asyncio.create_task(_monitor_snapclient(proc))


async def _reconcile_snapclient() -> None:
    async with snapclient_lock:
        if not agent_config.get("snapserver_host"):
            await _stop_snapclient()
            return
        if snapclient_process and snapclient_process.returncode is None:
            return
        await _start_snapclient()


class VolumePayload(BaseModel):
    percent: int = Field(ge=0, le=100)


class MutePayload(BaseModel):
    muted: bool


class EqBand(BaseModel):
    freq: float = Field(gt=10, lt=24000)
    gain: float = Field(ge=-24, le=24)
    q: float = Field(default=1.0, gt=0.05, lt=36)


class EqPayload(BaseModel):
    preset: str | None = None
    bands: List[EqBand] = Field(default_factory=list)
    band_count: int = Field(default=15, ge=1, le=31)


class PairPayload(BaseModel):
    force: bool = False


class SnapclientConfigPayload(BaseModel):
    snapserver_host: Optional[str] = None
    snapserver_port: int = Field(default=SNAPCLIENT_DEFAULT_PORT, ge=1, le=65535)


class OutputSelectionPayload(BaseModel):
    device: str


class CamillaController:
    def __init__(self, host: str, port: int, filter_path: str) -> None:
        self.host = host
        self.port = port
        self.filter_path = filter_path
        self._ws_uri = f"ws://{self.host}:{self.port}"

    # Camilla v3 exposes single filters, so we need a predictable per-slot path.
    def _filter_path_for_slot(self, slot: int) -> str:
        template = self.filter_path or ""
        replacements = {
            "slot": slot,
            "slot_index": slot,
            "slot1": slot + 1,
            "slot_one_based": slot + 1,
            "slot02": f"{slot:02d}",
            "slot03": f"{slot:03d}",
            "slot1_02": f"{slot + 1:02d}",
        }
        if "{" in template and "}" in template:
            try:
                return template.format(**replacements)
            except (KeyError, ValueError):
                pass
        if "%" in template:
            try:
                return template % slot
            except (TypeError, ValueError):
                pass
        suffix = f"{slot:02d}"
        if template.endswith(("_", "-")):
            return f"{template}{suffix}"
        if not template:
            return suffix
        return f"{template}_{suffix}"

    def _filter_name_for_slot(self, slot: int) -> str:
        path = self._filter_path_for_slot(slot)
        if "." in path:
            return path.split(".", 1)[1]
        return path

    async def apply_eq(self, bands: List[EqBand], target_slots: int) -> None:
        if not CAMILLA_ENABLED:
            return
        config = await self._get_config_json()
        filters = config.setdefault("filters", {})
        slots = max(1, min(CAMILLA_MAX_BANDS, target_slots))
        normalized: list[tuple[float, float, float]] = []
        for band in bands[:slots]:
            if isinstance(band, dict):
                freq = float(band.get("freq", 1000.0))
                gain = float(band.get("gain", 0.0))
                q_val = float(band.get("q", 1.0))
            else:
                freq = float(getattr(band, "freq", 1000.0))
                gain = float(getattr(band, "gain", 0.0))
                q_val = float(getattr(band, "q", 1.0))
            normalized.append((freq, gain, q_val))

        def _upsert_filter(name: str) -> dict:
            entry = filters.setdefault(name, {})
            entry["type"] = "Biquad"
            params = entry.setdefault("parameters", {})
            params["type"] = "Peaking"
            return params

        for idx in range(slots):
            name = self._filter_name_for_slot(idx)
            params = _upsert_filter(name)
            if idx < len(normalized):
                freq, gain, q_val = normalized[idx]
            else:
                freq = float(params.get("freq", 1000.0))
                gain = 0.0
                q_val = float(params.get("q", 1.0))
            params["freq"] = freq
            params["gain"] = gain
            params["q"] = q_val

        for idx in range(slots, CAMILLA_MAX_BANDS):
            name = self._filter_name_for_slot(idx)
            params = _upsert_filter(name)
            params["freq"] = float(params.get("freq", 1000.0))
            params["gain"] = 0.0
            params["q"] = float(params.get("q", 1.0))

        await self._set_config_json(config)

    def _parse_response(self, payload: str | bytes | bytearray, command: str) -> object | None:
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode()
        text = (payload or "").strip()
        if not text:
            raise RuntimeError("No response from CamillaDSP")
        response = json.loads(text)
        if "Invalid" in response:
            detail = response["Invalid"]
            if isinstance(detail, dict):
                detail = detail.get("error") or detail
            raise RuntimeError(f"Camilla rejected command: {detail}")
        if command not in response:
            return response
        result = response[command]
        if isinstance(result, dict):
            status = result.get("result")
            if status not in (None, "Ok"):
                raise RuntimeError(f"Camilla command failed: {status}")
            if "value" in result:
                return result["value"]
            return status
        return result

    async def _invoke(self, command: str, payload: object | None = None) -> object | None:
        if websockets is None:
            raise RuntimeError("websockets dependency missing; Camilla control unavailable")
        message = json.dumps({command: payload})
        async with websockets.connect(self._ws_uri, ping_interval=None) as ws:
            await ws.send(message)
            response_line = await ws.recv()
        return self._parse_response(response_line, command)

    async def _get_config_json(self) -> dict:
        raw = await self._invoke("GetConfigJson", None)
        if not isinstance(raw, str):
            raise RuntimeError("Unexpected Camilla config payload")
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:  # pragma: no cover - Camilla bug
            raise RuntimeError("Failed to decode Camilla config") from exc

    async def _set_config_json(self, config: dict) -> None:
        serialized = json.dumps(config, separators=(",", ":"))
        await self._invoke("SetConfigJson", serialized)


camilla = CamillaController(CAMILLA_HOST, CAMILLA_PORT, CAMILLA_FILTER_PATH)
camilla_retry_task: asyncio.Task | None = None
camilla_pending_eq = False


def _is_connection_error(exc: BaseException) -> bool:
    if ws_exceptions is not None and isinstance(exc, ws_exceptions.WebSocketException):
        return True
    if isinstance(exc, ConnectionError):
        return True
    errno = getattr(exc, "errno", None)
    return errno in {104, 107, 111, 113}


async def _apply_camilla_eq() -> None:
    if not CAMILLA_ENABLED:
        return
    state = eq_state or {}
    bands = state.get("bands") or []
    band_count = state.get("band_count") or len(bands) or 15
    await camilla.apply_eq(bands, band_count)


def _cancel_camilla_retry() -> None:
    global camilla_retry_task
    if camilla_retry_task and not camilla_retry_task.done():
        camilla_retry_task.cancel()
    camilla_retry_task = None


def _schedule_camilla_retry() -> None:
    if not CAMILLA_ENABLED:
        return
    global camilla_retry_task
    if camilla_retry_task and not camilla_retry_task.done():
        return

    async def _runner() -> None:
        global camilla_pending_eq, camilla_retry_task
        try:
            while True:
                await asyncio.sleep(max(1, CAMILLA_RETRY_INTERVAL))
                try:
                    await _apply_camilla_eq()
                except Exception as exc:  # pragma: no cover - hardware path
                    if _is_connection_error(exc):
                        log.warning("CamillaDSP still unavailable (%s); retrying", exc)
                        continue
                    log.exception("Failed to apply EQ via CamillaDSP")
                    return
                camilla_pending_eq = False
                log.info("Applied pending EQ after CamillaDSP recovery")
                return
        finally:
            camilla_retry_task = None

    camilla_retry_task = asyncio.create_task(_runner())


async def _ensure_camilla_config_current() -> None:
    if not CAMILLA_ENABLED or not _camilla_config_requires_migration():
        return
    playback_device = _current_playback_device()
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, _write_camilla_config, playback_device)
    except Exception as exc:  # pragma: no cover - filesystem edge
        log.warning("Failed to rewrite Camilla config during migration: %s", exc)
        return
    try:
        await _restart_camilla_service()
    except HTTPException as exc:
        log.warning("Unable to restart Camilla after config migration: %s", exc.detail)
        return
    global camilla_pending_eq
    try:
        await _apply_camilla_eq()
        camilla_pending_eq = False
    except Exception as exc:  # pragma: no cover - hardware path
        if _is_connection_error(exc):
            camilla_pending_eq = True
            log.warning("CamillaDSP not ready after config migration (%s); retry scheduled", exc)
            _schedule_camilla_retry()
        else:
            log.warning("Failed to reapply EQ after config migration: %s", exc)


def _auth(request: Request) -> None:
    if not agent_secret:
        raise HTTPException(status_code=409, detail="Node is not paired yet")
    if request.headers.get("x-agent-secret") != agent_secret:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _amixer_set(percent: int) -> None:
    if DRY_RUN:
        return
    clamped = max(0, min(100, int(percent)))
    _try_mixer_command(lambda control: ["-M", "set", control, f"{clamped}%"])


def _amixer_mute(muted: bool) -> None:
    global pre_mute_volume
    if DRY_RUN:
        if muted:
            pre_mute_volume = _effective_volume(last_requested_volume)
        else:
            pre_mute_volume = None
        return
    cmd = "mute" if muted else "unmute"
    try:
        _try_mixer_command(lambda control: ["-M", "set", control, cmd])
        if muted:
            pre_mute_volume = _effective_volume(last_requested_volume)
        else:
            pre_mute_volume = None
        return
    except HTTPException as exc:
        log.info("Mute switch not available via amixer; falling back to volume control: %s", exc.detail)

    target = _effective_volume(last_requested_volume)
    if muted:
        pre_mute_volume = target
        _amixer_set(0)
    else:
        restored = pre_mute_volume if pre_mute_volume is not None else target
        pre_mute_volume = None
        _amixer_set(restored)


async def _run_maintenance_command(args: list[str], label: str) -> None:
    global update_task
    if not args:
        raise HTTPException(status_code=500, detail=f"{label} is not configured")
    async with update_lock:
        if update_task and not update_task.done():
            raise HTTPException(status_code=409, detail="Another maintenance action is already running")
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except FileNotFoundError:
            raise HTTPException(status_code=500, detail=f"{label} executable not found")
        except Exception as exc:  # pragma: no cover - subprocess failures
            log.exception("Failed to launch %s", label)
            raise HTTPException(status_code=500, detail=str(exc))

        async def monitor() -> None:
            try:
                rc = await proc.wait()
                if rc != 0:
                    log.error("%s exited with code %s", label, rc)
            except Exception as exc:  # pragma: no cover
                log.exception("%s failed: %s", label, exc)

        update_task = asyncio.create_task(monitor())


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "eq": eq_state,
        "muted": muted_state,
        "paired": bool(agent_secret),
        "configured": bool(agent_config.get("snapserver_host")),
        "version": AGENT_VERSION,
        "updating": bool(update_task and not update_task.done()),
        "camilla_pending": camilla_pending_eq,
        "playback_device": _current_playback_device(),
        "outputs": _outputs_snapshot(),
        "fingerprint": node_uid,
        "max_volume_percent": _max_volume_percent(),
        "wifi": _wifi_signal_snapshot(),
    }


@app.get("/pair")
async def pair_status() -> dict:
    return {"paired": bool(agent_secret)}


@app.post("/pair")
async def pair(payload: PairPayload | None = None) -> dict:
    global agent_secret
    force = bool(payload.force) if payload else False
    if agent_secret and not force:
        raise HTTPException(status_code=409, detail="Already paired")
    agent_secret = secrets.token_urlsafe(32)
    _persist_agent_secret(agent_secret)
    return {"secret": agent_secret}


@app.get("/config/snapclient")
async def get_snapclient_config() -> dict:
    return {
        "config": agent_config,
        "configured": bool(agent_config.get("snapserver_host")),
        "running": bool(snapclient_process and snapclient_process.returncode is None),
    }


@app.post("/config/snapclient")
async def set_snapclient_config(payload: SnapclientConfigPayload, request: Request) -> dict:
    _auth(request)
    host = (payload.snapserver_host or "").strip() or None
    agent_config["snapserver_host"] = host
    agent_config["snapserver_port"] = int(payload.snapserver_port)
    _persist_agent_config(agent_config)
    await _reconcile_snapclient()
    return {"ok": True, "configured": bool(host)}


@app.post("/config/max-volume")
async def set_max_volume_limit(payload: VolumePayload, request: Request) -> dict:
    _auth(request)
    value = max(0, min(100, int(payload.percent)))
    agent_config["max_volume_percent"] = value
    _persist_agent_config(agent_config)
    effective = _effective_volume(last_requested_volume)
    _amixer_set(effective)
    return {"ok": True, "max_volume_percent": value, "applied_volume": effective}


@app.get("/outputs")
async def list_outputs(request: Request) -> dict:
    _auth(request)
    return _outputs_snapshot()


@app.post("/outputs")
async def set_output(payload: OutputSelectionPayload, request: Request) -> dict:
    _auth(request)
    snapshot = await _set_playback_device(payload.device)
    return {"ok": True, "outputs": snapshot}


@app.post("/volume")
async def set_volume(payload: VolumePayload, request: Request) -> dict:
    _auth(request)
    global last_requested_volume
    requested = max(0, min(100, int(payload.percent)))
    last_requested_volume = requested
    effective = _effective_volume(requested)
    _amixer_set(effective)
    return {
        "ok": True,
        "requested": requested,
        "volume": effective,
        "max_volume_percent": _max_volume_percent(),
    }


@app.post("/mute")
async def set_mute(payload: MutePayload, request: Request) -> dict:
    _auth(request)
    global muted_state
    muted_state = payload.muted
    _amixer_mute(payload.muted)
    return {"ok": True, "muted": muted_state}


@app.post("/eq")
async def set_eq(payload: EqPayload, request: Request) -> dict:
    _auth(request)
    eq_state["preset"] = payload.preset or eq_state.get("preset") or "peq15"
    eq_state["band_count"] = payload.band_count
    eq_state["bands"] = [band.model_dump() for band in payload.bands]
    global camilla_pending_eq
    try:
        await _apply_camilla_eq()
        camilla_pending_eq = False
        _cancel_camilla_retry()
        pending = False
    except Exception as exc:  # pragma: no cover - hardware/API path
        if CAMILLA_ENABLED and _is_connection_error(exc):
            camilla_pending_eq = True
            log.warning("CamillaDSP unavailable (%s); queueing EQ for retry", exc)
            _schedule_camilla_retry()
            pending = True
        else:
            log.exception("Failed to apply EQ via CamillaDSP")
            raise HTTPException(status_code=500, detail=f"Failed to apply EQ: {exc}")
    return {"ok": True, "eq": eq_state, "camilla_pending": pending}


@app.post("/update")
async def trigger_update(request: Request) -> dict:
    _auth(request)
    if not UPDATE_COMMAND_ARGS:
        raise HTTPException(status_code=500, detail="Update command is not configured")
    await _run_maintenance_command(UPDATE_COMMAND_ARGS, "update command")
    return {"ok": True, "status": "started"}


@app.post("/restart")
async def trigger_restart(request: Request) -> dict:
    _auth(request)
    if not RESTART_COMMAND_ARGS:
        raise HTTPException(status_code=500, detail="Restart command is not configured")
    await _run_maintenance_command(RESTART_COMMAND_ARGS, "restart command")
    return {"ok": True, "status": "restarting"}


@app.on_event("startup")
async def on_startup() -> None:
    await _ensure_camilla_config_current()
    await _reconcile_snapclient()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await _stop_snapclient()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=9700)
