#!/usr/bin/env python3
import asyncio
import logging
import os
import signal
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import httpx

POLL_INTERVAL = int(os.getenv("AUDIOBOOKSHELF_ASSIGNMENT_INTERVAL", "10"))
CONTROLLER_BASE_URL = os.getenv("CONTROLLER_BASE_URL", "http://localhost:8000").rstrip("/")
WORKER_TOKEN = os.getenv("AUDIOBOOKSHELF_WORKER_TOKEN", "")
FFMPEG_BIN = os.getenv("FFMPEG_BIN", "ffmpeg")
STARTUP_DELAY = int(os.getenv("AUDIOBOOKSHELF_RESTART_DELAY", "5"))
OUTPUT_RATE = os.getenv("AUDIOBOOKSHELF_OUTPUT_RATE", "48000")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=LOG_LEVEL, format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("audiobookshelf-worker")


@dataclass
class Assignment:
    channel_id: str
    enabled: bool
    fifo_path: str
    stream_url: Optional[str]
    token: Optional[str]
    playback_enabled: bool
    episode_id: Optional[str]
    library_item_id: Optional[str]

    @classmethod
    def from_dict(cls, data: dict) -> "Assignment":
        channel_id = data.get("channel_id")
        fifo_path = data.get("fifo_path") or f"/tmp/{channel_id or 'audiobookshelf'}"
        stream_url_raw = (data.get("stream_url") or "").strip()
        stream_url = stream_url_raw if stream_url_raw else None
        playback_flag = data.get("playback_enabled")
        if playback_flag is None:
            playback_enabled = True
        elif isinstance(playback_flag, str):
            playback_enabled = playback_flag.strip().lower() not in {"0", "false", "no"}
        else:
            playback_enabled = bool(playback_flag)
        token_raw = (data.get("token") or "").strip()
        token = token_raw if token_raw else None
        episode_id_raw = (data.get("episode_id") or "").strip()
        episode_id = episode_id_raw if episode_id_raw else None
        library_item_id_raw = (data.get("library_item_id") or "").strip()
        library_item_id = library_item_id_raw if library_item_id_raw else None
        return cls(
            channel_id=channel_id,
            enabled=bool(data.get("enabled")),
            fifo_path=fifo_path,
            stream_url=stream_url,
            token=token,
            playback_enabled=playback_enabled,
            episode_id=episode_id,
            library_item_id=library_item_id,
        )


class ControllerClient:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(base_url=CONTROLLER_BASE_URL, timeout=20)

    def _headers(self) -> dict:
        headers = {"Accept": "application/json"}
        if WORKER_TOKEN:
            headers["X-Audiobookshelf-Worker-Token"] = WORKER_TOKEN
        return headers

    async def fetch_assignments(self, since: Optional[int] = None, wait: int = POLL_INTERVAL) -> tuple[list[Assignment], Optional[int]]:
        params = {}
        if since is not None:
            params["since"] = since
            params["wait"] = max(1, min(wait, 30))
        resp = await self._client.get("/api/audiobookshelf/worker/assignments", params=params or None, headers=self._headers())
        resp.raise_for_status()
        payload = resp.json()
        assignments = []
        for entry in payload.get("assignments", []):
            try:
                assignments.append(Assignment.from_dict(entry))
            except Exception as exc:  # pragma: no cover
                log.warning("Invalid assignment payload skipped: %s", exc)
        version_raw = payload.get("version")
        try:
            version = int(version_raw)
        except (TypeError, ValueError):
            version = None
        return assignments, version

    async def send_status(self, channel_id: str, state: str, *, message: Optional[str] = None) -> None:
        body = {"state": state}
        if message:
            body["message"] = message[:480]
        endpoint = f"/api/audiobookshelf/worker/status/{channel_id}"
        try:
            resp = await self._client.post(endpoint, json=body, headers=self._headers())
            resp.raise_for_status()
        except Exception as exc:
            log.warning("Failed to send status for %s: %s", channel_id, exc)

    async def notify_ended(self, channel_id: str, *, library_item_id: Optional[str], episode_id: Optional[str]) -> None:
        body = {}
        if library_item_id:
            body["library_item_id"] = library_item_id
        if episode_id:
            body["episode_id"] = episode_id
        endpoint = f"/api/audiobookshelf/worker/ended/{channel_id}"
        try:
            resp = await self._client.post(endpoint, json=body or None, headers=self._headers())
            resp.raise_for_status()
        except Exception as exc:
            log.warning("Failed to notify ended for %s: %s", channel_id, exc)

    async def close(self) -> None:
        await self._client.aclose()


class ChannelRunner:
    def __init__(self, assignment: Assignment, controller: ControllerClient) -> None:
        self.assignment = assignment
        self.controller = controller
        self.process: Optional[asyncio.subprocess.Process] = None
        self.restart_delay = STARTUP_DELAY
        self._monitor_task: Optional[asyncio.Task] = None

    async def update(self, assignment: Assignment) -> None:
        self.assignment = assignment
        if not assignment.enabled:
            await self.stop(reason="channel disabled")
            return
        if not assignment.stream_url:
            await self.stop(reason="no episode selected")
            return
        if not assignment.playback_enabled:
            await self.stop(reason="playback disabled")
            return
        if self.process and self.process.returncode is None:
            return
        await self.start()

    async def start(self) -> None:
        await self.stop()
        ensure_fifo(self.assignment.fifo_path)
        cmd = [
            FFMPEG_BIN,
            "-nostdin",
            "-y",
            "-hide_banner",
            "-loglevel",
            os.getenv("FFMPEG_LOG_LEVEL", "warning"),
        ]
        if self.assignment.token:
            # ffmpeg expects CRLF-separated HTTP headers.
            cmd.extend(["-headers", f"Authorization: Bearer {self.assignment.token}\r\n"])
        cmd.extend([
            "-i",
            self.assignment.stream_url,
            "-vn",
            "-ac",
            "2",
            "-ar",
            str(OUTPUT_RATE),
            "-f",
            "s16le",
            "-acodec",
            "pcm_s16le",
            self.assignment.fifo_path,
        ])
        log.info("[%s] Starting ffmpeg -> %s", self.assignment.channel_id, self.assignment.fifo_path)
        await self.controller.send_status(self.assignment.channel_id, "connecting", message="Starting episode")
        try:
            self.process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        except FileNotFoundError as exc:
            await self.controller.send_status(self.assignment.channel_id, "error", message=f"ffmpeg missing: {exc}")
            log.error("ffmpeg binary not found: %s", exc)
            await asyncio.sleep(self.restart_delay)
            return
        except Exception as exc:  # pragma: no cover
            await self.controller.send_status(self.assignment.channel_id, "error", message=str(exc))
            log.error("Failed to start ffmpeg for %s: %s", self.assignment.channel_id, exc)
            await asyncio.sleep(self.restart_delay)
            return
        self.restart_delay = STARTUP_DELAY
        await self.controller.send_status(self.assignment.channel_id, "playing", message="Playing")
        self._monitor_task = asyncio.create_task(self._monitor_process())

    async def _monitor_process(self) -> None:
        assert self.process
        returncode = await self.process.wait()
        self.process = None
        message = f"ffmpeg exited with code {returncode}"
        if returncode == 0:
            log.info("[%s] %s", self.assignment.channel_id, message)
            await self.controller.send_status(self.assignment.channel_id, "idle", message=message)
            await self.controller.notify_ended(
                self.assignment.channel_id,
                library_item_id=self.assignment.library_item_id,
                episode_id=self.assignment.episode_id,
            )
        else:
            log.warning("[%s] %s", self.assignment.channel_id, message)
            await self.controller.send_status(self.assignment.channel_id, "error", message=message)

    async def stop(self, *, reason: Optional[str] = None) -> None:
        if reason:
            log.info("[%s] Stopping: %s", self.assignment.channel_id, reason)
        if self._monitor_task:
            self._monitor_task.cancel()
            self._monitor_task = None
        if self.process and self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.process.kill()
                await self.process.wait()
        self.process = None


class Manager:
    def __init__(self, controller: ControllerClient) -> None:
        self.controller = controller
        self.runners: Dict[str, ChannelRunner] = {}
        self.version: Optional[int] = None

    async def sync(self) -> None:
        assignments, version = await self.controller.fetch_assignments(self.version, POLL_INTERVAL)
        if version is not None:
            self.version = version
        seen = set()
        for assignment in assignments:
            if not assignment.channel_id:
                continue
            seen.add(assignment.channel_id)
            runner = self.runners.get(assignment.channel_id)
            if not runner:
                runner = ChannelRunner(assignment, self.controller)
                self.runners[assignment.channel_id] = runner
            await runner.update(assignment)
        to_remove = [cid for cid in self.runners.keys() if cid not in seen]
        for cid in to_remove:
            await self.runners[cid].stop(reason="assignment removed")
            self.runners.pop(cid, None)

    async def shutdown(self) -> None:
        await asyncio.gather(*(runner.stop(reason="shutdown") for runner in self.runners.values()), return_exceptions=True)
        self.runners.clear()


def ensure_fifo(path: str) -> None:
    fifo = Path(path)
    if fifo.exists():
        try:
            mode = fifo.stat().st_mode
            if stat.S_ISFIFO(mode):
                return
            fifo.unlink()
        except FileNotFoundError:
            pass
    fifo.parent.mkdir(parents=True, exist_ok=True)
    os.mkfifo(fifo, 0o666)


async def run_worker() -> None:
    controller = ControllerClient()
    manager = Manager(controller)
    stop_event = asyncio.Event()

    def _handle_signal(signum, frame):  # noqa: ARG001
        log.info("Received signal %s, shutting down", signum)
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: _handle_signal(s, None))
        except NotImplementedError:
            signal.signal(sig, _handle_signal)

    await asyncio.sleep(STARTUP_DELAY)

    try:
        while not stop_event.is_set():
            try:
                await manager.sync()
            except Exception as exc:
                log.warning("Sync failed: %s", exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=POLL_INTERVAL)
            except asyncio.TimeoutError:
                pass
    finally:
        await manager.shutdown()
        await controller.close()


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
