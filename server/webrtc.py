import asyncio
import logging
import math
import time
from array import array
from dataclasses import dataclass, field
from fractions import Fraction
from typing import Awaitable, Callable, Dict, Optional

import av
from aiortc import RTCConfiguration, RTCPeerConnection, RTCSessionDescription
from aiortc.rtcconfiguration import RTCIceServer
from aiortc.mediastreams import MediaStreamTrack

log = logging.getLogger("roomcast.webrtc")

AudioChunk = Optional[bytes]

BYTES_PER_SAMPLE = 2
CHANNELS = 2
SAMPLE_FORMAT = "16"
FRAME_DURATION_MS = 20
SAMPLE_WIDTH = CHANNELS * BYTES_PER_SAMPLE


def _to_dbfs(value: Optional[float]) -> Optional[float]:
    if value is None or value <= 0:
        return None
    try:
        return round(20.0 * math.log10(value / 32767.0), 2)
    except (ValueError, ZeroDivisionError):
        return None


@dataclass
class BroadcastStats:
    started_at: float = field(default_factory=time.time)
    total_chunks: int = 0
    total_bytes: int = 0
    queue_overflows: int = 0
    last_chunk_at: Optional[float] = None
    last_peak: Optional[float] = None
    last_rms: Optional[float] = None
    subscribers: int = 0
    last_queue_depth: int = 0
    max_queue_depth: int = 0
    last_channel_diff: Optional[float] = None

    def snapshot(self) -> dict:
        now = time.time()
        elapsed = max(now - self.started_at, 1e-3)
        avg_bitrate = (self.total_bytes * 8.0) / elapsed if self.total_bytes else 0.0
        return {
            "started_at": self.started_at,
            "uptime_sec": elapsed,
            "total_chunks": self.total_chunks,
            "total_bytes": self.total_bytes,
            "avg_bitrate_bps": avg_bitrate,
            "queue_overflows": self.queue_overflows,
            "last_chunk_at": self.last_chunk_at,
            "last_peak_dbfs": _to_dbfs(self.last_peak),
            "last_rms_dbfs": _to_dbfs(self.last_rms),
            "subscribers": self.subscribers,
            "last_queue_depth": self.last_queue_depth,
            "max_queue_depth": self.max_queue_depth,
            "avg_channel_difference": self.last_channel_diff,
        }


@dataclass
class PumpStats:
    started_at: float = field(default_factory=time.time)
    total_chunks: int = 0
    total_bytes: int = 0
    restarts: int = 0
    last_restart: Optional[float] = None
    last_chunk_at: Optional[float] = None
    last_error: Optional[str] = None

    def snapshot(self) -> dict:
        now = time.time()
        elapsed = max(now - self.started_at, 1e-3)
        avg_bitrate = (self.total_bytes * 8.0) / elapsed if self.total_bytes else 0.0
        return {
            "started_at": self.started_at,
            "uptime_sec": elapsed,
            "total_chunks": self.total_chunks,
            "total_bytes": self.total_bytes,
            "avg_bitrate_bps": avg_bitrate,
            "restarts": self.restarts,
            "last_restart": self.last_restart,
            "last_chunk_at": self.last_chunk_at,
            "last_error": self.last_error,
        }

def _frame_samples(sample_rate: int) -> int:
    return sample_rate * FRAME_DURATION_MS // 1000

def _frame_bytes(sample_rate: int) -> int:
    return _frame_samples(sample_rate) * SAMPLE_WIDTH


def _clamp_sample(value: int) -> int:
    if value > 32767:
        return 32767
    if value < -32768:
        return -32768
    return value


class AudioBroadcaster:
    """Fan-out PCM publisher so each WebRTC track gets the same PCM packets."""

    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[AudioChunk]] = set()
        self._lock = asyncio.Lock()
        self._stats = BroadcastStats()
        self._last_channel_log = 0.0

    async def subscribe(self) -> asyncio.Queue[AudioChunk]:
        queue: asyncio.Queue[AudioChunk] = asyncio.Queue(maxsize=50)
        async with self._lock:
            self._subscribers.add(queue)
            self._stats.subscribers = len(self._subscribers)
        return queue

    async def unsubscribe(self, queue: asyncio.Queue[AudioChunk]) -> None:
        async with self._lock:
            self._subscribers.discard(queue)
            self._stats.subscribers = len(self._subscribers)

    async def publish(self, chunk: bytes) -> None:
        if not chunk:
            return
        self._stats.total_chunks += 1
        self._stats.total_bytes += len(chunk)
        self._stats.last_chunk_at = time.time()
        peak, rms, channel_diff = self._measure_levels(chunk)
        if peak:
            self._stats.last_peak = peak
        if rms:
            self._stats.last_rms = rms
        if channel_diff is not None:
            self._stats.last_channel_diff = channel_diff
            now = time.time()
            if now - self._last_channel_log >= 5:
                self._last_channel_log = now
                log.info(
                    "WebRTC broadcaster channel difference avg=%.2f",
                    channel_diff,
                )
        latest_depth = 0
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(chunk)
                latest_depth = max(latest_depth, queue.qsize())
            except asyncio.QueueFull:
                self._stats.queue_overflows += 1
                # Keep playback close to real-time: drop one stale frame and enqueue the latest.
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    queue.put_nowait(chunk)
                    latest_depth = max(latest_depth, queue.qsize())
                except asyncio.QueueFull:
                    continue
        if latest_depth:
            self._stats.last_queue_depth = latest_depth
            self._stats.max_queue_depth = max(self._stats.max_queue_depth, latest_depth)

    def diagnostics(self) -> dict:
        return self._stats.snapshot()

    @staticmethod
    def _measure_levels(chunk: bytes) -> tuple[Optional[int], Optional[float], Optional[float]]:
        if len(chunk) < BYTES_PER_SAMPLE:
            return None, None, None
        try:
            samples = memoryview(chunk).cast("h")
        except TypeError:
            data = array("h")
            data.frombytes(chunk)
            samples = data
        peak = 0
        total = 0
        count = 0
        channel_diff_total = 0
        channel_diff_count = 0
        for sample in samples:
            value = int(sample)
            total += value * value
            abs_val = abs(value)
            if abs_val > peak:
                peak = abs_val
            count += 1
        if CHANNELS > 1 and len(samples) >= CHANNELS:
            for idx in range(0, len(samples) - 1, CHANNELS):
                left = int(samples[idx])
                right = int(samples[idx + 1])
                channel_diff_total += abs(left - right)
                channel_diff_count += 1
        if count == 0:
            return None, None, None
        rms = math.sqrt(total / count)
        channel_diff = (
            channel_diff_total / channel_diff_count if channel_diff_count else None
        )
        return peak, rms, channel_diff


class SnapclientPump:
    """Run snapclient in pipe mode and fan out PCM chunks."""

    def __init__(
        self,
        host: str,
        port: int,
        broadcaster: AudioBroadcaster,
        latency_ms: int = 150,
        sample_rate: int = 44100,
        *,
        client_id: Optional[str] = None,
        stream_id: Optional[str] = None,
        assign_stream: Optional[Callable[[str, str], Awaitable[None]]] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.latency_ms = latency_ms
        self.broadcaster = broadcaster
        self.sample_rate = max(8000, min(192000, int(sample_rate)))
        self.client_id = client_id
        self.stream_id = stream_id
        self._assign_stream_cb = assign_stream
        self._task: Optional[asyncio.Task[None]] = None
        self._stop = asyncio.Event()
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._buffer = bytearray()
        self._frame_bytes = _frame_bytes(self.sample_rate)
        self._assign_task: Optional[asyncio.Task[None]] = None
        self._got_audio = False
        self._stats = PumpStats()

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._proc and self._proc.returncode is None:
            self._proc.terminate()
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                self._proc.kill()
                await self._proc.wait()
        if self._task:
            await self._task
            self._task = None
        if self._assign_task and not self._assign_task.done():
            self._assign_task.cancel()
        self._assign_task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self._spawn()
                await self._read_stdout()
            except Exception as exc:  # pragma: no cover - safety net
                self._stats.last_error = repr(exc)
                log.exception("Snapclient pump crashed: %s", exc)
            finally:
                await self._cleanup_proc()
                if not self._stop.is_set():
                    await asyncio.sleep(2)

    async def _spawn(self) -> None:
        args = [
            "snapclient",
            "-h",
            self.host,
            "-p",
            str(self.port),
            "--player",
            "file:filename=stdout",
            "--sampleformat",
            f"{self.sample_rate}:{SAMPLE_FORMAT}:*",
            "--latency",
            str(self.latency_ms),
            "--logsink",
            "stderr",
            "--logfilter",
            "*:warn",
        ]
        if self.client_id:
            args.extend(["--hostID", self.client_id])
        log.info("Starting snapclient pipe: %s", " ".join(args))
        self._stats.restarts += 1
        self._stats.last_restart = time.time()
        self._proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        asyncio.create_task(self._log_stderr(self._proc.stderr))
        self._buffer.clear()
        self._got_audio = False
        self._schedule_stream_assignment()

    async def update_stream(self, stream_id: str) -> None:
        if not stream_id or stream_id == self.stream_id:
            self.stream_id = stream_id
            return
        self.stream_id = stream_id
        self._schedule_stream_assignment()
        await self._restart_proc()

    def _schedule_stream_assignment(self) -> None:
        if not self._assign_stream_cb or not self.client_id or not self.stream_id:
            return
        if self._assign_task and not self._assign_task.done():
            self._assign_task.cancel()

        async def _runner() -> None:
            deadline = time.time() + 15
            delay = 0.5
            while not self._stop.is_set() and time.time() < deadline:
                try:
                    await self._assign_stream_cb(self.client_id, self.stream_id)  # type: ignore[arg-type]
                    return
                except Exception as exc:  # pragma: no cover - defensive logging
                    log.warning(
                        "Failed to assign snapclient %s to %s: %s",
                        self.client_id,
                        self.stream_id,
                        exc,
                    )
                await asyncio.sleep(delay)
                delay = min(delay * 1.5, 5)

        self._assign_task = asyncio.create_task(_runner())

    async def _read_stdout(self) -> None:
        assert self._proc and self._proc.stdout
        stream = self._proc.stdout
        while not self._stop.is_set():
            chunk = await stream.read(4096)
            if not chunk:
                break
            self._buffer.extend(chunk)
            await self._drain_buffer()
        # Flush remainder to maintain continuity if we reconnect quickly.
        await self._drain_buffer()

    async def _drain_buffer(self) -> None:
        while len(self._buffer) >= self._frame_bytes:
            payload = bytes(self._buffer[:self._frame_bytes])
            del self._buffer[:self._frame_bytes]
            self._stats.total_chunks += 1
            self._stats.total_bytes += len(payload)
            self._stats.last_chunk_at = time.time()
            await self.broadcaster.publish(payload)
            if not self._got_audio:
                self._got_audio = True
                log.info("snapclient[%s]: first PCM chunk published", self.client_id or "unknown")

    async def _cleanup_proc(self) -> None:
        if self._proc:
            try:
                await self._proc.wait()
            finally:
                self._proc = None

    async def _log_stderr(self, stream: Optional[asyncio.StreamReader]) -> None:
        if not stream:
            return
        while not stream.at_eof():
            line = await stream.readline()
            if not line:
                break
            prefix = f"snapclient[{self.client_id}]" if self.client_id else "snapclient"
            log.warning("%s: %s", prefix, line.decode(errors="ignore").strip())

    async def _restart_proc(self) -> None:
        if not self._proc or self._proc.returncode is not None:
            return
        self._proc.terminate()
        try:
            await asyncio.wait_for(self._proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            self._proc.kill()
            await self._proc.wait()

    def diagnostics(self) -> dict:
        stats = self._stats.snapshot()
        stats.update(
            {
                "client_id": self.client_id,
                "stream_id": self.stream_id,
                "latency_ms": self.latency_ms,
                "sample_rate": self.sample_rate,
                "frame_bytes": self._frame_bytes,
                "got_audio": self._got_audio,
            }
        )
        return stats


class WebAudioTrack(MediaStreamTrack):
    kind = "audio"

    def __init__(
        self,
        relay: "WebAudioRelay",
        sample_rate: int,
        channel_id: Optional[str],
        pan: float = 0.0,
        stereo_mode: str = "both",
    ) -> None:
        super().__init__()
        self._relay = relay
        self._queue: Optional[asyncio.Queue[AudioChunk]] = None
        self._samples_sent = 0
        self._pan = float(pan)
        self._stereo_mode = "both"
        self._sample_rate = max(8000, min(192000, int(sample_rate)))
        self._channel_id = channel_id
        self._silence_frame_bytes = _frame_bytes(self._sample_rate)
        self._silence_chunk = bytes(self._silence_frame_bytes)
        self.set_stereo_mode(stereo_mode)

    async def _ensure_queue(self) -> asyncio.Queue[AudioChunk]:
        if not self._channel_id:
            raise RuntimeError("No channel assigned")
        if not self._queue:
            self._queue = await self._relay._subscribe_channel(self._channel_id)
        return self._queue

    def set_pan(self, pan: float) -> None:
        self._pan = max(-1.0, min(1.0, pan))

    def set_stereo_mode(self, mode: str) -> None:
        value = (mode or "both").strip().lower()
        self._stereo_mode = value if value in {"both", "left", "right"} else "both"

    async def set_channel(self, channel_id: Optional[str]) -> None:
        if channel_id == self._channel_id:
            return
        await self._release_queue()
        self._channel_id = channel_id

    @property
    def pan(self) -> float:
        return self._pan

    def pending_frames(self) -> int:
        if not self._queue:
            return 0
        return self._queue.qsize()

    async def recv(self) -> av.AudioFrame:
        while True:
            if not self._channel_id:
                await asyncio.sleep(FRAME_DURATION_MS / 1000.0)
                return self._build_frame(self._silence_chunk)
            try:
                queue = await self._ensure_queue()
            except RuntimeError:
                await asyncio.sleep(FRAME_DURATION_MS / 1000.0)
                continue
            chunk = await queue.get()
            if chunk is None:
                # Channel switched; resubscribe to the new source.
                self._queue = None
                continue
            chunk = self._apply_stereo_mode(chunk)
            chunk = self._apply_pan(chunk)
            return self._build_frame(chunk)

    def _build_frame(self, chunk: bytes) -> av.AudioFrame:
        samples = len(chunk) // SAMPLE_WIDTH
        frame = av.AudioFrame(format="s16", layout="stereo", samples=samples)
        frame.planes[0].update(chunk)
        frame.sample_rate = self._sample_rate
        frame.time_base = Fraction(1, self._sample_rate)
        pts = self._samples_sent
        frame.pts = pts
        self._samples_sent = pts + samples
        return frame

    def _apply_pan(self, chunk: bytes) -> bytes:
        if abs(self._pan) < 1e-3:
            return chunk
        data = array("h")
        data.frombytes(chunk)
        if not data:
            return chunk
        angle = (self._pan + 1) * math.pi / 4.0
        left_gain = math.cos(angle)
        right_gain = math.sin(angle)
        for i in range(0, len(data), CHANNELS):
            # Left channel
            data[i] = _clamp_sample(int(data[i] * left_gain))
            if CHANNELS > 1 and i + 1 < len(data):
                data[i + 1] = _clamp_sample(int(data[i + 1] * right_gain))
        return data.tobytes()

    def _apply_stereo_mode(self, chunk: bytes) -> bytes:
        mode = self._stereo_mode
        if mode == "both":
            return chunk
        data = array("h")
        data.frombytes(chunk)
        if not data or CHANNELS < 2:
            return chunk
        if mode == "left":
            for i in range(0, len(data) - 1, CHANNELS):
                left = data[i]
                data[i + 1] = left
            return data.tobytes()
        if mode == "right":
            for i in range(0, len(data) - 1, CHANNELS):
                right = data[i + 1]
                data[i] = right
            return data.tobytes()
        return chunk

    async def shutdown(self) -> None:
        await self._release_queue()

    def stop(self) -> None:
        super().stop()

    async def _release_queue(self) -> None:
        if not self._queue:
            return
        queue = self._queue
        await self._relay._unsubscribe_channel(self._channel_id, queue)
        self._queue = None
        await self._drain_and_signal(queue)

    @staticmethod
    async def _drain_and_signal(queue: asyncio.Queue[AudioChunk]) -> None:
        while True:
            try:
                queue.put_nowait(None)
                return
            except asyncio.QueueFull:
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0)
                else:
                    continue


@dataclass
class WebNodeSession:
    node_id: str
    pc: RTCPeerConnection
    track: WebAudioTrack
    sample_rate: int
    channel_id: Optional[str]

    async def accept(self, sdp: str, sdp_type: str) -> RTCSessionDescription:
        offer = RTCSessionDescription(sdp=sdp, type=sdp_type)
        await self.pc.setRemoteDescription(offer)
        answer = await self.pc.createAnswer()
        patched_sdp = self._enhance_audio_sdp(answer.sdp)
        patched_answer = RTCSessionDescription(sdp=patched_sdp, type=answer.type)
        await self.pc.setLocalDescription(patched_answer)
        await self._wait_for_ice_complete()
        assert self.pc.localDescription
        return self.pc.localDescription

    async def _wait_for_ice_complete(self) -> None:
        if self.pc.iceGatheringState == "complete":
            return
        done = asyncio.Event()

        @self.pc.on("icegatheringstatechange")
        async def _(_: object = None) -> None:  # type: ignore[override]
            if self.pc.iceGatheringState == "complete":
                done.set()

        try:
            await asyncio.wait_for(done.wait(), timeout=5)
        except asyncio.TimeoutError:
            log.warning("ICE gathering did not complete before timeout")

    async def close(self) -> None:
        await self.track.shutdown()
        await self.pc.close()
        self.track.stop()

    def _enhance_audio_sdp(self, sdp: str) -> str:
        opus_pt = self._find_opus_payload_type(sdp)
        if not opus_pt:
            return sdp
        fmtp_prefix = f"a=fmtp:{opus_pt}"
        lines = sdp.splitlines()
        fmtp_idx = None
        for idx, line in enumerate(lines):
            if line.startswith(fmtp_prefix):
                fmtp_idx = idx
                params = self._parse_fmtp_params(line[len(fmtp_prefix):].strip())
                params.update(
                    {
                        "stereo": "1",
                        "sprop-stereo": "1",
                        "maxaveragebitrate": "256000",
                        "maxplaybackrate": str(self.sample_rate),
                        "ptime": str(FRAME_DURATION_MS),
                        "minptime": "10",
                        "useinbandfec": params.get("useinbandfec", "1"),
                    }
                )
                lines[idx] = f"{fmtp_prefix} " + ";".join(f"{k}={v}" for k, v in params.items())
                break
        if fmtp_idx is None:
            insertion_idx = next((i for i, line in enumerate(lines) if line.startswith(f"a=rtpmap:{opus_pt}")), None)
            payload = ";".join(
                f"{k}={v}"
                for k, v in {
                    "stereo": "1",
                    "sprop-stereo": "1",
                    "maxaveragebitrate": "256000",
                    "maxplaybackrate": str(self.sample_rate),
                    "ptime": str(FRAME_DURATION_MS),
                    "minptime": "10",
                    "useinbandfec": "1",
                }.items()
            )
            new_line = f"{fmtp_prefix} {payload}"
            if insertion_idx is not None:
                lines.insert(insertion_idx + 1, new_line)
            else:
                lines.append(new_line)
        return "\r\n".join(lines) + "\r\n"

    @staticmethod
    def _parse_fmtp_params(payload: str) -> Dict[str, str]:
        params: Dict[str, str] = {}
        if not payload:
            return params
        # Remove leading separators if present
        payload = payload.lstrip(" ;")
        for part in payload.split(";"):
            part = part.strip()
            if not part:
                continue
            if "=" in part:
                key, value = part.split("=", 1)
            else:
                key, value = part, "1"
            params[key] = value
        return params

    @staticmethod
    def _find_opus_payload_type(sdp: str) -> Optional[str]:
        for line in sdp.splitlines():
            if line.startswith("a=rtpmap:") and "opus/48000" in line.lower():
                try:
                    return line.split(":", 1)[1].split()[0]
                except IndexError:
                    continue
        return None


@dataclass
class ChannelSource:
    channel_id: str
    stream_id: str
    broadcaster: AudioBroadcaster
    pump: SnapclientPump
    ref_count: int = 0


class WebAudioRelay:
    def __init__(
        self,
        snap_host: str,
        snap_port: int,
        *,
        latency_ms: int = 150,
        sample_rate: int = 44100,
        client_prefix: str = "roomcast-webrtc",
        channel_idle_timeout: float = 10.0,
        assign_stream: Optional[Callable[[str, str], Awaitable[None]]] = None,
        on_session_closed: Optional[Callable[[str], Awaitable[None]]] = None,
    ) -> None:
        self._sample_rate = max(8000, min(192000, int(sample_rate)))
        self._sessions: Dict[str, WebNodeSession] = {}
        self._on_session_closed = on_session_closed
        self._rtc_config = RTCConfiguration(iceServers=[RTCIceServer("stun:stun.l.google.com:19302")])
        self._lock = asyncio.Lock()
        self._snap_host = snap_host
        self._snap_port = snap_port
        self._latency_ms = latency_ms
        self._client_prefix = client_prefix
        self._channel_idle_timeout = max(1.0, float(channel_idle_timeout))
        self._assign_stream_cb = assign_stream
        self._channel_sources: Dict[str, ChannelSource] = {}
        self._channel_stop_tasks: Dict[str, asyncio.Task[None]] = {}

    async def start(self) -> None:
        return

    async def stop(self) -> None:
        async with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
            channels = list(self._channel_sources.items())
            self._channel_sources.clear()
            stop_tasks = list(self._channel_stop_tasks.values())
            self._channel_stop_tasks.clear()
        for task in stop_tasks:
            task.cancel()
        for session in sessions:
            await session.close()
        for _, source in channels:
            await source.pump.stop()

    async def create_session(
        self,
        node_id: str,
        channel_id: Optional[str],
        stream_id: Optional[str],
        pan: float = 0.0,
        stereo_mode: str = "both",
    ) -> WebNodeSession:
        if channel_id and stream_id:
            await self._ensure_channel_source(channel_id, stream_id)
        async with self._lock:
            existing = self._sessions.get(node_id)
            if existing:
                await existing.close()
            track = WebAudioTrack(
                self,
                sample_rate=self._sample_rate,
                channel_id=channel_id,
                pan=pan,
                stereo_mode=stereo_mode,
            )
            pc = RTCPeerConnection(configuration=self._rtc_config)
            sender = pc.addTrack(track)
            params = None
            if hasattr(sender, "getParameters"):
                params = sender.getParameters()
            encodings = (params.get("encodings") if params else None) or []
            for enc in encodings:
                enc["maxBitrate"] = 256_000
                enc["maxFramerate"] = 60
            if encodings and hasattr(sender, "setParameters") and params is not None:
                try:
                    await sender.setParameters(params)
                except Exception:  # pragma: no cover - defensive for older aiortc
                    log.warning("Failed to set sender parameters; continuing with defaults")
            session = WebNodeSession(
                node_id=node_id,
                pc=pc,
                track=track,
                sample_rate=self._sample_rate,
                channel_id=channel_id,
            )
            self._sessions[node_id] = session

            @pc.on("connectionstatechange")
            async def _(_: object = None) -> None:  # type: ignore[override]
                if pc.connectionState in ("failed", "closed", "disconnected"):
                    await self._handle_session_closed(node_id)

        return session

    async def drop_session(self, node_id: str) -> None:
        await self._handle_session_closed(node_id)

    async def update_session_channel(
        self,
        node_id: str,
        channel_id: Optional[str],
        stream_id: Optional[str],
    ) -> None:
        if channel_id and stream_id:
            await self._ensure_channel_source(channel_id, stream_id)
        async with self._lock:
            session = self._sessions.get(node_id)
        if not session:
            return
        if session.channel_id == channel_id:
            return
        await session.track.set_channel(channel_id)
        async with self._lock:
            if self._sessions.get(node_id) is session:
                session.channel_id = channel_id

    async def set_pan(self, node_id: str, pan: float) -> None:
        async with self._lock:
            session = self._sessions.get(node_id)
        if session:
            session.track.set_pan(pan)

    async def set_stereo_mode(self, node_id: str, mode: str) -> None:
        async with self._lock:
            session = self._sessions.get(node_id)
        if session:
            session.track.set_stereo_mode(mode)

    async def _handle_session_closed(self, node_id: str) -> None:
        async with self._lock:
            session = self._sessions.pop(node_id, None)
        if not session:
            return
        await session.close()
        if self._on_session_closed:
            await self._on_session_closed(node_id)

    def _client_id_for_channel(self, channel_id: str) -> str:
        return f"{self._client_prefix}-{channel_id}"

    async def _ensure_channel_source(self, channel_id: str, stream_id: str) -> ChannelSource:
        to_start: Optional[SnapclientPump] = None
        to_update: Optional[SnapclientPump] = None
        async with self._lock:
            source = self._channel_sources.get(channel_id)
            if source:
                if stream_id and stream_id != source.stream_id:
                    source.stream_id = stream_id
                    to_update = source.pump
            else:
                broadcaster = AudioBroadcaster()
                client_id = self._client_id_for_channel(channel_id)
                pump = SnapclientPump(
                    self._snap_host,
                    self._snap_port,
                    broadcaster,
                    self._latency_ms,
                    sample_rate=self._sample_rate,
                    client_id=client_id,
                    stream_id=stream_id,
                    assign_stream=self._assign_stream_cb,
                )
                source = ChannelSource(channel_id=channel_id, stream_id=stream_id, broadcaster=broadcaster, pump=pump)
                self._channel_sources[channel_id] = source
                to_start = pump
        if to_update:
            await to_update.update_stream(stream_id)
        if to_start:
            await to_start.start()
        async with self._lock:
            return self._channel_sources[channel_id]

    async def _subscribe_channel(self, channel_id: str) -> asyncio.Queue[AudioChunk]:
        async with self._lock:
            source = self._channel_sources.get(channel_id)
            if not source:
                raise RuntimeError(f"Channel {channel_id} not initialized")
            source.ref_count += 1
            stop_task = self._channel_stop_tasks.pop(channel_id, None)
        if stop_task:
            stop_task.cancel()
        return await source.broadcaster.subscribe()

    async def _unsubscribe_channel(self, channel_id: str, queue: asyncio.Queue[AudioChunk]) -> None:
        async with self._lock:
            source = self._channel_sources.get(channel_id)
            if not source:
                return
            source.ref_count = max(0, source.ref_count - 1)
            should_stop = source.ref_count == 0
        await source.broadcaster.unsubscribe(queue)
        if should_stop:
            self._schedule_channel_stop(channel_id)

    def _schedule_channel_stop(self, channel_id: str) -> None:
        if channel_id in self._channel_stop_tasks:
            return

        async def _delayed_stop() -> None:
            try:
                await asyncio.sleep(self._channel_idle_timeout)
                async with self._lock:
                    source = self._channel_sources.get(channel_id)
                    if not source or source.ref_count > 0:
                        return
                    self._channel_sources.pop(channel_id, None)
                await source.pump.stop()
            finally:
                self._channel_stop_tasks.pop(channel_id, None)
        self._channel_stop_tasks[channel_id] = asyncio.create_task(_delayed_stop())

    async def channel_listener_counts(self) -> dict[str, int]:
        async with self._lock:
            return {cid: source.ref_count for cid, source in self._channel_sources.items()}

    async def diagnostics(self) -> dict:
        async with self._lock:
            sessions = {node_id: session for node_id, session in self._sessions.items()}
            sources = {cid: source for cid, source in self._channel_sources.items()}
        payload: dict[str, dict] = {}
        for cid, source in sources.items():
            payload[cid] = {
                "stream_id": source.stream_id,
                "listeners": source.ref_count,
                "pump": source.pump.diagnostics(),
                "broadcaster": source.broadcaster.diagnostics(),
                "frame_duration_ms": FRAME_DURATION_MS,
                "channels": CHANNELS,
                "sample_rate": self._sample_rate,
                "sessions": [],
            }
        for node_id, session in sessions.items():
            entry = payload.setdefault(
                session.channel_id,
                {
                    "stream_id": session.channel_id,
                    "listeners": 0,
                    "pump": None,
                    "broadcaster": None,
                    "sessions": [],
                },
            )
            entry.setdefault("sessions", []).append(
                {
                    "node_id": node_id,
                    "channel_id": session.channel_id,
                    "pan": session.track.pan,
                    "sample_rate": session.sample_rate,
                    "pending_frames": session.track.pending_frames(),
                    "connection_state": getattr(session.pc, "connectionState", None),
                    "ice_state": getattr(session.pc, "iceConnectionState", None),
                    "signaling_state": getattr(session.pc, "signalingState", None),
                }
            )
        return {"sample_rate": self._sample_rate, "channels": payload}
