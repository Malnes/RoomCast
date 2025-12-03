import asyncio
import os
import sys
import logging
from pathlib import Path

import httpx

log = logging.getLogger("roomcast.local-agent")

LOCAL_AGENT_HOST = os.getenv("LOCAL_AGENT_HOST", "127.0.0.1").strip() or "127.0.0.1"
LOCAL_AGENT_PORT = int(os.getenv("LOCAL_AGENT_PORT", "9700"))
LOCAL_AGENT_URL = os.getenv("LOCAL_AGENT_URL", f"http://{LOCAL_AGENT_HOST}:{LOCAL_AGENT_PORT}").strip() or f"http://{LOCAL_AGENT_HOST}:{LOCAL_AGENT_PORT}"
LOCAL_AGENT_ROOT = Path(os.getenv("LOCAL_AGENT_ROOT", "/opt/node-agent"))
LOCAL_AGENT_STATE_DIR = Path(os.getenv("LOCAL_AGENT_STATE_DIR", "/config/local-agent"))

_agent_lock = asyncio.Lock()
_agent_process: asyncio.subprocess.Process | None = None


def _agent_env() -> dict:
    env = os.environ.copy()
    LOCAL_AGENT_STATE_DIR.mkdir(parents=True, exist_ok=True)
    env.setdefault("AGENT_CONFIG_PATH", str(LOCAL_AGENT_STATE_DIR / "agent-config.json"))
    env.setdefault("AGENT_SECRET_PATH", str(LOCAL_AGENT_STATE_DIR / "agent-secret"))
    env.setdefault("NODE_UID_PATH", str(LOCAL_AGENT_STATE_DIR / "node-uid"))
    env.setdefault("CAMILLA_CONFIG_PATH", str(LOCAL_AGENT_STATE_DIR / "camilladsp.yml"))
    env.setdefault("CAMILLA_TEMPLATE_PATH", str(LOCAL_AGENT_ROOT / "camilladsp-config.yml"))
    env.setdefault("ROOMCAST_UPDATE_COMMAND", "")
    env.setdefault("ROOMCAST_RESTART_COMMAND", "")
    env.setdefault("PLAYBACK_DEVICE", env.get("PLAYBACK_DEVICE", "hw:0,0"))
    env.setdefault("SNAPCLIENT_BIN", env.get("SNAPCLIENT_BIN", "snapclient"))
    return env


def _agent_command() -> list[str]:
    return [
        sys.executable,
        "-m",
        "uvicorn",
        "agent:app",
        "--host",
        LOCAL_AGENT_HOST,
        "--port",
        str(LOCAL_AGENT_PORT),
        "--no-use-colors",
    ]


async def _wait_for_health(timeout: float = 15.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(1.0, timeout)
    async with httpx.AsyncClient(timeout=2.0) as client:
        while True:
            if _agent_process and _agent_process.returncode is not None:
                raise RuntimeError("Local agent process exited early")
            try:
                resp = await client.get(f"{LOCAL_AGENT_URL}/health")
                if resp.status_code == 200:
                    return
            except Exception:
                await asyncio.sleep(0.4)
            if loop.time() >= deadline:
                raise RuntimeError("Local agent failed to start in time")


async def ensure_local_agent_running() -> None:
    global _agent_process
    async with _agent_lock:
        if _agent_process and _agent_process.returncode is None:
            return
        if not LOCAL_AGENT_ROOT.exists():
            raise RuntimeError("Local agent binaries are unavailable on this controller")
        cmd = _agent_command()
        env = _agent_env()
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(LOCAL_AGENT_ROOT),
            env=env,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        _agent_process = proc
        try:
            await _wait_for_health()
        except Exception:
            await stop_local_agent()
            raise


def is_local_agent_running() -> bool:
    return bool(_agent_process and _agent_process.returncode is None)


async def stop_local_agent() -> None:
    global _agent_process
    async with _agent_lock:
        if not _agent_process:
            return
        proc = _agent_process
        _agent_process = None
        if proc.returncode is not None:
            return
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            proc.kill()
            try:
                await proc.wait()
            except Exception:
                pass


def local_agent_url() -> str:
    return LOCAL_AGENT_URL