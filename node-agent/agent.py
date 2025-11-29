import os
import subprocess
from typing import List

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field


AGENT_SHARED_SECRET = os.getenv("AGENT_SHARED_SECRET", "changeme")
MIXER_CONTROL = os.getenv("MIXER_CONTROL", "Master")
DRY_RUN = os.getenv("AGENT_DRY_RUN", "").lower() in ("1", "true", "yes")

app = FastAPI(title="RoomCast Node Agent", version="0.1.0")
eq_state: List[dict] = []
muted_state: bool = False


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


def _auth(request: Request) -> None:
    if not AGENT_SHARED_SECRET:
        return
    if request.headers.get("x-agent-secret") != AGENT_SHARED_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _amixer_set(percent: int) -> None:
    if DRY_RUN:
        return
    try:
        subprocess.run(
            ["amixer", "-M", "set", MIXER_CONTROL, f"{percent}%"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="amixer not installed")
    except subprocess.CalledProcessError as exc:
        raise HTTPException(status_code=500, detail=exc.stderr.decode() or str(exc))


def _amixer_mute(muted: bool) -> None:
    if DRY_RUN:
        return
    cmd = "mute" if muted else "unmute"
    try:
        subprocess.run(
            ["amixer", "-M", "set", MIXER_CONTROL, cmd],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="amixer not installed")
    except subprocess.CalledProcessError as exc:
        raise HTTPException(status_code=500, detail=exc.stderr.decode() or str(exc))


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "eq": eq_state, "muted": muted_state}


@app.post("/volume")
async def set_volume(payload: VolumePayload, request: Request) -> dict:
    _auth(request)
    _amixer_set(payload.percent)
    return {"ok": True, "volume": payload.percent}


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
    eq_state.clear()
    for band in payload.bands:
        eq_state.append(band.model_dump())
    return {"ok": True, "preset": payload.preset, "bands": eq_state}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=9700)
