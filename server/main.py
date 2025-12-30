"""RoomCast controller entrypoint.

The Docker image runs `uvicorn main:app`.

To keep this file small, the full FastAPI application is implemented in
`app_impl.py` and re-exported here.
"""

from app_impl import app

__all__ = ["app"]
