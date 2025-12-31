from __future__ import annotations

import time
from typing import Optional
from urllib.parse import urlparse


class NodeTerminalService:
    def __init__(
        self,
        *,
        ssh_user_default: str,
        ssh_password_default: str,
        ssh_key_path_default: str,
        ssh_port_default: int,
    ) -> None:
        self._ssh_user_default = ssh_user_default
        self._ssh_password_default = ssh_password_default
        self._ssh_key_path_default = ssh_key_path_default
        self._ssh_port_default = int(ssh_port_default)
        self._sessions: dict[str, dict] = {}

    def terminal_sessions(self) -> dict[str, dict]:
        return self._sessions

    @staticmethod
    def _extract_node_host(node: dict) -> Optional[str]:
        override = (node.get("ssh_host") or "").strip()
        if override:
            return override
        url = node.get("url")
        if not url:
            return None
        try:
            parsed = urlparse(url)
        except ValueError:
            return None
        return parsed.hostname

    def resolve_terminal_target(self, node: dict) -> Optional[dict]:
        host = self._extract_node_host(node)
        if not host:
            return None
        port_raw = node.get("ssh_port") or self._ssh_port_default
        try:
            port = int(port_raw)
        except (TypeError, ValueError):
            port = self._ssh_port_default
        user = (node.get("ssh_user") or self._ssh_user_default).strip()
        password = (node.get("ssh_password") or self._ssh_password_default).strip()
        key_path = (node.get("ssh_key_path") or self._ssh_key_path_default).strip()
        if not user:
            return None
        if not password and not key_path:
            return None
        return {
            "host": host,
            "port": port if port and port > 0 else 22,
            "user": user,
            "password": password,
            "key_path": key_path,
        }

    def cleanup_terminal_sessions(self) -> None:
        if not self._sessions:
            return
        now = time.time()
        expired = [token for token, session in self._sessions.items() if session.get("expires_at", 0) <= now]
        for token in expired:
            self._sessions.pop(token, None)
