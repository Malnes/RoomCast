import asyncio
import logging
import re
import time
from typing import Any, Awaitable, Callable, Optional

import httpx
from fastapi import HTTPException
from xml.sax.saxutils import escape as xml_escape
from xml.etree import ElementTree
from urllib.parse import urlparse


log = logging.getLogger("roomcast")


class SonosService:
    def __init__(
        self,
        *,
        nodes: dict,
        get_channels_by_id: Callable[[], dict],
        resolve_channel_id: Callable[[Optional[str]], str],
        resolve_node_channel_id: Callable[[dict], Optional[str]],
        broadcast_nodes: Callable[[], Awaitable[None]],
        save_nodes: Callable[[], None],
        detect_primary_ipv4_host: Callable[[], Optional[str]],
        public_host: str,
        public_port: int,
        http_user_agent: str,
        control_timeout: float,
        device_type: str,
        ssdp_addr: tuple[str, int],
        reconcile_lock: asyncio.Lock,
        connect_grace_seconds: float,
        stream_stale_seconds: float,
        reconnect_attempts: int,
        reconnect_wait_for_stream_seconds: float,
        connection_poll_interval: float,
        discovery_timeout: float,
        scan_max_hosts: int,
        scan_concurrency: int,
        scan_http_timeout: float,
        detect_discovery_networks: Callable[[], list[str]],
        hosts_for_networks: Callable[[list[str], int], list[str]],
    ) -> None:
        self._nodes = nodes
        self._get_channels_by_id = get_channels_by_id
        self._resolve_channel_id = resolve_channel_id
        self._resolve_node_channel_id = resolve_node_channel_id
        self._broadcast_nodes = broadcast_nodes
        self._save_nodes = save_nodes
        self._detect_primary_ipv4_host = detect_primary_ipv4_host
        self._public_host = public_host
        self._public_port = int(public_port)
        self._http_user_agent = http_user_agent
        self._control_timeout = float(control_timeout)
        self._device_type = device_type
        self._ssdp_addr = ssdp_addr
        self._lock = reconcile_lock
        self._connect_grace_seconds = float(connect_grace_seconds)
        self._stream_stale_seconds = float(stream_stale_seconds)
        self._reconnect_attempts = int(reconnect_attempts)
        self._reconnect_wait_for_stream_seconds = float(reconnect_wait_for_stream_seconds)
        self._connection_poll_interval = float(connection_poll_interval)
        self._discovery_timeout = float(discovery_timeout)
        self._scan_max_hosts = int(scan_max_hosts)
        self._scan_concurrency = int(scan_concurrency)
        self._scan_http_timeout = float(scan_http_timeout)
        self._detect_discovery_networks = detect_discovery_networks
        self._hosts_for_networks = hosts_for_networks

    @staticmethod
    def normalize_stereo_mode(value: object) -> str:
        mode = (str(value) if value is not None else "both").strip().lower()
        return mode if mode in {"both", "left", "right"} else "both"

    @staticmethod
    def ffmpeg_pan_filter_for_stereo_mode(mode: str) -> Optional[str]:
        normalized = SonosService.normalize_stereo_mode(mode)
        if normalized == "left":
            return "pan=stereo|c0=c0|c1=c0"
        if normalized == "right":
            return "pan=stereo|c0=c1|c1=c1"
        return None

    @staticmethod
    def ip_from_url(url: Optional[str]) -> Optional[str]:
        if not url:
            return None
        if url.startswith("sonos://"):
            return url[len("sonos://") :].strip("/") or None
        return None

    def _effective_public_host(self) -> str:
        host = (self._public_host or "").strip() or "127.0.0.1"
        # Sonos must be able to reach the controller over the LAN; never hand out loopback.
        if host in {"127.0.0.1", "localhost"}:
            detected = self._detect_primary_ipv4_host()
            if detected:
                host = detected
        return host

    def roomcast_public_base_url(self) -> str:
        return f"http://{self._effective_public_host()}:{self._public_port}"

    def stream_url(self, channel_id: str) -> str:
        return f"{self.roomcast_public_base_url()}/api/sonos/stream/{channel_id}"

    def stream_uri(self, channel_id: str) -> str:
        """Return a Sonos-compatible URI for an HTTP live MP3 stream."""

        http_url = self.stream_url(channel_id)
        parsed = urlparse(http_url)
        suffix = f"{parsed.netloc}{parsed.path}" if parsed.netloc else http_url
        if parsed.query:
            suffix = f"{suffix}?{parsed.query}"
        return f"x-rincon-mp3radio://{suffix}"

    def stream_metadata(self, channel_id: str) -> str:
        """Return DIDL-Lite metadata for the Sonos mp3radio stream."""

        resolved = self._resolve_channel_id(channel_id)
        channel = (self._get_channels_by_id() or {}).get(resolved)
        channel_name = (channel.get("name") if isinstance(channel, dict) else None) or channel_id
        title = f"RoomCast - {channel_name}"
        http_url = self.stream_url(channel_id)

        image_url = f"{self.roomcast_public_base_url()}/static/icons/icon-512.png"

        return (
            '<DIDL-Lite xmlns:dc="http://purl.org/dc/elements/1.1/" '
            'xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/" '
            'xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/" '
            'xmlns:r="urn:schemas-rinconnetworks-com:metadata-1-0/" '
            'xmlns:dlna="urn:schemas-dlna-org:metadata-1-0/">'
            '<item id="flowmode" parentID="0" restricted="1">'
            f"<dc:title>{xml_escape(title)}</dc:title>"
            f"<upnp:albumArtURI>{xml_escape(image_url)}</upnp:albumArtURI>"
            "<dc:description>RoomCast</dc:description>"
            '<upnp:class>object.item.audioItem.audioBroadcast</upnp:class>'
            f"<res protocolInfo=\"http-get:*:audio/mpeg:DLNA.ORG_OP=01;DLNA.ORG_CI=0;DLNA.ORG_FLAGS=01700000000000000000000000000000\">{xml_escape(http_url)}</res>"
            '<desc id="cdudn" nameSpace="urn:schemas-rinconnetworks-com:metadata-1-0/">'
            "RINCON_AssociatedZPUDN"
            "</desc>"
            "</item>"
            "</DIDL-Lite>"
        )

    @staticmethod
    def _extract_rincon(udn: str | None) -> Optional[str]:
        if not udn:
            return None
        raw = udn.strip()
        if raw.lower().startswith("uuid:"):
            raw = raw[5:]
        return raw if raw.startswith("RINCON_") else None

    async def fetch_description(self, ip: str, *, timeout: Optional[float] = None) -> Optional[dict]:
        url = f"http://{ip}:1400/xml/device_description.xml"
        headers = {"User-Agent": self._http_user_agent}
        effective_timeout = self._control_timeout if timeout is None else float(timeout)
        try:
            async with httpx.AsyncClient(timeout=effective_timeout) as client:
                resp = await client.get(url, headers=headers)
            if resp.status_code != 200:
                return None
            root = ElementTree.fromstring(resp.text)
        except Exception:
            return None

        def _find_text(path: str) -> Optional[str]:
            el = root.find(path)
            if el is None or el.text is None:
                return None
            value = el.text.strip()
            return value or None

        friendly = _find_text(".//friendlyName") or _find_text(".//{*}friendlyName")
        udn = _find_text(".//UDN") or _find_text(".//{*}UDN")
        dtype = _find_text(".//deviceType") or _find_text(".//{*}deviceType")
        if dtype and dtype.strip() != self._device_type:
            return None
        rincon = self._extract_rincon(udn)
        return {
            "friendly_name": friendly,
            "udn": udn,
            "rincon": rincon,
            "device_type": dtype,
            "description_url": url,
        }

    async def soap_action_text(
        self,
        ip: str,
        *,
        service: str,
        action: str,
        control_path: str,
        arguments: dict[str, str],
        timeout: Optional[float] = None,
    ) -> str:
        target = f"http://{ip}:1400{control_path}"
        ns = f"urn:schemas-upnp-org:service:{service}:1"
        body_parts = [f"<{k}>{xml_escape(v)}</{k}>" for k, v in arguments.items()]
        envelope = (
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
            "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" "
            "s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">"
            "<s:Body>"
            f"<u:{action} xmlns:u=\"{ns}\">"
            + "".join(body_parts)
            + f"</u:{action}>"
            "</s:Body>"
            "</s:Envelope>"
        )
        headers = {
            "Content-Type": "text/xml; charset=\"utf-8\"",
            "SOAPACTION": f'\"{ns}#{action}\"',
            "User-Agent": self._http_user_agent,
            "Connection": "close",
        }
        effective_timeout = self._control_timeout if timeout is None else float(timeout)
        async with httpx.AsyncClient(timeout=effective_timeout) as client:
            resp = await client.post(target, content=envelope.encode("utf-8"), headers=headers)

        def _parse_upnp_fault(xml_text: str) -> Optional[str]:
            try:
                root = ElementTree.fromstring(xml_text)
            except Exception:
                return None
            fault = root.find(".//{*}Fault")
            if fault is None:
                return None
            error_code = root.findtext(".//{*}errorCode")
            error_desc = root.findtext(".//{*}errorDescription")
            if error_code or error_desc:
                code = (error_code or "").strip()
                desc = (error_desc or "").strip()
                if code and desc:
                    return f"UPnPError {code}: {desc}"
                return f"UPnPError {code or desc}".strip()
            fault_string = root.findtext(".//{*}faultstring")
            if fault_string:
                return fault_string.strip()
            return "UPnPError (unknown SOAP fault)"

        fault_msg = _parse_upnp_fault(resp.text or "")
        if resp.status_code >= 400 or fault_msg:
            detail = fault_msg or (resp.text or "").strip() or f"HTTP {resp.status_code}"
            raise HTTPException(status_code=502, detail=f"Sonos SOAP {service}.{action} failed: {detail}")
        return resp.text

    async def soap_action(self, ip: str, **kwargs: Any) -> None:
        await self.soap_action_text(ip, **kwargs)

    async def get_transport_state(self, ip: str) -> Optional[str]:
        xml_text = await self.soap_action_text(
            ip,
            service="AVTransport",
            action="GetTransportInfo",
            control_path="/MediaRenderer/AVTransport/Control",
            arguments={"InstanceID": "0"},
        )
        try:
            root = ElementTree.fromstring(xml_text)
        except Exception as exc:
            log.warning("Sonos GetTransportInfo parse failed (ip=%s): %s", ip, exc)
            return None
        state = root.findtext(".//{*}CurrentTransportState")
        return state.strip() if state else None

    async def get_current_uri(self, ip: str) -> Optional[str]:
        xml_text = await self.soap_action_text(
            ip,
            service="AVTransport",
            action="GetMediaInfo",
            control_path="/MediaRenderer/AVTransport/Control",
            arguments={"InstanceID": "0"},
        )
        try:
            root = ElementTree.fromstring(xml_text)
        except Exception as exc:
            log.warning("Sonos GetMediaInfo parse failed (ip=%s): %s", ip, exc)
            return None
        uri = root.findtext(".//{*}CurrentURI")
        return uri.strip() if uri else None

    async def get_zone_name(self, ip: str, *, timeout: Optional[float] = None) -> Optional[str]:
        """Return the Sonos app 'room' / zone name for this device."""

        try:
            xml_text = await self.soap_action_text(
                ip,
                service="DeviceProperties",
                action="GetZoneAttributes",
                control_path="/DeviceProperties/Control",
                arguments={},
                timeout=timeout,
            )
        except Exception as exc:
            log.warning("Sonos GetZoneAttributes failed (ip=%s): %s", ip, exc)
            return None

        try:
            root = ElementTree.fromstring(xml_text)
        except Exception as exc:
            log.warning("Sonos GetZoneAttributes parse failed (ip=%s): %s", ip, exc)
            return None
        name = root.findtext(".//{*}CurrentZoneName")
        if not name:
            return None
        resolved = name.strip()
        return resolved or None

    async def set_volume(self, ip: str, percent: int) -> None:
        await self.soap_action(
            ip,
            service="RenderingControl",
            action="SetVolume",
            control_path="/MediaRenderer/RenderingControl/Control",
            arguments={
                "InstanceID": "0",
                "Channel": "Master",
                "DesiredVolume": str(max(0, min(100, int(percent)))),
            },
        )

    async def set_mute(self, ip: str, muted: bool) -> None:
        await self.soap_action(
            ip,
            service="RenderingControl",
            action="SetMute",
            control_path="/MediaRenderer/RenderingControl/Control",
            arguments={
                "InstanceID": "0",
                "Channel": "Master",
                "DesiredMute": "1" if muted else "0",
            },
        )

    @staticmethod
    def normalize_eq(value: object) -> dict:
        data = value if isinstance(value, dict) else {}
        bass = data.get("bass")
        treble = data.get("treble")
        loudness = data.get("loudness")
        try:
            bass_i = int(bass)
        except (TypeError, ValueError):
            bass_i = 0
        try:
            treble_i = int(treble)
        except (TypeError, ValueError):
            treble_i = 0
        return {
            "bass": max(-10, min(10, bass_i)),
            "treble": max(-10, min(10, treble_i)),
            "loudness": bool(loudness) if loudness is not None else False,
        }

    async def set_eq(self, ip: str, *, eq_type: str, value: int) -> None:
        await self.soap_action(
            ip,
            service="RenderingControl",
            action="SetEQ",
            control_path="/MediaRenderer/RenderingControl/Control",
            arguments={
                "InstanceID": "0",
                "EQType": str(eq_type),
                "DesiredValue": str(int(value)),
            },
        )

    async def become_standalone(self, ip: str) -> None:
        await self.soap_action(
            ip,
            service="AVTransport",
            action="BecomeCoordinatorOfStandaloneGroup",
            control_path="/MediaRenderer/AVTransport/Control",
            arguments={"InstanceID": "0"},
        )

    async def join(self, ip: str, coordinator_rincon: str) -> None:
        uri = f"x-rincon:{coordinator_rincon}"
        await self.soap_action(
            ip,
            service="AVTransport",
            action="SetAVTransportURI",
            control_path="/MediaRenderer/AVTransport/Control",
            arguments={
                "InstanceID": "0",
                "CurrentURI": uri,
                "CurrentURIMetaData": "",
            },
        )

    async def set_uri_and_play(self, ip: str, uri: str, metadata: str = "") -> None:
        timeout_s = max(self._control_timeout, 12.0)
        retries = 2
        for attempt in range(1, retries + 1):
            try:
                await self.soap_action(
                    ip,
                    service="AVTransport",
                    action="SetAVTransportURI",
                    control_path="/MediaRenderer/AVTransport/Control",
                    arguments={
                        "InstanceID": "0",
                        "CurrentURI": uri,
                        "CurrentURIMetaData": metadata or "",
                    },
                    timeout=timeout_s,
                )
                break
            except httpx.TimeoutException as exc:
                if attempt >= retries:
                    raise HTTPException(status_code=502, detail=f"Sonos SetAVTransportURI timed out: {exc}") from exc
                await asyncio.sleep(0.25 * attempt)
            except httpx.RequestError as exc:
                if attempt >= retries:
                    raise HTTPException(status_code=502, detail=f"Sonos SetAVTransportURI failed: {exc}") from exc
                await asyncio.sleep(0.25 * attempt)

        for attempt in range(1, retries + 1):
            try:
                await self.soap_action(
                    ip,
                    service="AVTransport",
                    action="Play",
                    control_path="/MediaRenderer/AVTransport/Control",
                    arguments={
                        "InstanceID": "0",
                        "Speed": "1",
                    },
                    timeout=timeout_s,
                )
                return
            except httpx.TimeoutException as exc:
                if attempt >= retries:
                    raise HTTPException(status_code=502, detail=f"Sonos Play timed out: {exc}") from exc
                await asyncio.sleep(0.25 * attempt)
            except httpx.RequestError as exc:
                if attempt >= retries:
                    raise HTTPException(status_code=502, detail=f"Sonos Play failed: {exc}") from exc
                await asyncio.sleep(0.25 * attempt)

    async def set_uri_and_play_with_fallback(self, channel_id: str, coordinator_ip: str) -> None:
        mp3radio_uri = self.stream_uri(channel_id)
        http_uri = self.stream_url(channel_id)
        metadata_mp3radio = ""
        metadata_http = self.stream_metadata(channel_id)

        log.info(
            "Sonos start stream (ip=%s, channel=%s, mp3radio_uri=%s, http_uri=%s)",
            coordinator_ip,
            channel_id,
            mp3radio_uri,
            http_uri,
        )

        await self.set_uri_and_play(coordinator_ip, mp3radio_uri, metadata_mp3radio)
        state = None
        for _ in range(3):
            await asyncio.sleep(0.5)
            try:
                state = await self.get_transport_state(coordinator_ip)
            except Exception:
                state = None
            if state and state.upper() == "PLAYING":
                return

        log.warning(
            "Sonos coordinator transport not PLAYING after mp3radio start (ip=%s, state=%s); retrying with plain HTTP URI",
            coordinator_ip,
            state,
        )
        await self.set_uri_and_play(coordinator_ip, http_uri, metadata_http)
        state2 = None
        for _ in range(3):
            await asyncio.sleep(0.5)
            try:
                state2 = await self.get_transport_state(coordinator_ip)
            except Exception:
                state2 = None
            if state2 and state2.upper() == "PLAYING":
                return
        try:
            current_uri = await self.get_current_uri(coordinator_ip)
        except Exception:
            current_uri = None
        log.warning(
            "Sonos coordinator still not PLAYING after HTTP retry (ip=%s, state=%s, current_uri=%s)",
            coordinator_ip,
            state2,
            current_uri,
        )

    async def stop(self, ip: str) -> None:
        await self.soap_action(
            ip,
            service="AVTransport",
            action="Stop",
            control_path="/MediaRenderer/AVTransport/Control",
            arguments={"InstanceID": "0"},
        )

    async def ping(self, ip: str) -> bool:
        headers = {"User-Agent": self._http_user_agent}
        try:
            async with httpx.AsyncClient(timeout=self._control_timeout) as client:
                resp = await client.get(f"http://{ip}:1400/xml/device_description.xml", headers=headers)
            return resp.status_code == 200
        except Exception:
            return False

    @staticmethod
    def parse_network_from_review(text: str) -> Optional[dict]:
        if not text:
            return None
        normalized = text.strip()
        if not normalized:
            return None

        link_match = re.search(r"<Link>(\d+)</Link>", normalized)
        speed_match = re.search(r"<Speed>(\d+)</Speed>", normalized)
        try:
            link = int(link_match.group(1)) if link_match else 0
        except (TypeError, ValueError):
            link = 0
        try:
            speed = int(speed_match.group(1)) if speed_match else 0
        except (TypeError, ValueError):
            speed = 0

        wifi_mode_match = re.search(r"<WifiModeString>([^<]+)</WifiModeString>", normalized)
        wifi_mode_string = (wifi_mode_match.group(1).strip() if wifi_mode_match else "")

        if link == 1 and speed > 0:
            transport = "ethernet"
        elif wifi_mode_string:
            upper = wifi_mode_string.upper()
            if "SONOSNET" in upper:
                transport = "sonosnet"
            elif "STATION" in upper:
                transport = "wifi"
            else:
                transport = "wireless"
        else:
            transport = "unknown"

        return {
            "transport": transport,
            "wifi_mode_string": wifi_mode_string or None,
            "ethernet_link": bool(link == 1),
            "ethernet_speed": speed if speed > 0 else None,
        }

    def find_node_by_ip(self, ip: Optional[str]) -> Optional[dict]:
        if not ip:
            return None
        needle = ip.strip().lower()
        if not needle:
            return None
        for node in self._nodes.values():
            if node.get("type") != "sonos":
                continue
            node_ip = self.ip_from_url(node.get("url"))
            if node_ip and node_ip.strip().lower() == needle:
                return node
        return None

    def client_allows_stream(self, resolved_channel_id: str, client_ip: Optional[str]) -> bool:
        node = self.find_node_by_ip(client_ip)
        if not node:
            return True
        desired = self._resolve_node_channel_id(node)
        return bool(desired and desired == resolved_channel_id)

    def mark_stream_activity(self, channel_id: str, client_ip: Optional[str], kind: str) -> None:
        node = self.find_node_by_ip(client_ip)
        if not node:
            return
        now = time.time()
        node["sonos_stream_last_client"] = client_ip
        if kind == "head":
            node["sonos_last_stream_head_ts"] = now
            return
        if kind == "get":
            node["sonos_last_stream_get_ts"] = now
            node["sonos_stream_active"] = True
            node["connection_state"] = "playing"
            node["connection_error"] = None
            node["sonos_connecting_since"] = None
            return
        if kind == "bytes":
            node["sonos_last_stream_byte_ts"] = now

    def mark_stream_end(self, client_ip: Optional[str]) -> None:
        node = self.find_node_by_ip(client_ip)
        if not node:
            return
        node["sonos_stream_active"] = False

    def stream_is_stale(self, node: dict, *, now: float) -> bool:
        if not node or node.get("type") != "sonos":
            return False
        channel_id = self._resolve_node_channel_id(node)
        if not channel_id:
            return False
        if node.get("online") is False:
            return False

        connecting_since = node.get("sonos_connecting_since")
        if isinstance(connecting_since, (int, float)) and float(connecting_since) > 0:
            if now - float(connecting_since) <= self._connect_grace_seconds:
                return False
            last_get = node.get("sonos_last_stream_get_ts")
            if not isinstance(last_get, (int, float)):
                last_head = node.get("sonos_last_stream_head_ts")
                if isinstance(last_head, (int, float)):
                    return (now - float(last_head)) > self._stream_stale_seconds
                return True
            return (now - float(last_get)) > self._stream_stale_seconds

        last_byte = node.get("sonos_last_stream_byte_ts")
        if isinstance(last_byte, (int, float)):
            return (now - float(last_byte)) > self._stream_stale_seconds
        last_get = node.get("sonos_last_stream_get_ts")
        if isinstance(last_get, (int, float)):
            return (now - float(last_get)) > self._stream_stale_seconds
        last_head = node.get("sonos_last_stream_head_ts")
        if isinstance(last_head, (int, float)):
            return (now - float(last_head)) > self._stream_stale_seconds
        return True

    async def attempt_reconnect(self, channel_id: str, members: list[dict]) -> bool:
        async with self._lock:
            if not members:
                return True
            members_sorted = sorted(members, key=lambda item: (item.get("name") or "", item.get("id") or ""))
            coordinator = members_sorted[0]
            coordinator_ip = self.ip_from_url(coordinator.get("url"))
            coordinator_rincon = coordinator.get("sonos_rincon")
            if not coordinator_ip or not coordinator_rincon:
                return False

            connect_ts = time.time()
            for n in members_sorted:
                n["connection_state"] = "connecting"
                n["connection_error"] = None
                n["sonos_connecting_since"] = connect_ts
            await self._broadcast_nodes()

            for attempt in range(1, max(1, self._reconnect_attempts) + 1):
                attempt_start = time.time()
                try:
                    try:
                        await self.become_standalone(coordinator_ip)
                    except Exception:
                        pass

                    await self.set_uri_and_play_with_fallback(channel_id, coordinator_ip)

                    for member in members_sorted[1:]:
                        member_ip = self.ip_from_url(member.get("url"))
                        if not member_ip:
                            continue
                        try:
                            await self.join(member_ip, coordinator_rincon)
                        except Exception as exc:
                            member["connection_state"] = "error"
                            member["connection_error"] = f"Failed to join Sonos group: {exc}"

                    deadline = attempt_start + max(1.0, self._reconnect_wait_for_stream_seconds)
                    while time.time() < deadline:
                        coord_node = self.find_node_by_ip(coordinator_ip)
                        last_get = coord_node.get("sonos_last_stream_get_ts") if coord_node else None
                        last_byte = coord_node.get("sonos_last_stream_byte_ts") if coord_node else None
                        if isinstance(last_get, (int, float)) and float(last_get) >= attempt_start:
                            if isinstance(last_byte, (int, float)) and float(last_byte) >= attempt_start:
                                for n in members_sorted:
                                    if n.get("connection_state") != "error":
                                        n["connection_state"] = "playing"
                                        n["connection_error"] = None
                                    n["sonos_connecting_since"] = None
                                await self._broadcast_nodes()
                                return True
                        await asyncio.sleep(0.35)
                except Exception as exc:
                    log.warning(
                        "Sonos reconnect attempt %s/%s failed (channel=%s, coordinator=%s): %r",
                        attempt,
                        self._reconnect_attempts,
                        channel_id,
                        coordinator_ip,
                        exc,
                    )
                    await asyncio.sleep(min(1.5, 0.35 * attempt))

            err = f"Lost connection to Sonos stream; reconnect failed ({self._reconnect_attempts} attempts)."
            for n in members_sorted:
                n["connection_state"] = "error"
                n["connection_error"] = err
                n["sonos_connecting_since"] = None
                ip = self.ip_from_url(n.get("url"))
                if ip:
                    try:
                        await self.become_standalone(ip)
                    except Exception:
                        pass
                    try:
                        await self.stop(ip)
                    except Exception:
                        pass
            self._save_nodes()
            await self._broadcast_nodes()
            return False

    async def reconcile_groups(self) -> None:
        async with self._lock:
            by_channel: dict[str, list[dict]] = {}
            standalone: list[dict] = []
            for node in self._nodes.values():
                if node.get("type") != "sonos":
                    continue
                if node.get("online") is False:
                    continue
                cid = self._resolve_node_channel_id(node)
                if not cid:
                    standalone.append(node)
                    continue
                by_channel.setdefault(cid, []).append(node)

            for node in standalone:
                node["connection_state"] = None
                node["connection_error"] = None
                node["sonos_connecting_since"] = None
                ip = self.ip_from_url(node.get("url"))
                if not ip:
                    continue
                try:
                    await self.become_standalone(ip)
                    await self.stop(ip)
                except Exception:
                    pass

            for channel_id, members in by_channel.items():
                if not members:
                    continue

                connect_ts = time.time()
                for n in members:
                    n["connection_state"] = "connecting"
                    n["connection_error"] = None
                    n["sonos_connecting_since"] = connect_ts
                await self._broadcast_nodes()

                members_sorted = sorted(members, key=lambda item: (item.get("name") or "", item.get("id") or ""))
                coordinator = members_sorted[0]
                coordinator_ip = self.ip_from_url(coordinator.get("url"))
                coordinator_rincon = coordinator.get("sonos_rincon")
                if not coordinator_ip or not coordinator_rincon:
                    for n in members_sorted:
                        n["connection_state"] = "error"
                        n["connection_error"] = "Invalid Sonos coordinator configuration"
                        n["sonos_connecting_since"] = None
                    continue

                try:
                    await self.become_standalone(coordinator_ip)
                except Exception:
                    pass
                try:
                    await self.set_uri_and_play_with_fallback(channel_id, coordinator_ip)
                except HTTPException as exc:
                    detail = getattr(exc, "detail", None)
                    msg = detail or str(exc) or repr(exc)
                    log.warning(
                        "Sonos coordinator %s failed to start stream (ip=%s, channel=%s): %s",
                        coordinator.get("id"),
                        coordinator_ip,
                        channel_id,
                        msg,
                    )
                    for n in members_sorted:
                        n["connection_state"] = "error"
                        n["connection_error"] = f"Sonos failed to start stream: {msg}"
                        n["sonos_connecting_since"] = None
                    continue
                except Exception as exc:
                    log.warning(
                        "Sonos coordinator %s failed to start stream (ip=%s, channel=%s): %r",
                        coordinator.get("id"),
                        coordinator_ip,
                        channel_id,
                        exc,
                    )
                    for n in members_sorted:
                        n["connection_state"] = "error"
                        n["connection_error"] = f"Sonos failed to start stream: {exc}"
                        n["sonos_connecting_since"] = None
                    continue

                for member in members_sorted[1:]:
                    member_ip = self.ip_from_url(member.get("url"))
                    if not member_ip:
                        member["connection_state"] = "error"
                        member["connection_error"] = "Invalid Sonos member configuration"
                        member["sonos_connecting_since"] = None
                        continue
                    try:
                        await self.join(member_ip, coordinator_rincon)
                    except Exception as exc:
                        member["connection_state"] = "error"
                        member["connection_error"] = f"Failed to join Sonos group: {exc}"
                        member["sonos_connecting_since"] = None
                        log.warning("Sonos member %s failed to join group: %s", member.get("id"), exc)

    async def connection_loop(self) -> None:
        try:
            while True:
                try:
                    await asyncio.sleep(max(1.0, self._connection_poll_interval))
                    now = time.time()
                    by_channel: dict[str, list[dict]] = {}
                    for node in self._nodes.values():
                        if node.get("type") != "sonos":
                            continue
                        if node.get("online") is False:
                            continue
                        cid = self._resolve_node_channel_id(node)
                        if not cid:
                            continue
                        by_channel.setdefault(cid, []).append(node)

                    for channel_id, members in by_channel.items():
                        if not members:
                            continue
                        members_sorted = sorted(
                            members,
                            key=lambda item: (item.get("name") or "", item.get("id") or ""),
                        )
                        coordinator = members_sorted[0]
                        if not self.stream_is_stale(coordinator, now=now):
                            continue
                        log.warning(
                            "Sonos stream appears stale; attempting reconnect (channel=%s, coordinator=%s)",
                            channel_id,
                            self.ip_from_url(coordinator.get("url")),
                        )
                        await self.attempt_reconnect(channel_id, members_sorted)
                except asyncio.CancelledError:
                    break
                except Exception:
                    log.exception("Sonos connection monitor iteration failed")
                    await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass

    async def ssdp_discover(self) -> list[dict]:
        ssdp_addr = self._ssdp_addr
        message = (
            "M-SEARCH * HTTP/1.1\r\n"
            f"HOST: {ssdp_addr[0]}:{ssdp_addr[1]}\r\n"
            'MAN: "ssdp:discover"\r\n'
            "MX: 1\r\n"
            f"ST: {self._device_type}\r\n"
            "\r\n"
        ).encode("utf-8")
        loop = asyncio.get_running_loop()
        found: dict[str, dict] = {}

        class _Proto(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                self.transport = transport
                try:
                    self.transport.sendto(message, ssdp_addr)
                except Exception:
                    pass

            def datagram_received(self, data, addr):
                try:
                    text = data.decode("utf-8", errors="ignore")
                except Exception:
                    return
                lines = [line.strip() for line in text.split("\r\n") if line.strip()]
                headers = {}
                for line in lines[1:]:
                    if ":" not in line:
                        continue
                    k, v = line.split(":", 1)
                    headers[k.strip().lower()] = v.strip()
                location = headers.get("location")
                if not location:
                    return
                ip = addr[0]
                try:
                    parsed = urlparse(location)
                    if parsed.hostname:
                        ip = parsed.hostname
                except Exception:
                    pass
                if not ip:
                    return
                found[ip] = {"ip": ip, "location": location}

        transport = None
        try:
            transport, _ = await loop.create_datagram_endpoint(
                lambda: _Proto(),
                local_addr=("0.0.0.0", 0),
            )
            await asyncio.sleep(max(0.2, self._discovery_timeout))
        finally:
            if transport:
                transport.close()

        results: list[dict] = []
        for item in found.values():
            ip = item.get("ip")
            if not ip:
                continue
            desc = await self.fetch_description(ip)
            if not desc:
                continue

            zone_name = await self.get_zone_name(ip, timeout=self._scan_http_timeout)
            friendly_name = desc.get("friendly_name") if isinstance(desc, dict) else None
            display_name = zone_name or friendly_name or ip
            results.append(
                {
                    "kind": "sonos",
                    "host": display_name,
                    "url": f"sonos://{ip}",
                    "fingerprint": desc.get("udn"),
                    "sonos_zone_name": zone_name,
                    "sonos_friendly_name": friendly_name,
                    "version": "Sonos",
                }
            )
        return results

    async def http_scan(self, networks: list[str]) -> list[dict]:
        hosts = self._hosts_for_networks(networks, limit=max(1, self._scan_max_hosts))
        if not hosts:
            return []
        sem = asyncio.Semaphore(max(4, self._scan_concurrency))
        found: dict[str, dict] = {}

        async def _probe(ip: str) -> None:
            async with sem:
                desc = await self.fetch_description(ip, timeout=self._scan_http_timeout)
            if not desc:
                return
            udn = desc.get("udn")
            if not desc.get("rincon") and not (isinstance(udn, str) and "RINCON_" in udn):
                return

            zone_name = await self.get_zone_name(ip, timeout=self._scan_http_timeout)
            if isinstance(desc, dict):
                desc["zone_name"] = zone_name
            found[ip] = desc

        await asyncio.gather(*(_probe(ip) for ip in hosts))
        results: list[dict] = []
        for ip, desc in found.items():
            zone_name = desc.get("zone_name") if isinstance(desc, dict) else None
            friendly_name = desc.get("friendly_name") if isinstance(desc, dict) else None
            display_name = zone_name or friendly_name or ip
            results.append(
                {
                    "kind": "sonos",
                    "host": display_name,
                    "url": f"sonos://{ip}",
                    "fingerprint": desc.get("udn"),
                    "sonos_zone_name": zone_name,
                    "sonos_friendly_name": friendly_name,
                    "version": "Sonos",
                }
            )
        results.sort(key=lambda x: (x.get("host") or "", x.get("url") or ""))
        return results

    async def discover(self) -> list[dict]:
        ssdp_items: list[dict] = []
        try:
            ssdp_items = await self.ssdp_discover()
        except Exception as exc:
            log.warning("Sonos SSDP discovery failed: %s", exc)
            ssdp_items = []

        networks = []
        try:
            networks = self._detect_discovery_networks() or []
        except Exception:
            networks = []

        scan_items: list[dict] = []
        should_scan = bool(networks) and (not ssdp_items or len(ssdp_items) < 3)
        if should_scan:
            if not ssdp_items:
                log.info("Sonos SSDP returned no devices; falling back to HTTP scan")
            try:
                scan_items = await self.http_scan(networks)
            except Exception as exc:
                log.warning("Sonos HTTP scan failed: %s", exc)
                scan_items = []

        combined: list[dict] = []
        seen: set[str] = set()
        for item in (ssdp_items or []) + (scan_items or []):
            url = item.get("url") if isinstance(item, dict) else None
            fp = item.get("fingerprint") if isinstance(item, dict) else None
            key = (str(url) if url else str(fp or "")).strip()
            if not key or key in seen:
                continue
            seen.add(key)
            combined.append(item)

        combined.sort(key=lambda x: ((x or {}).get("host") or "", (x or {}).get("url") or ""))
        return combined
