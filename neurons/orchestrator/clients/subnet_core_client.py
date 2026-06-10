"""
BeamCore API Client for Orchestrators

Client for orchestrators to register, list workers, assign chunks, and report
orchestrator state to the BeamCore service.

Uses orch-gateway WebSocket for real-time orchestrator control-plane traffic.
BeamCore HTTP covers additional control-plane APIs alongside the WebSocket.
"""

import asyncio
import inspect
import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import httpx
import websockets
from websockets.exceptions import ConnectionClosed

from middleware.metrics import BEAMCORE_UPSTREAM_DEGRADED, BEAMCORE_UPSTREAM_DOWN_EVENTS

logger = logging.getLogger(__name__)


def _coerce_worker_metric(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_worker_list(workers: list[dict[str, Any]], transfer_id: str) -> list[dict[str, Any]]:
    normalized_workers: list[dict[str, Any]] = []
    skipped_workers = 0

    for worker in workers:
        worker_id = worker.get("worker_id")
        if not worker_id:
            skipped_workers += 1
            continue

        normalized_workers.append(
            {
                **worker,
                "worker_id": worker_id,
                "trust_score": _coerce_worker_metric(worker.get("trust_score"), 0.5),
                "bandwidth_mbps": _coerce_worker_metric(worker.get("bandwidth_mbps"), 100.0),
            }
        )

    if skipped_workers:
        logger.warning(
            "Skipped %s malformed worker entries for transfer %s",
            skipped_workers,
            transfer_id,
        )

    return normalized_workers


@dataclass
class TaskExecutionContext:
    """Execution context for real data transfer - passed to workers."""

    transfer_id: str
    stream_id: str
    gateway_url: str  # REQUIRED - where workers fetch chunks
    destination_url: str  # REQUIRED - where workers send data
    chunk_indices: List[int]
    source_type: str = "http"


@dataclass
class TaskCreate:
    """Task creation data."""

    task_id: str
    worker_id: str
    chunk_size: int
    chunk_hash: str
    deadline_us: int
    source_region: Optional[str] = None
    dest_region: Optional[str] = None
    canary_hex: Optional[str] = None
    canary_offset: Optional[int] = None
    # Execution context - REQUIRED for real data transfer
    execution_context: Optional[TaskExecutionContext] = None


@dataclass
class TaskUpdate:
    """Task update data."""

    status: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    bytes_relayed: Optional[int] = None
    bandwidth_mbps: Optional[float] = None
    latency_ms: Optional[float] = None


class SubnetCoreClient:
    """
    Client for communicating with BeamCore HTTP and orch-gateway WebSocket.

    Orchestrators use this client to:
    - Receive real-time notifications via orch-gateway WebSocket
    - Send orchestrator registration, readiness, worker-list requests, and chunk assignments via orch-gateway WebSocket
    - Use BeamCore HTTP for auth bootstrap and read APIs
    - Report task/proof state needed by BeamCore control-plane flows
    """

    def __init__(
        self,
        base_url: str,
        ws_base_url: str,
        orchestrator_hotkey: str,
        orchestrator_uid: int,
        timeout: float = 30.0,
        signer=None,
        *,
        ws_open_timeout: float = 60.0,
        ws_close_timeout: float = 20.0,
        ws_ping_interval: float = 30.0,
        ws_ping_timeout: float = 45.0,
    ):
        """
        Initialize the client.

        Args:
            base_url: Base URL of BeamCore (e.g., https://beamcore.b1m.ai)
            ws_base_url: Required base URL of the orchestrator gateway WebSocket edge
            orchestrator_hotkey: This orchestrator's hotkey for authentication
            orchestrator_uid: This orchestrator's UID
            timeout: Request timeout in seconds
            signer: Optional bittensor wallet hotkey with .sign() method
            ws_open_timeout: Seconds to wait for the WebSocket opening handshake (orch-gateway).
            ws_close_timeout: Seconds to wait when closing the WebSocket cleanly.
            ws_ping_interval / ws_ping_timeout: Transport keepalive; higher values help flaky paths.
        """
        self.base_url = base_url.rstrip("/")
        self.ws_base_url = ws_base_url.rstrip("/")
        self.orchestrator_hotkey = orchestrator_hotkey
        self.orchestrator_uid = orchestrator_uid
        self.timeout = timeout
        self.signer = signer
        self._ws_open_timeout = ws_open_timeout
        self._ws_close_timeout = ws_close_timeout
        self._ws_ping_interval = ws_ping_interval
        self._ws_ping_timeout = ws_ping_timeout
        self._client: Optional[httpx.AsyncClient] = None

        # WebSocket push handlers (task_result_summary via WS, worker_update via WS)
        self._task_completion_handler: Optional[Callable] = None
        self._worker_update_handler: Optional[Callable] = (
            None  # Handler for worker connect/disconnect push events
        )
        self._running = False

        # WebSocket state
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_connected = False
        self._ws_task: Optional[asyncio.Task] = None
        self._reconnect_delay = 5.0  # Seconds between reconnection attempts
        self._max_reconnect_delay = 60.0  # Max backoff delay

        # WebSocket registration state
        self._registered = False
        self._registration_config: Optional[Dict[str, Any]] = None
        self._registration_retry_task: Optional[asyncio.Task] = None
        self._registration_retry_interval = 5.0
        self._desired_ready = False
        self._last_confirmed_ready: Optional[bool] = None
        self._ready_sync_task: Optional[asyncio.Task] = None
        self._ready_sync_retry_interval = 5.0

        # API key authentication (for buffer service)
        self._api_key: Optional[str] = None
        self._api_key_expires: Optional[float] = None
        self._skip_env_key: bool = False

        # Pending worker-list requests keyed by transfer_id (WS protocol)
        self._pending_ws_requests: dict[str, asyncio.Future] = {}

        # DEDICATED mode: in-process worker gateway + relay-ack correlation (keyed by offer_id)
        self._dedicated_gateway = None
        self._pending_response_acks: dict[str, asyncio.Future] = {}
        self._pending_result_acks: dict[str, asyncio.Future] = {}

        # orch-gateway → BeamCore upstream relay (independent of orch ↔ orch-gateway edge socket)
        self._beamcore_upstream_degraded: bool = False

    # =========================================================================
    # Handlers for polling notifications
    # =========================================================================

    def set_task_completion_handler(self, handler: Callable):
        """
        Set handler for task completion notifications.

        Handler signature: async def handler(task_completion: dict) -> bool
        Returns True if task is verified and should be acknowledged.
        """
        self._task_completion_handler = handler

    def set_worker_update_handler(self, handler: Callable):
        """
        Set handler for worker connect/disconnect push events.

        Handler signature: async def handler(worker_id: str, event: str) -> None
        Where event is "connected" or "disconnected".
        """
        self._worker_update_handler = handler

    def prime_ready_state(self, ready: bool) -> None:
        """Set the desired ready state before the websocket auto-registers."""
        self._desired_ready = ready

    def is_beamcore_upstream_degraded(self) -> bool:
        """
        True when orch-gateway reported BeamCore upstream relay down (or request failed for relay loss).
        Edge WebSocket to orch-gateway can still be open; this flags control-plane path only.
        """
        return self._beamcore_upstream_degraded

    def _note_beamcore_upstream_down(self, reason: str) -> None:
        BEAMCORE_UPSTREAM_DOWN_EVENTS.inc()
        if self._beamcore_upstream_degraded:
            logger.debug("BeamCore upstream still degraded: %s", reason)
            return
        self._beamcore_upstream_degraded = True
        BEAMCORE_UPSTREAM_DEGRADED.set(1)
        logger.info(
            "================================================================================\n"
            "BEAMCORE UPSTREAM DEGRADED (orch-gateway → BeamCore relay is down or recovering)\n"
            "You are still connected to orch-gateway, but work cannot be relayed to BeamCore until the\n"
            "gateway reconnects upstream. Reason: %s\n"
            "================================================================================",
            reason,
        )

    def _note_beamcore_upstream_recovered(self, reason: str) -> None:
        if not self._beamcore_upstream_degraded:
            return
        self._beamcore_upstream_degraded = False
        BEAMCORE_UPSTREAM_DEGRADED.set(0)
        logger.info(
            "BeamCore upstream relay recovered (%s) — orchestrator path to BeamCore is live again",
            reason,
        )

    def _maybe_upstream_error_payload(self, data: dict) -> None:
        """Classify orch-gateway error payloads that imply upstream/backpressure loss."""
        if data.get("type") != "error":
            return
        msg = str(data.get("message") or data.get("error") or "")
        if msg in ("upstream_timeout", "upstream_backlog_full") or "upstream" in msg.lower():
            self._note_beamcore_upstream_down(f"gateway error: {msg}")

    # =========================================================================
    # WebSocket Connection (Primary) + HTTP Polling (Fallback)
    # =========================================================================

    def _get_ws_url(self) -> str:
        """Get WebSocket URL from the orchestrator gateway base URL."""
        ws_url = self.ws_base_url.replace("https://", "wss://").replace("http://", "ws://")
        return f"{ws_url}/ws/orchestrators/{self.orchestrator_hotkey}"

    def _sign_ws_auth(self) -> tuple[str, str]:
        """Generate WebSocket authentication signature."""
        timestamp = str(int(time.time()))
        message = f"{self.orchestrator_hotkey}:{timestamp}"
        signature = ""
        if self.signer:
            try:
                sig_bytes = self.signer.sign(message.encode("utf-8"))
                signature = "0x" + (
                    sig_bytes.hex() if isinstance(sig_bytes, bytes) else str(sig_bytes)
                )
            except Exception as e:
                logger.warning(f"Failed to sign WebSocket auth: {e}")
                signature = "unsigned"
        else:
            signature = "unsigned"
        return signature, timestamp

    async def _ensure_api_key(self) -> Optional[str]:
        """
        Ensure we have a valid API key for WebSocket authentication.

        First checks BEAMCORE_API_KEY env var, then uses challenge/verify flow.
        The key is cached and reused until it expires.

        Returns:
            API key string (b1m_xxx format) or None if auth fails
        """
        import os

        # Check if we have a valid cached key
        if self._api_key and self._api_key_expires:
            if time.time() < self._api_key_expires - 60:  # 1 min buffer
                return self._api_key

        # Check for API key in environment variable
        env_api_key = os.environ.get("BEAMCORE_API_KEY")
        if (
            env_api_key
            and not self._skip_env_key
            and (env_api_key.startswith("b1m_") or env_api_key.startswith("bck_"))
        ):
            self._api_key = env_api_key
            self._api_key_expires = time.time() + 86400 * 365  # Never expires
            logger.info(
                f"Using BEAMCORE_API_KEY from environment for {self.orchestrator_hotkey[:16]}..."
            )
            return self._api_key

        if not self.signer:
            logger.error("Cannot get API key: no signer configured and BEAMCORE_API_KEY not set")
            return None

        client = await self._get_client()

        try:
            # Step 1: Request challenge
            challenge_resp = await client.post(
                f"{self.base_url}/auth/challenge",
                json={
                    "hotkey": self.orchestrator_hotkey,
                    "role": "orchestrator",
                },
            )

            if challenge_resp.status_code != 200:
                logger.error(f"Failed to get auth challenge: {challenge_resp.status_code}")
                return None

            challenge_data = challenge_resp.json()
            challenge_id = challenge_data["challenge_id"]
            message = challenge_data["message"]

            # Step 2: Sign the challenge message
            try:
                sig_bytes = self.signer.sign(message.encode("utf-8"))
                signature = "0x" + (
                    sig_bytes.hex() if isinstance(sig_bytes, bytes) else str(sig_bytes)
                )
            except Exception as e:
                logger.error(f"Failed to sign challenge: {e}")
                return None

            # Step 3: Verify signature and get API key
            verify_resp = await client.post(
                f"{self.base_url}/auth/verify",
                json={
                    "challenge_id": challenge_id,
                    "hotkey": self.orchestrator_hotkey,
                    "signature": signature,
                    "role": "orchestrator",
                    "key_name": "Orchestrator WebSocket Key",
                },
            )

            if verify_resp.status_code == 409:
                logger.error(
                    "API key already exists for this orchestrator. "
                    "Set BEAMCORE_API_KEY env var with your existing key, or revoke the old key first."
                )
                return None

            if verify_resp.status_code != 200:
                logger.error(
                    f"Failed to verify signature: {verify_resp.status_code} - {verify_resp.text}"
                )
                return None

            verify_data = verify_resp.json()

            if not verify_data.get("success") or not verify_data.get("api_key"):
                logger.error(f"Auth verify failed: {verify_data.get('message', 'Unknown error')}")
                return None

            self._api_key = verify_data["api_key"]
            self._api_key_expires = time.time() + 86400
            self._skip_env_key = False

            logger.info(f"Obtained API key for orchestrator {self.orchestrator_hotkey[:16]}...")
            logger.info(f"Save this key as BEAMCORE_API_KEY={self._api_key}")
            return self._api_key

        except Exception as e:
            logger.error(f"Failed to get API key: {e}")
            return None

    async def start_polling(self):
        """
        Start WebSocket connection for real-time notifications.

        BeamCore pushes transfers (`transfer_assigned`) and task results
        (`task_result_summary`) over the orchestrator WebSocket — there is no HTTP
        polling fallback.
        """
        if self._running:
            logger.warning("Already running")
            return

        self._running = True

        self._ws_task = asyncio.create_task(self._ws_connection_loop())

        logger.info(
            f"Started WebSocket connection to {self._get_ws_url()} with transport keepalive"
        )

    async def stop_polling(self):
        """Stop WebSocket connection."""
        self._running = False
        self._ws_connected = False

        # Close WebSocket
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        # Cancel tasks
        for task in [self._ws_task, self._registration_retry_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._ws_task = None
        self._registration_retry_task = None
        logger.info("WebSocket connection stopped")

    async def _ws_connection_loop(self):
        """Maintain WebSocket connection with automatic reconnection."""
        reconnect_delay = self._reconnect_delay

        while self._running:
            try:
                await self._connect_websocket()
                reconnect_delay = self._reconnect_delay  # Reset on successful connection
                await self._ws_message_loop()
            except ConnectionClosed as e:
                self._log_websocket_closed(e)
            except Exception as e:
                logger.error(f"WebSocket error: {e}")

            self._ws_connected = False
            self._ws = None
            if self._registration_retry_task and not self._registration_retry_task.done():
                self._registration_retry_task.cancel()
            self._registration_retry_task = None

            if self._running:
                logger.info(f"Reconnecting WebSocket in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 1.5, self._max_reconnect_delay)

    def set_registration_config(
        self,
        url: str,
        region: str,
        max_workers: int = 10000,
        uid: int = None,
        fee_percentage: float = 0.0,
        gateway_url: Optional[str] = None,
    ):
        """
        Set registration config for auto-registration after WebSocket connects.

        This should be called before start_polling() so the orchestrator
        registers via WebSocket immediately after connection.

        Args:
            url: Orchestrator's API URL (e.g., http://ip:port)
            region: Geographic region
            max_workers: Maximum workers this orchestrator can handle
            uid: Bittensor UID (optional)
            fee_percentage: Fee percentage charged to workers
            gateway_url: Public URL of this orchestrator's worker gateway, if externally managed
        """
        self._registration_config = {
            "url": url,
            "region": region,
            "max_workers": max_workers,
            "uid": uid,
            "fee_percentage": fee_percentage,
            "gateway_url": gateway_url,
        }
        logger.info(f"Registration config set: region={region}, max_workers={max_workers}")

    def _log_websocket_closed(self, closed: ConnectionClosed) -> None:
        """Log orch-gateway close codes with operator-facing context."""
        code = closed.rcvd.code if closed.rcvd else None
        if code == 4001:
            logger.warning(
                "Orch-gateway closed the WebSocket with code 4001 (unauthorized) for hotkey %s. "
                "Use an active orchestrator-role API key that belongs to this hotkey. "
                "Typical causes: BEAMCORE_API_KEY is a worker or client key, the hotkey was first "
                "registered as a worker in Beam, or the key does not match the wallet in the URL. "
                "Obtain a key via POST /auth/challenge and POST /auth/verify with role orchestrator, "
                "then set BEAMCORE_API_KEY. Detail: %s",
                self.orchestrator_hotkey,
                closed,
            )
            self._api_key = None
            self._api_key_expires = None
            self._skip_env_key = True
            return

        logger.warning("WebSocket closed: %s", closed)
        if code == 1008:
            self._api_key = None
            self._api_key_expires = None
            self._skip_env_key = True

    async def _connect_websocket(self):
        """Connect to WebSocket endpoint."""
        # Get API key for authentication (required by buffer service)
        api_key = await self._ensure_api_key()
        if not api_key:
            logger.error("Failed to obtain API key for WebSocket connection")
            raise ConnectionError("Cannot connect without API key")

        signature, timestamp = self._sign_ws_auth()
        url = self._get_ws_url()

        headers = {
            "x-api-key": api_key,
            "x-signature": signature,
            "x-timestamp": timestamp,
        }

        logger.info(
            "Connecting to WebSocket: %s (open_timeout=%ss ping_interval=%ss ping_timeout=%ss)",
            url,
            self._ws_open_timeout,
            self._ws_ping_interval,
            self._ws_ping_timeout,
        )
        self._ws = await websockets.connect(
            url,
            additional_headers=headers,
            open_timeout=self._ws_open_timeout,
            close_timeout=self._ws_close_timeout,
            ping_interval=self._ws_ping_interval,
            ping_timeout=self._ws_ping_timeout,
        )
        self._ws_connected = True
        self._registered = False  # Reset on new connection
        self._last_confirmed_ready = None
        logger.info(
            "WebSocket transport open to %s — orch-gateway authorizes X-Api-Key after the handshake",
            url,
        )

        # Auto-register if config is set
        if self._registration_config:
            await self.register_via_websocket(
                url=self._registration_config["url"],
                region=self._registration_config["region"],
                max_workers=self._registration_config["max_workers"],
                uid=self._registration_config["uid"],
                fee_percentage=self._registration_config["fee_percentage"],
                gateway_url=self._registration_config.get("gateway_url"),
            )
            self._schedule_registration_retry_if_needed()
            # Ready sync runs from register_ack (after core persists the row). Scheduling here raced set_ready ahead
            # of register_ack and widened BeamCore←DB inconsistencies.

    def _schedule_registration_retry_if_needed(self) -> None:
        if (
            not self._running
            or not self._ws_connected
            or self._registered
            or not self._registration_config
        ):
            return
        if self._registration_retry_task and not self._registration_retry_task.done():
            return

        self._registration_retry_task = asyncio.create_task(self._registration_retry_loop())

    async def _registration_retry_loop(self) -> None:
        try:
            await asyncio.sleep(self._registration_retry_interval)
            while (
                self._running
                and self._ws_connected
                and not self._registered
                and self._registration_config
            ):
                logger.warning(
                    "Registration ack not received yet; resending websocket registration for %s",
                    self.orchestrator_hotkey,
                )
                await self.register_via_websocket(
                    url=self._registration_config["url"],
                    region=self._registration_config["region"],
                    max_workers=self._registration_config["max_workers"],
                    uid=self._registration_config["uid"],
                    fee_percentage=self._registration_config["fee_percentage"],
                    gateway_url=self._registration_config.get("gateway_url"),
                )
                await asyncio.sleep(self._registration_retry_interval)
        except asyncio.CancelledError:
            raise
        finally:
            self._registration_retry_task = None

    async def _ws_message_loop(self):
        """Process incoming WebSocket messages."""
        while self._running and self._ws:
            try:
                message = await self._ws.recv()
                data = json.loads(message)
                await self._handle_ws_message(data)
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON from WebSocket: {e}")

    async def _handle_ws_message(self, data: dict):
        """Handle incoming WebSocket message."""
        msg_type = data.get("type")
        request_id = data.get("request_id")

        if request_id:
            fut = self._pending_ws_requests.pop(request_id, None)
            if fut and not fut.done():
                fut.set_result(data)
                return

        if msg_type == "connected":
            logger.info(
                "WebSocket connected: hotkey=%s",
                data.get("hotkey") or data.get("buffer_id") or "unknown",
            )

        elif msg_type == "task_result_summary":
            self._note_beamcore_upstream_recovered("task_result_summary from BeamCore")
            asyncio.create_task(self._handle_task_result(data))

        elif msg_type == "upstream_down":
            detail = data.get("message") or "orch-gateway lost BeamCore upstream WebSocket"
            self._note_beamcore_upstream_down(detail)

        elif msg_type == "upstream_ok":
            detail = data.get("message") or "BeamCore upstream relay connected"
            self._note_beamcore_upstream_recovered(detail)

        elif msg_type == "worker_update":
            # Worker connect/disconnect push event — must not block the recv loop;
            # replies for list_workers / control-plane requests are dispatched here too.
            worker_id = data.get("worker_id")
            event = data.get("event")
            logger.debug(f"Worker update: {worker_id} - {event}")
            if self._worker_update_handler and worker_id and event:

                async def _run_worker_update(wid: str, ev: str, handler: Any) -> None:
                    try:
                        res = handler(wid, ev)
                        if inspect.isawaitable(res):
                            await res
                    except Exception as exc:
                        logger.error("Error handling worker_update: %s", exc)

                asyncio.create_task(
                    _run_worker_update(worker_id, event, self._worker_update_handler)
                )

        elif msg_type == "transfer_assigned":
            self._note_beamcore_upstream_recovered("transfer_assigned from BeamCore")
            asyncio.create_task(self._handle_transfer_assigned(data))

        # ----- DEDICATED mode relay (only meaningful when a gateway is attached) -----
        elif msg_type == "worker_task_offer":
            asyncio.create_task(self._handle_worker_task_offer(data))

        elif msg_type == "worker_response_ack":
            fut = self._pending_response_acks.pop(data.get("offer_id"), None)
            if fut and not fut.done():
                fut.set_result(data)

        elif msg_type == "task_result_summary_ack":
            fut = self._pending_result_acks.pop(data.get("offer_id"), None)
            if fut and not fut.done():
                fut.set_result(data)

        elif msg_type == "chunks_queued":
            self._note_beamcore_upstream_recovered("chunks_queued from BeamCore path")
            logger.info(
                f"Chunks queued: assignment={data.get('assignment_id')} count={data.get('task_count')}"
            )

        elif msg_type == "register_ack":
            logger.info(f"Registration acknowledged: {data.get('status')}")
            self._registered = True
            if self._registration_retry_task and not self._registration_retry_task.done():
                self._registration_retry_task.cancel()
            self._schedule_ready_sync_if_needed()

        elif msg_type == "register_result":
            status = data.get("status")
            slot = data.get("slot_number")
            logger.info(f"Registration result: status={status}, slot={slot}")
            self._registered = status in ("assigned", "updated")
            if (
                self._registered
                and self._registration_retry_task
                and not self._registration_retry_task.done()
            ):
                self._registration_retry_task.cancel()
            self._schedule_ready_sync_if_needed()

        elif msg_type == "register_error":
            logger.error(f"Registration failed: {data.get('error') or data.get('message')}")
            self._registered = False
            self._schedule_registration_retry_if_needed()

        elif msg_type == "ping":
            # Respond to server ping
            if self._ws:
                await self._ws.send(json.dumps({"type": "pong"}))

        elif msg_type == "error":
            if data.get("code") == "unauthorized":
                logger.warning(
                    "Orch-gateway authorization rejected hotkey %s: reason=%s message=%s",
                    data.get("hotkey") or self.orchestrator_hotkey,
                    data.get("reason"),
                    data.get("message"),
                )
            else:
                self._maybe_upstream_error_payload(data)

        else:
            logger.debug(f"Unknown WebSocket message type: {msg_type}")

    async def _handle_task_result(self, data: dict[str, Any]) -> None:
        """Process task results off the receive loop so WS request/reply stays live."""
        logger.info(f"Received task result: {data.get('task_id')}")
        if not self._task_completion_handler:
            return

        try:
            verified = await self._task_completion_handler(data)
            if not verified:
                return

            task_id = data.get("task_id")
            if task_id:
                await self.acknowledge_task_completions([task_id])
        except Exception as e:
            logger.error(f"Error handling task result: {e}")

    async def _send_ws_request(
        self, message: dict[str, Any], timeout: float = 10.0
    ) -> dict[str, Any]:
        """Send a request over the orchestrator gateway WS and await the correlated reply."""
        if not self._ws or not self._ws_connected:
            raise RuntimeError("orchestrator websocket is not connected")

        request_id = message.get("request_id") or uuid.uuid4().hex
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending_ws_requests[request_id] = future

        try:
            await self._ws.send(json.dumps({**message, "request_id": request_id}))
            response = await asyncio.wait_for(future, timeout=timeout)
        except Exception:
            self._pending_ws_requests.pop(request_id, None)
            raise

        if response.get("type") == "error":
            self._maybe_upstream_error_payload(response)
            raise RuntimeError(
                response.get("message") or response.get("error") or "gateway request failed"
            )

        return response

    async def _handle_transfer_assigned(self, data: dict) -> None:
        assignment_id = data.get("assignment_id")
        transfer_id = data.get("transfer_id")
        chunk_start = int(data.get("chunk_start", 0))
        chunk_end = int(data.get("chunk_end", 0))
        request_id = assignment_id or transfer_id

        logger.info(f"transfer_assigned: transfer={transfer_id} chunks={chunk_start}-{chunk_end}")

        try:
            if not self._ws:
                logger.error(f"No WS connection for transfer_assigned {transfer_id}")
                return

            if self._dedicated_gateway is not None:
                # DEDICATED: assign only to OUR connected workers (BeamCore returns an
                # empty worker_list for dedicated orchestrators). No list_public_workers.
                worker_ids = self._dedicated_gateway.connected_worker_ids()
                if not worker_ids:
                    logger.warning(
                        "dedicated: no workers connected to this orchestrator for "
                        "assignment %s (transfer %s)", assignment_id, transfer_id)
                    return
                logger.info("dedicated: assigning chunks %d-%d across %d local worker(s)",
                            chunk_start, chunk_end, len(worker_ids))
            else:
                try:
                    response = await self._send_ws_request(
                        {
                            "type": "list_public_workers",
                            "transfer_id": transfer_id,
                            "request_id": request_id,
                        },
                        timeout=max(30.0, float(self.timeout)),
                    )
                    workers = response.get("workers", [])
                except Exception as e:
                    logger.error(f"Failed to get worker list for transfer {transfer_id}: {e}")
                    return

                normalized_workers = _normalize_worker_list(workers, transfer_id)
                if not normalized_workers:
                    logger.warning(f"No compatible workers available for assignment {assignment_id}")
                    return

                def worker_score(worker: dict[str, Any]) -> float:
                    trust = worker["trust_score"]
                    bandwidth = worker["bandwidth_mbps"]
                    return trust * min(2.0, bandwidth / 100.0)

                sorted_workers = sorted(normalized_workers, key=worker_score, reverse=True)
                worker_ids = [worker["worker_id"] for worker in sorted_workers]

            assignments = [
                {"chunk_index": i, "worker_id": worker_ids[i % len(worker_ids)]}
                for i in range(chunk_start, chunk_end + 1)
            ]

            max_attempts = 5
            for attempt in range(1, max_attempts + 1):
                try:
                    response = await self._send_ws_request(
                        {
                            "type": "chunk_assignments",
                            "assignment_id": assignment_id,
                            "assignments": assignments,
                        },
                        timeout=max(30.0, float(self.timeout)),
                    )
                    task_count = int(response.get("task_count") or 0)
                    if response.get("type") != "chunks_queued":
                        raise RuntimeError(f"unexpected chunk assignment ack: {response}")
                    if task_count <= 0:
                        logger.warning(
                            "Chunk assignment ack reported zero newly queued tasks for "
                            "assignment %s; tasks may already be active from an earlier submit",
                            assignment_id,
                        )
                    elif task_count < len(assignments):
                        logger.warning(
                            "Chunk assignment ack queued fewer tasks than chunks: "
                            "assignment=%s chunks=%s tasks=%s",
                            assignment_id,
                            len(assignments),
                            task_count,
                        )
                    logger.info(
                        "Queued %s worker tasks from %s chunk_assignments for assignment %s",
                        task_count,
                        len(assignments),
                        assignment_id,
                    )
                    return
                except Exception as e:
                    if attempt >= max_attempts:
                        logger.error(
                            "Failed to queue chunk_assignments for assignment %s after %s attempts: %s",
                            assignment_id,
                            max_attempts,
                            e,
                        )
                        return
                    delay = min(30.0, 2.0 * attempt)
                    logger.warning(
                        "Failed to queue chunk_assignments for assignment %s "
                        "(attempt %s/%s): %s; retrying in %.1fs",
                        assignment_id,
                        attempt,
                        max_attempts,
                        e,
                        delay,
                    )
                    await asyncio.sleep(delay)
        except Exception:
            logger.exception(
                "Failed to process transfer_assigned for transfer %s assignment %s",
                transfer_id,
                assignment_id,
            )

    # =========================================================================
    # DEDICATED worker-gateway relay (orch <-> BeamCore side of the dedicated path)
    # =========================================================================
    def set_dedicated_gateway(self, gateway) -> None:
        """Attach the in-process worker gateway -> enables the dedicated assignment path."""
        self._dedicated_gateway = gateway
        logger.info("dedicated worker gateway attached; assignment uses local worker pool")

    async def _handle_worker_task_offer(self, data: dict) -> None:
        """BeamCore -> orch: route a per-chunk offer to the target worker as a task_offer."""
        if self._dedicated_gateway is None:
            logger.debug("worker_task_offer received but no dedicated gateway attached")
            return
        worker_id = data.get("worker_id")
        if not worker_id:
            logger.warning("worker_task_offer missing worker_id: %s", data)
            return
        offer = {k: v for k, v in data.items() if k not in ("type", "worker_id")}
        await self._dedicated_gateway.forward_offer(worker_id, offer)

    async def relay_worker_response(
        self, task_id, offer_id, worker_id, decision, reason=None
    ) -> Dict[str, Any]:
        """Relay a worker accept/reject to BeamCore; await worker_response_ack (by offer_id)."""
        if not self._ws:
            return {"accepted": False, "reason": "no_ws"}
        msg = {
            "type": "worker_response",
            "task_id": task_id, "offer_id": offer_id, "worker_id": worker_id,
            "decision": decision,
        }
        if reason:
            msg["reason"] = reason
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending_response_acks[offer_id] = fut
        try:
            await self._ws.send(json.dumps(msg))
            return await asyncio.wait_for(fut, timeout=5.0)
        except asyncio.TimeoutError:
            return {"accepted": True, "reason": "ack_timeout"}  # optimistic; worker proceeds
        except Exception as e:
            return {"accepted": True, "reason": f"relay_err:{e}"}
        finally:
            self._pending_response_acks.pop(offer_id, None)

    async def relay_task_result_summary(self, summary: dict) -> Dict[str, Any]:
        """Relay a worker's task_result_summary to BeamCore; await task_result_summary_ack."""
        offer_id = summary.get("offer_id")
        if not self._ws:
            return {"received": False, "completed": False, "reason": "no_ws"}
        msg = {
            "type": "task_result_summary",
            "task_id": summary.get("task_id"),
            "offer_id": offer_id,
            "worker_id": summary.get("worker_id"),
            "success": summary.get("success"),
            "bytes_transferred": summary.get("bytes_transferred"),
            "bandwidth_mbps": summary.get("bandwidth_mbps"),
            "chunk_hash": summary.get("chunk_hash"),
            "etag": summary.get("etag"),
        }
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending_result_acks[offer_id] = fut
        try:
            await self._ws.send(json.dumps(msg))
            # verification can take a moment; wait longer than the response ack
            return await asyncio.wait_for(fut, timeout=30.0)
        except asyncio.TimeoutError:
            return {"received": True, "completed": False, "reason": "ack_timeout"}
        except Exception as e:
            return {"received": True, "completed": False, "reason": f"relay_err:{e}"}
        finally:
            self._pending_result_acks.pop(offer_id, None)

    def _schedule_ready_sync_if_needed(self) -> None:
        if not self._running or not self._ws_connected:
            return
        if self._last_confirmed_ready == self._desired_ready:
            return
        if self._ready_sync_task and not self._ready_sync_task.done():
            return

        self._ready_sync_task = asyncio.create_task(self._sync_ready_state_in_background())

    async def _sync_ready_state_in_background(self) -> None:
        try:
            while (
                self._running
                and self._ws_connected
                and self._last_confirmed_ready != self._desired_ready
            ):
                try:
                    applied = await self._apply_desired_ready_state()
                    if applied:
                        return
                except Exception as exc:
                    logger.warning(
                        "Failed to sync queued ready=%s through orch-gateway: %s",
                        self._desired_ready,
                        exc,
                    )
                await asyncio.sleep(self._ready_sync_retry_interval)
        except asyncio.CancelledError:
            raise
        finally:
            self._ready_sync_task = None

    async def _apply_desired_ready_state(self) -> bool:
        requested_ready = self._desired_ready
        response = await self._send_ws_request({"type": "set_ready", "ready": requested_ready})
        confirmed = bool(response.get("ready", requested_ready))
        self._desired_ready = confirmed
        self._last_confirmed_ready = confirmed
        applied = confirmed == requested_ready
        logger.info(
            f"Orchestrator ready={confirmed} set on BeamCore " f"(uid={response.get('uid')})"
        )
        return applied

    async def register_via_websocket(
        self,
        url: str,
        region: str,
        max_workers: int = 10000,
        uid: int = None,
        fee_percentage: float = 0.0,
        gateway_url: Optional[str] = None,
    ) -> bool:
        """
        Register orchestrator via WebSocket.

        Sends a register message over the WebSocket connection instead of HTTP POST.
        The signature proves ownership of the hotkey.

        Args:
            url: Orchestrator's API URL (e.g., http://ip:port)
            region: Geographic region
            max_workers: Maximum workers this orchestrator can handle
            uid: Bittensor UID (optional)
            fee_percentage: Fee percentage charged to workers
            gateway_url: Public URL of this orchestrator's worker gateway, if externally managed

        Returns:
            True if registration message was sent successfully
        """
        if not self._ws or not self._ws_connected:
            logger.warning("Cannot register via WebSocket: not connected")
            return False

        # Sign registration data: "{hotkey}:{url}:{region}"
        reg_message = f"{self.orchestrator_hotkey}:{url}:{region}"
        signature = ""
        if self.signer:
            try:
                sig_bytes = self.signer.sign(reg_message.encode())
                signature = "0x" + (
                    sig_bytes.hex() if isinstance(sig_bytes, bytes) else str(sig_bytes)
                )
            except Exception as e:
                logger.warning(f"Failed to sign registration: {e}")

        message = {
            "type": "register",
            "url": url,
            "region": region,
            "max_workers": max_workers,
            "uid": uid,
            "fee_percentage": fee_percentage,
            "ready": self._desired_ready,
            "signature": signature,
        }
        if gateway_url:
            message["gateway_url"] = gateway_url

        try:
            await self._ws.send(json.dumps(message))
            logger.info(
                "Sent registration via WebSocket for %s (orch-gateway relays it only after "
                "orchestrator API key authorization): region=%s, fee=%s%%, desired_ready=%s",
                self.orchestrator_hotkey,
                region,
                fee_percentage,
                self._desired_ready,
            )
            return True
        except Exception as e:
            logger.error(f"Failed to send registration via WebSocket: {e}")
            return False

    async def update_worker_gateway(
        self, gateway_url: str, max_workers: int = 10000, health: str = "healthy"
    ) -> Dict[str, Any]:
        """Publish an externally managed orchestrator-owned worker gateway URL."""
        return await self._send_ws_request(
            {
                "type": "gateway_update",
                "gateway_url": gateway_url,
                "max_workers": max_workers,
                "health": health,
            }
        )

    async def set_ready(self, ready: bool) -> bool:
        """
        Toggle this orchestrator's readiness to receive transfers through the relay.
        """
        self._desired_ready = ready
        if not self._ws_connected:
            logger.info(
                "Queued ready=%s until orch-gateway websocket is connected",
                ready,
            )
            return False
        try:
            return await self._apply_desired_ready_state()
        except Exception as exc:
            self._schedule_ready_sync_if_needed()
            logger.info(
                "Queued ready=%s after transient orch-gateway sync failure: %s",
                ready,
                exc,
            )
            return False

    async def acknowledge_task_completions(
        self,
        task_ids: List[str],
        verified: bool = True,
    ) -> Dict[str, Any]:
        """
        Acknowledge task completions to SubnetCore.

        This records task completion state for BeamCore and operator workflows.

        Args:
            task_ids: List of task IDs to acknowledge
            verified: Whether the orchestrator verified the completions

        Returns:
            Acknowledgment result with counts
        """
        return await self._send_ws_request(
            {
                "type": "acknowledge_tasks",
                "task_ids": task_ids,
                "verified": verified,
            }
        )

    # =========================================================================
    # HTTP Auth & Client
    # =========================================================================

    def _auth_headers(self) -> dict:
        """Build fresh auth headers with current timestamp, nonce, and signature."""
        timestamp = str(int(time.time()))
        nonce = uuid.uuid4().hex[:8]
        action = "request"

        # Build canonical message matching Core API's expected format:
        # "{type}_auth:{hotkey}:{timestamp}:{action}:{nonce}"
        message = f"orchestrator_auth:{self.orchestrator_hotkey}:{timestamp}:{action}:{nonce}"

        # Sign with wallet if available, otherwise use placeholder
        signature = ""
        if self.signer:
            try:
                sig_bytes = self.signer.sign(message.encode("utf-8"))
                signature = sig_bytes.hex() if isinstance(sig_bytes, bytes) else str(sig_bytes)
            except Exception as e:
                logger.warning(f"Failed to sign auth message: {e}")
                signature = "unsigned"
        else:
            signature = "unsigned"

        headers = {
            "X-Hotkey": self.orchestrator_hotkey,  # Required for rate limiting
            "X-Orchestrator-Hotkey": self.orchestrator_hotkey,
            "X-Orchestrator-Uid": str(self.orchestrator_uid),
            "X-Orchestrator-Timestamp": timestamp,
            "X-Orchestrator-Nonce": nonce,
            "X-Orchestrator-Signature": signature,
            "X-Orchestrator-Action": action,
        }

        # Include API key if available (preferred auth method)
        if self._api_key:
            headers["X-Api-Key"] = self._api_key

        return headers

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client with auth headers injected per-request."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                event_hooks={
                    "request": [self._inject_auth_headers],
                },
            )
        return self._client

    async def _inject_auth_headers(self, request: httpx.Request):
        """Inject fresh auth headers into every outgoing request."""
        # Skip API key fetch for auth endpoints (they're public)
        if "/auth/challenge" not in str(request.url) and "/auth/verify" not in str(request.url):
            # Ensure we have an API key for protected endpoints
            if not self._api_key:
                await self._ensure_api_key()

        headers = self._auth_headers()
        for key, value in headers.items():
            request.headers[key] = value

    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    # =========================================================================
    # Worker Management
    # =========================================================================

    async def list_public_workers(
        self,
        status: Optional[str] = None,
        region: Optional[str] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """List workers on the public worker gateway eligible for this orchestrator."""
        payload: dict[str, Any] = {"type": "list_public_workers", "limit": limit}
        if status:
            payload["status"] = status
        if region:
            payload["region"] = region
        return await self._send_ws_request(payload)

    async def get_worker(self, worker_id: str) -> Dict[str, Any]:
        """Get a specific worker.

        BeamCore exposes the worker globally at GET /workers/{worker_id}.
        The
        legacy /orchestrators/workers/{id} affiliation-scoped route was
        removed.
        """
        return await self._send_ws_request({"type": "get_worker", "worker_id": worker_id})

    async def get_worker_hotkey(self, worker_id: str) -> Optional[str]:
        """
        Resolve a worker_id to its hotkey regardless of affiliation.

        Uses the unscoped /workers/{id}/hotkey endpoint — unlike get_worker()
        this works for workers completing tasks on behalf of other orchestrators
        (e.g. speculative/recovery tasks assigned cross-orchestrator).
        """
        try:
            data = await self._send_ws_request(
                {"type": "get_worker_hotkey", "worker_id": worker_id}
            )
            return data.get("hotkey")
        except Exception as e:
            logger.debug(f"Hotkey lookup failed for {worker_id[:16]}...: {e}")
            return None


# =============================================================================
# Global Client Instance
# =============================================================================

_client: Optional[SubnetCoreClient] = None


def get_subnet_core_client() -> Optional[SubnetCoreClient]:
    """Get the global SubnetCoreClient instance."""
    return _client


def init_subnet_core_client(
    base_url: str,
    ws_base_url: str,
    orchestrator_hotkey: str,
    orchestrator_uid: int,
    timeout: float = 30.0,
    signer=None,
    *,
    ws_open_timeout: float = 60.0,
    ws_close_timeout: float = 20.0,
    ws_ping_interval: float = 30.0,
    ws_ping_timeout: float = 45.0,
) -> SubnetCoreClient:
    """
    Initialize the global SubnetCoreClient instance.

    Args:
        base_url: Base URL of BeamCore
        ws_base_url: Required base URL of the orchestrator gateway WebSocket edge
        orchestrator_hotkey: This orchestrator's hotkey
        orchestrator_uid: This orchestrator's UID
        timeout: Request timeout
        signer: Optional bittensor wallet hotkey with .sign() method

    Returns:
        The initialized client
    """
    global _client
    _client = SubnetCoreClient(
        base_url,
        ws_base_url,
        orchestrator_hotkey,
        orchestrator_uid,
        timeout,
        signer=signer,
        ws_open_timeout=ws_open_timeout,
        ws_close_timeout=ws_close_timeout,
        ws_ping_interval=ws_ping_interval,
        ws_ping_timeout=ws_ping_timeout,
    )
    logger.info(
        "SubnetCoreClient initialized: http=%s ws=%s (signer=%s)",
        base_url,
        ws_base_url,
        "yes" if signer else "none",
    )
    return _client


async def close_subnet_core_client():
    """Close the global client."""
    global _client
    if _client:
        await _client.close()
        _client = None
