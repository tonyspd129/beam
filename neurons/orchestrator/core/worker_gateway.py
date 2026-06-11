"""
In-process DEDICATED worker gateway.

Lets your own workers connect to THIS orchestrator (wss://your-host/ws/{worker_id})
instead of the shared BeamCore public gateway, so your orchestrator dispatches only
to your workers and your workers serve only you.

The worker speaks the SAME data-path protocol it uses with the public gateway, so it
needs no code change — only point its WORKER_GATEWAY_URL at this orchestrator. This
module is the relay BeamCore's docs say the orchestrator must implement on a dedicated
deployment:

  worker  --task_accept/reject-->  gateway  --worker_response-->        BeamCore (orch WS)
  worker  --task_result_summary--> gateway  --task_result_summary-->    BeamCore (orch WS)
  BeamCore --worker_task_offer-->  gateway  --task_offer-->             worker

Workers still register with BeamCore over HTTP and POST payment-evidence (PoB) to
BeamCore directly — the gateway is NOT on the payment path.

Built to the documented dedicated protocol (data.b1m.ai/guide/orchestrators,
/guide/ws-protocol). The ack shapes from BeamCore are best-effort and degrade
gracefully (timeouts + optimistic acks) — validate/tune live. See deploy/DEDICATED.md.
"""
import asyncio
import hmac
import json
import logging
import time
from typing import Any, Dict, Optional, Set

logger = logging.getLogger(__name__)

_MAX_MSG_BYTES = 1 << 20   # 1 MiB inbound frame cap (control messages are tiny)
_IDLE_TIMEOUT_S = 120      # close a connection that goes silent this long


def _parse_allowlist(settings) -> Set[str]:
    raw = getattr(settings, "worker_hotkey_allowlist", None)
    if not raw:
        return set()
    return {h.strip() for h in str(raw).split(",") if h.strip()}


class DedicatedWorkerGateway:
    """Worker-facing WS endpoint + relay to BeamCore. One instance per orchestrator."""

    def __init__(self, get_client, settings, worker_manager=None):
        self._get_client = get_client            # () -> SubnetCoreClient | None
        self.settings = settings
        self._wm = worker_manager
        self._conns: Dict[str, Any] = {}         # worker_id -> starlette WebSocket
        self._meta: Dict[str, dict] = {}         # worker_id -> {hotkey, capacity, last_seen}
        self._ip_counts: Dict[str, int] = {}     # client_ip -> live connection count
        self._inflight: set = set()              # in-flight _on_worker_msg tasks (anti-GC)
        self.allowlist: Set[str] = _parse_allowlist(settings)
        # pre-shared secret: only workers presenting it may connect (you own both ends)
        self.secret: Optional[str] = getattr(settings, "worker_gateway_secret", None) or None
        self.max_conn: int = int(getattr(settings, "worker_gateway_max_conn", 64) or 64)
        self.max_per_ip: int = int(getattr(settings, "worker_gateway_max_per_ip", 8) or 8)
        logger.info(
            "dedicated gateway: secret=%s allowlist=%d max_conn=%d max_per_ip=%d",
            "set" if self.secret else "OFF", len(self.allowlist), self.max_conn, self.max_per_ip)

    # ---- assignment surface (used by SubnetCoreClient._handle_transfer_assigned) ----
    def connected_worker_ids(self) -> list:
        """Worker UUIDs currently connected, ordered by capacity desc then id."""
        return sorted(
            self._conns.keys(),
            key=lambda wid: (-self._meta.get(wid, {}).get("capacity", 1), wid),
        )

    def stats(self) -> dict:
        return {
            "connected_workers": len(self._conns),
            "worker_ids": list(self._conns.keys()),
            "allowlisted": len(self.allowlist),
        }

    # ---- worker WS lifecycle ----
    @staticmethod
    def _client_ip(websocket) -> str:
        """Real client IP — honor X-Forwarded-For when behind a TLS reverse proxy."""
        try:
            xff = websocket.headers.get("x-forwarded-for")
            if xff:
                return xff.split(",")[0].strip()
            return websocket.client.host if websocket.client else "?"
        except Exception:
            return "?"

    def _check_secret(self, websocket) -> bool:
        """Constant-time PSK check (header preferred, query fallback). True if OK / no secret set."""
        if not self.secret:
            return True
        token = None
        try:
            token = websocket.headers.get("x-gateway-secret") or websocket.query_params.get("token")
        except Exception:
            token = None
        return bool(token) and hmac.compare_digest(str(token), self.secret)

    async def _authorize(self, worker_id: str) -> Optional[str]:
        """Secondary identity check (post-accept). Empty allowlist => allow (skips BeamCore lookup)."""
        if not self.allowlist:
            return worker_id
        client = self._get_client()
        hotkey = None
        if client is not None:
            try:
                info = await client.get_worker(worker_id)
                hotkey = (info or {}).get("hotkey") or (info or {}).get("worker_hotkey")
            except Exception as e:
                logger.debug("worker %s identity lookup failed: %s", worker_id, e)
        if hotkey not in self.allowlist:
            logger.warning("rejecting worker %s: hotkey %s not in allowlist", worker_id, hotkey)
            return None
        return hotkey

    async def handle_worker(self, websocket, worker_id: str) -> None:
        """FastAPI/starlette WebSocket handler for /ws/{worker_id}.

        Reject cheaply BEFORE accept() (no state, no BeamCore call) so a flood of bad
        connections can't exhaust the orchestrator. Real anti-DoS is the IP allowlist at
        your firewall/proxy (see deploy/DEDICATED.md); these are defense-in-depth.
        """
        client_ip = self._client_ip(websocket)

        # 1) PSK — wrong/missing secret is closed during the handshake (never accepted).
        if not self._check_secret(websocket):
            logger.warning("gateway: rejected worker %s from %s (bad/no secret)", worker_id, client_ip)
            await websocket.close(code=4401)
            return
        # 2) global connection cap
        if len(self._conns) >= self.max_conn:
            logger.warning("gateway: pool full (%d); rejected %s from %s", self.max_conn, worker_id, client_ip)
            await websocket.close(code=4429)
            return
        # 3) per-IP cap
        if self._ip_counts.get(client_ip, 0) >= self.max_per_ip:
            logger.warning("gateway: per-IP cap (%d) hit for %s; rejected %s", self.max_per_ip, client_ip, worker_id)
            await websocket.close(code=4429)
            return
        # 4) duplicate worker_id
        if worker_id in self._conns:
            logger.warning("gateway: worker %s already connected; rejecting duplicate", worker_id)
            await websocket.close(code=4409)
            return

        await websocket.accept()

        # 5) secondary identity (only reached by secret-authed clients; lookup can't be amplified)
        hotkey = await self._authorize(worker_id)
        if hotkey is None:
            await websocket.close(code=4403)
            return

        self._conns[worker_id] = websocket
        self._meta[worker_id] = {"hotkey": hotkey, "capacity": 1, "last_seen": time.time(), "ip": client_ip}
        self._ip_counts[client_ip] = self._ip_counts.get(client_ip, 0) + 1
        if self._wm is not None:
            try:
                self._wm.register_worker_connection(worker_id, websocket)
            except Exception:
                pass
        await self._send(worker_id, {"type": "connected", "worker_id": worker_id})
        logger.info("dedicated worker connected: %s (%s) from %s — pool=%d",
                    worker_id, str(hotkey)[:12], client_ip, len(self._conns))
        try:
            while True:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=_IDLE_TIMEOUT_S)
                if len(raw) > _MAX_MSG_BYTES:
                    logger.warning("gateway: oversized frame from %s (%d bytes); closing", worker_id, len(raw))
                    break
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                # dispatch off the read loop so a blocking upstream relay (accept/result)
                # can't freeze this worker's connection (keeps acks within the worker's window)
                _t = asyncio.create_task(self._on_worker_msg(worker_id, msg))
                self._inflight.add(_t)
                _t.add_done_callback(self._inflight.discard)
        except asyncio.TimeoutError:
            logger.info("dedicated worker %s idle %ds; closing", worker_id, _IDLE_TIMEOUT_S)
        except Exception as e:
            logger.info("dedicated worker %s disconnected: %s", worker_id, e)
        finally:
            self._conns.pop(worker_id, None)
            self._meta.pop(worker_id, None)
            n = self._ip_counts.get(client_ip, 0) - 1
            if n > 0:
                self._ip_counts[client_ip] = n
            else:
                self._ip_counts.pop(client_ip, None)
            if self._wm is not None:
                try:
                    self._wm.unregister_worker_connection(worker_id)
                except Exception:
                    pass

    async def _send(self, worker_id: str, payload: dict) -> bool:
        ws = self._conns.get(worker_id)
        if ws is None:
            return False
        try:
            await ws.send_text(json.dumps(payload))
            return True
        except Exception as e:
            logger.warning("send to worker %s failed: %s", worker_id, e)
            return False

    # ---- BeamCore -> worker: forward a task offer ----
    async def forward_offer(self, worker_id: str, offer: dict) -> bool:
        """Forward a worker_task_offer's body to the target worker as a task_offer."""
        msg = {**offer, "type": "task_offer"}
        ok = await self._send(worker_id, msg)
        if not ok:
            logger.warning("worker %s not connected; task_offer dropped (task=%s)",
                           worker_id, offer.get("task_id"))
        return ok

    # ---- worker -> BeamCore: relay accept/reject/result ----
    async def _on_worker_msg(self, worker_id: str, msg: dict) -> None:
        t = msg.get("type")
        m = self._meta.setdefault(worker_id, {})
        m["last_seen"] = time.time()
        client = self._get_client()
        if client is None:
            return

        if t in ("task_accept", "task_reject"):
            decision = "task_accept" if t == "task_accept" else "task_reject"
            try:
                ack = await client.relay_worker_response(
                    task_id=msg.get("task_id"), offer_id=msg.get("offer_id"),
                    worker_id=worker_id, decision=decision, reason=msg.get("reason"))
            except Exception as e:
                logger.warning("relay worker_response failed (worker %s): %s", worker_id, e)
                ack = {"accepted": True}
            if t == "task_accept":
                # worker waits up to 5s for this before executing
                await self._send(worker_id, {
                    "type": "task_accept_ack",
                    "task_id": msg.get("task_id"), "offer_id": msg.get("offer_id"),
                    "accepted": bool((ack or {}).get("accepted", True)),
                })

        elif t == "task_result_summary":
            try:
                ack = await client.relay_task_result_summary(msg)
            except Exception as e:
                logger.warning("relay task_result_summary failed (worker %s): %s", worker_id, e)
                ack = {"received": True, "completed": False, "reason": "relay_error"}
            # worker gates its payment-evidence POST (= the PoB) on completed=true
            await self._send(worker_id, {
                "type": "task_result_summary_ack",
                "task_id": msg.get("task_id"), "offer_id": msg.get("offer_id"),
                "received": bool((ack or {}).get("received", True)),
                "completed": bool((ack or {}).get("completed", False)),
                "reason": (ack or {}).get("reason"),
            })

        elif t == "capacity_update":
            try:
                m["capacity"] = int(msg.get("capacity", m.get("capacity", 1)))
            except Exception:
                pass

        elif t == "stats_snapshot":
            await self._send(worker_id, {"type": "stats_snapshot_ack"})

        elif t == "bw_challenge_response":
            pass  # liveness echo; nothing to do

        else:
            logger.debug("worker %s sent unhandled msg: %s", worker_id, t)
