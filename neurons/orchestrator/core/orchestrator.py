"""
BEAM Orchestrator Core

Central coordinator for the BEAM decentralized bandwidth network.
Facade that delegates to specialized manager classes.

Architecture:
┌─────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATOR                                  │
│                  (Subnet-operated, NOT a miner)                      │
│                                                                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │
│  │   Worker    │  │    Task     │  │    Proof    │                  │
│  │  Registry   │  │  Scheduler  │  │ Aggregator  │                  │
│  └─────────────┘  └─────────────┘  └─────────────┘                  │
│         │                │                │                          │
│         └────────────────┴────────────────┘                          │
│                          │                                           │
│  ┌───────────────────────┴───────────────────────┐                  │
│  │              Work Coordinator                  │                  │
│  └───────────────────────────────────────────────┘                  │
│                          │                                           │
└──────────────────────────┼──────────────────────────────────────────┘
                           │
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
    ┌──────────┐    ┌──────────┐    ┌──────────┐
    │ Worker 1 │    │ Worker 2 │    │ Worker N │  (Off-chain, unlimited)
    └──────────┘    └──────────┘    └──────────┘
           │               │               │
           └───────────────┴───────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │  Validators  │  (On-chain, ~64 UIDs)
                    │  (verify &   │
                    │  set weights)│
                    └──────────────┘

Key differences from Connection model:
1. Workers register directly with Orchestrator (no miner slot needed)
2. Orchestrator aggregates ALL work and reports to validators
3. Validators verify aggregated proofs and score the Orchestrator
4. Single emission distribution path vs multiple competing miners
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set

import bittensor as bt

from .config import OrchestratorSettings, get_settings
from .epoch_manager import EpochManager
from .proof_aggregator import ProofAggregator
from .reward_manager import RewardManager
from .task_scheduler import TaskScheduler
from .worker_manager import WorkerManager

# BlindWorkerManager removed
# GatewayManager removed


# SubnetCoreClient imports (BeamCore contract)
try:
    from clients import (
        SubnetCoreClient,
        close_subnet_core_client,
        init_subnet_core_client,
    )

    SUBNET_CORE_CLIENT_AVAILABLE = True
except ImportError:
    SUBNET_CORE_CLIENT_AVAILABLE = False
    SubnetCoreClient = None

logger = logging.getLogger(__name__)

SUBTENSOR_INIT_MAX_ATTEMPTS = 5
SUBTENSOR_INIT_BASE_DELAY_SECONDS = 2.0


# =============================================================================
# Data Models
# =============================================================================


class WorkerStatus(Enum):
    """Worker lifecycle status."""

    PENDING = "pending"  # Registered, awaiting verification
    ACTIVE = "active"  # Verified and accepting tasks
    SUSPENDED = "suspended"  # Temporarily disabled (failed checks)
    OFFLINE = "offline"  # Session or telemetry timeout
    BANNED = "banned"  # Permanently banned (fraud detected)


@dataclass
class Worker:
    """
    Registered worker in the Orchestrator network.

    Workers are NOT on-chain entities - they register directly with the
    Orchestrator and earn rewards through verified bandwidth work.
    """

    worker_id: str
    hotkey: str
    ip: str
    port: int
    region: str

    # Geographic coordinates (set during verification)
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Status
    status: WorkerStatus = WorkerStatus.PENDING
    registered_at: datetime = field(default_factory=datetime.utcnow)
    last_seen: datetime = field(default_factory=datetime.utcnow)

    # Performance metrics
    bandwidth_mbps: float = 0.0
    bandwidth_ema: float = 0.0
    latency_ms: float = 0.0
    success_rate: float = 1.0

    # Task tracking
    total_tasks: int = 0
    successful_tasks: int = 0
    failed_tasks: int = 0
    active_tasks: int = 0  # Local: tasks this orchestrator assigned
    global_pending_tasks: int = 0  # Global: tasks across ALL orchestrators (from BeamCore)
    max_concurrent_tasks: int = 10

    # Bytes relayed
    bytes_relayed_total: int = 0
    bytes_relayed_epoch: int = 0

    # Trust scoring
    trust_score: float = 0.5  # Starts at neutral
    fraud_score: float = 0.0  # Higher = more suspicious

    # Reward tracking
    rewards_earned_epoch: int = 0
    rewards_earned_total: int = 0

    @property
    def is_available(self) -> bool:
        """Check if worker can accept new tasks."""
        return self.status == WorkerStatus.ACTIVE and self.active_tasks < self.max_concurrent_tasks

    @property
    def load_factor(self) -> float:
        """Current load as fraction of capacity, using max of local and global counts."""
        if self.max_concurrent_tasks == 0:
            return 1.0
        # Use max of local active_tasks and global pending_tasks from BeamCore
        # This ensures we see tasks assigned by OTHER orchestrators too
        effective_tasks = max(self.active_tasks, self.global_pending_tasks)
        return effective_tasks / self.max_concurrent_tasks

    def update_bandwidth_ema(self, bandwidth: float, alpha: float = 0.3) -> None:
        """Update bandwidth EMA with new measurement."""
        if self.bandwidth_ema == 0:
            self.bandwidth_ema = bandwidth
        else:
            self.bandwidth_ema = alpha * bandwidth + (1 - alpha) * self.bandwidth_ema

    def update_success_rate(self) -> None:
        """Recalculate success rate."""
        if self.total_tasks > 0:
            self.success_rate = self.successful_tasks / self.total_tasks


@dataclass
class BandwidthTask:
    """A bandwidth relay task assigned to a worker."""

    task_id: str
    worker_id: str

    # Task details
    chunk_size: int
    chunk_hash: str
    source_region: str
    dest_region: str

    # Timing
    created_at: float  # Unix timestamp
    deadline_us: int  # Microsecond deadline
    started_at: Optional[float] = None
    completed_at: Optional[float] = None

    # Status
    status: str = "pending"  # pending, in_progress, completed, failed, timeout

    # Anti-cheat
    canary: bytes = field(default_factory=bytes)
    canary_offset: int = 0

    # Results
    bytes_relayed: int = 0
    bandwidth_mbps: float = 0.0
    latency_ms: float = 0.0


@dataclass
class PendingOffer:
    """
    Tracks a task offer broadcast to all workers.

    The broadcast offer model:
    1. Orchestrator broadcasts offer to ALL connected workers
    2. First worker to accept wins (atomic)
    3. Non-winners receive TASK_ASSIGNED notification
    4. If no one accepts within timeout, offer expires
    """

    offer_id: str
    task_id: str

    # Task preview info (sent to workers - no actual chunk data)
    chunk_size: int
    chunk_hash: str
    source_region: str
    dest_region: str
    estimated_reward: float = 0.0  # Estimated dTAO reward

    # The actual chunk data (only sent to winner)
    chunk_data: bytes = field(default_factory=bytes, repr=False)
    chunk_index: int = 0
    transfer_id: str = ""
    destination_url: Optional[str] = None
    sender_hotkey: Optional[str] = None
    filename: Optional[str] = None
    total_chunks: Optional[int] = None
    receiver_filename: Optional[str] = None

    # Timing
    created_at: float = field(default_factory=time.time)
    timeout_seconds: float = 5.0  # How long workers have to accept
    deadline_us: int = 0

    # Anti-cheat (for winner's task)
    canary: bytes = field(default_factory=bytes, repr=False)
    canary_offset: int = 0

    # State
    status: str = "pending"  # pending, accepted, expired, cancelled
    accepted_by: Optional[str] = None  # worker_id of winner
    accepted_at: Optional[float] = None

    # Track which workers received the offer
    workers_offered: Set[str] = field(default_factory=set)
    workers_rejected: Set[str] = field(default_factory=set)

    @property
    def is_expired(self) -> bool:
        """Check if offer has timed out."""
        return time.time() > (self.created_at + self.timeout_seconds)

    @property
    def is_available(self) -> bool:
        """Check if offer can still be accepted."""
        return self.status == "pending" and not self.is_expired


@dataclass
class BandwidthProof:
    """Proof of bandwidth work completed by a worker."""

    task_id: str
    worker_id: str
    worker_hotkey: str

    # Timing (microseconds)
    start_time_us: int
    end_time_us: int

    # Metrics
    bytes_relayed: int
    bandwidth_mbps: float

    # Verification
    chunk_hash: str
    canary_proof: str

    # Signatures
    worker_signature: str = ""
    orchestrator_signature: str = ""
    worker_coldkey: str = ""

    # Metadata
    source_region: str = ""
    dest_region: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)

    @property
    def duration_ms(self) -> float:
        """Task duration in milliseconds."""
        return (self.end_time_us - self.start_time_us) / 1000


@dataclass
class EpochSummary:
    """Aggregated work summary for a validation epoch."""

    epoch: int
    start_time: datetime
    end_time: datetime

    # Aggregated metrics
    total_tasks: int = 0
    successful_tasks: int = 0
    failed_tasks: int = 0

    total_bytes_relayed: int = 0
    total_bandwidth_seconds: float = 0.0

    # Worker participation
    active_workers: int = 0
    worker_contributions: Dict[str, int] = field(default_factory=dict)  # worker_id -> bytes

    # Proof aggregation
    proof_count: int = 0
    merkle_root: str = ""

    # Scoring inputs for validators
    avg_bandwidth_mbps: float = 0.0
    avg_latency_ms: float = 0.0
    success_rate: float = 0.0


# =============================================================================
# Orchestrator Core (Facade)
# =============================================================================


class Orchestrator:
    """
    BEAM Orchestrator - Central coordinator for bandwidth mining.

    Facade that delegates to specialized manager classes while preserving
    the public interface used by route handlers.
    """

    def __init__(self, settings: Optional[OrchestratorSettings] = None):
        self.settings = settings or get_settings()

        # Bittensor (for signing and validator communication)
        self.wallet: Optional[bt.wallet] = None
        self.subtensor: Optional[bt.subtensor] = None
        self.metagraph: Optional[bt.metagraph] = None
        self.hotkey: Optional[str] = None

        # --- Manager instances ---
        self._worker_mgr = WorkerManager(self.settings, lambda: self.subnet_core_client)
        self._task_sched = TaskScheduler(
            self.settings, self._worker_mgr, get_subnet_core_client=lambda: self.subnet_core_client
        )
        self._proof_agg = ProofAggregator(self.settings)
        self._reward_mgr = RewardManager(self.settings)
        self._epoch_mgr = EpochManager(self.settings)
        # BlindWorkerManager removed
        # GatewayManager removed

        # --- Expose manager state as public attributes (backward compat) ---
        # Worker state (from WorkerManager)
        self.workers = self._worker_mgr.workers
        self.workers_by_hotkey = self._worker_mgr.workers_by_hotkey
        self.workers_by_region = self._worker_mgr.workers_by_region
        self.worker_connections = self._worker_mgr.worker_connections

        # Task state (from TaskScheduler)
        self.active_tasks = self._task_sched.active_tasks
        self.completed_tasks = self._task_sched.completed_tasks
        self.pending_offers = self._task_sched.pending_offers
        self._offer_lock = self._task_sched._offer_lock

        # Proof state (from ProofAggregator)
        self.pending_proofs = self._proof_agg.pending_proofs
        self.epoch_proofs = self._proof_agg.epoch_proofs

        # Epoch tracking
        self.current_epoch: int = 0
        self.epoch_start_time: datetime = datetime.utcnow()
        self.epoch_summaries: Dict[int, EpochSummary] = {}

        # Note: Validator tracking removed - BeamCore handles PoB centrally

        # Statistics
        self.total_bytes_relayed: int = 0
        self.total_tasks_completed: int = 0

        # Reward tracking (delegate to reward manager but expose)
        self.our_uid: Optional[int] = None

        # SubnetCoreClient for API-based data operations
        self.subnet_core_client: Optional[Any] = None
        self.dedicated_gateway: Optional[Any] = None  # set in dedicated mode

        # Async control
        self._running: bool = False
        self._background_tasks: List[asyncio.Task] = []

        # Orchestrator manager for incentive mechanism
        self.orch_manager: Optional[Any] = None

    # --- Reward-share tracking properties (delegated to RewardManager) ---
    @property
    def last_emission_check(self) -> float:
        return self._reward_mgr.last_emission_check

    @last_emission_check.setter
    def last_emission_check(self, value: float):
        self._reward_mgr.last_emission_check = value

    @property
    def epoch_start_emission(self) -> float:
        return self._reward_mgr.epoch_start_emission

    @epoch_start_emission.setter
    def epoch_start_emission(self, value: float):
        self._reward_mgr.epoch_start_emission = value

    @property
    def total_rewards_distributed(self) -> float:
        return self._reward_mgr.total_rewards_distributed

    @total_rewards_distributed.setter
    def total_rewards_distributed(self, value: float):
        self._reward_mgr.total_rewards_distributed = value

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def initialize(self) -> None:
        """Initialize the Orchestrator."""
        logger.info("Initializing BEAM Orchestrator...")

        # Skip bittensor initialization in local mode
        if self.settings.local_mode:
            logger.info(
                "Running in LOCAL MODE - skipping Bittensor wallet/subtensor initialization"
            )
            self.wallet = None
            self.hotkey = self.settings.local_orchestrator_hotkey
            self.subtensor = None
            self.metagraph = None
            self.our_uid = 0
        else:
            # Load wallet (for signing reports)
            self.wallet = bt.Wallet(
                name=self.settings.wallet_name,
                hotkey=self.settings.wallet_hotkey,
                path=self.settings.wallet_path,
            )
            self.hotkey = self.wallet.hotkey.ss58_address
            logger.info(f"Orchestrator wallet: {self.hotkey}")

            self._initialize_subtensor_and_metagraph_with_retry()

            # Find our UID in the metagraph
            self._find_our_uid()

        # Initialize SubnetCoreClient for API-based data operations
        await self._init_subnet_core_client()

        # Skip chain-dependent initialization in local mode
        if not self.settings.local_mode:
            # Sync epoch from chain block number so it matches the validator's numbering
            self._sync_epoch_from_chain()

            # Initialize epoch emission tracking
            self._reward_mgr.epoch_start_emission = self.get_our_emission()
            self._reward_mgr.last_emission_check = self._reward_mgr.epoch_start_emission
            logger.info(f"Initial emission: {self._reward_mgr.epoch_start_emission:.6f} ध")

            # Note: Validator discovery removed - BeamCore handles PoB centrally
        else:
            logger.info("LOCAL MODE: Skipping chain sync and emission tracking")

        # Initialize orchestrator manager for incentive mechanism
        await self._init_orch_manager()

        # Add mock worker if requested
        if self.settings.add_mock_worker:
            await self._add_local_mock_worker()
            logger.info("Added mock worker for testing")

        logger.info("Orchestrator initialized")

    def _initialize_subtensor_and_metagraph_with_retry(self) -> None:
        target = self.settings.subtensor_address or self.settings.subtensor_network
        last_error: Optional[Exception] = None

        for attempt in range(1, SUBTENSOR_INIT_MAX_ATTEMPTS + 1):
            try:
                # The public test endpoint can intermittently return transient
                # internal errors during runtime bootstrap. Treat those as
                # retryable instead of aborting the whole orchestrator process.
                if self.settings.subtensor_address:
                    self.subtensor = bt.Subtensor(network=self.settings.subtensor_address)
                else:
                    self.subtensor = bt.Subtensor(network=self.settings.subtensor_network)
                logger.info(f"Connected to subtensor: {self.subtensor.network}")

                self.metagraph = bt.Metagraph(
                    netuid=self.settings.netuid,
                    network=self.subtensor.network,
                    sync=False,
                )
                self.metagraph.sync(subtensor=self.subtensor)
                return
            except Exception as exc:
                last_error = exc
                if attempt >= SUBTENSOR_INIT_MAX_ATTEMPTS:
                    break

                delay_seconds = SUBTENSOR_INIT_BASE_DELAY_SECONDS * attempt
                logger.warning(
                    "Subtensor bootstrap attempt %s/%s failed for %s: %s. Retrying in %.1fs",
                    attempt,
                    SUBTENSOR_INIT_MAX_ATTEMPTS,
                    target,
                    exc,
                    delay_seconds,
                )
                time.sleep(delay_seconds)

        assert last_error is not None
        raise RuntimeError(
            f"Failed to initialize subtensor/metagraph after {SUBTENSOR_INIT_MAX_ATTEMPTS} attempts for {target}"
        ) from last_error

    async def start(self) -> None:
        """Start the Orchestrator background services."""
        self._running = True

        def running() -> bool:
            return self._running

        self._background_tasks = [
            asyncio.create_task(self._metagraph_sync_loop()),
            asyncio.create_task(self._worker_mgr.worker_health_loop(running)),
            asyncio.create_task(self._worker_mgr.worker_sync_loop(running, interval_seconds=60)),
            asyncio.create_task(
                self._proof_agg.proof_aggregation_loop(
                    running,
                    subnet_core_client_ref=lambda: self.subnet_core_client,
                )
            ),
            # Removed: validator_report_loop - BeamCore handles PoB centrally, validators read from BeamCore
            asyncio.create_task(self._epoch_management_loop()),
            # Removed: _stale_task_reassignment_loop - deprecated endpoint replaced by WebSocket push
        ]
        logger.info("Worker sync loop started (syncs from SubnetCore every 60s)")

        logger.info("Orchestrator started")

    async def stop(self) -> None:
        """Stop the Orchestrator."""
        self._running = False

        for task in self._background_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        if SUBNET_CORE_CLIENT_AVAILABLE and self.subnet_core_client:
            # Stop HTTP polling
            if hasattr(self.subnet_core_client, "stop_polling"):
                await self.subnet_core_client.stop_polling()
            await close_subnet_core_client()
            logger.info("SubnetCoreClient closed")

        logger.info("Orchestrator stopped")

    # =========================================================================
    # Worker Management (delegate to WorkerManager)
    # =========================================================================

    async def register_worker(self, hotkey, ip, port, region, bandwidth_mbps=0.0):
        return await self._worker_mgr.register_worker(
            hotkey,
            ip,
            port,
            region,
            bandwidth_mbps,
            subnet_core_client=self.subnet_core_client,
        )

    async def deregister_worker(self, worker_id):
        return await self._worker_mgr.deregister_worker(worker_id)

    async def apply_worker_stats_snapshot(
        self, worker_id, bandwidth_mbps, active_tasks, bytes_relayed=0
    ):
        return await self._worker_mgr.apply_worker_stats_snapshot(
            worker_id, bandwidth_mbps, active_tasks, bytes_relayed
        )

    def get_worker(self, worker_id):
        return self._worker_mgr.get_worker(worker_id)

    async def get_available_workers(self, region=None, min_bandwidth=0.0):
        return await self._worker_mgr.get_available_workers(region, min_bandwidth)

    def register_worker_connection(self, worker_id, websocket):
        self._worker_mgr.register_worker_connection(worker_id, websocket)

    def unregister_worker_connection(self, worker_id):
        self._worker_mgr.unregister_worker_connection(worker_id)

    # =========================================================================
    # Task Management (delegate to TaskScheduler)
    # =========================================================================

    async def assign_task(
        self,
        task_id,
        chunk_size,
        chunk_hash,
        source_region,
        dest_region,
        deadline_us,
        canary,
        canary_offset,
    ):
        return await self._task_sched.assign_task(
            task_id,
            chunk_size,
            chunk_hash,
            source_region,
            dest_region,
            deadline_us,
            canary,
            canary_offset,
        )

    async def send_task_to_worker(
        self,
        worker_id,
        task_id,
        chunk_data,
        chunk_index,
        chunk_hash,
        transfer_id,
        destination_url=None,
        sender_hotkey=None,
        filename=None,
        total_chunks=None,
        receiver_filename=None,
    ):
        return await self._task_sched.send_task_to_worker(
            worker_id,
            task_id,
            chunk_data,
            chunk_index,
            chunk_hash,
            transfer_id,
            destination_url,
            sender_hotkey,
            filename,
            total_chunks,
            receiver_filename,
        )

    async def send_pull_task_to_worker(
        self,
        worker_id,
        task_id,
        chunk_index,
        chunk_hash,
        transfer_id,
        gateway_address,
        gateway_port,
        sender_hotkey=None,
        filename=None,
        total_chunks=None,
        destination_url=None,
        receiver_filename=None,
    ):
        return await self._task_sched.send_pull_task_to_worker(
            worker_id,
            task_id,
            chunk_index,
            chunk_hash,
            transfer_id,
            gateway_address,
            gateway_port,
            sender_hotkey,
            filename,
            total_chunks,
            destination_url,
            receiver_filename,
        )

    async def broadcast_task_offer(
        self,
        task_id,
        chunk_data,
        chunk_index,
        chunk_hash,
        transfer_id,
        source_region="",
        dest_region="",
        estimated_reward=0.0,
        timeout_seconds=5.0,
        deadline_us=0,
        canary=b"",
        canary_offset=0,
        destination_url=None,
        sender_hotkey=None,
        filename=None,
        total_chunks=None,
        receiver_filename=None,
    ):
        return await self._task_sched.broadcast_task_offer(
            task_id,
            chunk_data,
            chunk_index,
            chunk_hash,
            transfer_id,
            source_region,
            dest_region,
            estimated_reward,
            timeout_seconds,
            deadline_us,
            canary,
            canary_offset,
            destination_url,
            sender_hotkey,
            filename,
            total_chunks,
            receiver_filename,
        )

    async def accept_task_offer(self, offer_id, worker_id):
        return await self._task_sched.accept_task_offer(offer_id, worker_id)

    async def reject_task_offer(self, offer_id, worker_id, reason=""):
        return await self._task_sched.reject_task_offer(offer_id, worker_id, reason)

    async def _notify_offer_result(self, offer_id, winner_id, status):
        return await self._task_sched._notify_offer_result(offer_id, winner_id, status)

    # =========================================================================
    # Cross-cutting: Task Completion & Relay Results
    # =========================================================================

    async def complete_task(
        self,
        task_id: str,
        bytes_relayed: int,
        bandwidth_mbps: float,
        start_time_us: int,
        end_time_us: int,
        canary_proof: str,
        worker_signature: str,
    ) -> Optional[BandwidthProof]:
        """Record task completion and generate proof."""
        logger.info(f"complete_task called: task_id={task_id[:20]}..., bytes={bytes_relayed}")
        task = self.active_tasks.get(task_id)
        if not task:
            logger.warning(f"Unknown task: {task_id[:16]}...")
            return None

        worker = self.workers.get(task.worker_id)
        worker_hotkey = None
        worker_region = ""
        logger.info(
            f"complete_task: worker_id={task.worker_id}, in_memory={worker is not None}, client={self.subnet_core_client is not None}"
        )
        if worker:
            worker_hotkey = worker.hotkey
            worker_region = getattr(worker, "region", "") or task.source_region or ""
            logger.info(f"complete_task: using in-memory worker hotkey={worker_hotkey[:16]}...")
        elif self.subnet_core_client:
            # Fall back to database lookup
            try:
                logger.info(f"complete_task: looking up worker {task.worker_id} from SubnetCore...")
                worker_data = await self.subnet_core_client.get_worker(task.worker_id)
                worker_hotkey = worker_data.get("hotkey", "")
                worker_region = worker_data.get("region", "") or task.source_region or ""
                logger.info(
                    f"complete_task: got worker_hotkey={worker_hotkey[:16] if worker_hotkey else 'None'}..."
                )
            except Exception as e:
                logger.warning(f"Worker lookup failed for task {task_id[:16]}...: {e}")

        if not worker_hotkey:
            logger.warning(f"Worker not found for task: {task_id[:16]}... (no hotkey)")
            return None

        # Update task
        task.status = "completed"
        task.completed_at = time.time()
        task.bytes_relayed = bytes_relayed
        task.bandwidth_mbps = bandwidth_mbps
        task.latency_ms = (end_time_us - start_time_us) / 1000

        # Update worker stats (only if worker is in memory)
        if worker:
            worker.active_tasks = max(0, worker.active_tasks - 1)
            worker.successful_tasks += 1
            worker.bytes_relayed_total += bytes_relayed
            worker.bytes_relayed_epoch += bytes_relayed
            worker.update_bandwidth_ema(bandwidth_mbps)
            worker.update_success_rate()
            worker.trust_score = min(1.0, worker.trust_score + 0.001)

        # Move to completed
        del self.active_tasks[task_id]
        self.completed_tasks[task_id] = task

        # Update global stats
        self.total_bytes_relayed += bytes_relayed
        self.total_tasks_completed += 1

        # Generate proof
        proof = BandwidthProof(
            task_id=task_id,
            worker_id=task.worker_id,
            worker_hotkey=worker_hotkey,
            start_time_us=start_time_us,
            end_time_us=end_time_us,
            bytes_relayed=bytes_relayed,
            bandwidth_mbps=bandwidth_mbps,
            chunk_hash=task.chunk_hash,
            canary_proof=canary_proof,
            worker_signature=worker_signature,
            source_region=task.source_region or worker_region,
            dest_region=task.dest_region or worker_region,
        )

        proof.orchestrator_signature = self._sign_proof(proof)

        # BeamCore manages task lifecycle server-side: tasks are queued
        # from WS chunk_assignments and completion is reflected from worker
        # task_result_summary pushes (see task-lifecycle.ts). No HTTP write needed.

        # Add to aggregation queue
        self.pending_proofs.append(proof)
        self.epoch_proofs[self.current_epoch].append(proof)

        # Persist proof
        await self._proof_agg.persist_bandwidth_proof(
            proof, self.current_epoch, self.subnet_core_client
        )

        # NOTE: Worker ALPHA payments are intentionally NOT triggered here.
        # This orchestrator no longer pays workers; it only records emission
        # share / bandwidth stats and publishes PoB. See plan
        # `remove_orch_payment_machinery_30534c30`.

        logger.info(
            f"Task {task_id[:16]}... completed: "
            f"{bytes_relayed} bytes @ {bandwidth_mbps:.2f} Mbps"
        )
        return proof

    async def record_relay_result(
        self,
        worker_id: str,
        task_id: str,
        success: bool,
        bytes_relayed: int,
        bandwidth_mbps: float,
        chunks_relayed: int,
        latency_ms: float,
        proof_of_bandwidth: dict,
    ) -> Optional[BandwidthProof]:
        """Record a relay result from a worker (P2P transfer)."""
        worker = self.workers.get(worker_id)
        worker_hotkey = None
        worker_region = ""
        if worker:
            worker_hotkey = worker.hotkey
            worker_region = getattr(worker, "region", "")
        elif self.subnet_core_client:
            try:
                worker_data = await self.subnet_core_client.get_worker(worker_id)
                worker_hotkey = worker_data.get("hotkey", "")
                worker_region = worker_data.get("region", "")
            except Exception as e:
                logger.warning(f"Worker lookup failed for relay result {worker_id}: {e}")

        if not worker_hotkey:
            logger.warning(f"Unknown worker {worker_id} sent relay result (no hotkey)")
            return None

        if not success:
            if worker:
                worker.failed_tasks += 1
                worker.update_success_rate()
            return None

        # Update worker stats (only if worker is in memory)
        if worker:
            worker.successful_tasks += 1
            worker.bytes_relayed_total += bytes_relayed
            worker.bytes_relayed_epoch += bytes_relayed
            if bandwidth_mbps > 0:
                worker.update_bandwidth_ema(bandwidth_mbps)
            worker.update_success_rate()
            worker.trust_score = min(1.0, worker.trust_score + 0.001)

        # Update global stats
        self.total_bytes_relayed += bytes_relayed
        self.total_tasks_completed += 1

        # Generate proof with realistic timestamps
        current_time_us = int(time.time() * 1_000_000)
        if latency_ms > 0:
            estimated_duration_us = int(latency_ms * 1000)
        elif bandwidth_mbps > 0 and bytes_relayed > 0:
            estimated_duration_us = int(
                (bytes_relayed * 8) / (bandwidth_mbps * 1_000_000) * 1_000_000
            )
        else:
            estimated_duration_us = 100_000
        estimated_duration_us = max(estimated_duration_us, 10_000)

        start_time_us = proof_of_bandwidth.get(
            "start_time_us", current_time_us - estimated_duration_us
        )
        end_time_us = proof_of_bandwidth.get("end_time_us", current_time_us)

        proof = BandwidthProof(
            task_id=task_id or f"relay-{worker_id}-{int(time.time())}",
            worker_id=worker_id,
            worker_hotkey=worker_hotkey,
            start_time_us=start_time_us,
            end_time_us=end_time_us,
            bytes_relayed=bytes_relayed,
            bandwidth_mbps=bandwidth_mbps,
            chunk_hash=proof_of_bandwidth.get("transfer_id", ""),
            canary_proof="",
            worker_signature=proof_of_bandwidth.get("signature", ""),
            source_region=worker_region,
            dest_region="local",
        )

        if hasattr(self, "_sign_proof"):
            proof.orchestrator_signature = self._sign_proof(proof)

        # BeamCore manages task lifecycle server-side: tasks are queued
        # from WS chunk_assignments and completion is reflected from worker
        # task_result_summary pushes (see task-lifecycle.ts). No HTTP write needed.

        self.pending_proofs.append(proof)
        self.epoch_proofs[self.current_epoch].append(proof)

        await self._proof_agg.persist_bandwidth_proof(
            proof, self.current_epoch, self.subnet_core_client
        )

        # NOTE: Worker ALPHA payments are intentionally NOT triggered here.
        # See `complete_task` for the same rationale.

        logger.info(
            f"Recorded relay result from worker {worker_id}: "
            f"{bytes_relayed} bytes at {bandwidth_mbps:.1f} Mbps"
        )
        return proof

    async def fail_task(self, task_id: str, reason: str) -> None:
        """Record task failure."""
        task = self.active_tasks.get(task_id)
        if not task:
            return

        worker = self.workers.get(task.worker_id)
        if worker:
            worker.active_tasks = max(0, worker.active_tasks - 1)
            worker.failed_tasks += 1
            worker.update_success_rate()
            worker.trust_score = max(0.0, worker.trust_score - 0.01)

        task.status = "failed"
        del self.active_tasks[task_id]

        logger.warning(f"Task {task_id[:16]}... failed: {reason}")

    # =========================================================================
    # Local reward-share accounting (delegate to RewardManager)
    # =========================================================================

    def distribute_rewards_at_epoch_end(self) -> Dict[str, float]:
        return self._reward_mgr.distribute_rewards_at_epoch_end(
            self.workers.values(), self.get_our_emission
        )

    def distribute_rewards_to_workers(self) -> Dict[str, float]:
        return self._reward_mgr.distribute_rewards_to_workers(self.get_our_emission)

    # =========================================================================
    # Metagraph & Validators
    # =========================================================================

    def _find_our_uid(self) -> None:
        if self.metagraph is None or self.hotkey is None:
            return
        for uid in range(len(self.metagraph.hotkeys)):
            if self.metagraph.hotkeys[uid] == self.hotkey:
                logger.info(f"Found our UID: {uid}")
                self.our_uid = uid
                return
        logger.warning(f"Hotkey {self.hotkey[:16]}... not found in metagraph")

    def get_our_emission(self) -> float:
        """Get our emission converted from alpha to TAO.

        Caches the subnet price for 60s to avoid hammering the subtensor
        websocket (which is not safe for concurrent recv calls).
        """
        if self.metagraph is None or self.our_uid is None:
            return 0.0
        try:
            emission_alpha = float(self.metagraph.E[self.our_uid])
        except Exception as e:
            logger.error(f"Error getting emission: {e}")
            return 0.0
        if emission_alpha <= 0 or not self.subtensor:
            return emission_alpha

        # metagraph.E returns emission in alpha (ध), not TAO.
        # Convert using cached subnet price (refreshed every 5 min).
        import time as _time

        now = _time.time()
        cache_ttl = 300  # 5 minutes
        if (
            not hasattr(self, "_cached_tao_per_alpha")
            or (now - getattr(self, "_cached_price_at", 0)) > cache_ttl
        ):
            try:
                price = self.subtensor.get_subnet_price(self.settings.netuid)
                # get_subnet_price returns TAO per 1 alpha
                self._cached_tao_per_alpha = float(price)
                self._cached_price_at = now
            except Exception as e:
                logger.warning(f"Could not convert emission alpha→TAO: {e}")
                # Use stale cache if available
                if not hasattr(self, "_cached_tao_per_alpha"):
                    return emission_alpha

        tao_per_alpha = getattr(self, "_cached_tao_per_alpha", 0)
        if tao_per_alpha > 0:
            emission_tao = emission_alpha * tao_per_alpha
            logger.debug(
                f"Emission: {emission_alpha:.4f} ध → {emission_tao:.9f} TAO "
                f"(rate: {tao_per_alpha:.4f} τ/ध)"
            )
            return emission_tao

        return emission_alpha

    # Note: _discover_validators removed - BeamCore handles PoB centrally

    # =========================================================================
    # Background Loops
    # =========================================================================

    async def _metagraph_sync_loop(self) -> None:
        """Background loop for syncing metagraph."""
        sync_interval = 300  # Sync every 5 minutes (validators/stake change infrequently)

        while self._running:
            try:
                await asyncio.sleep(sync_interval)
                if self.metagraph and self.subtensor:
                    self.metagraph.sync(subtensor=self.subtensor)

                # Always re-check UID after sync — hotkey may have moved to a
                # different UID slot after re-registration (stale UID causes
                # PoB proofs to be filtered by BeamCore).
                old_uid = self.our_uid
                self._find_our_uid()
                if self.our_uid != old_uid and self.subnet_core_client is not None:
                    logger.info(
                        f"UID changed {old_uid} → {self.our_uid}, updating SubnetCoreClient"
                    )
                    self.subnet_core_client.orchestrator_uid = self.our_uid

                self.distribute_rewards_to_workers()
                # Note: Validator discovery removed - BeamCore handles PoB centrally

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error syncing metagraph: {e}")

    def _sync_epoch_from_chain(self) -> None:
        """Set current_epoch from the chain block number to match validator epoch numbering."""
        if self.subtensor:
            try:
                block = self.subtensor.block
                # Use 360 blocks per epoch to match SubnetCore's epoch calculation
                epoch_length_blocks = 360
                chain_epoch = block // epoch_length_blocks

                # Always sync if epochs differ significantly or chain_epoch is newer
                # This handles the case where old epoch calculation (block//25) was used
                # and produced epochs like 258007 which are > correct epoch ~17925
                should_sync = (
                    chain_epoch > self.current_epoch  # Normal case: new epoch
                    or self.current_epoch > 100_000  # Old epoch calculation was used
                    or chain_epoch != self.current_epoch  # Any mismatch (first sync)
                )

                if should_sync and chain_epoch != self.current_epoch:
                    logger.info(
                        f"Synced epoch from chain: {self.current_epoch} -> {chain_epoch} "
                        f"(block={block})"
                    )
                    self.current_epoch = chain_epoch
            except Exception as e:
                logger.warning(f"Failed to sync epoch from chain: {e}")

    async def _epoch_management_loop(self) -> None:
        """Background loop for managing epochs."""
        epoch_duration = timedelta(minutes=5)
        chain_sync_interval = 600  # Re-sync with chain every 10 minutes
        last_chain_sync = time.time()

        while self._running:
            try:
                await asyncio.sleep(60)

                # Re-sync epoch from chain periodically (every 10 min) to correct drift
                now = time.time()
                if now - last_chain_sync >= chain_sync_interval:
                    self._sync_epoch_from_chain()
                    last_chain_sync = now

                # Check if epoch should change (time-based fallback)
                if datetime.utcnow() - self.epoch_start_time >= epoch_duration:
                    await self._advance_epoch()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in epoch management: {e}")

    async def _advance_epoch(self) -> None:
        """Advance to next epoch.  Must always increment the counter."""
        prev_epoch = self.current_epoch
        summary = None

        try:
            summary = self._build_epoch_summary()
            self.epoch_summaries[self.current_epoch] = summary
        except Exception as e:
            logger.error(f"Error building epoch summary: {e}")

        try:
            self.distribute_rewards_at_epoch_end()
        except Exception as e:
            logger.error(f"Error distributing epoch rewards: {e}")

        try:
            await self._epoch_mgr.generate_payment_proofs(
                self.current_epoch,
                self.workers.values(),
                self.hotkey or "",
                self.our_uid,
                self.wallet,
            )
        except Exception as e:
            logger.error(f"Error generating payment proofs: {e}")

        for worker in self.workers.values():
            worker.bytes_relayed_epoch = 0
            worker.rewards_earned_epoch = 0

        self._reward_mgr.epoch_start_emission = self.get_our_emission()

        # Always advance — never let a sub-step failure block epoch progression
        self.current_epoch += 1
        self.epoch_start_time = datetime.utcnow()

        tasks = summary.total_tasks if summary else "?"
        bytes_relayed = summary.total_bytes_relayed if summary else "?"
        logger.info(
            f"Advanced to epoch {self.current_epoch} "
            f"(previous epoch {prev_epoch}: {tasks} tasks, {bytes_relayed} bytes)"
        )

    # Stale-task reassignment is owned by BeamCore server-side.

    # =========================================================================
    # State & Metrics
    # =========================================================================

    def _build_epoch_summary(self) -> EpochSummary:
        return self._proof_agg.build_epoch_summary(self.current_epoch, self.epoch_start_time)

    def get_state(self) -> dict:
        """Get current Orchestrator state."""
        active_workers = [w for w in self.workers.values() if w.is_available]

        return {
            "hotkey": self.hotkey,
            "beamcore_upstream_degraded": (
                self.subnet_core_client.is_beamcore_upstream_degraded()
                if getattr(self, "subnet_core_client", None)
                else None
            ),
            "current_epoch": self.current_epoch,
            "epoch_start": self.epoch_start_time.isoformat(),
            "total_workers": len(self.workers),
            "active_workers": len(active_workers),
            "workers_by_status": {
                status.value: len([w for w in self.workers.values() if w.status == status])
                for status in WorkerStatus
            },
            "active_tasks": len(self.active_tasks),
            "pending_proofs": len(self.pending_proofs),
            "total_bytes_relayed": self.total_bytes_relayed,
            "total_tasks_completed": self.total_tasks_completed,
            # validators_known removed - BeamCore handles PoB centrally
        }

    def get_worker_stats(self) -> List[dict]:
        """Get statistics for all workers."""
        return [
            {
                "worker_id": w.worker_id,
                "region": w.region,
                "status": w.status.value,
                "trust_score": round(w.trust_score, 4),
                "bandwidth_mbps": round(w.bandwidth_ema, 2),
                "success_rate": round(w.success_rate, 4),
                "total_tasks": w.total_tasks,
                "bytes_relayed": w.bytes_relayed_total,
                "load_factor": round(w.load_factor, 2),
            }
            for w in self.workers.values()
        ]

    def get_epoch_stats(self, epoch: Optional[int] = None) -> Optional[dict]:
        """Get statistics for an epoch."""
        if epoch is None:
            epoch = self.current_epoch

        summary = self.epoch_summaries.get(epoch)
        if not summary:
            if epoch == self.current_epoch:
                summary = self._build_epoch_summary()
            else:
                return None

        return {
            "epoch": summary.epoch,
            "start_time": summary.start_time.isoformat(),
            "end_time": summary.end_time.isoformat(),
            "total_tasks": summary.total_tasks,
            "total_bytes_relayed": summary.total_bytes_relayed,
            "active_workers": summary.active_workers,
            "avg_bandwidth_mbps": round(summary.avg_bandwidth_mbps, 2),
            "avg_latency_ms": round(summary.avg_latency_ms, 2),
            "success_rate": round(summary.success_rate, 4),
        }

    # =========================================================================
    # Utilities
    # =========================================================================

    def _generate_worker_id(self, hotkey: str, ip: str, port: int) -> str:
        """Generate unique worker ID."""
        import hashlib

        data = f"{hotkey}:{ip}:{port}:{time.time()}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def _sign_proof(self, proof: BandwidthProof) -> str:
        """Sign a proof with orchestrator key."""
        message = f"{proof.task_id}:{proof.worker_id}:{proof.bytes_relayed}"

        if self.wallet is None:
            import hashlib

            return hashlib.sha256(message.encode()).hexdigest()

        signature = self.wallet.hotkey.sign(message.encode())
        return signature.hex() if isinstance(signature, bytes) else str(signature)

    # =========================================================================
    # Initialization Helpers
    # =========================================================================

    async def _init_subnet_core_client(self) -> None:
        """Initialize SubnetCoreClient for API-based data operations."""
        if not SUBNET_CORE_CLIENT_AVAILABLE:
            logger.warning(
                "SubnetCoreClient not available - data will not be persisted to BeamCore"
            )
            return

        if not self.settings.core_server_url:
            logger.warning(
                "CORE_SERVER_URL not configured - data will not be persisted to BeamCore"
            )
            return

        try:
            signer = None
            if self.wallet and hasattr(self.wallet, "hotkey"):
                signer = self.wallet.hotkey
            self.subnet_core_client = init_subnet_core_client(
                base_url=self.settings.core_server_url,
                ws_base_url=self.settings.orch_gateway_url,
                orchestrator_hotkey=self.hotkey or "unknown",
                orchestrator_uid=self.our_uid or 0,
                signer=signer,
                ws_open_timeout=self.settings.orch_ws_open_timeout,
                ws_close_timeout=self.settings.orch_ws_close_timeout,
                ws_ping_interval=self.settings.orch_ws_ping_interval,
                ws_ping_timeout=self.settings.orch_ws_ping_timeout,
            )
            logger.info(
                "SubnetCoreClient initialized: http=%s ws=%s",
                self.settings.core_server_url,
                self.settings.orch_gateway_url,
            )

            # WS push handlers. Transfer assignments are owned by the client
            # itself (`_handle_transfer_assigned` -> WS chunk_assignments) per
            # the BeamCore contract.
            self.subnet_core_client.set_task_completion_handler(
                self._handle_task_completion_notification
            )
            self.subnet_core_client.set_worker_update_handler(self._worker_mgr.handle_worker_update)

            # Configure registration message sent on every WS connect
            import socket as _socket

            try:
                _s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
                _s.connect(("8.8.8.8", 80))
                local_ip = self.settings.external_ip or _s.getsockname()[0]
                _s.close()
            except Exception:
                local_ip = self.settings.external_ip or "127.0.0.1"
            orch_url = f"http://{local_ip}:{self.settings.api_port}"
            self.subnet_core_client.set_registration_config(
                url=orch_url,
                region=self.settings.region,
                max_workers=self.settings.max_workers,
                uid=self.our_uid,
                fee_percentage=self.settings.fee_percentage,
                gateway_url=self.settings.worker_gateway_public_url,
            )
            self.subnet_core_client.prime_ready_state(bool(self.settings.ready))
            logger.info(
                f"WS registration config set: url={orch_url}, region={self.settings.region}"
            )

            # DEDICATED mode: stand up the in-process worker gateway and attach it so
            # transfer_assigned dispatches to OUR connected workers (not the public pool).
            if getattr(self.settings, "dedicated_mode", False):
                try:
                    from .worker_gateway import DedicatedWorkerGateway
                    self.dedicated_gateway = DedicatedWorkerGateway(
                        get_client=lambda: self.subnet_core_client,
                        settings=self.settings,
                        worker_manager=self._worker_mgr,
                    )
                    self.subnet_core_client.set_dedicated_gateway(self.dedicated_gateway)
                    logger.info("DEDICATED mode ON — workers connect to this orchestrator's /ws/{worker_id}")
                except Exception as e:
                    logger.error(f"Failed to start dedicated worker gateway: {e}")

            # Start WebSocket connection for real-time notifications and
            # orchestrator control-plane requests.
            await self.subnet_core_client.start_polling()
            logger.info("SubnetCore WebSocket connection started")

        except Exception as e:
            logger.warning(f"Failed to initialize SubnetCoreClient: {e}")
            self.subnet_core_client = None

    async def _handle_task_completion_notification(self, message: dict) -> bool:
        """
        Handle task completion notifications from SubnetCore.

        Verifies the completion (worker match + bytes sanity) and updates
        local task / per-worker stats. Returning True triggers the
        ``acknowledge_task_completions`` ack in
        ``SubnetCoreClient._handle_ws_message`` (BeamCore task lifecycle).

        Worker payment management is operator-defined.
        This handler updates local task and worker accounting.

        Args:
            message: Task completion message with task_id, worker_id, bytes, etc.

        Returns:
            True if verified and should be acknowledged, False otherwise
        """
        task_id = message.get("task_id")
        worker_id = message.get("worker_id")
        bytes_transferred = message.get("bytes_relayed", 0) or message.get("bytes_transferred", 0)
        bandwidth_mbps = message.get("bandwidth_mbps", 0.0)

        logger.info(
            f"Verifying task completion: task={task_id[:16] if task_id else 'none'}... "
            f"worker={worker_id[:16] if worker_id else 'none'}... bytes={bytes_transferred}"
        )

        task = self.active_tasks.get(task_id) or self.completed_tasks.get(task_id)

        # Get reassigned_worker_id (set by BeamCore when task was reassigned)
        # worker_id = original assignee, reassigned_worker_id = new worker after reassignment
        reassigned_worker_id = message.get("reassigned_worker_id")

        if task:
            worker_matches = task.worker_id == worker_id or (
                reassigned_worker_id and reassigned_worker_id == worker_id
            )
            if not worker_matches:
                logger.warning(
                    f"Worker mismatch for task {task_id}: expected {task.worker_id} "
                    f"or reassigned={reassigned_worker_id}, got {worker_id}"
                )
                return False

            if task.chunk_size:
                expected_bytes = task.chunk_size
                tolerance = expected_bytes * 0.2
                if abs(bytes_transferred - expected_bytes) > tolerance:
                    logger.warning(
                        f"Bytes mismatch for task {task_id}: expected ~{expected_bytes}, "
                        f"got {bytes_transferred}"
                    )

            if task_id in self.active_tasks:
                task.status = "completed"
                task.completed_at = time.time()
                task.bytes_relayed = bytes_transferred
                task.bandwidth_mbps = bandwidth_mbps
                del self.active_tasks[task_id]
                self.completed_tasks[task_id] = task
                self.total_bytes_relayed += bytes_transferred
                self.total_tasks_completed += 1
                logger.info(f"Task {task_id[:16]}... marked completed via SubnetCore notification")
        else:
            logger.info(
                f"Task {task_id[:16]}... not in memory, recording stats from SubnetCore data"
            )
            self.total_bytes_relayed += bytes_transferred
            self.total_tasks_completed += 1

        return True

    # Transfer notification + chunk assignment is handled inside
    # SubnetCoreClient._handle_transfer_assigned (BeamCore WS push):
    # the client receives transfer_assigned, requests workers via WS
    # list_workers, and submits WS chunk_assignments. No HTTP path needed.

    async def _init_orch_manager(self) -> None:
        """Initialize the orchestrator manager for incentive mechanism."""
        try:
            from beam.orchestrator import OrchestratorManager

            self.orch_manager = OrchestratorManager()
            logger.info("Orchestrator manager initialized (in-memory mode)")
        except ImportError:
            # OrchestratorManager is optional - not needed for normal operation
            self.orch_manager = None
        except Exception as e:
            logger.error(f"Failed to initialize orchestrator manager: {e}")
            self.orch_manager = None

    async def _add_local_mock_worker(self) -> None:
        """Add a mock worker for local mode bandwidth challenges."""
        worker_hotkey = (
            self.settings.mock_worker_hotkey or "5LocalMockWorkerHotkey0000000000000000000000"
        )
        worker_id = (
            f"worker-{worker_hotkey[:8]}"
            if self.settings.mock_worker_hotkey
            else "local-mock-worker"
        )

        mock_worker = Worker(
            worker_id=worker_id,
            hotkey=worker_hotkey,
            ip="127.0.0.1",
            port=9100,
            region="local",
            status=WorkerStatus.ACTIVE,
            bandwidth_mbps=1000.0,
            bandwidth_ema=1000.0,
            latency_ms=1.0,
            success_rate=1.0,
            trust_score=1.0,
            max_concurrent_tasks=100,
        )

        self.workers[mock_worker.worker_id] = mock_worker
        self.workers_by_region["local"].add(mock_worker.worker_id)

        logger.info(
            f"Added mock worker for local mode: {mock_worker.worker_id} (hotkey: {worker_hotkey})"
        )


# =============================================================================
# Singleton
# =============================================================================

_orchestrator: Optional[Orchestrator] = None


def get_orchestrator() -> Orchestrator:
    """Get the global Orchestrator instance."""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = Orchestrator()
    return _orchestrator
