"""
Async worker pool – optimised for 500 TPS.

Strategy: "Decoupled Write"
  1. Ingest transaction into Neo4j (lock-free hot path)
  2. Score via RiskEngine (parallel reads, no writes)
  3. Write-back risk score (fire-and-forget)

Deadlock mitigation:
  - TransientException retry with exponential backoff
  - Consistent lock ordering (sender < receiver) in Cypher
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from typing import Any, Callable, Dict, List, Optional

from app.config import settings
from app.neo4j_manager import Neo4jManager
from app.models.transaction import TransactionInput
from app.core.risk_engine import RiskEngine
from app.utils.cypher_queries import INGEST_TRANSACTION, INGEST_TRANSACTION_SAFE, INGEST_IP
from app.features.asn_intelligence import resolve as asn_resolve

logger = logging.getLogger(__name__)

# Deadlock retry settings
_MAX_RETRIES = 3
_BASE_BACKOFF_SEC = 0.02  # 20ms


class WorkerPool:
    """Pool of async workers that drain a Redis stream."""

    def __init__(
        self,
        neo4j: Neo4jManager,
        risk_engine: RiskEngine,
        redis_client: Any,              # redis.asyncio.Redis
        alert_callback: Optional[Callable] = None,
    ) -> None:
        self.neo4j = neo4j
        self.risk_engine = risk_engine
        self.redis = redis_client
        self.alert_callback = alert_callback  # called with RiskResponse dict

        self._tasks: List[asyncio.Task] = []
        self._running = False

        # metrics
        self.processed_count = 0
        self.total_latency_ms = 0.0
        self.deadlock_retries = 0
        self.ingest_errors = 0
        self._start_time: float = 0

    # ── lifecycle ────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        self._start_time = time.time()

        # ensure consumer group exists
        try:
            await self.redis.xgroup_create(
                settings.REDIS_STREAM_KEY,
                settings.REDIS_CONSUMER_GROUP,
                id="0",
                mkstream=True,
            )
        except Exception:  # noqa: BLE001  (group may already exist)
            pass

        for i in range(settings.WORKER_COUNT):
            task = asyncio.create_task(self._worker(f"worker-{i}"))
            self._tasks.append(task)

        logger.info("🏭 Worker pool started (%d workers)", settings.WORKER_COUNT)

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        logger.info("Worker pool stopped")

    # ── metrics ──────────────────────────────────────────────

    @property
    def avg_latency_ms(self) -> float:
        if self.processed_count == 0:
            return 0
        return self.total_latency_ms / self.processed_count

    @property
    def tps(self) -> float:
        elapsed = time.time() - self._start_time if self._start_time else 1
        return self.processed_count / max(elapsed, 1)

    # ── deadlock-safe write ──────────────────────────────────

    async def _ingest_with_retry(self, query: str, params: Dict) -> None:
        """Execute write with exponential backoff on TransientException."""
        for attempt in range(_MAX_RETRIES):
            try:
                await self.neo4j.write_async(query, params)
                return
            except Exception as exc:
                err_str = str(exc).lower()
                is_transient = "deadlock" in err_str or "transient" in err_str
                is_not_found = "not found" in err_str or "no node" in err_str

                # If MATCH-based ingest fails (user not in graph), fall back
                if is_not_found and query == INGEST_TRANSACTION:
                    logger.debug("User not pre-seeded, falling back to SAFE ingest")
                    await self.neo4j.write_async(INGEST_TRANSACTION_SAFE, params)
                    return

                if is_transient and attempt < _MAX_RETRIES - 1:
                    backoff = _BASE_BACKOFF_SEC * (2 ** attempt) + random.uniform(0, 0.01)
                    self.deadlock_retries += 1
                    logger.debug("Deadlock retry %d (backoff=%.3fs)", attempt + 1, backoff)
                    await asyncio.sleep(backoff)
                    continue
                raise

    # ── worker loop ──────────────────────────────────────────

    async def _worker(self, name: str) -> None:
        logger.info("  ▶ %s started", name)
        while self._running:
            try:
                messages = await self.redis.xreadgroup(
                    groupname=settings.REDIS_CONSUMER_GROUP,
                    consumername=name,
                    streams={settings.REDIS_STREAM_KEY: ">"},
                    count=settings.WORKER_BATCH_SIZE,
                    block=1000,  # ms
                )
                if not messages:
                    continue

                for stream_name, entries in messages:
                    for msg_id, data in entries:
                        await self._process_message(name, msg_id, data)

            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                logger.error("%s error: %s", name, exc)
                await asyncio.sleep(0.5)

    async def _process_message(
        self, worker_name: str, msg_id: bytes, data: Dict[bytes, bytes]
    ) -> None:
        t0 = time.perf_counter()

        try:
            # Decode Redis hash fields
            decoded = {
                k.decode() if isinstance(k, bytes) else k:
                v.decode() if isinstance(v, bytes) else v
                for k, v in data.items()
            }

            # Parse payload (stored as JSON in "payload" field)
            if "payload" in decoded:
                tx_data = json.loads(decoded["payload"])
            else:
                tx_data = decoded

            tx = TransactionInput(**tx_data)

            # ── Step 1: Ingest (lock-free hot path with retry) ──
            ingest_params = {
                "sender_id": tx.sender_id,
                "receiver_id": tx.receiver_id,
                "device_id": tx.device_id,
                "device_os": tx.device_os,
                "device_type": tx.device_type.value if tx.device_type else None,
                "app_version": tx.app_version,
                "capability_mask": tx.capability_mask,
                "tx_id": tx.tx_id,
                "amount": tx.amount,
                "timestamp": tx.timestamp.isoformat(),
                "currency": tx.currency,
                "txn_type": tx.txn_type.value,
                "credential_type": tx.credential_type.value if tx.credential_type else None,
                "credential_sub_type": tx.credential_sub_type.value if tx.credential_sub_type else None,
                "receiver_type": tx.receiver_type.value,
                "mcc_code": tx.mcc_code,
            }
            await self._ingest_with_retry(INGEST_TRANSACTION, ingest_params)

            # ── Step 1b: IP intelligence write (MMDB-enriched) ──
            if tx.ip_address:
                asn_info = asn_resolve(tx.ip_address)
                await self.neo4j.write_async(
                    INGEST_IP,
                    {
                        "ip_address": tx.ip_address,
                        "geo_lat": tx.sender_lat,
                        "geo_lon": tx.sender_lon,
                        "is_vpn": False,
                        "city": None,
                        "country": asn_info.get("country") or None,
                        "asn": asn_info.get("asn") or None,
                        "asn_type": asn_info.get("asn_class") or None,
                        "asn_org": asn_info.get("org_name") or None,
                        "asn_country": asn_info.get("country") or None,
                        "user_id": tx.sender_id,
                    },
                )

            # ── Step 2: Score (parallel reads, no Neo4j writes) ──
            risk_response = await self.risk_engine.score_transaction(tx)

            # ── Step 3: Push alert if high-risk ──
            if self.alert_callback and risk_response.risk_score >= settings.MEDIUM_RISK_THRESHOLD:
                await self.alert_callback(risk_response.model_dump(mode="json"))

            # ── Step 4: ACK message ──
            await self.redis.xack(
                settings.REDIS_STREAM_KEY,
                settings.REDIS_CONSUMER_GROUP,
                msg_id,
            )

            elapsed_ms = (time.perf_counter() - t0) * 1000
            self.processed_count += 1
            self.total_latency_ms += elapsed_ms

            if self.processed_count % 200 == 0:
                logger.info(
                    "📈 %s | processed=%d | avg=%.1fms | tps=%.0f | retries=%d",
                    worker_name,
                    self.processed_count,
                    self.avg_latency_ms,
                    self.tps,
                    self.deadlock_retries,
                )

        except Exception as exc:  # noqa: BLE001
            self.ingest_errors += 1
            logger.error("Failed to process message %s: %s", msg_id, exc)
