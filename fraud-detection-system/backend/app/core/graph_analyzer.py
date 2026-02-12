"""
Batch graph analytics runner – optimised.

Periodically (every GRAPH_ANALYTICS_INTERVAL_SEC):
  1. Drops & recreates the GDS in-memory projection
  2. Runs Louvain community detection
  3. Runs betweenness centrality
  4. Runs PageRank
  5. Runs local clustering coefficient
  6. Triggers collusive fraud pattern detection refresh
  7. Runs background user-stats aggregation (moved from hot path)
  8. Refreshes device account counts
  9. Flags dormant accounts

Heavy algorithms run here so the per-transaction fast-path only reads
pre-computed node properties.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, Optional

from app.neo4j_manager import Neo4jManager
from app.utils import cypher_queries as CQ
from app.detection.collusive_fraud import CollusiveFraudDetector
from app.config import settings

logger = logging.getLogger(__name__)


class GraphAnalyzer:
    """Background task that runs GDS algorithms on a timer."""

    def __init__(
        self,
        neo4j: Neo4jManager,
        collusive_detector: CollusiveFraudDetector,
    ) -> None:
        self.neo4j = neo4j
        self.collusive = collusive_detector
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self.last_run_stats: Dict = {}

    async def start(self) -> None:
        """Launch the periodic loop."""
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info(
            "📊 Graph analyzer started (interval=%ds)",
            settings.GRAPH_ANALYTICS_INTERVAL_SEC,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Graph analyzer stopped")

    # ── internal loop ────────────────────────────────────────

    async def _loop(self) -> None:
        # initial delay to let some data accumulate
        await asyncio.sleep(3)
        while self._running:
            try:
                stats = await self.run_once()
                self.last_run_stats = stats
            except Exception as exc:  # noqa: BLE001
                logger.error("Graph analytics batch failed: %s", exc)
            await asyncio.sleep(settings.GRAPH_ANALYTICS_INTERVAL_SEC)

    async def run_once(self) -> Dict:
        """Execute a single analytics cycle.  Returns timing dict."""
        t0 = time.perf_counter()
        stats: Dict = {}

        # ── Phase 1: Background aggregation (decoupled from hot path) ──
        try:
            res = await self.neo4j.run_async(
                CQ.BATCH_UPDATE_USER_STATS,
                {"window_sec": settings.GRAPH_ANALYTICS_INTERVAL_SEC * 3},
            )
            stats["user_stats_updated"] = res[0].get("users_updated", 0) if res else 0
            logger.info("  User stats aggregated: %d users", stats["user_stats_updated"])
        except Exception as exc:  # noqa: BLE001
            logger.warning("User stats aggregation failed: %s", exc)

        try:
            res = await self.neo4j.run_async(CQ.BATCH_UPDATE_DEVICE_STATS)
            stats["device_stats_updated"] = res[0].get("devices_updated", 0) if res else 0
        except Exception as exc:  # noqa: BLE001
            logger.warning("Device stats update failed: %s", exc)

        # ── Phase 1b: Flag dormant accounts ──
        try:
            res = await self.neo4j.run_async(
                CQ.QUERY_FLAG_DORMANT_ACCOUNTS,
                {"dormant_days": settings.DORMANT_DAYS_THRESHOLD},
            )
            stats["dormant_flagged"] = res[0].get("dormant_count", 0) if res else 0
        except Exception as exc:  # noqa: BLE001
            logger.warning("Dormant flagging failed: %s", exc)

        # ── Phase 2: GDS projection ──

        # Drop old projection (ignore if it doesn't exist)
        try:
            await self.neo4j.run_async(CQ.GDS_DROP_PROJECTION)
        except Exception:  # noqa: BLE001
            pass

        # Create fresh projection
        try:
            proj = await self.neo4j.run_async(CQ.GDS_CREATE_PROJECTION)
            if proj:
                stats["projection"] = proj[0]
                logger.info(
                    "  GDS projection: %d nodes, %d rels",
                    proj[0].get("nodeCount", 0),
                    proj[0].get("relationshipCount", 0),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("GDS projection failed: %s", exc)
            stats["projection_error"] = str(exc)
            # Still return stats from aggregation phase
            elapsed = time.perf_counter() - t0
            stats["elapsed_sec"] = round(elapsed, 3)
            return stats

        # ── Phase 3: GDS algorithms ──

        # Louvain community detection
        try:
            res = await self.neo4j.run_async(CQ.GDS_LOUVAIN)
            if res:
                stats["louvain"] = res[0]
                logger.info("  Louvain: %d communities", res[0].get("communityCount", 0))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Louvain failed: %s", exc)

        # Betweenness centrality
        try:
            res = await self.neo4j.run_async(CQ.GDS_BETWEENNESS)
            if res:
                stats["betweenness"] = res[0]
        except Exception as exc:  # noqa: BLE001
            logger.warning("Betweenness failed: %s", exc)

        # PageRank
        try:
            res = await self.neo4j.run_async(CQ.GDS_PAGERANK)
            if res:
                stats["pagerank"] = res[0]
        except Exception as exc:  # noqa: BLE001
            logger.warning("PageRank failed: %s", exc)

        # Local clustering coefficient
        try:
            res = await self.neo4j.run_async(CQ.GDS_LOCAL_CLUSTERING)
            if res:
                stats["clustering"] = res[0]
        except Exception as exc:  # noqa: BLE001
            logger.warning("Local clustering failed: %s", exc)

        # ── Phase 4: Collusive pattern refresh + relay mule ──
        try:
            detect_counts = await self.collusive.refresh()
            stats["detection"] = detect_counts
        except Exception as exc:  # noqa: BLE001
            logger.warning("Collusive detection refresh failed: %s", exc)

        elapsed = time.perf_counter() - t0
        stats["elapsed_sec"] = round(elapsed, 3)
        logger.info("📊 Graph analytics cycle complete in %.1f s", elapsed)
        return stats
