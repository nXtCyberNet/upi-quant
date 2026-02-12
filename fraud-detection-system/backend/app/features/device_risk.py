"""
Device-risk feature extraction.

Queries the Neo4j :Device node and its :USES_DEVICE relationships to
compute risk signals around shared devices, emulators, and risk
propagation from linked accounts.

Risk sub-score  S_device  ∈ [0, 100]
"""

from __future__ import annotations

import logging
from typing import Dict

from app.neo4j_manager import Neo4jManager
from app.utils.cypher_queries import (
    QUERY_DEVICE_INFO,
    QUERY_DEVICE_RISK_PROPAGATION,
)
from app.config import settings

logger = logging.getLogger(__name__)


class DeviceRiskExtractor:
    """Evaluate device-level fraud risk."""

    def __init__(self, neo4j: Neo4jManager) -> None:
        self.neo4j = neo4j

    async def compute(self, device_hash: str) -> Dict:
        """Return feature dict + fused device risk 0–100."""

        # ── basic device info ────────────────────────────────
        info_rows = await self.neo4j.read_async(
            QUERY_DEVICE_INFO, {"device_hash": device_hash}
        )
        if not info_rows:
            return {"device_hash": device_hash, "risk": 0.0, "flags": []}

        info = info_rows[0]
        account_count: int = info.get("account_count", 1) or 1
        is_emulator: bool = info.get("is_emulator", False)
        device_os = (info.get("os") or "").strip()

        # ── risk propagation from linked users ───────────────
        prop_rows = await self.neo4j.read_async(
            QUERY_DEVICE_RISK_PROPAGATION, {"device_hash": device_hash}
        )
        prop = prop_rows[0] if prop_rows else {}
        device_risk_score: float = prop.get("device_risk_score", 0)
        avg_user_risk: float = prop.get("avg_user_risk", 0) or 0
        max_user_risk: float = prop.get("max_user_risk", 0) or 0

        # ── scoring components ───────────────────────────────
        # 1. multi-account penalty  (up to 40)
        multi_account_score = 0.0
        if account_count >= settings.DEVICE_ACCOUNT_THRESHOLD:
            multi_account_score = 40.0
        elif account_count >= 3:
            multi_account_score = 25.0
        elif account_count >= 2:
            multi_account_score = 10.0

        # 2. emulator penalty (flat 25)
        emulator_score = 25.0 if is_emulator else 0.0

        # 3. risk propagation (up to 25)
        propagation_score = min(device_risk_score / 100.0, 1.0) * 25

        # 4. neighbour high-risk bonus (up to 10)
        high_risk_bonus = 10.0 if max_user_risk > 80 else 0.0

        # 5. OS anomaly (non-Android/iOS) (up to 15)
        os_anomaly_score = 0.0
        if device_os:
            os_lc = device_os.lower()
            if not (os_lc.startswith("android") or os_lc.startswith("ios")):
                os_anomaly_score = 15.0

        risk = (
            multi_account_score
            + emulator_score
            + propagation_score
            + high_risk_bonus
            + os_anomaly_score
        )
        risk = min(risk, 100.0)

        flags = []
        if account_count >= settings.DEVICE_ACCOUNT_THRESHOLD:
            flags.append(f"Shared Device: {account_count} accounts")
        if is_emulator:
            flags.append("Emulator Detected")
        if max_user_risk > 80:
            flags.append("Device Linked to High-Risk User")
        if os_anomaly_score > 0:
            flags.append(f"Unsupported Device OS: {device_os}")

        return {
            "device_hash": device_hash,
            "account_count": account_count,
            "is_emulator": is_emulator,
            "device_os": device_os,
            "avg_user_risk": round(avg_user_risk, 2),
            "max_user_risk": round(max_user_risk, 2),
            "multi_account_score": round(multi_account_score, 2),
            "emulator_score": round(emulator_score, 2),
            "propagation_score": round(propagation_score, 2),
            "os_anomaly_score": round(os_anomaly_score, 2),
            "risk": round(risk, 2),
            "flags": flags,
        }
