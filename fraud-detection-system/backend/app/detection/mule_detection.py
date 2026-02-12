"""
Mule account detection – optimised with relay mule integration.

Combines multiple feature signals to classify a user as a potential
money mule.  A mule typically:
  • Has a dormant or newly-created account (first-strike pattern)
  • Receives large inflows and quickly redistributes them
  • Uses a shared or emulated device
  • Is embedded in a high-risk graph cluster
  • Acts as a high-velocity relay (outflow ≈ inflow in short window)

This module doesn't compute its own sub-score – it aggregates the
outputs of the five feature extractors and applies heuristic rules
to generate explicit mule flags.
"""

from __future__ import annotations

import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

# Thresholds (could be tuned or moved to config)
_MULE_RISK_THRESHOLD = 65
_PASSTHROUGH_THRESHOLD = 0.75
_DEVICE_SHARE_THRESHOLD = 3
_DORMANT_DAYS_THRESHOLD = 20


class MuleDetector:
    """Heuristic mule-classification on top of feature vectors."""

    def evaluate(
        self,
        behavioral: Dict,
        dead_account: Dict,
        device: Dict,
        graph: Dict,
        velocity: Dict,
        fused_risk: float,
    ) -> Dict:
        """
        Return:
            is_mule : bool
            confidence : float 0-1
            reasons : List[str]
        """
        reasons: List[str] = []
        score = 0.0  # accumulator 0 → 1

        # ── first-strike dormant activation ──────────────────
        if dead_account.get("is_first_strike"):
            score += 0.30
            days = dead_account.get("days_slept") or dead_account.get("days_inactive", 0)
            reasons.append(f"First-strike: dormant {int(days)}d → suddenly active")
        elif dead_account.get("is_dormant") and dead_account.get("risk", 0) > 40:
            score += 0.25
            reasons.append("Dormant account activated with suspicious inflow")

        # ── high pass-through (relay pattern) ────────────────
        pt_ratio = velocity.get("outflow_inflow_ratio", 0)
        if pt_ratio > _PASSTHROUGH_THRESHOLD:
            score += 0.20
            reasons.append(f"High pass-through ratio ({pt_ratio:.2f})")

        # ── shared / emulated device ─────────────────────────
        if device.get("account_count", 0) >= _DEVICE_SHARE_THRESHOLD:
            score += 0.15
            reasons.append(
                f"Device shared across {device['account_count']} accounts"
            )
        if device.get("is_emulator"):
            score += 0.10
            reasons.append("Transaction from emulator")

        # ── graph cluster membership ─────────────────────────
        if graph.get("community_risk", 0) > 50:
            score += 0.15
            reasons.append(
                f"Member of high-risk cluster (risk={graph['community_risk']:.0f})"
            )

        # ── relay mule flag from collusive detector ──────────
        # Check if velocity indicates relay behaviour even without
        # the collusive detector's cached result
        if velocity.get("tx_per_min", 0) > 5 and pt_ratio > 0.6:
            score += 0.10
            reasons.append(
                f"Relay pattern: {velocity['tx_per_min']:.1f} tx/min, "
                f"ratio={pt_ratio:.2f}"
            )

        # ── behavioural anomaly ──────────────────────────────
        if behavioral.get("impossible_travel"):
            score += 0.10
            reasons.append("Impossible travel detected")
        if behavioral.get("spike_flag"):
            score += 0.05
            reasons.append("Amount spike vs historical baseline")

        score = min(score, 1.0)
        is_mule = score >= 0.5 or fused_risk >= _MULE_RISK_THRESHOLD

        return {
            "is_mule": is_mule,
            "confidence": round(score, 3),
            "reasons": reasons,
        }
