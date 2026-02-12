"""
Behavioural feature extraction.

Computes per-transaction anomaly signals from the sender's recent history
stored in Neo4j.  Returns a 0–100 behavioural risk score.

Features
────────
• amount_zscore          – how many σ the current amount deviates
• rolling_mean / std     – 25-tx rolling stats
• time_since_last_tx     – seconds since previous transaction
• velocity_score         – tx/min in the recent window
• geo_distance           – km from last known location
• impossible_travel_flag – travel speed > 250 km/h
• ip_risk_score          – cloud ASN + reuse density
• sim_not_verified       – SIM verification risk
• night_anomaly_flag     – tx between 23:00 – 05:00
• mahalanobis_distance   – multivariate deviation from baseline
"""

from __future__ import annotations

import math
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import numpy as np

from app.neo4j_manager import Neo4jManager
from app.utils.cypher_queries import QUERY_USER_TX_HISTORY, QUERY_USER_PROFILE
from app.features.asn_intelligence import compute_asn_risk
from app.config import settings
from app.models.transaction import TransactionChannel

logger = logging.getLogger(__name__)


# ── helpers ──────────────────────────────────────────────────

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points on Earth."""
    R = 6371.0  # Earth radius km
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    Δφ = math.radians(lat2 - lat1)
    Δλ = math.radians(lon2 - lon1)
    a = math.sin(Δφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(Δλ / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _mahalanobis(x: np.ndarray, mean: np.ndarray, cov_inv: np.ndarray) -> float:
    """Mahalanobis distance of x from the given distribution."""
    diff = x - mean
    left = diff @ cov_inv
    return float(np.sqrt(left @ diff))


# ── main class ───────────────────────────────────────────────

class BehavioralFeatureExtractor:
    """Extract behavioural anomaly features for a single transaction."""

    def __init__(self, neo4j: Neo4jManager) -> None:
        self.neo4j = neo4j

    async def compute(
        self,
        sender_id: str,
        amount: float,
        timestamp: datetime,
        sender_lat: Optional[float] = None,
        sender_lon: Optional[float] = None,
        channel: TransactionChannel = TransactionChannel.UPI,
        ip_address: Optional[str] = None,
        sim_verified: Optional[bool] = None,
    ) -> Dict:
        """Return a dict of feature values + a fused behavioural risk 0–100."""

        # Fetch recent history + profile in parallel-ish (same event loop)
        history = await self.neo4j.read_async(
            QUERY_USER_TX_HISTORY,
            {"user_id": sender_id, "limit": settings.BEHAVIORAL_HISTORY_COUNT},
        )
        profile_rows = await self.neo4j.read_async(
            QUERY_USER_PROFILE, {"user_id": sender_id}
        )
        profile = profile_rows[0] if profile_rows else {}

        amounts: List[float] = [r["amount"] for r in history if r.get("amount")]
        timestamps: List[datetime] = [r["timestamp"] for r in history if r.get("timestamp")]

        # ── amount features (UPI-only, 3σ rule for spike detection) ────
        # Primary: use rolling history; Fallback: use stored profile stats
        profile_mean = profile.get("avg_tx_amount") or 0.0
        profile_std = profile.get("std_tx_amount") or 0.0

        is_upi = channel == TransactionChannel.UPI
        if not is_upi:
            amount_zscore = 0.0
            rolling_mean = profile_mean or amount
            rolling_std = profile_std or 0.0
            spike = False
        elif len(amounts) >= 2:
            mean_a = float(np.mean(amounts))
            std_a = float(np.std(amounts)) or 1.0
            amount_zscore = (amount - mean_a) / std_a
            rolling_mean = mean_a
            rolling_std = std_a
            spike = amount > mean_a + 3 * std_a
        elif profile_mean > 0:
            # Thin history → use stored behavioral baseline
            mean_a = profile_mean
            std_a = profile_std if profile_std > 0 else profile_mean * 0.5
            amount_zscore = (amount - mean_a) / std_a
            rolling_mean = mean_a
            rolling_std = std_a
            spike = amount > mean_a + 3 * std_a
        else:
            amount_zscore = 0.0
            rolling_mean = amount
            rolling_std = 0.0
            spike = False

        # Dormant-burst cross-signal: dormant + any amount > avg → high risk
        is_dormant = profile.get("is_dormant", False)
        dormant_burst = is_dormant and profile_mean > 0 and amount > profile_mean

        # ── ASN intelligence (MMDB-based Indian IPv4 classification) ──
        asn_result: Dict = {}
        asn_risk_scaled = 0.0
        if ip_address:
            asn_result = await compute_asn_risk(sender_id, ip_address, self.neo4j)
            asn_risk_scaled = asn_result.get("asn_risk_scaled", 0.0)

        # ── SIM verification awareness ─────────────────────
        sim_not_verified = sim_verified is False

        # ── temporal features ────────────────────────────────
        if timestamps:
            last_ts = timestamps[0]
            if isinstance(last_ts, datetime):
                time_since_last = max((timestamp - last_ts).total_seconds(), 0)
            else:
                time_since_last = 0.0
        else:
            time_since_last = 0.0

        # velocity = tx in last 60 s
        recent_count = sum(
            1
            for ts in timestamps
            if isinstance(ts, datetime)
            and (timestamp - ts).total_seconds() <= settings.VELOCITY_WINDOW_SEC
        )
        velocity_score = min(recent_count / max(settings.BURST_TX_THRESHOLD, 1), 1.0)

        # night-time flag
        hour = timestamp.hour
        night_flag = hour >= settings.NIGHT_START_HOUR or hour <= settings.NIGHT_END_HOUR

        # ── geo features ─────────────────────────────────────
        geo_distance = 0.0
        impossible_travel = False
        last_lat = profile.get("last_lat")
        last_lon = profile.get("last_lon")
        if sender_lat and sender_lon and last_lat and last_lon:
            geo_distance = _haversine_km(last_lat, last_lon, sender_lat, sender_lon)
            if time_since_last > 0:
                speed_kmh = geo_distance / (time_since_last / 3600)
                impossible_travel = speed_kmh > settings.IMPOSSIBLE_TRAVEL_KMH

        # ── Mahalanobis distance ─────────────────────────────
        mahal_distance = 0.0
        if len(amounts) >= 5:
            try:
                feat_matrix = np.column_stack(
                    [
                        amounts[: min(len(amounts), len(timestamps))],
                        [
                            (timestamps[i] - timestamps[i + 1]).total_seconds()
                            if i + 1 < len(timestamps)
                            and isinstance(timestamps[i], datetime)
                            and isinstance(timestamps[i + 1], datetime)
                            else 0
                            for i in range(min(len(amounts), len(timestamps)))
                        ],
                    ]
                )
                mean_vec = np.mean(feat_matrix, axis=0)
                cov = np.cov(feat_matrix, rowvar=False)
                cov_inv = np.linalg.pinv(cov)
                x_vec = np.array([amount, time_since_last])
                mahal_distance = _mahalanobis(x_vec, mean_vec, cov_inv)
            except Exception:  # noqa: BLE001
                mahal_distance = 0.0

        # ── fuse into 0–100 risk ─────────────────────────────
        risk = 0.0
        risk += min(abs(amount_zscore) * 10, 30)          # up to 30 (3σ rule, UPI-only)
        risk += velocity_score * 20                        # up to 20
        risk += (1.0 if impossible_travel else 0.0) * 20   # 0 or 20
        risk += (1.0 if night_flag else 0.0) * 5           # 0 or 5
        risk += min(mahal_distance * 2, 15)                 # up to 15
        risk += (1.0 if spike else 0.0) * 10               # 0 or 10 (statistical outlier, UPI-only)
        risk += (1.0 if dormant_burst else 0.0) * 15       # 0 or 15 (dormant + above avg)
        risk += asn_risk_scaled                             # 0–20 (MMDB ASN intelligence)
        risk += (10.0 if sim_not_verified else 0.0)        # 0 or 10
        risk = min(risk, 100.0)

        flags = []
        if spike:
            flags.append(f"Amount spike: {amount_zscore:.1f}σ above baseline")
        if dormant_burst:
            flags.append("Dormant Burst: tx amount exceeds historical avg")
        if impossible_travel:
            flags.append(f"Impossible travel: {geo_distance:.0f}km")
        if night_flag:
            flags.append("Night-time transaction")
        if sim_not_verified:
            flags.append("SIM not verified")
        if asn_result.get("asn_risk", 0) >= 0.5:
            flags.append(f"ASN Risk ({asn_result.get('asn_class', 'UNKNOWN')}): score={asn_result['asn_risk']:.2f}")
        if asn_result.get("foreign_flag"):
            flags.append(f"Foreign IP: {asn_result.get('org_name', '?')} ({asn_result.get('country', '?')})")
        if asn_result.get("asn_drift"):
            flags.append("ASN Drift: IP network differs from user's usual pattern")

        features = {
            "amount_zscore": round(amount_zscore, 4),
            "rolling_mean": round(rolling_mean, 2),
            "rolling_std": round(rolling_std, 2),
            "time_since_last_tx": round(time_since_last, 2),
            "velocity_score": round(velocity_score, 4),
            "geo_distance_km": round(geo_distance, 2),
            "impossible_travel": impossible_travel,
            "is_night": night_flag,
            "spike_flag": spike,
            "dormant_burst": dormant_burst,
            "ip_risk_score": round(asn_risk_scaled, 2),
            "asn_risk": asn_result.get("asn_risk", 0.0),
            "asn_risk_scaled": round(asn_risk_scaled, 2),
            "asn_class": asn_result.get("asn_class", "UNKNOWN"),
            "asn_country": asn_result.get("country", ""),
            "foreign_flag": asn_result.get("foreign_flag", 0),
            "asn_drift": asn_result.get("asn_drift", 0),
            "asn_entropy": asn_result.get("asn_entropy", 0.0),
            "asn_density": asn_result.get("asn_density", 0.0),
            "asn_base": asn_result.get("asn_base", 0.0),
            "sim_not_verified": sim_not_verified,
            "mahalanobis_distance": round(mahal_distance, 4),
            "risk": round(risk, 2),
            "flags": flags,
        }
        return features
