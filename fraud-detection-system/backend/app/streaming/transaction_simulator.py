"""
Transaction simulator.

Generates realistic-looking UPI transactions at a configurable TPS rate
and pushes them onto the Redis stream for the worker pool to consume.

Includes built-in fraud scenarios so the detection engine has something
to catch:
  • Mule rings   (circular A→B→C→A transfers)
  • Dormant activation  (sleeping accounts suddenly active)
  • Device-sharing  (one device, many accounts)
  • Rapid pass-through  (receive then immediately send)
  • Normal traffic  (baseline)
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis

from app.config import settings
from app.streaming.redis_stream import publish_transaction

logger = logging.getLogger(__name__)

# ── deterministic user / device pools ────────────────────────

_CITIES = [
    ("Mumbai", 19.076, 72.8777),
    ("Delhi", 28.7041, 77.1025),
    ("Bangalore", 12.9716, 77.5946),
    ("Hyderabad", 17.385, 78.4867),
    ("Chennai", 13.0827, 80.2707),
    ("Kolkata", 22.5726, 88.3639),
    ("Pune", 18.5204, 73.8567),
    ("Jaipur", 26.9124, 75.7873),
]

_OS_OPTIONS = ["Android 13", "Android 14", "iOS 17", "iOS 16", "Android 12", "Windows 11"]

_ASN_TYPES = ["MOBILE_ISP", "BROADBAND", "INDIAN_CLOUD", "FOREIGN"]


def _random_ip(asn_hint: str) -> str:
    """Generate a public IPv4 address.  Actual ASN resolved from MMDB at ingestion."""
    if asn_hint == "INDIAN_CLOUD":
        # AWS Mumbai / Indian DC ranges (public routable)
        return f"3.{random.choice([6, 7, 108])}.{random.randint(0, 255)}.{random.randint(1, 254)}"
    if asn_hint == "FOREIGN":
        # Known foreign IPs (Google, Cloudflare)
        return f"8.8.{random.randint(0, 8)}.{random.randint(1, 254)}"
    if asn_hint == "MOBILE_ISP":
        # Jio / Airtel ranges
        return f"49.{random.randint(32, 47)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
    # BROADBAND — BSNL / ACT ranges
    return f"59.{random.randint(88, 95)}.{random.randint(0, 255)}.{random.randint(1, 254)}"


def _generate_users(n: int) -> List[Dict[str, Any]]:
    users = []
    for i in range(n):
        city, lat, lon = random.choice(_CITIES)
        users.append({
            "user_id": f"U{i:04d}",
            "upi_id": f"user{i}@upi",
            "city": city,
            "lat": lat + random.uniform(-0.05, 0.05),
            "lon": lon + random.uniform(-0.05, 0.05),
            "avg_amount": random.uniform(200, 5000),
        })
    return users


def _generate_devices(n: int) -> List[Dict[str, Any]]:
    devices = []
    for i in range(n):
        devices.append({
            "device_hash": f"DEV{i:04d}",
            "os": random.choice(_OS_OPTIONS),
            "model": f"Model-{random.randint(1, 50)}",
            "is_emulator": random.random() < 0.03,
        })
    return devices


class TransactionSimulator:
    """Push simulated transactions onto the Redis stream."""

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self.redis = redis_client
        self.users = _generate_users(settings.SIMULATION_USER_COUNT)
        self.devices = _generate_devices(settings.SIMULATION_DEVICE_COUNT)

        # designate mule accounts
        mule_count = int(len(self.users) * settings.SIMULATION_MULE_RATIO)
        self.mule_ids = {u["user_id"] for u in self.users[:mule_count]}

        # shared devices (5+ users on same device)
        self.shared_device = self.devices[0]["device_hash"]

        # dormant accounts
        self.dormant_ids = {u["user_id"] for u in self.users[mule_count : mule_count + 10]}

        self._stop = False
        self.sent_count = 0

    # ── public API ───────────────────────────────────────────

    async def run(
        self,
        total_tx: int | None = None,
        tps: int | None = None,
    ) -> int:
        """
        Generate and publish transactions.
        Returns the number of transactions sent.
        """
        total_tx = total_tx or settings.SIMULATION_TOTAL_TX
        tps = tps or settings.SIMULATION_TPS
        delay = 1.0 / tps if tps > 0 else 0.002
        self.sent_count = 0
        self._stop = False

        logger.info("🚀 Simulator started: %d tx at %d TPS", total_tx, tps)
        t0 = time.time()

        for i in range(total_tx):
            if self._stop:
                break

            # Choose scenario
            roll = random.random()
            if roll < 0.08:
                tx = self._make_mule_ring_tx()
            elif roll < 0.14:
                tx = self._make_dormant_activation_tx()
            elif roll < 0.20:
                tx = self._make_shared_device_tx()
            elif roll < 0.26:
                tx = self._make_rapid_passthrough_tx()
            else:
                tx = self._make_normal_tx()

            await publish_transaction(self.redis, tx)
            self.sent_count += 1

            # throttle to target TPS
            if i % 50 == 0:
                elapsed = time.time() - t0
                expected = (i + 1) * delay
                if elapsed < expected:
                    await asyncio.sleep(expected - elapsed)

        elapsed = time.time() - t0
        actual_tps = self.sent_count / max(elapsed, 0.01)
        logger.info(
            "✅ Simulator done: %d tx in %.1fs (%.0f TPS)",
            self.sent_count, elapsed, actual_tps,
        )
        return self.sent_count

    def stop(self) -> None:
        self._stop = True

    # ── scenario generators ──────────────────────────────────

    def _base_tx(self, sender: Dict, receiver: Dict, amount: float, device_hash: str) -> Dict:
        device = next((d for d in self.devices if d["device_hash"] == device_hash), None)
        asn_type = random.choices(_ASN_TYPES, weights=[0.50, 0.30, 0.10, 0.10], k=1)[0]
        return {
            "tx_id": str(uuid.uuid4()),
            "sender_id": sender["user_id"],
            "receiver_id": receiver["user_id"],
            "amount": round(amount, 2),
            "timestamp": datetime.utcnow().isoformat(),
            "device_hash": device_hash,
            "device_os": device.get("os") if device else None,
            "device_model": device.get("model") if device else None,
            "device_is_emulator": device.get("is_emulator") if device else None,
            "ip_address": _random_ip(asn_type),
            "ip_asn_type": asn_type,
            "sim_verified": random.random() > 0.03,
            "sender_lat": sender["lat"],
            "sender_lon": sender["lon"],
            "channel": "UPI",
        }

    def _make_normal_tx(self) -> Dict:
        sender = random.choice(self.users)
        receiver = random.choice(self.users)
        while receiver["user_id"] == sender["user_id"]:
            receiver = random.choice(self.users)
        device = random.choice(self.devices)
        amount = max(random.gauss(sender["avg_amount"], sender["avg_amount"] * 0.3), 10)
        return self._base_tx(sender, receiver, amount, device["device_hash"])

    def _make_mule_ring_tx(self) -> Dict:
        """A→B→C ring with mule accounts, high amounts."""
        mule_list = [u for u in self.users if u["user_id"] in self.mule_ids]
        if len(mule_list) < 2:
            return self._make_normal_tx()
        sender, receiver = random.sample(mule_list, 2)
        amount = random.uniform(10000, 50000)  # suspiciously high
        device = random.choice(self.devices[:5])  # limited device pool
        return self._base_tx(sender, receiver, amount, device["device_hash"])

    def _make_dormant_activation_tx(self) -> Dict:
        """Dormant account suddenly sends large tx."""
        dormant_list = [u for u in self.users if u["user_id"] in self.dormant_ids]
        if not dormant_list:
            return self._make_normal_tx()
        sender = random.choice(dormant_list)
        receiver = random.choice(self.users)
        amount = random.uniform(20000, 100000)
        device = random.choice(self.devices)
        return self._base_tx(sender, receiver, amount, device["device_hash"])

    def _make_shared_device_tx(self) -> Dict:
        """Multiple users on the same device."""
        sender = random.choice(self.users)
        receiver = random.choice(self.users)
        while receiver["user_id"] == sender["user_id"]:
            receiver = random.choice(self.users)
        amount = random.uniform(500, 15000)
        return self._base_tx(sender, receiver, amount, self.shared_device)

    def _make_rapid_passthrough_tx(self) -> Dict:
        """User who just received money immediately sends it on."""
        mule_list = [u for u in self.users if u["user_id"] in self.mule_ids]
        if not mule_list:
            return self._make_normal_tx()
        sender = random.choice(mule_list)
        receiver = random.choice(self.users)
        amount = random.uniform(5000, 30000)
        device = random.choice(self.devices[:10])
        return self._base_tx(sender, receiver, amount, device["device_hash"])
