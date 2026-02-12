#!/usr/bin/env python3
"""
seed_data.py – Populate Neo4j with synthetic users, devices, and a
baseline transaction history so the detection engine has data to
work with before the simulator runs.

Usage:
    python scripts/seed_data.py                 # from project root
    python scripts/seed_data.py --clear         # wipe DB first
"""

import sys
import os
import argparse
import random
import uuid
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from neo4j import GraphDatabase
from app.config import settings
from app.utils.cypher_queries import (
    SCHEMA_CONSTRAINTS,
    SCHEMA_INDEXES,
    MAINT_CLEAR_ALL,
    MAINT_COUNT_NODES,
)

# ── constants ────────────────────────────────────────────────

USER_COUNT = settings.SIMULATION_USER_COUNT       # 500
DEVICE_COUNT = settings.SIMULATION_DEVICE_COUNT   # 300
MULE_RATIO = settings.SIMULATION_MULE_RATIO       # 0.08
SEED_TX_PER_USER = 8                               # baseline history

CITIES = [
    ("Mumbai", 19.076, 72.8777),
    ("Delhi", 28.7041, 77.1025),
    ("Bangalore", 12.9716, 77.5946),
    ("Hyderabad", 17.385, 78.4867),
    ("Chennai", 13.0827, 80.2707),
    ("Kolkata", 22.5726, 88.3639),
    ("Pune", 18.5204, 73.8567),
    ("Jaipur", 26.9124, 75.7873),
]

OS_OPTIONS = ["Android 13", "Android 14", "iOS 17", "iOS 16"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--clear", action="store_true", help="Clear DB before seeding")
    args = parser.parse_args()

    print(f"🔗 Connecting to Neo4j at {settings.NEO4J_URI} …")
    driver = GraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD),
    )
    driver.verify_connectivity()
    print("✅ Connected\n")

    with driver.session(database=settings.NEO4J_DATABASE) as session:
        if args.clear:
            print("🗑  Clearing database …")
            session.run(MAINT_CLEAR_ALL)
            print("   Done\n")

        # Schema
        print("📋 Ensuring schema …")
        for stmt in SCHEMA_CONSTRAINTS + SCHEMA_INDEXES:
            try:
                session.run(stmt)
            except Exception:
                pass
        print("   Done\n")

        # ── Users ────────────────────────────────────────────
        print(f"👤 Creating {USER_COUNT} users …")
        mule_count = int(USER_COUNT * MULE_RATIO)
        dormant_count = 10
        now = datetime.utcnow()

        for i in range(USER_COUNT):
            city, lat, lon = random.choice(CITIES)
            is_mule = i < mule_count
            is_dormant = mule_count <= i < mule_count + dormant_count

            created = now - timedelta(days=random.randint(60, 365))
            last_active = (
                now - timedelta(days=random.randint(35, 90))
                if is_dormant
                else now - timedelta(minutes=random.randint(1, 1440))
            )

            session.run(
                """
                MERGE (u:User {user_id: $uid})
                SET u.created_at    = datetime($created),
                    u.last_active   = datetime($last_active),
                    u.is_dormant    = $dormant,
                    u.risk_score    = 0.0,
                    u.tx_count      = 0,
                    u.total_outflow = 0.0,
                    u.avg_tx_amount = $avg_amt,
                    u.std_tx_amount = 0.0,
                    u.last_lat      = $lat,
                    u.last_lon      = $lon,
                    u.kyc_status    = 'VERIFIED',
                    u.is_mule_label = $is_mule
                """,
                {
                    "uid": f"U{i:04d}",
                    "created": created.isoformat(),
                    "last_active": last_active.isoformat(),
                    "dormant": is_dormant,
                    "avg_amt": round(random.uniform(300, 5000), 2),
                    "lat": lat + random.uniform(-0.05, 0.05),
                    "lon": lon + random.uniform(-0.05, 0.05),
                    "is_mule_label": is_mule,
                },
            )

        # ── Devices ──────────────────────────────────────────
        print(f"📱 Creating {DEVICE_COUNT} devices …")
        for i in range(DEVICE_COUNT):
            session.run(
                """
                MERGE (d:Device {device_hash: $hash})
                SET d.os           = $os,
                    d.model        = $model,
                    d.is_emulator  = $emu,
                    d.device_score = 0.0,
                    d.account_count = 0,
                    d.created_at   = datetime()
                """,
                {
                    "hash": f"DEV{i:04d}",
                    "os": random.choice(OS_OPTIONS),
                    "model": f"Model-{random.randint(1, 50)}",
                    "emu": random.random() < 0.03,
                },
            )

        # Shared device: link first 8 users to DEV0000
        print("   Linking shared device DEV0000 to 8 users …")
        for i in range(8):
            session.run(
                """
                MATCH (u:User {user_id: $uid}), (d:Device {device_hash: 'DEV0000'})
                MERGE (u)-[:USES_DEVICE]->(d)
                """,
                {"uid": f"U{i:04d}"},
            )

        # Normal device assignments
        print("   Assigning devices to remaining users …")
        for i in range(8, USER_COUNT):
            dev_idx = random.randint(1, DEVICE_COUNT - 1)
            session.run(
                """
                MATCH (u:User {user_id: $uid}), (d:Device {device_hash: $dev})
                MERGE (u)-[:USES_DEVICE]->(d)
                """,
                {"uid": f"U{i:04d}", "dev": f"DEV{dev_idx:04d}"},
            )

        # ── Seed transactions (baseline history) ─────────────
        total_tx = USER_COUNT * SEED_TX_PER_USER
        print(f"💸 Creating ~{total_tx} seed transactions …")

        batch = []
        for i in range(USER_COUNT):
            for _ in range(SEED_TX_PER_USER):
                receiver_idx = random.randint(0, USER_COUNT - 1)
                while receiver_idx == i:
                    receiver_idx = random.randint(0, USER_COUNT - 1)

                ts = now - timedelta(
                    days=random.randint(1, 30),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                )
                amount = round(random.uniform(100, 5000), 2)
                batch.append({
                    "sender": f"U{i:04d}",
                    "receiver": f"U{receiver_idx:04d}",
                    "tx_id": str(uuid.uuid4()),
                    "amount": amount,
                    "timestamp": ts.isoformat(),
                })

                # flush in batches of 500
                if len(batch) >= 500:
                    _flush_batch(session, batch)
                    batch.clear()

        if batch:
            _flush_batch(session, batch)

        # ── Counts ───────────────────────────────────────────
        print("\n📊 Node counts:")
        for row in session.run(MAINT_COUNT_NODES):
            r = row.data()
            print(f"   {r['label']}: {r['count']}")

    driver.close()
    print("\n✅ Seed data complete!")


def _flush_batch(session, batch):
    """Insert a batch of transactions via UNWIND."""
    session.run(
        """
        UNWIND $rows AS row
        MATCH (s:User {user_id: row.sender})
        MATCH (r:User {user_id: row.receiver})
        CREATE (tx:Transaction {
            tx_id: row.tx_id,
            amount: row.amount,
            timestamp: datetime(row.timestamp),
            channel: 'UPI',
            status: 'COMPLETED',
            risk_score: 0.0
        })
        CREATE (s)-[:SENT]->(tx)-[:RECEIVED_BY]->(r)
        MERGE (s)-[edge:TRANSFERRED_TO]->(r)
          ON CREATE SET edge.total_amount = row.amount,
                        edge.tx_count = 1,
                        edge.last_tx = datetime(row.timestamp)
          ON MATCH  SET edge.total_amount = edge.total_amount + row.amount,
                        edge.tx_count = edge.tx_count + 1,
                        edge.last_tx = datetime(row.timestamp)
        SET s.last_active = datetime(row.timestamp)
        """,
        {"rows": batch},
    )
    print(f"   … flushed {len(batch)} transactions")


if __name__ == "__main__":
    main()
