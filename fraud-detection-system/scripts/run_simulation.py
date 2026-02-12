#!/usr/bin/env python3
"""
run_simulation.py – Start the 500-TPS transaction simulator against
a running backend (or directly against Redis + Neo4j).

Usage:
    python scripts/run_simulation.py                    # default 10k tx
    python scripts/run_simulation.py --tx 5000 --tps 200
"""

import sys
import os
import argparse
import asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from app.config import settings
from app.streaming.redis_stream import get_redis_client
from app.streaming.transaction_simulator import TransactionSimulator


async def run(total_tx: int, tps: int):
    print(f"🔗 Connecting to Redis at {settings.REDIS_HOST}:{settings.REDIS_PORT} …")
    redis_client = await get_redis_client()
    print("✅ Connected\n")

    sim = TransactionSimulator(redis_client)
    print(f"🚀 Simulating {total_tx} transactions at {tps} TPS …\n")

    sent = await sim.run(total_tx=total_tx, tps=tps)

    print(f"\n📊 Summary:")
    print(f"   Transactions sent: {sent}")

    # Check stream length
    stream_len = await redis_client.xlen(settings.REDIS_STREAM_KEY)
    print(f"   Stream length:     {stream_len}")

    await redis_client.close()
    print("\n✅ Simulation complete!")


def main():
    parser = argparse.ArgumentParser(description="Run fraud transaction simulation")
    parser.add_argument("--tx", type=int, default=settings.SIMULATION_TOTAL_TX, help="Total transactions")
    parser.add_argument("--tps", type=int, default=settings.SIMULATION_TPS, help="Transactions per second")
    args = parser.parse_args()

    asyncio.run(run(args.tx, args.tps))


if __name__ == "__main__":
    main()
