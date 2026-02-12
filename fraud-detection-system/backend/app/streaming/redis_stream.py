"""
Redis Streams producer / consumer helpers.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

import redis.asyncio as aioredis

from app.config import settings

logger = logging.getLogger(__name__)


async def get_redis_client() -> aioredis.Redis:
    """Create and return an async Redis client."""
    client = aioredis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        decode_responses=False,
    )
    await client.ping()
    logger.info("✅ Redis connected at %s:%d", settings.REDIS_HOST, settings.REDIS_PORT)
    return client


async def publish_transaction(
    redis_client: aioredis.Redis,
    tx_data: Dict[str, Any],
) -> str:
    """
    Push a transaction onto the Redis stream.
    Returns the stream message ID.
    """
    payload = json.dumps(tx_data, default=str)
    msg_id = await redis_client.xadd(
        settings.REDIS_STREAM_KEY,
        {"payload": payload},
    )
    return msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)


async def publish_alert(
    redis_client: aioredis.Redis,
    alert_data: Dict[str, Any],
) -> int:
    """
    Publish a fraud alert on the pub/sub channel.
    Returns the number of subscribers that received the message.
    """
    payload = json.dumps(alert_data, default=str)
    return await redis_client.publish(settings.REDIS_ALERTS_CHANNEL, payload)


async def stream_length(redis_client: aioredis.Redis) -> int:
    """Return the current length of the transaction stream."""
    return await redis_client.xlen(settings.REDIS_STREAM_KEY)
