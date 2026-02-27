"""Async Redis pub/sub listener for progress events."""
from __future__ import annotations

import asyncio
import json
import logging

logger = logging.getLogger("deid.orchestrator")


async def listen_progress(redis_url: str):
    """Async generator yielding progress events from Redis pub/sub."""
    import redis.asyncio as aioredis

    r = aioredis.from_url(redis_url)
    pubsub = r.pubsub()
    await pubsub.subscribe("deid:progress")

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    yield json.loads(message["data"])
                except json.JSONDecodeError:
                    logger.warning("Invalid progress message: %s", message["data"])
    finally:
        await pubsub.unsubscribe("deid:progress")
        await r.aclose()
