"""Async Redis pub/sub listener for progress events."""
from __future__ import annotations

import asyncio
import json
import logging

from deid.config.task_models import ProgressEvent

logger = logging.getLogger("deid.orchestrator")


async def listen_progress(redis_url: str):
    """Async generator yielding progress events from Redis pub/sub."""
    assert redis_url, "redis_url must not be empty"

    import redis.asyncio as aioredis

    r = aioredis.from_url(redis_url)
    pubsub = r.pubsub()
    await pubsub.subscribe("deid:progress")

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                data = json.loads(message["data"])
                ProgressEvent(**data)
                yield data
    finally:
        await pubsub.unsubscribe("deid:progress")
        await r.aclose()
