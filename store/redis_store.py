import json
import time
from typing import Any

import redis

from .interface import KeyValueStore


class RedisStore(KeyValueStore):
    def __init__(self, url: str, default_ttl_seconds: int = 86400) -> None:
        self._client = redis.Redis.from_url(url)
        self._default_ttl_seconds = max(1, int(default_ttl_seconds))

    def get(self, key: str) -> tuple[float, Any] | None:
        # Use pipeline to fetch value and TTL together
        with self._client.pipeline() as pipe:
            pipe.get(key)
            pipe.pttl(key)
            val_bytes, ttl_ms = pipe.execute()

        # Missing or expired
        if val_bytes is None or ttl_ms is None or int(ttl_ms) <= 0:
            return None

        # Decode JSON
        try:
            value = json.loads(val_bytes)
        except Exception:
            # If decoding fails, treat as miss
            return None

        expiry_epoch = time.time() + (int(ttl_ms) / 1000.0)
        return expiry_epoch, value

    def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        val_str = json.dumps(value, separators=(",", ":"))
        ttl = (
            self._default_ttl_seconds
            if not ttl_seconds or ttl_seconds <= 0
            else int(ttl_seconds)
        )
        self._client.setex(key, ttl, val_str)
