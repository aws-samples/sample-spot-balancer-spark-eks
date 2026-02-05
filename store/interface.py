from typing import Any


class KeyValueStore:
    def get(self, key: str) -> tuple[float, Any] | None:
        """
        Return (expiry_epoch_seconds, value) if present and not expired; otherwise None.
        """
        raise NotImplementedError

    def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        """
        Store value with expiry now + ttl_seconds.
        """
        raise NotImplementedError
