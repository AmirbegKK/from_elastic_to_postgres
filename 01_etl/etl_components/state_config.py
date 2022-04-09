from typing import Any

from .config import REDIS_HOST, REDIS_PORT
from redis import Redis


class RedisStorage:
    def __init__(self):
        self.redis_adapter = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    def save_state(self, key: str, value: Any) -> None:
        self.redis_adapter.set(key, value)

    def retrieve_state(self) -> dict:
        return self.redis_adapter


class State:

    def __init__(self, storage: RedisStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        self.storage.save_state(key, value)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        state = self.storage.retrieve_state()
        return state.get(key)
