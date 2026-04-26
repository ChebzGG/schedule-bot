import os
import logging
import redis
from typing import List, Set

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, redis_url: str = None):
        self.redis_url = redis_url or os.getenv('UPSTASH_REDIS_URL')
        if not self.redis_url:
            raise ValueError("UPSTASH_REDIS_URL не задан")

        self.client = redis.from_url(
            self.redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            health_check_interval=30
        )

        self.subscribers_key = "bot:subscribers"
        self.processed_key = "bot:processed_emails"

        # Локальный кэш хешей писем — не дергаем Redis на каждую проверку
        self._processed_cache: Set[str] = set()
        self._load_processed_cache()

        logger.info("Redis Database инициализирован")

    def _load_processed_cache(self):
        """Загружаем существующие хеши в память при старте бота"""
        try:
            self._processed_cache = set(self.client.smembers(self.processed_key))
            logger.info(f"Загружено {len(self._processed_cache)} хешей в локальный кэш")
        except Exception as e:
            logger.error(f"Ошибка загрузки хешей в кэш: {e}")

    # === Подписчики (Redis Set) ===
    def add_subscriber(self, chat_id: int) -> bool:
        try:
            return self.client.sadd(self.subscribers_key, str(chat_id)) == 1
        except Exception as e:
            logger.error(f"Ошибка добавления подписчика: {e}")
            return False

    def remove_subscriber(self, chat_id: int) -> bool:
        try:
            return self.client.srem(self.subscribers_key, str(chat_id)) == 1
        except Exception as e:
            logger.error(f"Ошибка удаления подписчика: {e}")
            return False

    def is_subscriber(self, chat_id: int) -> bool:
        try:
            return self.client.sismember(self.subscribers_key, str(chat_id))
        except Exception as e:
            logger.error(f"Ошибка проверки подписчика: {e}")
            return False

    def get_subscribers(self) -> List[int]:
        try:
            members = self.client.smembers(self.subscribers_key)
            return [int(m) for m in members]
        except Exception as e:
            logger.error(f"Ошибка получения подписчиков: {e}")
            return []

    # === Обработанные письма (Redis Set + локальный кэш) ===
    def is_email_processed(self, email_hash: str) -> bool:
        # Проверяем локальный кэш — мгновенно, без сети и без расхода лимита
        return email_hash in self._processed_cache

    def mark_email_processed(self, email_hash: str):
        if email_hash in self._processed_cache:
            return

        self._processed_cache.add(email_hash)

        try:
            self.client.sadd(self.processed_key, email_hash)
            # Оставляем в Redis только последние 250 хешей
            count = self.client.scard(self.processed_key)
            if count > 250:
                excess = count - 250
                removed = self.client.spop(self.processed_key, excess)
                if removed:
                    for h in removed:
                        self._processed_cache.discard(h)
        except Exception as e:
            logger.error(f"Ошибка сохранения хеша письма: {e}")

    def get_processed_count(self) -> int:
        try:
            return self.client.scard(self.processed_key)
        except Exception as e:
            logger.error(f"Ошибка подсчёта хешей: {e}")
            return len(self._processed_cache)

    # === Статистика ===
    def get_stats(self):
        return {
            'subscribers_count': len(self.get_subscribers()),
            'processed_emails': self.get_processed_count()
        }