import json
import os
import logging
from datetime import datetime
from typing import Set, List, Dict

logger = logging.getLogger(__name__)


class NotificationManager:
    def __init__(self, subscribers_file: str = 'cache/subscribers.json'):
        self.subscribers_file = subscribers_file
        self.processed_emails_file = 'cache/processed_emails.json'
        self.subscribers: Set[int] = set()
        self.processed_email_hashes: Set[str] = set()
        self._load_subscribers()
        self._load_processed_emails()

    def _load_subscribers(self):
        """Загружает список подписчиков"""
        try:
            if os.path.exists(self.subscribers_file):
                with open(self.subscribers_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.subscribers = set(data.get('subscribers', []))
                    logger.info(f"Загружено {len(self.subscribers)} подписчиков")
        except Exception as e:
            logger.error(f"Ошибка загрузки подписчиков: {e}")

    def _save_subscribers(self):
        """Сохраняет список подписчиков"""
        try:
            os.makedirs(os.path.dirname(self.subscribers_file), exist_ok=True)
            with open(self.subscribers_file, 'w', encoding='utf-8') as f:
                json.dump({'subscribers': list(self.subscribers)}, f)
        except Exception as e:
            logger.error(f"Ошибка сохранения подписчиков: {e}")

    def _load_processed_emails(self):
        """Загружает хеши уже обработанных писем"""
        try:
            if os.path.exists(self.processed_emails_file):
                with open(self.processed_emails_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Храним только последние 100 хешей
                    self.processed_email_hashes = set(data.get('hashes', [])[-100:])
        except Exception as e:
            logger.error(f"Ошибка загрузки обработанных писем: {e}")

    def _save_processed_emails(self):
        """Сохраняет хеши обработанных писем"""
        try:
            # Оставляем только последние 100
            hashes = list(self.processed_email_hashes)[-100:]
            with open(self.processed_emails_file, 'w', encoding='utf-8') as f:
                json.dump({'hashes': hashes, 'last_updated': datetime.now().isoformat()}, f)
        except Exception as e:
            logger.error(f"Ошибка сохранения обработанных писем: {e}")

    def add_subscriber(self, chat_id: int) -> bool:
        """Добавляет подписчика"""
        if chat_id not in self.subscribers:
            self.subscribers.add(chat_id)
            self._save_subscribers()
            logger.info(f"Добавлен подписчик: {chat_id}")
            return True
        return False

    def remove_subscriber(self, chat_id: int) -> bool:
        """Удаляет подписчика"""
        if chat_id in self.subscribers:
            self.subscribers.discard(chat_id)
            self._save_subscribers()
            logger.info(f"Удален подписчик: {chat_id}")
            return True
        return False

    def is_subscriber(self, chat_id: int) -> bool:
        """Проверяет, является ли пользователь подписчиком"""
        return chat_id in self.subscribers

    def get_subscribers(self) -> List[int]:
        """Возвращает список подписчиков"""
        return list(self.subscribers)

    def is_email_processed(self, email_hash: str) -> bool:
        """Проверяет, было ли письмо уже обработано"""
        return email_hash in self.processed_email_hashes

    def mark_email_processed(self, email_hash: str):
        """Отмечает письмо как обработанное"""
        self.processed_email_hashes.add(email_hash)
        self._save_processed_emails()

    def get_stats(self) -> Dict:
        """Возвращает статистику"""
        return {
            'subscribers_count': len(self.subscribers),
            'processed_emails': len(self.processed_email_hashes)
        }