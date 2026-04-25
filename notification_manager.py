import logging
from datetime import datetime
from typing import Set, List, Dict
import os
import json
import httpx

logger = logging.getLogger(__name__)


class NotificationManager:
    def __init__(self):
        self.supabase_url = os.getenv('SUPABASE_URL', '').rstrip('/')
        self.supabase_key = os.getenv('SUPABASE_KEY', '')

        self.rest_url = f"{self.supabase_url}/rest/v1" if self.supabase_url else None
        self.headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }

        self.client = None
        if self.supabase_url and self.supabase_key:
            try:
                self.client = httpx.Client(timeout=10.0, headers=self.headers)
                resp = self.client.get(f"{self.rest_url}/subscribers?select=chat_id&limit=1")
                if resp.status_code in (200, 401, 403, 406):
                    logger.info("✅ Подключение к Supabase REST API установлено")
                else:
                    logger.warning(f"⚠️ Supabase вернул статус {resp.status_code}")
            except Exception as e:
                logger.error(f"❌ Ошибка подключения к Supabase: {e}")
                self.client = None

        self.fallback_file = 'cache/subscribers_fallback.json'
        os.makedirs(os.path.dirname(self.fallback_file) or '.', exist_ok=True)

        self._subscribers_cache: Set[int] = set()
        self._processed_emails_cache: Set[str] = set()
        self._cache_loaded = False

    def _load_fallback(self):
        try:
            if os.path.exists(self.fallback_file):
                with open(self.fallback_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    fallback_subs = set(data.get('subscribers', []))
                    if fallback_subs:
                        logger.info(f"📁 Загружено {len(fallback_subs)} подписчиков из fallback")
                        return fallback_subs
        except Exception as e:
            logger.error(f"Ошибка загрузки fallback: {e}")
        return set()

    def _save_fallback(self):
        try:
            with open(self.fallback_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'subscribers': sorted(list(self._subscribers_cache)),
                    'updated_at': datetime.now().isoformat()
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения fallback: {e}")

    def _load_to_cache(self):
        if self._cache_loaded:
            return

        if self.client:
            try:
                resp = self.client.get(f"{self.rest_url}/subscribers?select=chat_id")
                if resp.status_code == 200:
                    self._subscribers_cache = set(row['chat_id'] for row in resp.json())
                    logger.info(f"📥 Загружено {len(self._subscribers_cache)} подписчиков из Supabase")
                else:
                    logger.warning(f"⚠️ Supabase subscribers: статус {resp.status_code}")

                resp = self.client.get(
                    f"{self.rest_url}/processed_emails?select=hash&order=processed_at.desc&limit=100"
                )
                if resp.status_code == 200:
                    self._processed_emails_cache = set(row['hash'] for row in resp.json())
                    logger.info(f"📥 Загружено {len(self._processed_emails_cache)} хешей писем")
                else:
                    logger.warning(f"⚠️ Supabase emails: статус {resp.status_code}")

                self._cache_loaded = True
                self._save_fallback()
                return
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки из Supabase: {e}")

        logger.warning("⚠️ Используем локальный fallback для подписчиков")
        self._subscribers_cache = self._load_fallback()
        self._processed_emails_cache = set()
        self._cache_loaded = True

    def add_subscriber(self, chat_id: int) -> bool:
        self._load_to_cache()

        if chat_id in self._subscribers_cache:
            return False

        if self.client:
            try:
                resp = self.client.post(
                    f"{self.rest_url}/subscribers",
                    json={"chat_id": chat_id, "created_at": datetime.now().isoformat()}
                )
                if resp.status_code in (201, 200, 204, 409):
                    logger.info(f"✅ Добавлен подписчик {chat_id} в Supabase")
                else:
                    logger.warning(f"⚠️ Supabase вернул {resp.status_code} при добавлении")
            except Exception as e:
                logger.error(f"❌ Ошибка добавления в Supabase: {e}")

        self._subscribers_cache.add(chat_id)
        self._save_fallback()
        return True

    def remove_subscriber(self, chat_id: int) -> bool:
        self._load_to_cache()

        if chat_id not in self._subscribers_cache:
            return False

        if self.client:
            try:
                resp = self.client.delete(f"{self.rest_url}/subscribers?chat_id=eq.{chat_id}")
                if resp.status_code in (200, 204):
                    logger.info(f"✅ Удален подписчик {chat_id} из Supabase")
                else:
                    logger.warning(f"⚠️ Supabase вернул {resp.status_code} при удалении")
            except Exception as e:
                logger.error(f"❌ Ошибка удаления из Supabase: {e}")

        self._subscribers_cache.discard(chat_id)
        self._save_fallback()
        return True

    def is_subscriber(self, chat_id: int) -> bool:
        self._load_to_cache()
        return chat_id in self._subscribers_cache

    def get_subscribers(self) -> List[int]:
        self._load_to_cache()
        return list(self._subscribers_cache)

    def is_email_processed(self, email_hash: str) -> bool:
        self._load_to_cache()
        return email_hash in self._processed_emails_cache

    def mark_email_processed(self, email_hash: str):
        if not self.client:
            return

        if email_hash in self._processed_emails_cache:
            return

        try:
            resp = self.client.post(
                f"{self.rest_url}/processed_emails",
                json={"hash": email_hash, "processed_at": datetime.now().isoformat()}
            )
            if resp.status_code in (201, 200, 204, 409):
                self._processed_emails_cache.add(email_hash)
            else:
                logger.warning(f"⚠️ Supabase вернул {resp.status_code} при сохранении хеша")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения хеша: {e}")

    def get_stats(self) -> Dict:
        self._load_to_cache()
        return {
            'subscribers_count': len(self._subscribers_cache),
            'processed_emails': len(self._processed_emails_cache)
        }

    def sync_to_supabase(self):
        if not self.client:
            logger.warning("Supabase недоступен, синхронизация невозможна")
            return

        fallback_subs = self._load_fallback()
        if not fallback_subs:
            return

        synced = 0
        for chat_id in fallback_subs:
            try:
                resp = self.client.post(
                    f"{self.rest_url}/subscribers",
                    json={"chat_id": chat_id, "created_at": datetime.now().isoformat()}
                )
                if resp.status_code in (201, 200, 204, 409):
                    synced += 1
            except Exception as e:
                logger.warning(f"Ошибка синхронизации {chat_id}: {e}")

        if synced > 0:
            logger.info(f"🔄 Синхронизировано {synced} подписчиков в Supabase")