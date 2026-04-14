import logging
from datetime import datetime
from typing import Set, List, Dict
from supabase import create_client, Client
import os

logger = logging.getLogger(__name__)


class NotificationManager:
    def __init__(self):
        # Supabase конфигурация из env
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_KEY')
        self.supabase: Client = None

        # Локальный кэш для быстрого доступа
        self._subscribers_cache: Set[int] = set()
        self._processed_emails_cache: Set[str] = set()
        self._cache_loaded = False

        self._init_supabase()
        self._ensure_tables_exist()

    def _init_supabase(self):
        """Инициализирует подключение к Supabase"""
        if not self.supabase_url or not self.supabase_key:
            logger.error("SUPABASE_URL или SUPABASE_KEY не заданы!")
            return

        try:
            # Новый API для supabase-py 2.x
            from supabase.client import ClientOptions

            options = ClientOptions(
                schema="public",
                headers={},
                auto_refresh_token=True,
                persist_session=True
            )

            self.supabase = create_client(
                self.supabase_url,
                self.supabase_key,
                options=options
            )
            logger.info("✅ Подключение к Supabase установлено")
        except ImportError:
            # Fallback для старых версий
            try:
                self.supabase = create_client(self.supabase_url, self.supabase_key)
                logger.info("✅ Подключение к Supabase установлено (legacy)")
            except Exception as e:
                logger.error(f"❌ Ошибка подключения (legacy): {e}")
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к Supabase: {e}")

    def _ensure_tables_exist(self):
        """Проверяет/создаёт таблицы в Supabase"""
        if not self.supabase:
            return

        try:
            # Проверяем таблицу subscribers
            self.supabase.table('subscribers').select('chat_id').limit(1).execute()
            logger.info("✅ Таблица subscribers существует")
        except Exception as e:
            if 'relation' in str(e).lower() and 'does not exist' in str(e).lower():
                logger.warning("⚠️ Таблица subscribers не найдена. Создай вручную в Supabase Dashboard:")
                logger.warning("""
                CREATE TABLE subscribers (
                    chat_id BIGINT PRIMARY KEY,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
                """)
            else:
                logger.error(f"Ошибка проверки таблицы: {e}")

        try:
            # Проверяем таблицу processed_emails
            self.supabase.table('processed_emails').select('hash').limit(1).execute()
            logger.info("✅ Таблица processed_emails существует")
        except Exception as e:
            if 'relation' in str(e).lower() and 'does not exist' in str(e).lower():
                logger.warning("⚠️ Таблица processed_emails не найдена. Создай вручную:")
                logger.warning("""
                CREATE TABLE processed_emails (
                    hash VARCHAR(16) PRIMARY KEY,
                    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
                """)
            else:
                logger.error(f"Ошибка проверки таблицы: {e}")

    def _load_to_cache(self):
        """Загружает данные из Supabase в локальный кэш"""
        if not self.supabase or self._cache_loaded:
            return

        try:
            # Загружаем подписчиков
            response = self.supabase.table('subscribers').select('chat_id').execute()
            self._subscribers_cache = set(row['chat_id'] for row in response.data)
            logger.info(f"📥 Загружено {len(self._subscribers_cache)} подписчиков из Supabase")

            # Загружаем обработанные письма (только последние 100)
            response = self.supabase.table('processed_emails') \
                .select('hash') \
                .order('processed_at', desc=True) \
                .limit(100) \
                .execute()
            self._processed_emails_cache = set(row['hash'] for row in response.data)
            logger.info(f"📥 Загружено {len(self._processed_emails_cache)} хешей писем")

            self._cache_loaded = True
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки из Supabase: {e}")

    def add_subscriber(self, chat_id: int) -> bool:
        """Добавляет подписчика в Supabase"""
        if not self.supabase:
            logger.error("Supabase не подключен")
            return False

        if chat_id in self._subscribers_cache:
            return False

        try:
            self.supabase.table('subscribers').insert({
                'chat_id': chat_id,
                'created_at': datetime.now().isoformat()
            }).execute()
            self._subscribers_cache.add(chat_id)
            logger.info(f"✅ Добавлен подписчик: {chat_id}")
            return True
        except Exception as e:
            # Если уже существует (duplicate key)
            if 'duplicate' in str(e).lower():
                self._subscribers_cache.add(chat_id)
                return False
            logger.error(f"❌ Ошибка добавления подписчика: {e}")
            return False

    def remove_subscriber(self, chat_id: int) -> bool:
        """Удаляет подписчика из Supabase"""
        if not self.supabase:
            return False

        if chat_id not in self._subscribers_cache:
            return False

        try:
            self.supabase.table('subscribers') \
                .delete() \
                .eq('chat_id', chat_id) \
                .execute()
            self._subscribers_cache.discard(chat_id)
            logger.info(f"✅ Удален подписчик: {chat_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка удаления подписчика: {e}")
            return False

    def is_subscriber(self, chat_id: int) -> bool:
        """Проверяет, является ли пользователь подписчиком"""
        self._load_to_cache()
        return chat_id in self._subscribers_cache

    def get_subscribers(self) -> List[int]:
        """Возвращает список подписчиков"""
        self._load_to_cache()
        return list(self._subscribers_cache)

    def is_email_processed(self, email_hash: str) -> bool:
        """Проверяет, было ли письмо уже обработано"""
        self._load_to_cache()
        return email_hash in self._processed_emails_cache

    def mark_email_processed(self, email_hash: str):
        """Отмечает письмо как обработанное в Supabase"""
        if not self.supabase:
            return

        if email_hash in self._processed_emails_cache:
            return

        try:
            self.supabase.table('processed_emails').insert({
                'hash': email_hash,
                'processed_at': datetime.now().isoformat()
            }).execute()
            self._processed_emails_cache.add(email_hash)

            # Ограничиваем кэш 100 элементами
            if len(self._processed_emails_cache) > 100:
                self._cleanup_old_hashes()
        except Exception as e:
            if 'duplicate' not in str(e).lower():
                logger.error(f"❌ Ошибка сохранения хеша: {e}")

    def _cleanup_old_hashes(self):
        """Удаляет старые хеши, оставляя только 100 последних"""
        if not self.supabase:
            return

        try:
            # Получаем все хеши, сортируем по времени
            response = self.supabase.table('processed_emails') \
                .select('hash, processed_at') \
                .order('processed_at', desc=True) \
                .execute()

            hashes = response.data
            if len(hashes) > 100:
                # Удаляем старые (все кроме первых 100)
                to_delete = [h['hash'] for h in hashes[100:]]
                for hash_val in to_delete:
                    self.supabase.table('processed_emails') \
                        .delete() \
                        .eq('hash', hash_val) \
                        .execute()
                logger.info(f"🧹 Очищено {len(to_delete)} старых хешей")
        except Exception as e:
            logger.error(f"Ошибка очистки старых хешей: {e}")

    def get_stats(self) -> Dict:
        """Возвращает статистику"""
        self._load_to_cache()
        return {
            'subscribers_count': len(self._subscribers_cache),
            'processed_emails': len(self._processed_emails_cache)
        }