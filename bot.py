import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Set
from collections import defaultdict
from functools import wraps
import hashlib
import hmac
import json
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from config import TELEGRAM_TOKEN, ADMIN_USER_ID, EMAIL_USER, EMAIL_PASSWORD, IMAP_SERVER
from email_parser import EmailParser
from schedule_manager import ScheduleManager
from image_generator import ScheduleImageGenerator

EMAIL_CHECK_TIMEOUT = 30

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# === УПРАВЛЕНИЕ ПОДПИСЧИКАМИ ===
class SubscriberManager:
    """Менеджер подписчиков на уведомления о расписании"""
    
    def __init__(self, subscribers_file: str = 'cache/subscribers.json'):
        self.subscribers_file = subscribers_file
        self.subscribers: Set[int] = set()
        self._load_subscribers()
    
    def _load_subscribers(self):
        try:
            if os.path.exists(self.subscribers_file):
                with open(self.subscribers_file, 'r') as f:
                    data = json.load(f)
                    self.subscribers = set(data.get('users', []))
        except Exception as e:
            logger.error(f"Ошибка загрузки списка подписчиков: {e}")
    
    def _save_subscribers(self):
        try:
            os.makedirs(os.path.dirname(self.subscribers_file), exist_ok=True)
            with open(self.subscribers_file, 'w') as f:
                json.dump({'users': list(self.subscribers)}, f)
        except Exception as e:
            logger.error(f"Ошибка сохранения списка подписчиков: {e}")
    
    def subscribe(self, user_id: int):
        self.subscribers.add(user_id)
        self._save_subscribers()
    
    def unsubscribe(self, user_id: int):
        self.subscribers.discard(user_id)
        self._save_subscribers()
    
    def is_subscribed(self, user_id: int) -> bool:
        return user_id in self.subscribers
    
    def get_all_subscribers(self) -> Set[int]:
        return self.subscribers.copy()


# === ОТСЛЕЖИВАНИЕ ИЗМЕНЕНИЙ РАСПИСАНИЯ ===
class ScheduleTracker:
    """Отслеживает изменения в расписании для уведомлений"""
    
    def __init__(self, tracker_file: str = 'cache/schedule_tracker.json'):
        self.tracker_file = tracker_file
        self.last_schedule_hash: str = ""
        self._load_tracker()
    
    def _load_tracker(self):
        try:
            if os.path.exists(self.tracker_file):
                with open(self.tracker_file, 'r') as f:
                    data = json.load(f)
                    self.last_schedule_hash = data.get('last_hash', "")
        except Exception as e:
            logger.error(f"Ошибка загрузки трекера: {e}")
    
    def _save_tracker(self):
        try:
            os.makedirs(os.path.dirname(self.tracker_file), exist_ok=True)
            with open(self.tracker_file, 'w') as f:
                json.dump({'last_hash': self.last_schedule_hash}, f)
        except Exception as e:
            logger.error(f"Ошибка сохранения трекера: {e}")
    
    def compute_schedule_hash(self, schedule_data: dict) -> str:
        """Вычисляет хэш расписания для сравнения"""
        # Сериализуем данные в строку
        schedule_str = json.dumps(schedule_data, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(schedule_str.encode()).hexdigest()
    
    def has_changed(self, new_schedule_data: dict) -> bool:
        """Проверяет, изменилось ли расписание"""
        new_hash = self.compute_schedule_hash(new_schedule_data)
        if new_hash != self.last_schedule_hash:
            self.last_schedule_hash = new_hash
            self._save_tracker()
            return True
        return False


# === HTTP HEALTH CHECK SERVER ===
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'Bot is running')

    def log_message(self, format, *args):
        pass  # Отключаем логи HTTP


def run_health_server():
    port = int(os.getenv('PORT', 8080))
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    logger.info(f"Health server started on port {port}")
    server.serve_forever()


# === АНТИ-DDoS ЗАЩИТА ===
class RateLimiter:
    """Ограничитель частоты запросов"""

    def __init__(self, max_requests: int = 10, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests: Dict[int, list] = defaultdict(list)

    def is_allowed(self, user_id: int) -> bool:
        now = datetime.now().timestamp()
        self.requests[user_id] = [
            req_time for req_time in self.requests[user_id]
            if now - req_time < self.time_window
        ]

        if len(self.requests[user_id]) >= self.max_requests:
            return False

        self.requests[user_id].append(now)
        return True


class UserBlacklist:
    """Черный список пользователей"""

    def __init__(self, blacklist_file: str = 'cache/blacklist.json'):
        self.blacklist_file = blacklist_file
        self.blacklist: Set[int] = set()
        self._load_blacklist()

    def _load_blacklist(self):
        try:
            if os.path.exists(self.blacklist_file):
                with open(self.blacklist_file, 'r') as f:
                    data = json.load(f)
                    self.blacklist = set(data.get('users', []))
        except Exception as e:
            logger.error(f"Ошибка загрузки черного списка: {e}")

    def _save_blacklist(self):
        try:
            os.makedirs(os.path.dirname(self.blacklist_file), exist_ok=True)
            with open(self.blacklist_file, 'w') as f:
                json.dump({'users': list(self.blacklist)}, f)
        except Exception as e:
            logger.error(f"Ошибка сохранения черного списка: {e}")

    def add_user(self, user_id: int):
        self.blacklist.add(user_id)
        self._save_blacklist()

    def remove_user(self, user_id: int):
        self.blacklist.discard(user_id)
        self._save_blacklist()

    def is_blocked(self, user_id: int) -> bool:
        return user_id in self.blacklist


class RequestLogger:
    """Логгер запросов для анализа атак"""

    def __init__(self, log_file: str = 'cache/requests.log'):
        self.log_file = log_file

    def log_request(self, user_id: int, username: str, command: str, success: bool = True):
        try:
            os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
            with open(self.log_file, 'a', encoding='utf-8') as f:
                timestamp = datetime.now().isoformat()
                f.write(f"{timestamp}|{user_id}|{username}|{command}|{success}\n")
        except Exception:
            pass


# Глобальные экземпляры защиты
rate_limiter = RateLimiter(max_requests=15, time_window=60)
user_blacklist = UserBlacklist()
request_logger = RequestLogger()


# === ДЕКОРАТОРЫ ДОСТУПА ===

def public_command(func):
    """Декоратор для публичных команд (все могут использовать)"""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        username = update.effective_user.username or str(user_id)

        # Проверка черного списка
        if user_blacklist.is_blocked(user_id):
            request_logger.log_request(user_id, username, func.__name__, success=False)
            await update.effective_message.reply_text("⛔ Доступ запрещен.")
            return

        # Rate limiting
        if not rate_limiter.is_allowed(user_id):
            request_logger.log_request(user_id, username, func.__name__, success=False)
            logger.warning(f"Rate limit exceeded for user {user_id}")
            await update.effective_message.reply_text(
                "⚠️ Слишком много запросов. Пожалуйста, подождите минуту."
            )
            return

        request_logger.log_request(user_id, username, func.__name__, success=True)
        return await func(update, context, *args, **kwargs)

    return wrapper


def admin_command(func):
    """Декоратор только для администратора"""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        username = update.effective_user.username or str(user_id)

        # Проверка черного списка
        if user_blacklist.is_blocked(user_id):
            request_logger.log_request(user_id, username, func.__name__, success=False)
            await update.effective_message.reply_text("⛔ Доступ запрещен.")
            return

        # Rate limiting
        if not rate_limiter.is_allowed(user_id):
            request_logger.log_request(user_id, username, func.__name__, success=False)
            logger.warning(f"Rate limit exceeded for user {user_id}")
            await update.effective_message.reply_text(
                "⚠️ Слишком много запросов. Пожалуйста, подождите минуту."
            )
            return

        # Проверка администратора
        if user_id != ADMIN_USER_ID:
            request_logger.log_request(user_id, username, func.__name__, success=False)
            await update.effective_message.reply_text("⛔ Только для администратора.")
            return

        request_logger.log_request(user_id, username, func.__name__, success=True)
        return await func(update, context, *args, **kwargs)

    return wrapper


def public_callback(func):
    """Декоратор для публичных callback кнопок"""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        query = update.callback_query
        user_id = query.from_user.id
        username = query.from_user.username or str(user_id)

        if user_blacklist.is_blocked(user_id):
            await query.answer("⛔ Доступ запрещен", show_alert=True)
            return

        if not rate_limiter.is_allowed(user_id):
            logger.warning(f"Rate limit exceeded for user {user_id} in callback")
            await query.answer("⚠️ Слишком много запросов. Подождите минуту.", show_alert=True)
            return

        request_logger.log_request(user_id, username, f"callback_{func.__name__}", success=True)
        return await func(update, context, *args, **kwargs)

    return wrapper


# === ИНИЦИАЛИЗАЦИЯ ===
email_parser = EmailParser(EMAIL_USER, EMAIL_PASSWORD, IMAP_SERVER)
schedule_manager = ScheduleManager()
image_generator = ScheduleImageGenerator()
subscriber_manager = SubscriberManager()
schedule_tracker = ScheduleTracker()

# Кэш для изображений
image_cache: Dict[str, tuple] = {}
IMAGE_CACHE_TTL = 3600


def get_day_name(day_code: str) -> str:
    return {'пн': 'Понедельник', 'вт': 'Вторник', 'ср': 'Среда', 'чт': 'Четверг',
            'пт': 'Пятница', 'сб': 'Суббота', 'вс': 'Воскресенье'}.get(day_code, day_code.upper())


# === ПУБЛИЧНЫЕ КОМАНДЫ (все могут использовать) ===

@public_command
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start с авто-подпиской пользователя"""
    user_id = update.effective_user.id
    # Автоматически подписываем пользователя при первом запуске
    if not subscriber_manager.is_subscribed(user_id):
        subscriber_manager.subscribe(user_id)
        logger.info(f"Пользователь {user_id} автоматически подписан на уведомления")
    
    await show_main_menu(update, context, edit=False)


@public_callback
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    if data == 'back':
        await show_main_menu(update, context, edit=True)
    elif data == 'today':
        await send_day_schedule_fast(update, context, 0)
    elif data == 'tomorrow':
        await send_day_schedule_fast(update, context, 1)
    elif data == 'yesterday':
        await send_day_schedule_fast(update, context, -1)
    elif data == 'update':
        await update_from_email(update, context)
    elif data.startswith('admin_'):
        # Проверка админа внутри admin_commands
        await admin_commands(update, context, data)
    else:
        await query.edit_message_text("❌ Неизвестная команда.")


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, edit: bool = True):
    cache_info = schedule_manager.get_cache_info()
    has_cache = cache_info['total_lessons'] > 0

    # Проверка на админа
    is_admin = update.effective_user.id == ADMIN_USER_ID

    keyboard = [
        [InlineKeyboardButton("📅 Сегодня", callback_data='today')],
        [InlineKeyboardButton("📆 Завтра", callback_data='tomorrow')],
        [InlineKeyboardButton("📅 Вчера", callback_data='yesterday')],
        [InlineKeyboardButton("🔄 Обновить расписание", callback_data='update')]
    ]

    # Админские кнопки только для админа
    if is_admin:
        keyboard.append([InlineKeyboardButton("⚙️ Админ-панель", callback_data='admin_panel')])

    status = f"📦 Кэш: {cache_info['age_text']}" if has_cache else "📭 Кэш пуст"
    message_text = (
        f"👋 Привет! Я бот расписания МКП.\n"
        f"⏰ {status}\n\n"
        f"📊 Статистика: {cache_info['total_lessons']} уроков в кэше\n"
        f"👥 Публичный бот\n\n"
        f"Выберите действие:"
    )

    reply_markup = InlineKeyboardMarkup(keyboard)
    if edit and hasattr(update, 'callback_query') and update.callback_query:
        await update.callback_query.edit_message_text(message_text, reply_markup=reply_markup)
    elif hasattr(update, 'message') and update.message:
        await update.message.reply_text(message_text, reply_markup=reply_markup)
    else:
        await update.effective_chat.send_message(message_text, reply_markup=reply_markup)


@public_callback
async def send_day_schedule_fast(update: Update, context: ContextTypes.DEFAULT_TYPE, day_offset: int):
    query = update.callback_query
    target_date = datetime.now().date() + timedelta(days=day_offset)
    days_codes = ['пн', 'вт', 'ср', 'чт', 'пт', 'сб', 'вс']
    day_code = days_codes[target_date.weekday()]
    date_str = target_date.strftime('%d.%m.%y')
    day_name = get_day_name(day_code)
    cache_key = f"{target_date.isoformat()}_{day_code}"

    if cache_key in image_cache:
        image_path, cache_time = image_cache[cache_key]
        if datetime.now().timestamp() - cache_time < IMAGE_CACHE_TTL:
            with open(image_path, 'rb') as photo:
                await context.bot.send_photo(
                    chat_id=update.effective_chat.id,
                    photo=photo,
                    caption=f"📅 {day_name} - {date_str} (из кэша)"
                )
            return

    await query.edit_message_text(f"📅 Генерирую расписание на {day_name} - {date_str}...")

    lessons = schedule_manager.get_schedule_by_date(target_date)

    if not lessons:
        await query.edit_message_text(f"📭 Нет данных на {day_name} - {date_str}. Загружаю из почты...")
        if await _try_load_from_email(days_back=3):
            lessons = schedule_manager.get_schedule_by_date(target_date)

    if not lessons:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data='back')]])
        text = f"📅 {day_name} - {date_str} — выходной!" if day_code == 'вс' else f"📭 Нет расписания на {day_name} - {date_str}."
        await query.edit_message_text(text, reply_markup=kb)
        return

    try:
        image_path = f'cache/schedule_{target_date.isoformat()}.png'
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None,
                                   lambda: image_generator.generate_day_schedule_image(day_code, lessons, date_str,
                                                                                       image_path))

        image_cache[cache_key] = (image_path, datetime.now().timestamp())

        caption = f"📅 {day_name} - {date_str}\n📚 {len(lessons)} уроков"
        with open(image_path, 'rb') as photo:
            await context.bot.send_photo(chat_id=update.effective_chat.id, photo=photo, caption=caption)

        await asyncio.sleep(0.5)
        await show_main_menu(update, context, edit=False)
    except Exception as e:
        logger.error(f"Ошибка генерации: {e}")
        await query.edit_message_text(f"❌ Ошибка: {str(e)}")


@public_callback
async def update_from_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.edit_message_text("🔄 Загружаю расписание из email...")
    try:
        if await _try_load_from_email(days_back=14, notify_users=True, context=context):
            cache_info = schedule_manager.get_cache_info()
            image_cache.clear()
            await query.edit_message_text(
                f"✅ Расписание обновлено!\n📅 Дат: {len(cache_info['dates'])}\n📚 Уроков: {cache_info['total_lessons']}"
            )
        else:
            await query.edit_message_text("❌ Не удалось получить расписание.")
    except asyncio.TimeoutError:
        await query.edit_message_text("⏰ Таймаут загрузки. Попробуйте позже.")
    except Exception as e:
        logger.error(f"Ошибка обновления: {e}")
        await query.edit_message_text(f"❌ Ошибка: {str(e)}")

    await asyncio.sleep(1)
    await show_main_menu(update, context, edit=True)


@public_command
async def update_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔄 Начинаю обновление расписания из email...")
    try:
        all_schedules = await asyncio.get_running_loop().run_in_executor(
            None, lambda: email_parser.get_all_schedules(days_back=14)
        )
        if not all_schedules:
            return await update.message.reply_text("❌ Не удалось получить расписание из email.")

        schedule_manager.update_schedule_from_email(all_schedules)
        image_cache.clear()
        cache_info = schedule_manager.get_cache_info()
        await update.message.reply_text(
            f"✅ Расписание обновлено!\n\n"
            f"📅 Дат в кэше: {len(cache_info['dates'])}\n"
            f"📚 Всего уроков: {cache_info['total_lessons']}\n"
            f"🕐 Возраст кэша: {cache_info['age_text']}"
        )
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


@public_command
async def cache_info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cache_info = schedule_manager.get_cache_info()
    status = "✅ Свежий" if cache_info['is_fresh'] else "⚠️ Устарел"
    text = (
        f"📦 Информация о кэше\n\n"
        f"Статус: {status}\n"
        f"Возраст: {cache_info['age_text']}\n"
        f"Дат в кэше: {len(cache_info['dates'])}\n"
        f"Уроков: {cache_info['total_lessons']}\n"
    )
    if cache_info['dates']:
        text += "\nПоследние даты:\n" + "\n".join(f"• {d}" for d in sorted(cache_info['dates'])[-5:])
    await update.message.reply_text(text)


@public_command
async def clear_cache_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    schedule_manager.clear_cache()
    image_cache.clear()
    await update.message.reply_text("🗑️ Кэш полностью очищен.")


@public_command
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Публичная статистика"""
    cache_info = schedule_manager.get_cache_info()
    subscribers_count = len(subscriber_manager.get_all_subscribers())
    await update.message.reply_text(
        f"📊 Статистика бота:\n\n"
        f"📅 Дат в расписании: {len(cache_info['dates'])}\n"
        f"📚 Всего уроков: {cache_info['total_lessons']}\n"
        f"🕐 Обновлено: {cache_info['age_text']}\n"
        f"🔔 Подписчиков на уведомления: {subscribers_count}\n"
        f"👥 Бот доступен для всех"
    )


@public_command
async def subscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подписка на уведомления о новом расписании"""
    user_id = update.effective_user.id
    if subscriber_manager.is_subscribed(user_id):
        await update.message.reply_text("✅ Вы уже подписаны на уведомления о расписании.")
    else:
        subscriber_manager.subscribe(user_id)
        await update.message.reply_text(
            "✅ Вы успешно подписались на уведомления!\n\n"
            "🔔 Теперь вы будете получать сообщения, когда придёт новое расписание."
        )


@public_command
async def unsubscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отписка от уведомлений о новом расписании"""
    user_id = update.effective_user.id
    if subscriber_manager.is_subscribed(user_id):
        subscriber_manager.unsubscribe(user_id)
        await update.message.reply_text("❌ Вы отписались от уведомлений о расписании.")
    else:
        await update.message.reply_text("ℹ️ Вы не были подписаны на уведомления.")


# === АДМИНСКИЕ КОМАНДЫ (только для тебя) ===

async def admin_commands(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
    """Административные команды"""
    query = update.callback_query
    user_id = update.effective_user.id

    # Проверка админа
    if user_id != ADMIN_USER_ID:
        await query.edit_message_text("⛔ Только для администратора.")
        return

    if data == 'admin_panel':
        keyboard = [
            [InlineKeyboardButton("📊 Статистика", callback_data='admin_stats')],
            [InlineKeyboardButton("🗑️ Очистить кэш", callback_data='admin_clear_cache')],
            [InlineKeyboardButton("🔄 Принудительное обновление", callback_data='admin_force_update')],
            [InlineKeyboardButton("🚫 Заблокировать пользователя", callback_data='admin_block_user')],
            [InlineKeyboardButton("◀️ Назад", callback_data='back')]
        ]
        await query.edit_message_text(
            "⚙️ Админ-панель\n\nВыберите действие:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data == 'admin_stats':
        cache_info = schedule_manager.get_cache_info()
        stats = (
            f"📊 Статистика бота:\n\n"
            f"📅 Дат в кэше: {len(cache_info['dates'])}\n"
            f"📚 Всего уроков: {cache_info['total_lessons']}\n"
            f"🕐 Возраст кэша: {cache_info['age_text']}\n"
            f"✅ Кэш свежий: {cache_info['is_fresh']}\n\n"
            f"🚫 Заблокировано: {len(user_blacklist.blacklist)} пользователей"
        )
        await query.edit_message_text(stats, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("◀️ Назад", callback_data='admin_panel')]
        ]))

    elif data == 'admin_clear_cache':
        schedule_manager.clear_cache()
        image_cache.clear()
        await query.edit_message_text(
            "✅ Кэш очищен!",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data='admin_panel')]])
        )

    elif data == 'admin_force_update':
        await query.edit_message_text("🔄 Принудительное обновление...")
        if await _try_load_from_email(days_back=14, notify_users=True, context=context):
            cache_info = schedule_manager.get_cache_info()
            await query.edit_message_text(
                f"✅ Расписание обновлено!\n📅 Дат: {len(cache_info['dates'])}\n📚 Уроков: {cache_info['total_lessons']}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data='admin_panel')]])
            )
        else:
            await query.edit_message_text(
                "❌ Ошибка обновления",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data='admin_panel')]])
            )

    elif data == 'admin_block_user':
        await query.edit_message_text(
            "🚫 Для блокировки пользователя используйте команду:\n"
            "/block <user_id>\n\n"
            "Пример: /block 123456789",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data='admin_panel')]])
        )


@admin_command
async def block_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Блокировка пользователя"""
    try:
        if not context.args:
            await update.message.reply_text("❌ Укажите ID пользователя: /block <user_id>")
            return

        target_id = int(context.args[0])
        user_blacklist.add_user(target_id)
        await update.message.reply_text(f"✅ Пользователь {target_id} заблокирован.")
    except ValueError:
        await update.message.reply_text("❌ Неверный формат ID.")


@admin_command
async def unblock_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Разблокировка пользователя"""
    try:
        if not context.args:
            await update.message.reply_text("❌ Укажите ID пользователя: /unblock <user_id>")
            return

        target_id = int(context.args[0])
        user_blacklist.remove_user(target_id)
        await update.message.reply_text(f"✅ Пользователь {target_id} разблокирован.")
    except ValueError:
        await update.message.reply_text("❌ Неверный формат ID.")


async def _try_load_from_email(days_back=3, notify_users=False, context=None):
    """
    Загрузка расписания из email с опциональным уведомлением подписчиков
    
    Args:
        days_back: количество дней для поиска
        notify_users: если True, отправить уведомления подписчикам при изменении расписания
        context: контекст бота для отправки сообщений
    """
    try:
        loop = asyncio.get_running_loop()
        all_schedule = await asyncio.wait_for(
            loop.run_in_executor(None, lambda: email_parser.get_all_schedules(days_back=days_back)),
            timeout=EMAIL_CHECK_TIMEOUT
        )
        if all_schedule:
            # Проверяем, изменилось ли расписание
            schedule_changed = schedule_tracker.has_changed(all_schedule)
            
            schedule_manager.update_schedule_from_email(all_schedule)
            
            # Уведомляем подписчиков если расписание изменилось
            if notify_users and schedule_changed and context:
                await _notify_subscribers_about_update(all_schedule, context)
            
            return True
    except asyncio.TimeoutError:
        logger.error("Таймаут загрузки email")
    except Exception as e:
        logger.error(f"Ошибка авто-загрузки: {e}")
    return False


async def _notify_subscribers_about_update(schedule_data, context):
    """Отправляет уведомления всем подписчикам о новом расписании"""
    subscribers = subscriber_manager.get_all_subscribers()
    if not subscribers:
        logger.info("Нет подписчиков для уведомления")
        return
    
    # Формируем сводку по расписанию
    dates_count = len(schedule_data)
    total_lessons = sum(len(lessons) for day_schedules in schedule_data.values() for lessons in day_schedules.values())
    
    # Получаем ближайшие даты для отображения
    sorted_dates = sorted(schedule_data.keys())
    notification_text = "🔔 Пришло новое расписание:\n\n"
    
    for date_obj in sorted_dates[:3]:  # Показываем первые 3 даты
        date_str = date_obj.strftime('%d.%m.%y')
        day_code = ('пн', 'вт', 'ср', 'чт', 'пт', 'сб', 'вс')[date_obj.weekday()]
        day_name = get_day_name(day_code)
        
        # Считаем уроки в этот день
        day_lessons_count = sum(len(lessons) for lessons in schedule_data[date_obj].values())
        
        notification_text += f"📅 {day_name} - {date_str}\n"
        notification_text += f"📚 {day_lessons_count} уроков\n\n"
    
    if dates_count > 3:
        notification_text += f"... и ещё {dates_count - 3} дат\n"
    
    # Отправляем каждому подписчику
    success_count = 0
    for user_id in subscribers:
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=notification_text.strip()
            )
            success_count += 1
            await asyncio.sleep(0.1)  # Небольшая задержка чтобы не спамить
        except Exception as e:
            logger.warning(f"Не удалось отправить уведомление пользователю {user_id}: {e}")
            # Если пользователь заблокировал бота, отписываем его
            if "blocked" in str(e).lower() or "forbidden" in str(e).lower():
                subscriber_manager.unsubscribe(user_id)
    
    logger.info(f"Уведомления отправлены: {success_count}/{len(subscribers)} подписчиков")


def main():
    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN не задан!")
        return

    # Создаём необходимые директории
    os.makedirs('cache', exist_ok=True)
    os.makedirs('Fonts', exist_ok=True)

    # Запускаем health-check сервер в отдельном потоке
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

    application = Application.builder().token(TELEGRAM_TOKEN).build()

    # Публичные команды
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("update", update_command))
    application.add_handler(CommandHandler("cache", cache_info_command))
    application.add_handler(CommandHandler("clearcache", clear_cache_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("subscribe", subscribe_command))
    application.add_handler(CommandHandler("unsubscribe", unsubscribe_command))

    # Админские команды
    application.add_handler(CommandHandler("block", block_user_command))
    application.add_handler(CommandHandler("unblock", unblock_user_command))

    # Обработчик callback кнопок
    application.add_handler(CallbackQueryHandler(button_handler))

    logger.info("Бот запущен с защитой от DDoS!")
    logger.info(f"Rate limit: 15 запросов в минуту на пользователя")
    logger.info(f"Admin ID: {ADMIN_USER_ID}")
    logger.info("Команды для подписки: /subscribe, /unsubscribe")

    # Используем polling
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()