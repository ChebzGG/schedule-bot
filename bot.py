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
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, JobQueue
from config import TELEGRAM_TOKEN, ADMIN_USER_ID, EMAIL_USER, EMAIL_PASSWORD, IMAP_SERVER, EMAIL_CHECK_INTERVAL
from email_parser import EmailParser
from schedule_manager import ScheduleManager
from image_generator import ScheduleImageGenerator

EMAIL_CHECK_TIMEOUT = 30

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# === NOTIFICATION MANAGER ===
class NotificationManager:
    def __init__(self, subscribers_file: str = 'cache/subscribers.json'):
        self.subscribers_file = subscribers_file
        self.processed_emails_file = 'cache/processed_emails.json'
        self.subscribers: Set[int] = set()
        self.processed_email_hashes: Set[str] = set()
        self._load_subscribers()
        self._load_processed_emails()

    def _load_subscribers(self):
        try:
            if os.path.exists(self.subscribers_file):
                with open(self.subscribers_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.subscribers = set(data.get('subscribers', []))
                    logger.info(f"Загружено {len(self.subscribers)} подписчиков")
        except Exception as e:
            logger.error(f"Ошибка загрузки подписчиков: {e}")

    def _save_subscribers(self):
        try:
            os.makedirs(os.path.dirname(self.subscribers_file), exist_ok=True)
            with open(self.subscribers_file, 'w', encoding='utf-8') as f:
                json.dump({'subscribers': list(self.subscribers)}, f)
        except Exception as e:
            logger.error(f"Ошибка сохранения подписчиков: {e}")

    def _load_processed_emails(self):
        try:
            if os.path.exists(self.processed_emails_file):
                with open(self.processed_emails_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processed_email_hashes = set(data.get('hashes', [])[-100:])
        except Exception as e:
            logger.error(f"Ошибка загрузки обработанных писем: {e}")

    def _save_processed_emails(self):
        try:
            hashes = list(self.processed_email_hashes)[-100:]
            with open(self.processed_emails_file, 'w', encoding='utf-8') as f:
                json.dump({'hashes': hashes, 'last_updated': datetime.now().isoformat()}, f)
        except Exception as e:
            logger.error(f"Ошибка сохранения обработанных писем: {e}")

    def add_subscriber(self, chat_id: int) -> bool:
        if chat_id not in self.subscribers:
            self.subscribers.add(chat_id)
            self._save_subscribers()
            logger.info(f"Добавлен подписчик: {chat_id}")
            return True
        return False

    def remove_subscriber(self, chat_id: int) -> bool:
        if chat_id in self.subscribers:
            self.subscribers.discard(chat_id)
            self._save_subscribers()
            logger.info(f"Удален подписчик: {chat_id}")
            return True
        return False

    def is_subscriber(self, chat_id: int) -> bool:
        return chat_id in self.subscribers

    def get_subscribers(self):
        return list(self.subscribers)

    def is_email_processed(self, email_hash: str) -> bool:
        return email_hash in self.processed_email_hashes

    def mark_email_processed(self, email_hash: str):
        self.processed_email_hashes.add(email_hash)
        self._save_processed_emails()

    def get_stats(self):
        return {
            'subscribers_count': len(self.subscribers),
            'processed_emails': len(self.processed_email_hashes)
        }


# === HTTP HEALTH CHECK SERVER ===
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'Bot is running')

    def log_message(self, format, *args):
        pass


def run_health_server():
    port = int(os.getenv('PORT', 8080))
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    logger.info(f"Health server started on port {port}")
    server.serve_forever()


# === АНТИ-DDoS ЗАЩИТА ===
class RateLimiter:
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


rate_limiter = RateLimiter(max_requests=15, time_window=60)
user_blacklist = UserBlacklist()
request_logger = RequestLogger()
notification_manager = NotificationManager()


# === ДЕКОРАТОРЫ ДОСТУПА ===
def public_command(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        username = update.effective_user.username or str(user_id)

        if user_blacklist.is_blocked(user_id):
            request_logger.log_request(user_id, username, func.__name__, success=False)
            await update.effective_message.reply_text("⛔ Доступ запрещен.")
            return

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
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        username = update.effective_user.username or str(user_id)

        if user_blacklist.is_blocked(user_id):
            request_logger.log_request(user_id, username, func.__name__, success=False)
            await update.effective_message.reply_text("⛔ Доступ запрещен.")
            return

        if not rate_limiter.is_allowed(user_id):
            request_logger.log_request(user_id, username, func.__name__, success=False)
            logger.warning(f"Rate limit exceeded for user {user_id}")
            await update.effective_message.reply_text(
                "⚠️ Слишком много запросов. Пожалуйста, подождите минуту."
            )
            return

        if user_id != ADMIN_USER_ID:
            request_logger.log_request(user_id, username, func.__name__, success=False)
            await update.effective_message.reply_text("⛔ Только для администратора.")
            return

        request_logger.log_request(user_id, username, func.__name__, success=True)
        return await func(update, context, *args, **kwargs)

    return wrapper


def public_callback(func):
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

image_cache: Dict[str, tuple] = {}
IMAGE_CACHE_TTL = 3600


def get_day_name(day_code: str) -> str:
    return {'пн': 'Понедельник', 'вт': 'Вторник', 'ср': 'Среда', 'чт': 'Четверг',
            'пт': 'Пятница', 'сб': 'Суббота', 'вс': 'Воскресенье'}.get(day_code, day_code.upper())


# === АВТОМАТИЧЕСКАЯ ПРОВЕРКА ПОЧТЫ ===
async def check_new_schedules(context: ContextTypes.DEFAULT_TYPE):
    """Периодическая проверка новых расписаний в почте"""
    try:
        logger.info("🔍 Проверка новых писем с расписанием...")

        emails_data = await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: email_parser.search_emails_with_hash(days_back=2, max_emails=20)
        )

        new_schedules_found = []

        for email_data in emails_data:
            email_hash = email_data['hash']

            if notification_manager.is_email_processed(email_hash):
                continue

            body = email_parser.extract_body(email_data['message'])
            if not body:
                notification_manager.mark_email_processed(email_hash)
                continue

            parsed = email_parser.parse_schedule_from_text(body)
            if not parsed:
                notification_manager.mark_email_processed(email_hash)
                continue

            new_schedules_found.append({
                'hash': email_hash,
                'data': parsed,
                'subject': email_data['subject']
            })

            schedule_manager.update_schedule_from_email(parsed)
            notification_manager.mark_email_processed(email_hash)

            logger.info(f"✅ Найдено новое расписание: {email_data['subject']}")

        if new_schedules_found:
            await notify_subscribers(context, new_schedules_found)

    except Exception as e:
        logger.error(f"Ошибка при проверке почты: {e}")


async def notify_subscribers(context: ContextTypes.DEFAULT_TYPE, new_schedules: list):
    """Отправляет уведомления всем подписчикам о новом расписании"""
    subscribers = notification_manager.get_subscribers()

    if not subscribers:
        logger.info("Нет подписчиков для уведомления")
        return

    for schedule_info in new_schedules:
        parsed_data = schedule_info['data']

        for date_obj, day_schedules in parsed_data.items():
            date_str = date_obj.strftime('%d.%m.%y') if hasattr(date_obj, 'strftime') else str(date_obj)
            day_code = list(day_schedules.keys())[0] if day_schedules else None

            if not day_code:
                continue

            day_name = get_day_name(day_code)
            lessons = day_schedules.get(day_code, [])
            lessons_count = len(lessons)

            message_text = (
                f"📬 <b>Пришло новое расписание!</b>\n\n"
                f"📅 <b>{day_name}</b> - {date_str}\n"
                f"📚 <b>{lessons_count}</b> уроков\n\n"
                f"Нажмите кнопку ниже, чтобы посмотреть полное расписание:"
            )

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📅 Посмотреть расписание", callback_data=f'notify_{date_obj.isoformat()}')],
                [InlineKeyboardButton("🔕 Отключить уведомления", callback_data='unsubscribe')]
            ])

            for chat_id in subscribers:
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=message_text,
                        reply_markup=keyboard,
                        parse_mode='HTML'
                    )
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logger.error(f"Ошибка отправки уведомления {chat_id}: {e}")
                    if "blocked" in str(e).lower() or "not found" in str(e).lower():
                        notification_manager.remove_subscriber(chat_id)


# === ПУБЛИЧНЫЕ КОМАНДЫ ===
@public_command
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_main_menu(update, context, edit=False)


@public_callback
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data

    if data.startswith('notify_'):
        date_str = data.replace('notify_', '')
        try:
            target_date = datetime.fromisoformat(date_str).date()
            day_offset = (target_date - datetime.now().date()).days
            await send_day_schedule_fast(update, context, day_offset)
        except Exception as e:
            logger.error(f"Ошибка обработки уведомления: {e}")
            await query.edit_message_text("❌ Ошибка загрузки расписания")
        return

    elif data == 'subscribe':
        chat_id = update.effective_chat.id
        if notification_manager.add_subscriber(chat_id):
            await query.edit_message_text(
                "✅ Вы подписались на уведомления о новом расписании!\n"
                "Бот будет автоматически сообщать, когда придет новое расписание на почту."
            )
        else:
            await query.answer("Вы уже подписаны!", show_alert=True)
        return

    elif data == 'unsubscribe':
        chat_id = update.effective_chat.id
        if notification_manager.remove_subscriber(chat_id):
            await query.edit_message_text(
                "🔕 Уведомления отключены.\n"
                "Вы больше не будете получать автоматические сообщения о новом расписании.\n\n"
                "Чтобы снова включить, используйте /subscribe"
            )
        else:
            await query.answer("Вы не были подписаны.", show_alert=True)
        return

    elif data == 'back':
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
        await admin_commands(update, context, data)
    else:
        await query.edit_message_text("❌ Неизвестная команда.")


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, edit: bool = True):
    cache_info = schedule_manager.get_cache_info()
    has_cache = cache_info['total_lessons'] > 0
    is_admin = update.effective_user.id == ADMIN_USER_ID
    chat_id = update.effective_chat.id
    is_subscribed = notification_manager.is_subscriber(chat_id)

    keyboard = [
        [InlineKeyboardButton("📅 Сегодня", callback_data='today')],
        [InlineKeyboardButton("📆 Завтра", callback_data='tomorrow')],
        [InlineKeyboardButton("📅 Вчера", callback_data='yesterday')],
        [InlineKeyboardButton("🔄 Обновить расписание", callback_data='update')]
    ]

    if is_subscribed:
        keyboard.append([InlineKeyboardButton("🔕 Отключить уведомления", callback_data='unsubscribe')])
    else:
        keyboard.append([InlineKeyboardButton("🔔 Включить уведомления", callback_data='subscribe')])

    if is_admin:
        keyboard.append([InlineKeyboardButton("⚙️ Админ-панель", callback_data='admin_panel')])

    status = f"📦 Кэш: {cache_info['age_text']}" if has_cache else "📭 Кэш пуст"
    sub_status = "🔔 Уведомления: включены" if is_subscribed else "🔕 Уведомления: выключены"

    message_text = (
        f"👋 Привет! Я бот расписания МКП.\n"
        f"⏰ {status}\n"
        f"🔔 {sub_status}\n\n"
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
        if await _try_load_from_email(days_back=14):
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
    cache_info = schedule_manager.get_cache_info()
    notif_stats = notification_manager.get_stats()
    await update.message.reply_text(
        f"📊 Статистика бота:\n\n"
        f"📅 Дат в расписании: {len(cache_info['dates'])}\n"
        f"📚 Всего уроков: {cache_info['total_lessons']}\n"
        f"🕐 Обновлено: {cache_info['age_text']}\n"
        f"🔔 Подписчиков: {notif_stats['subscribers_count']}\n"
        f"👥 Публичный бот"
    )


@public_command
async def subscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if notification_manager.add_subscriber(chat_id):
        await update.message.reply_text(
            "✅ <b>Подписка оформлена!</b>\n\n"
            "Теперь вы будете получать уведомления, когда приходит новое расписание на почту.\n"
            "Бот проверяет почту каждые 5 минут.",
            parse_mode='HTML'
        )
    else:
        await update.message.reply_text(
            "ℹ️ Вы уже подписаны на уведомления.\n"
            "Используйте /unsubscribe чтобы отписаться."
        )


@public_command
async def unsubscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if notification_manager.remove_subscriber(chat_id):
        await update.message.reply_text(
            "🔕 <b>Уведомления отключены</b>\n\n"
            "Вы больше не будете получать автоматические сообщения.\n"
            "Чтобы снова включить, используйте /subscribe",
            parse_mode='HTML'
        )
    else:
        await update.message.reply_text(
            "ℹ️ Вы не были подписаны на уведомления.\n"
            "Используйте /subscribe чтобы подписаться."
        )


# === АДМИНСКИЕ КОМАНДЫ ===
async def admin_commands(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
    query = update.callback_query
    user_id = update.effective_user.id

    if user_id != ADMIN_USER_ID:
        await query.edit_message_text("⛔ Только для администратора.")
        return

    if data == 'admin_panel':
        keyboard = [
            [InlineKeyboardButton("📊 Статистика", callback_data='admin_stats')],
            [InlineKeyboardButton("🗑️ Очистить кэш", callback_data='admin_clear_cache')],
            [InlineKeyboardButton("🔄 Принудительное обновление", callback_data='admin_force_update')],
            [InlineKeyboardButton("🚫 Заблокировать пользователя", callback_data='admin_block_user')],
            [InlineKeyboardButton("🔔 Рассылка подписчикам", callback_data='admin_broadcast')],
            [InlineKeyboardButton("◀️ Назад", callback_data='back')]
        ]
        await query.edit_message_text(
            "⚙️ Админ-панель\n\nВыберите действие:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data == 'admin_stats':
        cache_info = schedule_manager.get_cache_info()
        notif_stats = notification_manager.get_stats()
        stats = (
            f"📊 Статистика бота:\n\n"
            f"📅 Дат в кэше: {len(cache_info['dates'])}\n"
            f"📚 Всего уроков: {cache_info['total_lessons']}\n"
            f"🕐 Возраст кэша: {cache_info['age_text']}\n"
            f"✅ Кэш свежий: {cache_info['is_fresh']}\n\n"
            f"🔔 Подписчиков: {notif_stats['subscribers_count']}\n"
            f"📧 Обработано писем: {notif_stats['processed_emails']}\n"
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
        if await _try_load_from_email(days_back=14):
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

    elif data == 'admin_broadcast':
        await query.edit_message_text(
            "📢 Для рассылки сообщения всем подписчикам используйте:\n"
            "/broadcast <текст>\n\n"
            "Пример: /broadcast Внимание! Завтра изменения в расписании.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data='admin_panel')]])
        )


@admin_command
async def block_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    try:
        if not context.args:
            await update.message.reply_text("❌ Укажите ID пользователя: /unblock <user_id>")
            return

        target_id = int(context.args[0])
        user_blacklist.remove_user(target_id)
        await update.message.reply_text(f"✅ Пользователь {target_id} разблокирован.")
    except ValueError:
        await update.message.reply_text("❌ Неверный формат ID.")


@admin_command
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Рассылка сообщения всем подписчикам"""
    if not context.args:
        await update.message.reply_text("❌ Укажите текст сообщения: /broadcast <текст>")
        return

    message_text = ' '.join(context.args)
    subscribers = notification_manager.get_subscribers()

    if not subscribers:
        await update.message.reply_text("❌ Нет подписчиков для рассылки.")
        return

    sent_count = 0
    failed_count = 0

    await update.message.reply_text(f"📢 Начинаю рассылку {len(subscribers)} подписчикам...")

    for chat_id in subscribers:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"📢 <b>Сообщение от администратора:</b>\n\n{message_text}",
                parse_mode='HTML'
            )
            sent_count += 1
            await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Ошибка рассылки {chat_id}: {e}")
            failed_count += 1
            if "blocked" in str(e).lower():
                notification_manager.remove_subscriber(chat_id)

    await update.message.reply_text(
        f"✅ Рассылка завершена!\n"
        f"📤 Отправлено: {sent_count}\n"
        f"❌ Ошибок: {failed_count}"
    )


async def _try_load_from_email(days_back=3):
    try:
        loop = asyncio.get_running_loop()
        all_schedule = await asyncio.wait_for(
            loop.run_in_executor(None, lambda: email_parser.get_all_schedules(days_back=days_back)),
            timeout=EMAIL_CHECK_TIMEOUT
        )
        if all_schedule:
            schedule_manager.update_schedule_from_email(all_schedule)
            return True
    except asyncio.TimeoutError:
        logger.error("Таймаут загрузки email")
    except Exception as e:
        logger.error(f"Ошибка авто-загрузки: {e}")
    return False


def main():
    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN не задан!")
        return

    os.makedirs('cache', exist_ok=True)
    os.makedirs('Fonts', exist_ok=True)

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
    application.add_handler(CommandHandler("broadcast", broadcast_command))

    # Обработчик callback кнопок
    application.add_handler(CallbackQueryHandler(button_handler))

    # Настраиваем периодическую проверку почты
    job_queue = application.job_queue
    job_queue.run_repeating(
        check_new_schedules,
        interval=EMAIL_CHECK_INTERVAL,
        first=10
    )

    logger.info("Бот запущен с авто-проверкой почты!")
    logger.info(f"Интервал проверки: {EMAIL_CHECK_INTERVAL} сек")
    logger.info(f"Подписчиков: {len(notification_manager.get_subscribers())}")

    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()