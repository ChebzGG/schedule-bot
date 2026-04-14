import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Set
from collections import defaultdict
from functools import wraps
import hashlib
import json
import os
import threading
import signal
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from config import TELEGRAM_TOKEN, ADMIN_USER_ID, EMAIL_USER, EMAIL_PASSWORD, IMAP_SERVER, EMAIL_CHECK_INTERVAL
from email_parser import EmailParser
from schedule_manager import ScheduleManager
from image_generator import ScheduleImageGenerator
from notification_manager import NotificationManager  # ← ЭТО ПРАВИЛЬНЫЙ

EMAIL_CHECK_TIMEOUT = 30

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


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
    try:
        server = HTTPServer(('0.0.0.0', port), HealthHandler)
        logger.info(f"Health server started on port {port}")
        server.serve_forever()
    except OSError as e:
        logger.warning(f"Health server не запущен (порт {port} занят): {e}")


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

# === ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ===
application = None
shutdown_event = asyncio.Event()


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
async def check_new_schedules():
    """Периодическая проверка новых расписаний в почте"""
    global application

    await asyncio.sleep(10)

    while not shutdown_event.is_set():
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

            if new_schedules_found and application:
                await notify_subscribers(new_schedules_found)

        except Exception as e:
            logger.error(f"Ошибка при проверке почты: {e}")

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=EMAIL_CHECK_INTERVAL)
        except asyncio.TimeoutError:
            continue


async def notify_subscribers(new_schedules: list):
    """Отправляет уведомления всем подписчикам о новом расписании"""
    global application
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
                    await application.bot.send_message(
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
    """Только очищает кэш расписания, подписчиков не трогает"""
    schedule_manager.clear_cache()
    image_cache.clear()
    # Подписчики НЕ трогаем!
    await update.message.reply_text("🗑️ Кэш расписания очищен.\n👥 Подписчики сохранены.")


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
# === НОВЫЕ АДМИН-КОМАНДЫ ===

async def admin_commands(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
    query = update.callback_query
    user_id = update.effective_user.id

    if user_id != ADMIN_USER_ID:
        await query.edit_message_text("⛔ Только для администратора.")
        return

    # === ГЛАВНОЕ МЕНЮ АДМИНА ===
    if data == 'admin_panel':
        keyboard = [
            [InlineKeyboardButton("📦 Управление кэшем", callback_data='admin_cache_menu')],
            [InlineKeyboardButton("👥 Управление подписчиками", callback_data='admin_subs_menu')],
            [InlineKeyboardButton("🔄 Принудительное обновление", callback_data='admin_force_update')],
            [InlineKeyboardButton("🚫 Заблокировать пользователя", callback_data='admin_block_user')],
            [InlineKeyboardButton("📢 Рассылка подписчикам", callback_data='admin_broadcast')],
            [InlineKeyboardButton("📊 Статистика", callback_data='admin_stats')],
            [InlineKeyboardButton("◀️ Назад", callback_data='back')]
        ]
        await query.edit_message_text(
            "⚙️ Админ-панель\n\nВыберите раздел:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    # === МЕНЮ КЭША ===
    elif data == 'admin_cache_menu':
        cache_info = schedule_manager.get_cache_info()

        text = (
            f"📦 <b>Управление кэшем расписания</b>\n\n"
            f"📅 Дат в кэше: <code>{len(cache_info['dates'])}</code>\n"
            f"📚 Всего уроков: <code>{cache_info['total_lessons']}</code>\n"
            f"🕐 Обновлено: {cache_info['age_text']}\n"
            f"{'✅' if cache_info['is_fresh'] else '⚠️'} Статус: {'Свежий' if cache_info['is_fresh'] else 'Устарел'}\n\n"
        )

        # Показываем последние 10 дат
        if cache_info['dates']:
            recent_dates = sorted(cache_info['dates'])[-10:]
            text += "<b>Последние даты:</b>\n"
            for d in recent_dates:
                try:
                    date_obj = datetime.fromisoformat(d)
                    text += f"• <code>{date_obj.strftime('%d.%m.%Y')}</code>\n"
                except:
                    text += f"• <code>{d}</code>\n"

        keyboard = [
            [InlineKeyboardButton("📋 Показать весь кэш", callback_data='admin_cache_view_full')],
            [InlineKeyboardButton("🗑️ Очистить кэш", callback_data='admin_cache_clear')],
            [InlineKeyboardButton("◀️ Назад", callback_data='admin_panel')]
        ]
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )

    # === ПРОСМОТР ВСЕГО КЭША ===
    elif data == 'admin_cache_view_full':
        cache = schedule_manager._load_cache()

        if not cache:
            await query.edit_message_text(
                "📭 Кэш пуст",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ Назад", callback_data='admin_cache_menu')
                ]])
            )
            return

        # Формируем текст с кэшем (разбиваем на части если большой)
        text = "📦 <b>Полный кэш расписания:</b>\n\n"

        for date_str in sorted(cache.keys()):
            try:
                date_obj = datetime.fromisoformat(date_str)
                day_code = ('пн', 'вт', 'ср', 'чт', 'пт', 'сб', 'вс')[date_obj.weekday()]
                day_names = {'пн': 'Пн', 'вт': 'Вт', 'ср': 'Ср', 'чт': 'Чт', 'пт': 'Пт', 'сб': 'Сб', 'вс': 'Вс'}
                date_formatted = f"{date_obj.strftime('%d.%m.%Y')} ({day_names.get(day_code, day_code)})"
            except:
                date_formatted = date_str

            lessons = cache[date_str]
            text += f"📅 <b>{date_formatted}</b> — {len(lessons)} уроков\n"

            for lesson in lessons[:5]:  # Показываем первые 5 уроков
                text += f"  <code>{lesson['number']}.</code> {lesson['timebegin']}-{lesson['timeend']} {lesson['name'][:30]}\n"

            if len(lessons) > 5:
                text += f"  <i>... и ещё {len(lessons) - 5} уроков</i>\n"
            text += "\n"

            # Telegram ограничение ~4000 символов
            if len(text) > 3500:
                text += "<i>... (кэш обрезан, слишком большой)</i>"
                break

        keyboard = [
            [InlineKeyboardButton("🗑️ Очистить кэш", callback_data='admin_cache_clear')],
            [InlineKeyboardButton("◀️ Назад", callback_data='admin_cache_menu')]
        ]

        # Если текст слишком длинный, отправляем как файл
        if len(text) > 4000:
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
                f.write(
                    text.replace('<b>', '').replace('</b>', '').replace('<code>', '').replace('</code>', '').replace(
                        '<i>', '').replace('</i>', ''))
                temp_path = f.name

            await query.delete_message()
            with open(temp_path, 'rb') as f:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=f,
                    caption="📦 Полный кэш расписания",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            os.remove(temp_path)
        else:
            await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )

    # === ОЧИСТКА КЭША (только расписание!) ===
    elif data == 'admin_cache_clear':
        schedule_manager.clear_cache()
        image_cache.clear()
        # Подписчики НЕ трогаем!

        await query.edit_message_text(
            "✅ <b>Кэш расписания очищен!</b>\n\n"
            "📅 Даты и уроки удалены\n"
            "👥 Подписчики сохранены\n"
            "🖼️ Кэш изображений очищен",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📦 В меню кэша", callback_data='admin_cache_menu')],
                [InlineKeyboardButton("⚙️ В админ-панель", callback_data='admin_panel')]
            ]),
            parse_mode='HTML'
        )

    # === МЕНЮ ПОДПИСЧИКОВ ===
    elif data == 'admin_subs_menu':
        notif_stats = notification_manager.get_stats()
        subscribers = notification_manager.get_subscribers()

        text = (
            f"👥 <b>Управление подписчиками</b>\n\n"
            f"🔔 Всего подписчиков: <code>{notif_stats['subscribers_count']}</code>\n"
            f"📧 Обработано писем: <code>{notif_stats['processed_emails']}</code>\n\n"
        )

        keyboard = [
            [InlineKeyboardButton("📋 Список подписчиков", callback_data='admin_subs_view')],
            [InlineKeyboardButton("🧹 Очистить неактивных", callback_data='admin_subs_cleanup')],
            [InlineKeyboardButton("◀️ Назад", callback_data='admin_panel')]
        ]
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )

    # === ПРОСМОТР ПОДПИСЧИКОВ ===
    elif data == 'admin_subs_view':
        subscribers = notification_manager.get_subscribers()

        if not subscribers:
            await query.edit_message_text(
                "👥 Нет подписчиков",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ Назад", callback_data='admin_subs_menu')
                ]])
            )
            return

        text = f"👥 <b>Список подписчиков ({len(subscribers)}):</b>\n\n"

        for i, chat_id in enumerate(subscribers, 1):
            # Пытаемся получить информацию о пользователе
            try:
                chat = await context.bot.get_chat(chat_id)
                username = f"@{chat.username}" if chat.username else "нет username"
                first_name = chat.first_name or "Без имени"
                text += f"{i}. <code>{chat_id}</code> — {first_name} ({username})\n"
            except Exception:
                text += f"{i}. <code>{chat_id}</code> — (недоступен)\n"

        keyboard = [
            [InlineKeyboardButton("🧹 Очистить неактивных", callback_data='admin_subs_cleanup')],
            [InlineKeyboardButton("◀️ Назад", callback_data='admin_subs_menu')]
        ]

        # Если слишком длинно — отправляем файлом
        if len(text) > 4000:
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
                f.write(text.replace('<b>', '').replace('</b>', '').replace('<code>', '').replace('</code>', ''))
                temp_path = f.name

            await query.delete_message()
            with open(temp_path, 'rb') as f:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=f,
                    caption=f"👥 Список подписчиков ({len(subscribers)})",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            os.remove(temp_path)
        else:
            await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )

    # === ОЧИСТКА НЕАКТИВНЫХ ПОДПИСЧИКОВ ===
    elif data == 'admin_subs_cleanup':
        """Проверяет и удаляет заблокировавших бота"""
        subscribers = notification_manager.get_subscribers()
        removed = []

        await query.edit_message_text("🧹 Проверяю подписчиков...")

        for chat_id in subscribers[:]:  # Копия списка
            try:
                await context.bot.send_chat_action(chat_id=chat_id, action='typing')
            except Exception as e:
                # Бот заблокирован или пользователь не найден
                if "blocked" in str(e).lower() or "not found" in str(e).lower() or "deactivated" in str(e).lower():
                    notification_manager.remove_subscriber(chat_id)
                    removed.append(chat_id)

        if removed:
            text = f"✅ Удалено неактивных: <code>{len(removed)}</code>\n\n"
            text += "Удалённые ID:\n" + "\n".join(f"<code>{rid}</code>" for rid in removed[:20])
            if len(removed) > 20:
                text += f"\n... и ещё {len(removed) - 20}"
        else:
            text = "✅ Все подписчики активны, удалять некого"

        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◀️ Назад", callback_data='admin_subs_menu')
            ]]),
            parse_mode='HTML'
        )

    # === ОСТАЛЬНЫЕ КНОПКИ ===
    elif data == 'admin_stats':
        cache_info = schedule_manager.get_cache_info()
        notif_stats = notification_manager.get_stats()
        stats = (
            f"📊 Статистика бота:\n\n"
            f"<b>Кэш расписания:</b>\n"
            f"📅 Дат: <code>{len(cache_info['dates'])}</code>\n"
            f"📚 Уроков: <code>{cache_info['total_lessons']}</code>\n"
            f"🕐 Возраст: {cache_info['age_text']}\n\n"
            f"<b>Подписчики:</b>\n"
            f"🔔 Активных: <code>{notif_stats['subscribers_count']}</code>\n"
            f"📧 Обработано писем: <code>{notif_stats['processed_emails']}</code>\n\n"
            f"<b>Безопасность:</b>\n"
            f"🚫 Заблокировано: <code>{len(user_blacklist.blacklist)}</code>"
        )
        await query.edit_message_text(
            stats,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◀️ Назад", callback_data='admin_panel')
            ]]),
            parse_mode='HTML'
        )

    elif data == 'admin_force_update':
        await query.edit_message_text("🔄 Принудительное обновление...")
        if await _try_load_from_email(days_back=14):
            cache_info = schedule_manager.get_cache_info()
            await query.edit_message_text(
                f"✅ Расписание обновлено!\n"
                f"📅 Дат: <code>{len(cache_info['dates'])}</code>\n"
                f"📚 Уроков: <code>{cache_info['total_lessons']}</code>",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data='admin_panel')]]),
                parse_mode='HTML'
            )
        else:
            await query.edit_message_text(
                "❌ Ошибка обновления",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data='admin_panel')]])
            )

    elif data == 'admin_block_user':
        await query.edit_message_text(
            "🚫 Для блокировки пользователя используйте команду:\n"
            "<code>/block &lt;user_id&gt;</code>\n\n"
            "Пример: <code>/block 123456789</code>",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data='admin_panel')]]),
            parse_mode='HTML'
        )

    elif data == 'admin_broadcast':
        await query.edit_message_text(
            "📢 Для рассылки сообщения всем подписчикам используйте:\n"
            "<code>/broadcast &lt;текст&gt;</code>\n\n"
            "Пример: <code>/broadcast Внимание! Завтра изменения в расписании.</code>",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data='admin_panel')]]),
            parse_mode='HTML'
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


def signal_handler(sig, frame):
    """Обработчик сигналов для graceful shutdown"""
    logger.info(f"Получен сигнал {sig}, завершаю работу...")
    shutdown_event.set()


async def main_async():
    """Асинхронная main функция"""
    global application

    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN не задан!")
        return

    os.makedirs('cache', exist_ok=True)
    os.makedirs('Fonts', exist_ok=True)

    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Запускаем health-check сервер в отдельном потоке
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

    # Создаем приложение
    application = Application.builder().token(TELEGRAM_TOKEN).build()

    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("update", update_command))
    application.add_handler(CommandHandler("cache", cache_info_command))
    application.add_handler(CommandHandler("clearcache", clear_cache_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("subscribe", subscribe_command))
    application.add_handler(CommandHandler("unsubscribe", unsubscribe_command))
    application.add_handler(CommandHandler("block", block_user_command))
    application.add_handler(CommandHandler("unblock", unblock_user_command))
    application.add_handler(CommandHandler("broadcast", broadcast_command))
    application.add_handler(CallbackQueryHandler(button_handler))

    logger.info("Бот запущен с авто-проверкой почты!")
    logger.info(f"Интервал проверки почты: {EMAIL_CHECK_INTERVAL} сек")
    logger.info(f"Подписчиков: {len(notification_manager.get_subscribers())}")

    # Запускаем фоновые задачи
    email_check_task = asyncio.create_task(check_new_schedules())


    # Запускаем бота
    await application.initialize()
    await application.start()

    # Запускаем polling в отдельной задаче
    polling_task = asyncio.create_task(
        application.updater.start_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True))

    # Ждем сигнала завершения
    await shutdown_event.wait()

    # Graceful shutdown
    logger.info("Остановка бота...")


    polling_task.cancel()
    email_check_task.cancel()

    try:
        await polling_task
    except asyncio.CancelledError:
        pass
    try:
        await email_check_task
    except asyncio.CancelledError:
        pass

    await application.updater.stop()
    await application.stop()
    await application.shutdown()
    logger.info("Бот остановлен")


def main():
    """Точка входа"""
    asyncio.run(main_async())


if __name__ == '__main__':
    main()