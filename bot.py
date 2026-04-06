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


# === АНТИ-DDoS ЗАЩИТА ===
class RateLimiter:
    """Ограничитель частоты запросов"""

    def __init__(self, max_requests: int = 10, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests: Dict[int, list] = defaultdict(list)

    def is_allowed(self, user_id: int) -> bool:
        now = datetime.now().timestamp()
        # Очищаем старые запросы
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
rate_limiter = RateLimiter(max_requests=15, time_window=60)  # 15 запросов в минуту
user_blacklist = UserBlacklist()
request_logger = RequestLogger()


# Декоратор для защиты команд
def secure_command(func):
    """Декоратор для защиты команд от атак"""

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

        # Проверка доступа (если задан ALLOWED_USER_ID)
        if ADMIN_USER_ID != 0 and user_id != ADMIN_USER_ID:
            request_logger.log_request(user_id, username, func.__name__, success=False)
            await update.effective_message.reply_text("⛔ У вас нет доступа к этому боту.")
            return

        request_logger.log_request(user_id, username, func.__name__, success=True)
        return await func(update, context, *args, **kwargs)

    return wrapper


def secure_callback(func):
    """Декоратор для защиты callback запросов"""

    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        query = update.callback_query
        user_id = query.from_user.id
        username = query.from_user.username or str(user_id)

        # Проверка черного списка
        if user_blacklist.is_blocked(user_id):
            await query.answer("⛔ Доступ запрещен", show_alert=True)
            return

        # Rate limiting
        if not rate_limiter.is_allowed(user_id):
            logger.warning(f"Rate limit exceeded for user {user_id} in callback")
            await query.answer("⚠️ Слишком много запросов. Подождите минуту.", show_alert=True)
            return

        # Проверка доступа
        if ADMIN_USER_ID != 0 and user_id != ADMIN_USER_ID:
            await query.answer("⛔ Нет доступа", show_alert=True)
            return

        request_logger.log_request(user_id, username, f"callback_{func.__name__}", success=True)
        return await func(update, context, *args, **kwargs)

    return wrapper


# === ИНИЦИАЛИЗАЦИЯ ===
email_parser = EmailParser(EMAIL_USER, EMAIL_PASSWORD, IMAP_SERVER)
schedule_manager = ScheduleManager()
image_generator = ScheduleImageGenerator()

# Кэш для изображений (чтобы не генерировать повторно)
image_cache: Dict[str, tuple] = {}
IMAGE_CACHE_TTL = 3600  # 1 час


def get_day_name(day_code: str) -> str:
    return {'пн': 'Понедельник', 'вт': 'Вторник', 'ср': 'Среда', 'чт': 'Четверг',
            'пт': 'Пятница', 'сб': 'Суббота', 'вс': 'Воскресенье'}.get(day_code, day_code.upper())


@secure_command
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_main_menu(update, context, edit=False)


@secure_callback
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
        await admin_commands(update, context, data)
    else:
        await query.edit_message_text("❌ Неизвестная команда.")


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, edit: bool = True):
    cache_info = schedule_manager.get_cache_info()
    has_cache = cache_info['total_lessons'] > 0

    # Проверка на админа
    is_admin = update.effective_user.id == ADMIN_USER_ID if ADMIN_USER_ID != 0 else False

    keyboard = [
        [InlineKeyboardButton("📅 Сегодня", callback_data='today')],
        [InlineKeyboardButton("📆 Завтра", callback_data='tomorrow')],
        [InlineKeyboardButton("📅 Вчера", callback_data='yesterday')],
        [InlineKeyboardButton("🔄 Обновить расписание", callback_data='update')]
    ]

    # Админские кнопки
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


async def admin_commands(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
    """Административные команды"""
    query = update.callback_query
    user_id = update.effective_user.id

    # Только для админа
    if ADMIN_USER_ID != 0 and user_id != ADMIN_USER_ID:
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


async def send_day_schedule_fast(update: Update, context: ContextTypes.DEFAULT_TYPE, day_offset: int):
    query = update.callback_query
    target_date = datetime.now().date() + timedelta(days=day_offset)
    days_codes = ['пн', 'вт', 'ср', 'чт', 'пт', 'сб', 'вс']
    day_code = days_codes[target_date.weekday()]
    date_str = target_date.strftime('%d.%m.%y')
    day_name = get_day_name(day_code)
    cache_key = f"{target_date.isoformat()}_{day_code}"

    # Проверка кэша изображений
    if cache_key in image_cache:
        image_path, cache_time = image_cache[cache_key]
        if datetime.now().timestamp() - cache_time < IMAGE_CACHE_TTL:
            # Используем кэшированное изображение
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

        # Сохраняем в кэш изображений
        image_cache[cache_key] = (image_path, datetime.now().timestamp())

        caption = f"📅 {day_name} - {date_str}\n📚 {len(lessons)} уроков"
        with open(image_path, 'rb') as photo:
            await context.bot.send_photo(chat_id=update.effective_chat.id, photo=photo, caption=caption)

        await asyncio.sleep(0.5)
        await show_main_menu(update, context, edit=False)
    except Exception as e:
        logger.error(f"Ошибка генерации: {e}")
        await query.edit_message_text(f"❌ Ошибка: {str(e)}")


async def update_from_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.edit_message_text("🔄 Загружаю расписание из email...")
    try:
        if await _try_load_from_email(days_back=14):
            cache_info = schedule_manager.get_cache_info()
            # Очищаем кэш изображений при обновлении
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


@secure_command
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


@secure_command
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


@secure_command
async def clear_cache_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    schedule_manager.clear_cache()
    image_cache.clear()
    await update.message.reply_text("🗑️ Кэш полностью очищен.")


@secure_command
async def block_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Блокировка пользователя (только для админа)"""
    user_id = update.effective_user.id

    if ADMIN_USER_ID != 0 and user_id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Только для администратора.")
        return

    try:
        if not context.args:
            await update.message.reply_text("❌ Укажите ID пользователя: /block <user_id>")
            return

        target_id = int(context.args[0])
        user_blacklist.add_user(target_id)
        await update.message.reply_text(f"✅ Пользователь {target_id} заблокирован.")
    except ValueError:
        await update.message.reply_text("❌ Неверный формат ID.")


@secure_command
async def unblock_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Разблокировка пользователя (только для админа)"""
    user_id = update.effective_user.id

    if ADMIN_USER_ID != 0 and user_id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Только для администратора.")
        return

    try:
        if not context.args:
            await update.message.reply_text("❌ Укажите ID пользователя: /unblock <user_id>")
            return

        target_id = int(context.args[0])
        user_blacklist.remove_user(target_id)
        await update.message.reply_text(f"✅ Пользователь {target_id} разблокирован.")
    except ValueError:
        await update.message.reply_text("❌ Неверный формат ID.")


@secure_command
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Публичная статистика"""
    cache_info = schedule_manager.get_cache_info()
    await update.message.reply_text(
        f"📊 Статистика бота:\n\n"
        f"📅 Дат в расписании: {len(cache_info['dates'])}\n"
        f"📚 Всего уроков: {cache_info['total_lessons']}\n"
        f"🕐 Обновлено: {cache_info['age_text']}\n"
        f"👥 Бот доступен для всех"
    )


def main():
    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN не задан!")
        return

    os.makedirs('cache', exist_ok=True)
    os.makedirs('Fonts', exist_ok=True)

    application = Application.builder().token(TELEGRAM_TOKEN).build()

    # Команды для всех
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("update", update_command))
    application.add_handler(CommandHandler("cache", cache_info_command))
    application.add_handler(CommandHandler("clearcache", clear_cache_command))
    application.add_handler(CommandHandler("stats", stats_command))

    # Админские команды
    application.add_handler(CommandHandler("block", block_user_command))
    application.add_handler(CommandHandler("unblock", unblock_user_command))

    # Обработчик callback кнопок
    application.add_handler(CallbackQueryHandler(button_handler))

    logger.info("Бот запущен с защитой от DDoS!")
    logger.info(f"Rate limit: 15 запросов в минуту на пользователя")

    # Используем polling (webhook требует дополнительной настройки)
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()