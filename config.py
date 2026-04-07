import os
from dotenv import load_dotenv

load_dotenv()

# Telegram
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
# 0 = публичный доступ для всех, твой ID = админ
ADMIN_USER_ID = 1160991959

# Gmail IMAP
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
IMAP_SERVER = os.getenv('IMAP_SERVER', 'imap.gmail.com')

# Настройки
TIMES_FILE = 'times.json'
CACHE_FILE = 'cache/schedule.json'
EMAIL_DAYS_BACK = 3


NOTIFY_CHAT_IDS = []  # Список ID чатов для уведомлений (заполняется автоматически)
SUBSCRIBERS_FILE = 'cache/subscribers.json'  # Файл с подписчиками
EMAIL_CHECK_INTERVAL = 60  # Интервал проверки почты в секундах (5 минут)