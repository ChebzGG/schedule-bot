import os
from dotenv import load_dotenv

load_dotenv()

# Telegram
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
ALLOWED_USER_ID = int(os.getenv('ALLOWED_USER_ID', '0'))

# Gmail IMAP
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
IMAP_SERVER = os.getenv('IMAP_SERVER', 'imap.gmail.com')

# Настройки (используются внутри менеджеров)
TIMES_FILE = 'times.json'
CACHE_FILE = 'cache/schedule.json'
EMAIL_DAYS_BACK = 3