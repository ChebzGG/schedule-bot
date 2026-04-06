FROM python:3.11-slim

WORKDIR /app

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Копируем зависимости первыми (для кэширования)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код
COPY . .

# Создаём необходимые папки
RUN mkdir -p cache Fonts

# Открываем порт для health-check
EXPOSE 8080

# Запуск бота
CMD ["python", "bot.py"]