FROM python:3.11-slim

WORKDIR /app

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Копирование файла зависимостей
COPY requirements.txt .

# Установка Python зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода бота
COPY python/main.py .

# Создание директории для логов
RUN mkdir -p /app/logs

# Установка прав доступа
RUN chmod +x main.py

# Переменные окружения
ENV PYTHONUNBUFFERED=1

# Команда запуска
CMD ["python", "main.py"]