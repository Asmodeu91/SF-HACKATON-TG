import os
import asyncio
import json
import uuid
import logging
import hashlib
import requests
import time
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from io import BytesIO
import aiosqlite
from enum import Enum
import traceback
import html

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import Message, BufferedInputFile
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from minio import Minio
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

import pandas as pd
from io import StringIO


from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class Config:
    TELEGRAM_BOT_TOKEN: str = field(default_factory=lambda: os.getenv('TELEGRAM_BOT_TOKEN', ''))
    MINIO_ENDPOINT: str = field(default_factory=lambda: os.getenv('MINIO_ENDPOINT', 'localhost:9000'))
    MINIO_ACCESS_KEY: str = field(default_factory=lambda: os.getenv('MINIO_ACCESS_KEY', 'minioadmin'))
    MINIO_SECRET_KEY: str = field(default_factory=lambda: os.getenv('MINIO_SECRET_KEY', 'minioadmin'))
    MINIO_SECURE: bool = field(default_factory=lambda: os.getenv('MINIO_SECURE', 'False').lower() == 'true')
    MINIO_INPUT_BUCKET: str = field(default_factory=lambda: os.getenv('MINIO_INPUT_BUCKET', 'input-files'))
    MINIO_OUTPUT_BUCKET: str = field(default_factory=lambda: os.getenv('MINIO_OUTPUT_BUCKET', 'output-files'))
    KAFKA_BOOTSTRAP_SERVERS: str = field(default_factory=lambda: os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9094'))
    KAFKA_INPUT_TOPIC: str = field(default_factory=lambda: os.getenv('KAFKA_INPUT_TOPIC', 'INPUT'))
    KAFKA_OUTPUT_TOPIC: str = field(default_factory=lambda: os.getenv('KAFKA_OUTPUT_TOPIC', 'OUTPUT'))
    KAFKA_CONSUMER_GROUP: str = field(default_factory=lambda: os.getenv('KAFKA_CONSUMER_GROUP', 'telegram-bot-group'))
    PROCESSING_TIMEOUT: int = field(default_factory=lambda: int(os.getenv('PROCESSING_TIMEOUT', '300')))  # 5 минут
    MAX_FILE_SIZE: int = field(default_factory=lambda: int(os.getenv('MAX_FILE_SIZE', '209715200')))  # 200 MB
    STATE_DB_PATH: str = field(default_factory=lambda: os.getenv('STATE_DB_PATH', 'bot_state.db'))
    MAX_GROUP_FILES: int = field(default_factory=lambda: int(os.getenv('MAX_GROUP_FILES', '50')))  # Макс файлов в группе

config = Config()

if not config.TELEGRAM_BOT_TOKEN:
    logger.error("❌ TELEGRAM_BOT_TOKEN не найден!")
    exit(1)


class TaskStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    COLLECTING = "collecting"  # Новый статус для сбора файлов


class StateManager:
    """Управление состоянием для устойчивости к перезапускам"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db = None

    async def init(self):
        """Инициализация базы данных"""
        self.db = await aiosqlite.connect(self.db_path)

        # Создаем таблицы если их нет
        await self.create_tables()

        # Проверяем и добавляем недостающие колонки
        await self.migrate_tables()

        await self.db.commit()
        logger.info(f"✅ База данных инициализирована: {self.db_path}")

    async def create_tables(self):
        """Создание таблиц"""
        await self.db.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                user_id INTEGER,
                chat_id INTEGER,
                processing_message_id INTEGER,
                file_name TEXT,
                original_minio_path TEXT,
                processed_minio_path TEXT,
                status TEXT,
                created_at TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                error_message TEXT,
                kafka_message_sent BOOLEAN DEFAULT 0,
                kafka_response_received BOOLEAN DEFAULT 0
            )
        ''')

        await self.db.execute('''
            CREATE TABLE IF NOT EXISTS kafka_messages (
                message_id TEXT PRIMARY KEY,
                task_id TEXT,
                topic TEXT,
                key TEXT,
                value TEXT,
                sent_at TIMESTAMP,
                FOREIGN KEY (task_id) REFERENCES tasks (task_id)
            )
        ''')

        await self.db.execute('''
            CREATE TABLE IF NOT EXISTS group_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                file_name TEXT NOT NULL,
                minio_path TEXT NOT NULL,
                order_index INTEGER DEFAULT 0,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (task_id) REFERENCES tasks (task_id)
            )
        ''')

    async def migrate_tables(self):
        """Миграция таблиц - добавление недостающих колонок"""
        try:
            # Проверяем наличие колонок в таблице tasks
            cursor = await self.db.execute("PRAGMA table_info(tasks)")
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]

            # Добавляем недостающие колонки
            missing_columns = []

            if 'is_group_task' not in column_names:
                missing_columns.append('is_group_task')
                await self.db.execute('ALTER TABLE tasks ADD COLUMN is_group_task BOOLEAN DEFAULT 0')
                logger.info("✅ Добавлена колонка is_group_task в таблицу tasks")

            if 'group_task_id' not in column_names:
                missing_columns.append('group_task_id')
                await self.db.execute('ALTER TABLE tasks ADD COLUMN group_task_id TEXT')
                logger.info("✅ Добавлена колонка group_task_id в таблицу tasks")

            if 'parent_task_id' not in column_names:
                missing_columns.append('parent_task_id')
                await self.db.execute('ALTER TABLE tasks ADD COLUMN parent_task_id TEXT')
                logger.info("✅ Добавлена колонка parent_task_id в таблицу tasks")

            if missing_columns:
                logger.info(f"✅ Миграция выполнена. Добавлены колонки: {', '.join(missing_columns)}")

        except Exception as e:
            logger.error(f"❌ Ошибка миграции таблиц: {e}")

        # Создаем индекс для групповых файлов
        await self.db.execute('''
            CREATE INDEX IF NOT EXISTS idx_group_files_group ON group_files (group_id)
        ''')

    async def save_task(self, task: 'ProcessingTask'):
        """Сохраняет задачу в БД"""
        try:
            await self.db.execute('''
                INSERT OR REPLACE INTO tasks
                (task_id, user_id, chat_id, processing_message_id, file_name,
                 original_minio_path, processed_minio_path, status, created_at,
                 started_at, completed_at, error_message, kafka_message_sent,
                 kafka_response_received, is_group_task, group_task_id, parent_task_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                task.task_id, task.user_id, task.chat_id, task.processing_message_id,
                task.file_name, task.original_minio_path, task.processed_minio_path,
                task.status.value, task.created_at, task.started_at, task.completed_at,
                task.error_message, task.kafka_message_sent, task.kafka_response_received,
                task.is_group_task, task.group_task_id, task.parent_task_id
            ))
            await self.db.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения задачи в БД: {e}")
            raise

    async def update_task_status(self, task_id: str, status: TaskStatus, error_message: str = None):
        """Обновляет статус задачи"""
        query = "UPDATE tasks SET status = ?"
        params = [status.value]

        if error_message:
            query += ", error_message = ?"
            params.append(error_message)

        if status == TaskStatus.COMPLETED:
            query += ", completed_at = ?"
            params.append(datetime.now())

        query += " WHERE task_id = ?"
        params.append(task_id)

        await self.db.execute(query, params)
        await self.db.commit()

    async def mark_kafka_message_sent(self, task_id: str):
        """Отмечает, что сообщение Kafka отправлено"""
        await self.db.execute(
            "UPDATE tasks SET kafka_message_sent = 1 WHERE task_id = ?",
            (task_id,)
        )
        await self.db.commit()

    async def mark_kafka_response_received(self, task_id: str):
        """Отмечает, что ответ Kafka получен"""
        query = "UPDATE tasks SET kafka_response_received = 1 WHERE task_id = ?"
        await self.db.execute(query, (task_id,))
        await self.db.commit()

    async def save_kafka_message(self, task_id: str, topic: str, key: str, value: Dict):
        """Сохраняет отправленное сообщение Kafka"""
        message_id = hashlib.md5(f"{task_id}:{key}:{datetime.now()}".encode()).hexdigest()

        await self.db.execute('''
            INSERT INTO kafka_messages (message_id, task_id, topic, key, value, sent_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            message_id, task_id, topic, key, json.dumps(value), datetime.now()
        ))
        await self.db.commit()

    async def get_pending_tasks(self) -> List[str]:
        """Возвращает список задач в статусе pending/processing"""
        cursor = await self.db.execute('''
            SELECT task_id FROM tasks
            WHERE status IN (?, ?, ?)
            AND kafka_response_received = 0
            AND created_at > datetime('now', '-1 hour')
        ''', (TaskStatus.PENDING.value, TaskStatus.PROCESSING.value, TaskStatus.COLLECTING.value))

        rows = await cursor.fetchall()
        return [row[0] for row in rows]

    async def get_task(self, task_id: str) -> Optional[Dict]:
        """Получает задачу по ID"""
        cursor = await self.db.execute('''
            SELECT * FROM tasks WHERE task_id = ?
        ''', (task_id,))

        row = await cursor.fetchone()
        if row:
            columns = [description[0] for description in cursor.description]
            return dict(zip(columns, row))
        return None

    async def save_group_file(self, group_id: str, task_id: str, file_name: str, minio_path: str, order_index: int = 0):
        """Сохраняет информацию о файле в группе"""
        await self.db.execute('''
            INSERT INTO group_files (group_id, task_id, file_name, minio_path, order_index)
            VALUES (?, ?, ?, ?, ?)
        ''', (group_id, task_id, file_name, minio_path, order_index))
        await self.db.commit()

    async def get_group_files(self, group_id: str) -> List[Dict]:
        """Получает все файлы в группе"""
        cursor = await self.db.execute('''
            SELECT * FROM group_files
            WHERE group_id = ?
            ORDER BY order_index, added_at
        ''', (group_id,))

        rows = await cursor.fetchall()
        result = []
        if rows:
            columns = [description[0] for description in cursor.description]
            for row in rows:
                result.append(dict(zip(columns, row)))
        return result

    async def get_group_files_count(self, group_id: str) -> int:
        """Получает количество файлов в группе"""
        cursor = await self.db.execute('''
            SELECT COUNT(*) FROM group_files WHERE group_id = ?
        ''', (group_id,))
        row = await cursor.fetchone()
        return row[0] if row else 0

    async def delete_group_files(self, group_id: str):
        """Удаляет все файлы группы"""
        await self.db.execute('DELETE FROM group_files WHERE group_id = ?', (group_id,))
        await self.db.commit()

    async def close(self):
        """Закрывает соединение с БД"""
        if self.db:
            await self.db.close()

state_manager = StateManager(config.STATE_DB_PATH)


@dataclass
class ProcessingTask:
    """Задача обработки файла"""
    task_id: str
    user_id: int
    chat_id: int
    processing_message_id: Optional[int] = None
    file_name: str = ""
    original_minio_path: str = ""
    processed_minio_path: str = ""
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    kafka_message_sent: bool = False
    kafka_response_received: bool = False
    is_group_task: bool = False  # Является ли задачей группы
    group_task_id: Optional[str] = None  # ID группы (если это групповой файл)
    parent_task_id: Optional[str] = None  # ID родительской задачи (для групповых)

    def to_dict(self) -> Dict:
        """Конвертирует в словарь для сериализации"""
        return {
            'task_id': self.task_id,
            'user_id': self.user_id,
            'chat_id': self.chat_id,
            'processing_message_id': self.processing_message_id,
            'file_name': self.file_name,
            'original_minio_path': self.original_minio_path,
            'processed_minio_path': self.processed_minio_path,
            'status': self.status.value,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'error_message': self.error_message,
            'kafka_message_sent': self.kafka_message_sent,
            'kafka_response_received': self.kafka_response_received,
            'is_group_task': self.is_group_task,
            'group_task_id': self.group_task_id,
            'parent_task_id': self.parent_task_id
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'ProcessingTask':
        """Создает из словаря"""
        # Устанавливаем значения по умолчанию для старых записей
        is_group_task = data.get('is_group_task', False)
        if isinstance(is_group_task, str):
            is_group_task = is_group_task.lower() in ('true', '1', 't')

        task = cls(
            task_id=data['task_id'],
            user_id=data['user_id'],
            chat_id=data['chat_id'],
            processing_message_id=data['processing_message_id'],
            file_name=data['file_name'],
            original_minio_path=data['original_minio_path'],
            processed_minio_path=data['processed_minio_path'],
            status=TaskStatus(data['status']),
            error_message=data['error_message'],
            kafka_message_sent=bool(data.get('kafka_message_sent', False)),
            kafka_response_received=bool(data.get('kafka_response_received', False)),
            is_group_task=is_group_task,
            group_task_id=data.get('group_task_id'),
            parent_task_id=data.get('parent_task_id')
        )

        if data['created_at']:
            task.created_at = datetime.fromisoformat(data['created_at'])
        if data['started_at']:
            task.started_at = datetime.fromisoformat(data['started_at'])
        if data['completed_at']:
            task.completed_at = datetime.fromisoformat(data['completed_at'])

        return task


try:
    bot = Bot(
        token=config.TELEGRAM_BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher()
    logger.info("✅ Бот инициализирован с увеличенными таймаутами")
except Exception as e:
    logger.error(f"❌ Ошибка инициализации бота: {e}")
    exit(1)

minio_client = None
try:
    minio_client = Minio(
        config.MINIO_ENDPOINT,
        access_key=config.MINIO_ACCESS_KEY,
        secret_key=config.MINIO_SECRET_KEY,
        secure=config.MINIO_SECURE
    )
    logger.info(f"✅ MinIO клиент подключен к {config.MINIO_ENDPOINT}")

    for bucket in [config.MINIO_INPUT_BUCKET, config.MINIO_OUTPUT_BUCKET]:
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)
            logger.info(f"✅ Создан бакет: {bucket}")

except Exception as e:
    logger.error(f"⚠️ Ошибка MinIO: {e}")
    minio_client = None

kafka_producer = None
try:
    kafka_producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS.split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3,
        max_block_ms=5000,
        max_in_flight_requests_per_connection=1
    )
    logger.info(f"✅ Kafka Producer подключен к {config.KAFKA_BOOTSTRAP_SERVERS}")
except Exception as e:
    logger.error(f"⚠️ Ошибка Kafka Producer: {e}")
    kafka_producer = None

active_tasks = {}  # task_id -> ProcessingTask
user_group_sessions = {}  # user_id -> group_session_info

# Глобальная ссылка на основной event loop
main_loop = None

def set_main_loop(loop):
    """Устанавливает основной event loop"""
    global main_loop
    main_loop = loop

async def run_in_main_loop(coro):
    """Запускает корутину в основном event loop"""
    if main_loop and main_loop != asyncio.get_event_loop():
        future = asyncio.run_coroutine_threadsafe(coro, main_loop)
        try:
            return future.result(timeout=30)
        except Exception as e:
            logger.error(f"❌ Ошибка выполнения в основном loop: {e}")
            return None
    else:
        return await coro

def send_telegram_message_sync(chat_id: int, text: str):
    """Синхронная отправка сообщения через HTTP API"""
    try:
        url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text[:4000],
            "parse_mode": "HTML"
        }

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.post(url, json=payload, timeout=30)
                response.raise_for_status()
                logger.info(f"✅ HTTP сообщение отправлено в chat_id={chat_id}")
                return True
            except requests.exceptions.Timeout:
                logger.warning(f"⏰ Таймаут HTTP запроса (попытка {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                continue
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ HTTP ошибка (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                continue

        logger.error(f"❌ Не удалось отправить HTTP сообщение после {max_retries} попыток")
        return False

    except Exception as e:
        logger.error(f"❌ Критическая ошибка HTTP отправки: {e}")
        return False

async def send_direct_message(chat_id: int, text: str):
    """Прямая отправка сообщения через основной event loop"""
    try:
        logger.info(f"📤 [send_direct_message] Отправка сообщения в chat_id={chat_id}")

        async def send_coro():
            try:
                result = await bot.send_message(
                    chat_id=chat_id,
                    text=text[:4000],
                    parse_mode=ParseMode.HTML
                )
                return result
            except Exception as e:
                logger.error(f"❌ [send_coro] Ошибка: {e}")
                raise

        result = await run_in_main_loop(send_coro())

        if result:
            logger.info(f"✅ [send_direct_message] Сообщение отправлено в chat_id={chat_id}")
            return True
        else:
            logger.error(f"❌ [send_direct_message] Не удалось отправить сообщение в chat_id={chat_id}")
            return send_telegram_message_sync(chat_id, text)

    except Exception as e:
        logger.error(f"❌ [send_direct_message] Критическая ошибка отправки в chat_id={chat_id}: {e}")
        return send_telegram_message_sync(chat_id, text)


async def upload_to_minio(file_content: bytes, file_name: str, bucket: str, content_type: str = "application/octet-stream") -> str:
    """Загружает bytes в MinIO и возвращает путь"""
    if not minio_client:
        raise Exception("MinIO клиент не инициализирован")

    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_name = f"{timestamp}_{uuid.uuid4().hex[:8]}_{file_name}"

        file_stream = BytesIO(file_content)

        minio_client.put_object(
            bucket_name=bucket,
            object_name=unique_name,
            data=file_stream,
            length=len(file_content),
            content_type=content_type
        )

        minio_path = f"{bucket}/{unique_name}"
        logger.info(f"✅ Файл загружен в MinIO: {minio_path}")

        return minio_path

    except Exception as e:
        logger.error(f"❌ Ошибка при загрузке в MinIO: {e}")
        raise

async def download_from_minio(minio_path: str) -> bytes:
    """Скачивает файл из MinIO"""
    if not minio_client:
        raise Exception("MinIO клиент не инициализирован")

    try:
        bucket_name, object_name = minio_path.split('/', 1)

        response = minio_client.get_object(bucket_name, object_name)
        file_content = response.read()
        response.close()
        response.release_conn()

        logger.info(f"✅ Файл скачан из MinIO: {minio_path} ({len(file_content)} bytes)")

        return file_content

    except Exception as e:
        logger.error(f"❌ Ошибка при скачивании из MinIO: {e}")
        raise

async def merge_html_files(file_paths: List[str], file_names: List[str]) -> bytes:
    """
    Склеивает несколько HTML файлов в один.

    Args:
        file_paths: Список путей к файлам в MinIO
        file_names: Список оригинальных имен файлов

    Returns:
        bytes: Склеенный HTML контент
    """
    merged_content = []

    # Создаем заголовок без использования .format() с фигурными скобками
    header = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Объединенные HTML файлы</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            line-height: 1.6;
        }}
        .file-section {{
            border: 1px solid #ddd;
            margin-bottom: 20px;
            padding: 15px;
            border-radius: 5px;
            page-break-inside: avoid;
        }}
        .file-header {{
            background-color: #f5f5f5;
            padding: 10px;
            margin: -15px -15px 15px -15px;
            border-bottom: 1px solid #ddd;
            font-weight: bold;
            color: #333;
        }}
        hr {{
            border: none;
            border-top: 2px dashed #ccc;
            margin: 30px 0;
        }}
        .metadata {{
            font-size: 12px;
            color: #666;
            margin-bottom: 10px;
        }}
        .error {{
            color: #d32f2f;
            background-color: #ffebee;
            padding: 10px;
            border-radius: 4px;
            border: 1px solid #ffcdd2;
        }}
    </style>
</head>
<body>
    <h1>Объединенные HTML файлы</h1>
    <div class="metadata">
        Объединено: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}<br>
        Количество файлов: {len(file_paths)}<br>
        Объединение выполнено ботом
    </div>
    <hr>
"""

    merged_content.append(header)

    # Загружаем и добавляем каждый файл
    for i, (file_path, file_name) in enumerate(zip(file_paths, file_names), 1):
        try:
            logger.info(f"📁 Обрабатываю файл {i}/{len(file_paths)}: {file_name}")

            file_content = await download_from_minio(file_path)
            logger.info(f"✅ Загружено {len(file_content)} байт из {file_path}")

            if not file_content:
                logger.warning(f"⚠️ Файл {file_name} пустой")
                merged_content.append(f"""
    <div class="file-section">
        <div class="file-header">
            ⚠️ Файл {i}: {file_name} (пустой файл)
        </div>
        <p class="error">Файл пустой</p>
    </div>
""")
                if i < len(file_paths):
                    merged_content.append('    <hr>\n')
                continue

            # Декодируем контент с обработкой ошибок
            html_content = ""
            try:
                html_content = file_content.decode('utf-8', errors='ignore')
                logger.info(f"✅ Файл {file_name} успешно декодирован")
            except Exception as decode_error:
                logger.error(f"❌ Ошибка декодирования файла {file_name}: {decode_error}")
                # Пробуем другие кодировки
                for encoding in ['cp1251', 'iso-8859-1', 'koi8-r', 'windows-1251']:
                    try:
                        html_content = file_content.decode(encoding, errors='ignore')
                        logger.info(f"✅ Файл {file_name} декодирован как {encoding}")
                        break
                    except:
                        continue

                if not html_content:
                    html_content = "<!-- Ошибка декодирования файла -->"

            # Очищаем контент от потенциально проблемных символов
            html_content = html_content.strip()

            # Удаляем потенциально опасные строки
            # Удаляем неполные CSS свойства
            lines_to_remove = [
                'font-family', 'margin:', 'padding:', 'color:',
                'background:', 'border:', 'width:', 'height:'
            ]

            for line in lines_to_remove:
                html_content = html_content.replace(f'\n            {line}', '')
                html_content = html_content.replace(f' {line}', '')

            # Удаляем лишние теги если они есть (осторожно, чтобы не сломать структуру)
            html_content = html_content.replace('</body>', '').replace('</html>', '')

            # Удаляем открывающие теги <html>, <head>, <body> если они в начале файла
            lines = html_content.split('\n')
            cleaned_lines = []
            for line in lines:
                line = line.strip()
                if line.lower() in ['<html>', '<head>', '<body>']:
                    continue
                # Пропускаем пустые строки с фигурными скобками (могут быть из CSS)
                if line in ['{', '}']:
                    continue
                cleaned_lines.append(line)

            html_content = '\n'.join(cleaned_lines)

            # Экранируем специальные символы в имени файла для HTML
            safe_filename = (file_name
                           .replace('&', '&amp;')
                           .replace('<', '&lt;')
                           .replace('>', '&gt;')
                           .replace('"', '&quot;')
                           .replace("'", '&#39;'))

            # Очищаем HTML контент от потенциально опасных конструкций
            html_content = clean_html_content(html_content)

            merged_content.append(f"""
    <div class="file-section">
        <div class="file-header">
            📄 Файл {i}: {safe_filename}
        </div>
        <div class="file-content">
            {html_content}
        </div>
    </div>
""")

            if i < len(file_paths):
                merged_content.append('    <hr>\n')

        except Exception as e:
            logger.error(f"❌ Критическая ошибка при обработке файла {file_name}: {e}")
            logger.exception(e)

            safe_filename = (file_name
                           .replace('&', '&amp;')
                           .replace('<', '&lt;')
                           .replace('>', '&gt;')
                           .replace('"', '&quot;')
                           .replace("'", '&#39;'))

            merged_content.append(f"""
    <div class="file-section">
        <div class="file-header">
            ❌ Файл {i}: {safe_filename} (ошибка обработки)
        </div>
        <div class="error">
            <p><strong>Ошибка при обработке файла:</strong></p>
            <pre>{html.escape(str(e)[:200])}</pre>
        </div>
    </div>
""")

            if i < len(file_paths):
                merged_content.append('    <hr>\n')

    # Добавляем закрывающие теги
    merged_content.append("""
</body>
</html>
""")

    try:
        # Конвертируем в bytes
        final_html = ''.join(merged_content)
        logger.info(f"✅ Объединенный HTML создан, длина: {len(final_html)} символов")

        # Проверяем валидность UTF-8
        encoded_bytes = final_html.encode('utf-8', errors='replace')
        logger.info(f"✅ HTML сконвертирован в bytes, размер: {len(encoded_bytes)} байт")

        return encoded_bytes
    except Exception as e:
        logger.error(f"❌ Ошибка при создании итогового HTML: {e}")
        logger.exception(e)

        # Возвращаем минимальный валидный HTML с ошибкой
        error_html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <title>Ошибка объединения файлов</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .error {{ color: #d32f2f; background-color: #ffebee; padding: 20px; border-radius: 5px; }}
    </style>
</head>
<body>
    <h1>❌ Ошибка при объединении файлов</h1>
    <div class="error">
        <p><strong>Произошла ошибка:</strong></p>
        <p>{html.escape(str(e)[:500])}</p>
        <p>Попробуйте перезагрузить файлы или обратитесь к администратору.</p>
    </div>
</body>
</html>"""

        return error_html.encode('utf-8')


async def send_csv_content_to_chat(chat_id: int, csv_content: bytes, task_id: str = None, max_rows: int = 50):
    """
    Читает CSV файл и отправляет его содержимое в чат

    Args:
        chat_id: ID чата
        csv_content: Байты CSV
        task_id: ID задачи
        max_rows: Максимальное количество строк для показа
    """
    try:
        # Декодируем CSV
        csv_text = csv_content.decode('utf-8', errors='ignore').strip()

        if not csv_text:
            await send_direct_message(chat_id, "❌ CSV файл пустой")
            return False

        # Определяем разделитель
        delimiter = ','
        if ';' in csv_text.split('\n')[0]:
            delimiter = ';'
        elif '\t' in csv_text.split('\n')[0]:
            delimiter = '\t'

        # Читаем CSV
        try:
            df = pd.read_csv(StringIO(csv_text), delimiter=delimiter)
        except Exception as e:
            logger.error(f"❌ Ошибка чтения CSV: {e}")
            # Пробуем без указания разделителя
            df = pd.read_csv(StringIO(csv_text))

        # Базовая информация
        total_rows = len(df)
        total_cols = len(df.columns)

        info_message = (
            f"📊 <b>CSV файл проанализирован</b>\n\n"
            f"📈 <b>Всего строк:</b> {total_rows:,}\n"
            f"📋 <b>Колонок:</b> {total_cols}\n"
            f"🔢 <b>Разделитель:</b> {delimiter}\n"
        )

        if task_id:
            info_message += f"📋 <b>ID задачи:</b> <code>{task_id}</code>\n"

        # Показываем колонки
        info_message += f"\n<b>📋 Список колонок:</b>\n"
        for i, col in enumerate(df.columns, 1):
            dtype = str(df[col].dtype)
            non_null = df[col].notna().sum()
            info_message += f"{i}. <code>{col}</code> ({dtype}, {non_null}/{total_rows} заполнены)\n"

        await send_direct_message(chat_id, info_message)

        # Если мало строк, показываем всю таблицу
        if total_rows <= max_rows and total_cols <= 10:
            # Формируем Markdown таблицу
            table_message = "<b>📋 Полное содержимое:</b>\n\n"

            # Заголовок
            headers = "| " + " | ".join(df.columns.astype(str)) + " |\n"
            separator = "|" + "|".join(["---"] * len(df.columns)) + "|\n"

            table_message += "<pre>" + headers + separator

            # Данные (первые max_rows строк)
            for _, row in df.head(max_rows).iterrows():
                row_str = "| " + " | ".join([
                    str(val)[:50].replace('\n', ' ') if pd.notna(val) else "NULL"
                    for val in row.values
                ]) + " |\n"
                table_message += row_str

            table_message += "</pre>"

            if total_rows < max_rows:
                table_message += f"\n✅ Показаны все {total_rows} строк"

            await send_direct_message(chat_id, table_message[:4000])

        elif total_rows <= 1000:
            # Для средних файлов показываем статистику и первые строки
            stats_message = "<b>📈 Статистика (первые 10 строк):</b>\n\n"

            # Показываем первые 10 строк
            preview_df = df.head(10)

            # Формируем компактную таблицу
            preview_headers = "| " + " | ".join(preview_df.columns.astype(str)) + " |\n"
            preview_separator = "|" + "|".join(["---"] * len(preview_df.columns)) + "|\n"

            stats_message += "<pre>" + preview_headers + preview_separator

            for _, row in preview_df.iterrows():
                row_str = "| " + " | ".join([
                    str(val)[:30].replace('\n', ' ') if pd.notna(val) else "NULL"
                    for val in row.values
                ]) + " |\n"
                stats_message += row_str

            stats_message += "</pre>"

            # Добавляем статистику по числовым колонкам
            numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
            if len(numeric_cols) > 0:
                stats_message += f"\n<b>📊 Статистика по числовым колонкам:</b>\n"
                for col in numeric_cols[:3]:  # Первые 3 числовые колонки
                    stats = df[col].describe()
                    stats_message += (
                        f"\n<b>{col}:</b>\n"
                        f"  Среднее: {stats.get('mean', 0):.2f}\n"
                        f"  Мин: {stats.get('min', 0):.2f}\n"
                        f"  Макс: {stats.get('max', 0):.2f}\n"
                        f"  Медиана: {stats.get('50%', 0):.2f}\n"
                    )

            await send_direct_message(chat_id, stats_message[:4000])

            # Предупреждение о большом размере
            if total_rows > max_rows:
                await send_direct_message(
                    chat_id,
                    f"⚠️ <b>Файл содержит {total_rows:,} строк</b>\n"
                    f"Показаны только первые 10 строк.\n"
                    f"Используйте полную версию для просмотра всех данных."
                )

        else:
            # Для очень больших файлов только статистика
            stats_message = f"<b>📈 Очень большой CSV файл ({total_rows:,} строк)</b>\n\n"

            # Информация о типах данных
            dtypes = df.dtypes.value_counts()
            stats_message += "<b>Типы данных:</b>\n"
            for dtype, count in dtypes.items():
                stats_message += f"  {dtype}: {count} колонок\n"

            # Информация о пропущенных значениях
            missing_percent = (df.isnull().sum() / len(df) * 100)
            high_missing = missing_percent[missing_percent > 50]
            if len(high_missing) > 0:
                stats_message += f"\n⚠️ <b>Колонки с >50% пропусков:</b>\n"
                for col, percent in high_missing.items():
                    stats_message += f"  {col}: {percent:.1f}%\n"

            await send_direct_message(chat_id, stats_message)

        logger.info(f"✅ Содержимое CSV отправлено в чат {chat_id}")
        return True

    except Exception as e:
        logger.error(f"❌ Ошибка при отправке содержимого CSV: {e}")
        await send_direct_message(
            chat_id,
            f"❌ Не удалось проанализировать CSV файл: {str(e)[:200]}"
        )
        return False


def clean_html_content(content: str) -> str:
    """
    Очищает HTML контент от потенциально опасных конструкций.

    Args:
        content: Исходный HTML контент

    Returns:
        str: Очищенный HTML контент
    """
    if not content:
        return ""

    # Удаляем фигурные скобки, которые могут вызывать ошибки форматирования
    content = content.replace('{', '&#123;').replace('}', '&#125;')

    # Разбиваем на строки и обрабатываем каждую
    lines = content.split('\n')
    cleaned_lines = []

    for line in lines:
        # Удаляем строки, содержащие только фигурные скобки
        stripped_line = line.strip()
        if stripped_line in ['{', '}']:
            continue

        # Пропускаем строки с неполными CSS свойствами
        if (':' in stripped_line and
            not stripped_line.endswith(';') and
            not stripped_line.endswith('}') and
            '{' not in stripped_line):
            # Это может быть неполное CSS свойство
            continue

        # Добавляем строку
        cleaned_lines.append(line)

    return '\n'.join(cleaned_lines)


def clean_html_content(content: str) -> str:
    """
    Очищает HTML контент от потенциально опасных конструкций.

    Args:
        content: Исходный HTML контент

    Returns:
        str: Очищенный HTML контент
    """
    if not content:
        return ""

    # Удаляем неполные CSS свойства
    lines = content.split('\n')
    cleaned_lines = []

    for line in lines:
        # Проверяем, не является ли строка неполным CSS свойством
        stripped_line = line.strip()
        if (stripped_line and
            not stripped_line.startswith('<') and
            not stripped_line.endswith(';') and
            not stripped_line.endswith('}') and
            ':' in stripped_line and
            '{' not in stripped_line):

            # Это может быть неполное CSS свойство, пропускаем его
            logger.warning(f"⚠️ Обнаружено неполное CSS свойство: {stripped_line[:50]}")
            continue

        # Удаляем строки, содержащие только открывающую фигурную скобку
        if stripped_line == '{' or stripped_line == '}':
            continue

        # Удаляем строки с незавершенными CSS правилами
        if stripped_line.startswith('font-family') and ';' not in stripped_line:
            continue

        cleaned_lines.append(line)

    return '\n'.join(cleaned_lines)


async def cleanup_minio_file(minio_path: str):
    """Удаляет файл из MinIO после успешной отправки"""
    if not minio_client or not minio_path:
        return False

    try:
        bucket_name, object_name = minio_path.split('/', 1)

        # Удаляем объект
        minio_client.remove_object(bucket_name, object_name)

        logger.info(f"✅ Файл удален из MinIO: {minio_path}")
        return True

    except Exception as e:
        logger.error(f"⚠️ Ошибка удаления файла из MinIO {minio_path}: {e}")
        return False

async def create_group_task(user_id: int, chat_id: int, group_id: str) -> ProcessingTask:
    """
    Создает групповую задачу для объединения файлов.

    Args:
        user_id: ID пользователя
        chat_id: ID чата
        group_id: ID группы файлов

    Returns:
        ProcessingTask: Созданная задача
    """
    task_id = str(uuid.uuid4())
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"merged_group_{group_id}_{timestamp}.html"

    task = ProcessingTask(
        task_id=task_id,
        user_id=user_id,
        chat_id=chat_id,
        file_name=file_name,
        status=TaskStatus.COLLECTING,
        is_group_task=True,
        group_task_id=group_id
    )

    await state_manager.save_task(task)
    active_tasks[task_id] = task

    return task

async def process_group_files(group_id: str, parent_task: ProcessingTask):
    """
    Обрабатывает групповые файлы: объединяет и отправляет на обработку.

    Args:
        group_id: ID группы
        parent_task: Родительская задача
    """
    try:
        logger.info(f"🔄 Начинаю обработку группы файлов: {group_id}")

        # Получаем файлы группы
        group_files = await state_manager.get_group_files(group_id)

        if not group_files:
            logger.error(f"❌ В группе {group_id} нет файлов")
            parent_task.status = TaskStatus.FAILED
            parent_task.error_message = "Нет файлов для объединения"
            await state_manager.save_task(parent_task)

            await send_direct_message(
                parent_task.chat_id,
                "❌ Ошибка: в группе нет файлов для объединения"
            )
            return

        logger.info(f"📊 Найдено файлов в группе {group_id}: {len(group_files)}")

        # Отправляем уведомление о начале объединения
        await send_direct_message(
            parent_task.chat_id,
            f"🔄 Начинаю объединение {len(group_files)} файлов..."
        )

        # Обновляем статус
        parent_task.status = TaskStatus.PROCESSING
        parent_task.started_at = datetime.now()
        await state_manager.save_task(parent_task)

        # Получаем пути и имена файлов
        file_paths = [file['minio_path'] for file in group_files]
        file_names = [file['file_name'] for file in group_files]

        logger.info(f"📁 Пути файлов: {file_paths[:3]}...")  # Логируем первые 3 пути

        # Объединяем файлы
        merged_content = await merge_html_files(file_paths, file_names)

        # Проверяем, что результат не пустой
        if not merged_content:
            raise Exception("Объединенный контент пустой")

        # Сохраняем объединенный файл в MinIO
        merged_file_name = f"merged_{group_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"

        await send_direct_message(
            parent_task.chat_id,
            f"📤 Загружаю объединенный файл в MinIO..."
        )

        logger.info(f"📤 Загружаю объединенный файл в MinIO: {merged_file_name}")

        merged_minio_path = await upload_to_minio(
            file_content=merged_content,
            file_name=merged_file_name,
            bucket=config.MINIO_INPUT_BUCKET,
            content_type='text/html'
        )

        logger.info(f"✅ Файл загружен в MinIO: {merged_minio_path}")

        # Обновляем задачу
        parent_task.original_minio_path = merged_minio_path
        parent_task.file_name = merged_file_name
        await state_manager.save_task(parent_task)

        # Отправляем в Kafka для обработки
        if kafka_producer:
            logger.info(f"📤 Отправляю объединенный файл в Kafka: {parent_task.task_id}")

            await send_to_kafka_input(parent_task)

            await send_direct_message(
                parent_task.chat_id,
                f"✅ Файлы объединены и отправлены на обработку!\n\n"
                f"📊 Объединено файлов: {len(group_files)}\n"
                f"📁 Итоговый файл: {merged_file_name}\n"
                f"📋 ID задачи: <code>{parent_task.task_id}</code>\n"
                f"⏳ Ожидаю обработки (макс. {config.PROCESSING_TIMEOUT} сек)..."
            )

            # Запускаем проверку таймаута
            asyncio.create_task(check_processing_timeout(parent_task.task_id))
        else:
            # Если Kafka недоступен, используем эмуляцию
            logger.warning("⚠️ Kafka недоступен, использую эмуляцию обработки")

            await send_direct_message(
                parent_task.chat_id,
                f"⚠️ Kafka недоступен, запускаю эмуляцию обработки...\n"
                f"📋 ID задачи: <code>{parent_task.task_id}</code>"
            )

            await emulate_processing(parent_task, merged_content)

        # Очищаем временные данные группы
        await state_manager.delete_group_files(group_id)
        logger.info(f"✅ Данные группы {group_id} очищены")

    except Exception as e:
        logger.error(f"❌ Ошибка при обработке группы файлов {group_id}: {e}")
        logger.exception(e)  # Добавляем полный traceback

        parent_task.status = TaskStatus.FAILED
        parent_task.error_message = f"Ошибка объединения: {str(e)[:200]}"
        await state_manager.save_task(parent_task)

        # Подробное сообщение об ошибке
        error_message = (
            f"❌ Ошибка объединения файлов:\n"
            f"Ошибка: {str(e)[:300]}\n\n"
            f"📋 ID задачи: <code>{parent_task.task_id}</code>\n"
            f"📋 ID группы: <code>{group_id}</code>"
        )

        await send_direct_message(parent_task.chat_id, error_message)

async def send_to_kafka_input(task: ProcessingTask) -> bool:
    """Отправляет сообщение в Kafka input topic"""
    if not kafka_producer:
        raise Exception("Kafka Producer не инициализирован")

    kafka_message = {
        "event_id": str(uuid.uuid4()),
        "event_type": "file_uploaded",
        "event_timestamp": datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',

        "task": {
            "task_id": task.task_id,
            "user_id": task.user_id,
            "chat_id": task.chat_id,
            "source": "telegram_bot",
            "is_group_task": task.is_group_task,
            "group_task_id": task.group_task_id,
        },

        "file": {
            "original_name": task.file_name,
            "file_size": None,
            "file_type": "json" if task.file_name.endswith('.json') else "html",
            "encoding": "utf-8",
        },

        "storage": {
            "type": "minio",
            "bucket": config.MINIO_INPUT_BUCKET,
            "object_path": task.original_minio_path,
            "access_url": f"http://{config.MINIO_ENDPOINT}/{task.original_minio_path}",
        },

        "processing": {
            "required_operations": ["validate", "transform"],
            "priority": "normal",
            "timeout_seconds": config.PROCESSING_TIMEOUT,
            "expected_format": "json" if task.file_name.endswith('.json') else "html",
        },

        "recovery": {
            "retry_count": 0,
            "last_attempt": None,
            "original_message_id": task.processing_message_id,
            "bot_token_hash": hashlib.md5(config.TELEGRAM_BOT_TOKEN.encode()).hexdigest()[:8],
        },

        "metadata": {
            "version": "1.0",
            "environment": os.getenv("ENVIRONMENT", "development"),
            "processing_pipeline": "default",
        }
    }

    try:
        await state_manager.save_kafka_message(
            task_id=task.task_id,
            topic=config.KAFKA_INPUT_TOPIC,
            key=task.task_id,
            value=kafka_message
        )

        future = kafka_producer.send(
            topic=config.KAFKA_INPUT_TOPIC,
            key=task.task_id,
            value=kafka_message
        )

        record_metadata = future.get(timeout=10)

        task.kafka_message_sent = True
        await state_manager.mark_kafka_message_sent(task.task_id)
        await state_manager.save_task(task)

        logger.info(f"✅ Сообщение отправлено в Kafka: "
                   f"topic={record_metadata.topic}, "
                   f"partition={record_metadata.partition}, "
                   f"offset={record_metadata.offset}")

        return True

    except Exception as e:
        logger.error(f"❌ Ошибка отправки в Kafka: {e}")
        raise

async def start_kafka_consumer():
    """Запускает асинхронный Kafka Consumer для получения ответов"""
    if not config.KAFKA_BOOTSTRAP_SERVERS:
        logger.warning("⚠️ Kafka bootstrap servers не указаны, consumer не запущен")
        return

    consumer = None
    try:
        consumer = KafkaConsumer(
            config.KAFKA_OUTPUT_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS.split(','),
            group_id=config.KAFKA_CONSUMER_GROUP,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            max_poll_records=10,
            max_poll_interval_ms=300000,
            fetch_max_wait_ms=500,
            fetch_min_bytes=1,
            fetch_max_bytes=52428800
        )

        logger.info(f"✅ Kafka Consumer запущен. Топик: {config.KAFKA_OUTPUT_TOPIC}")

        while True:
            try:
                await asyncio.sleep(0.1)
                msg_pack = consumer.poll(timeout_ms=100)

                for tp, messages in msg_pack.items():
                    for message in messages:
                        try:
                            logger.info(f"📥 [CONSUMER] Получено сообщение для offset {message.offset}")

                            task_id = message.key
                            response_data = message.value

                            if not response_data:
                                logger.error(f"❌ [CONSUMER] Response_data пустой!")
                                consumer.commit()
                                continue

                            if not task_id:
                                task_id = response_data.get('task_id')

                            if not task_id:
                                logger.error(f"❌ [CONSUMER] Не удалось извлечь task_id!")
                                consumer.commit()
                                continue

                            logger.info(f"📥 [CONSUMER] Задача: {task_id}, статус: {response_data.get('status')}")

                            asyncio.create_task(handle_kafka_response(task_id, response_data))
                            consumer.commit()

                        except Exception as e:
                            logger.error(f"❌ [CONSUMER] Ошибка обработки сообщения: {e}")
                            logger.exception(e)

            except Exception as e:
                logger.error(f"❌ [CONSUMER] Ошибка в цикле: {e}")
                logger.exception(e)
                await asyncio.sleep(1)

    except NoBrokersAvailable:
        logger.error("❌ Не удалось подключиться к Kafka брокерам")
    except Exception as e:
        logger.error(f"❌ Не удалось запустить Kafka Consumer: {e}")
        logger.exception(e)
    finally:
        if consumer:
            consumer.close()

async def send_processed_file_to_user(
    task: ProcessingTask,
    chat_id: int = None,
    should_send_file: bool = True
):
    """
    Отправляет обработанный файл пользователю в зависимости от режима

    Args:
        task: Задача обработки
        chat_id: ID чата (если не указан, берется из задачи)
        should_send_file: Нужно ли отправлять файл (True) или только показывать содержимое (False)
    """
    try:
        logger.info(f"📤 Отправка файла для задачи {task.task_id}, should_send_file={should_send_file}")

        if not task.processed_minio_path:
            logger.error(f"❌ Нет пути к обработанному файлу")
            await send_direct_message(chat_id or task.chat_id, f"❌ Файл не найден: {task.file_name}")
            return

        # Используем chat_id из параметра или из задачи
        target_chat_id = chat_id or task.chat_id

        # Скачиваем файл
        file_content = await download_from_minio(task.processed_minio_path)

        # Определяем тип файла по расширению
        file_ext = os.path.splitext(task.processed_minio_path)[-1].lower()
        output_filename = f"processed_{task.task_id[:8]}{file_ext}"

        if should_send_file:
            # Режим 1: Отправляем файл
            caption = f"✅ {task.file_name}"
            if task.is_group_task:
                caption = f"✅ Объединенный файл ({task.file_name})"

            # Добавляем информацию о записях
            caption += f"\n📊 Файл готов к скачиванию"

            # Отправляем файл
            async def send_file_coro():
                try:
                    await bot.send_document(
                        chat_id=target_chat_id,
                        document=BufferedInputFile(file_content, filename=output_filename),
                        caption=caption[:1024]
                    )
                    logger.info(f"✅ Файл отправлен пользователю {task.user_id}")

                    # Если это CSV файл, дополнительно показываем содержимое
                    if file_ext == '.csv':
                        logger.info(f"📄 CSV файл отправлен, показываю содержимое...")
                        await send_csv_content_to_chat(
                            chat_id=target_chat_id,
                            csv_content=file_content,
                            task_id=task.task_id
                        )

                except Exception as e:
                    logger.error(f"❌ Ошибка отправки файла: {e}")

            if main_loop and main_loop != asyncio.get_event_loop():
                asyncio.run_coroutine_threadsafe(send_file_coro(), main_loop)
            else:
                await send_file_coro()

            # УДАЛЯЕМ ФАЙЛ ПОСЛЕ УСПЕШНОЙ ОТПРАВКИ
            await cleanup_minio_file(task.processed_minio_path)

        else:
            # Режим 2: Только показываем содержимое
            if file_ext == '.csv':
                # Для CSV файлов показываем содержимое в чате
                logger.info(f"📄 Показываю содержимое CSV файла в чате...")
                await send_csv_content_to_chat(
                    chat_id=target_chat_id,
                    csv_content=file_content,
                    task_id=task.task_id
                )

                # Отправляем сообщение, что файл не прикреплен
                await send_direct_message(
                    target_chat_id,
                    f"📄 <b>Содержимое CSV файла показано выше</b>\n\n"
                    f"📋 <b>Файл:</b> {task.file_name}\n"
                    f"📋 <b>ID задачи:</b> <code>{task.task_id}</code>\n"
                    f"ℹ️ Файл не был прикреплен по запросу бэкенда (should_send_file=false)"
                )

            elif file_ext == '.json':
                # Для JSON файлов показываем красивый формат
                try:
                    json_data = json.loads(file_content.decode('utf-8', errors='ignore'))
                    json_preview = json.dumps(json_data, indent=2, ensure_ascii=False)[:2000]

                    await send_direct_message(
                        target_chat_id,
                        f"📄 <b>Предпросмотр JSON файла:</b>\n\n"
                        f"<pre>{json_preview}</pre>\n\n"
                        f"📋 <b>Файл:</b> {task.file_name}\n"
                        f"📋 <b>ID задачи:</b> <code>{task.task_id}</code>"
                    )
                except Exception as json_error:
                    logger.error(f"❌ Ошибка парсинга JSON: {json_error}")
                    await send_direct_message(
                        target_chat_id,
                        f"📄 <b>Содержимое файла ({len(file_content)} байт)</b>\n\n"
                        f"📋 <b>Файл:</b> {task.file_name}\n"
                        f"📋 <b>ID задачи:</b> <code>{task.task_id}</code>"
                    )
            else:
                # Для других типов файлов просто показываем информацию
                await send_direct_message(
                    target_chat_id,
                    f"📄 <b>Файл обработан успешно!</b>\n\n"
                    f"📋 <b>Файл:</b> {task.file_name}\n"
                    f"📋 <b>Тип:</b> {file_ext}\n"
                    f"📋 <b>Размер:</b> {len(file_content)} байт\n"
                    f"📋 <b>ID задачи:</b> <code>{task.task_id}</code>\n"
                    f"ℹ️ Файл не был прикреплен по запросу бэкенда (should_send_file=false)"
                )

            # УДАЛЯЕМ ФАЙЛ ПОСЛЕ ПОКАЗА СОДЕРЖИМОГО
            await cleanup_minio_file(task.processed_minio_path)

        # Также удаляем оригинальный файл если он есть
        if task.original_minio_path:
            await cleanup_minio_file(task.original_minio_path)

    except Exception as e:
        logger.error(f"❌ Ошибка отправки файла: {e}")
        await send_direct_message(
            chat_id or task.chat_id,
            f"❌ Ошибка обработки файла: {task.file_name}"
        )

async def handle_kafka_response(task_id: str, response_data: Dict[str, Any]):
    """Обрабатывает ответ из Kafka OUTPUT topic"""
    try:
        logger.info(f"🔧 [handle_kafka_response] Начало обработки task_id: {task_id}")

        if not task_id:
            logger.error(f"❌ [handle_kafka_response] Task ID пустой!")
            return

        task_data = await state_manager.get_task(task_id)

        if not task_data:
            if len(task_id) >= 8:
                short_id = task_id[:8]
                cursor = await state_manager.db.execute('''
                    SELECT task_id FROM tasks WHERE task_id LIKE ? LIMIT 1
                ''', (f'{short_id}%',))
                row = await cursor.fetchone()
                if row:
                    task_data = await state_manager.get_task(row[0])
                    task_id = row[0]
                else:
                    logger.error(f"❌ [handle_kafka_response] Задача не найдена!")
                    return
            else:
                return

        task = ProcessingTask.from_dict(task_data)
        logger.info(f"✅ [handle_kafka_response] Задача найдена: {task.file_name}")

        status = response_data.get('status', '').lower()

        if status == 'success':
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now()
            task.kafka_response_received = True

            output_path = response_data.get('output', {}).get('file_path')
            if output_path:
                task.processed_minio_path = output_path
                logger.info(f"📁 [handle_kafka_response] Путь к файлу: {output_path}")

            await state_manager.save_task(task)

            # Получаем настройки уведомлений из response
            notifications = response_data.get('notifications', {})
            should_send_file = notifications.get('should_send_file', True)

            # Получаем chat_id и message_id из response или из задачи
            telegram_chat_id = notifications.get('telegram_chat_id', task.chat_id)
            telegram_message_id = notifications.get('telegram_message_id', task.processing_message_id)

            message = f"✅ Обработка завершена: {task.file_name}"
            if task.is_group_task:
                message = f"✅ Объединение и обработка завершены: {task.file_name}"

            # Добавляем информацию о записях
            results = response_data.get('results', {})
            if results:
                analysis = results.get('analysis', {})
                if analysis:
                    record_count = analysis.get('record_count')
                    if record_count is not None:
                        message += f"\n📊 Обработано записей: {record_count}"

            try:
                await bot.send_message(
                    chat_id=telegram_chat_id,
                    text=message,
                    parse_mode=ParseMode.HTML
                )
                logger.info(f"✅ [handle_kafka_response] Сообщение отправлено!")
            except Exception as send_error:
                logger.error(f"❌ [handle_kafka_response] Ошибка отправки: {send_error}")

            # Проверяем, нужно ли отправлять файл
            if should_send_file and task.processed_minio_path:
                logger.info(f"📤 [handle_kafka_response] should_send_file=True, отправляю файл...")
                await send_processed_file_to_user(task, telegram_chat_id, should_send_file=True)
            else:
                logger.info(f"📄 [handle_kafka_response] should_send_file=False, только показываю содержимое...")
                # Если не нужно отправлять файл, но есть CSV - показываем содержимое
                if task.processed_minio_path and task.processed_minio_path.endswith('.csv'):
                    try:
                        file_content = await download_from_minio(task.processed_minio_path)
                        await send_csv_content_to_chat(
                            chat_id=telegram_chat_id,
                            csv_content=file_content,
                            task_id=task.task_id
                        )
                        # После показа содержимого удаляем файл
                        await cleanup_minio_file(task.processed_minio_path)
                    except Exception as e:
                        logger.error(f"❌ Ошибка при показе CSV содержимого: {e}")
                elif not should_send_file:
                    # Если файл не нужно отправлять, удаляем его
                    await cleanup_minio_file(task.processed_minio_path)

                # Удаляем также оригинальный файл
                if task.original_minio_path:
                    await cleanup_minio_file(task.original_minio_path)

        else:
            logger.warning(f"⚠️ [handle_kafka_response] Неуспешный статус: {status}")
            task.status = TaskStatus.FAILED
            task.error_message = f"Статус: {status}"
            task.kafka_response_received = True
            await state_manager.save_task(task)

            error_msg = f"❌ Ошибка обработки: {task.file_name}"
            if task.is_group_task:
                error_msg = f"❌ Ошибка обработки объединенного файла: {task.file_name}"

            await bot.send_message(chat_id=task.chat_id, text=error_msg)

        await state_manager.mark_kafka_response_received(task.task_id)

        if task.task_id in active_tasks:
            del active_tasks[task.task_id]

        logger.info(f"🎉 [handle_kafka_response] Обработка завершена успешно!")

    except Exception as e:
        logger.error(f"❌ [handle_kafka_response] КРИТИЧЕСКАЯ ОШИБКА: {e}")
        logger.exception(e)

@dataclass
class GroupSession:
    """Сессия для сбора групповых файлов"""
    group_id: str
    user_id: int
    chat_id: int
    task_id: str
    files_count: int = 0
    max_files: int = config.MAX_GROUP_FILES
    created_at: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)
    is_active: bool = True

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "📁 Бот для обработки файлов с Kafka\n\n"
        "Отправьте мне JSON или HTML файл, и я:\n"
        "1. 📤 Сохраню в MinIO\n"
        "2. 🔄 Отправлю задачу в Kafka\n"
        "3. ⏳ Дождусь обработки\n"
        "4. 📥 Отправлю результат\n\n"
        "Для объединения HTML файлов в один:\n"
        "1. Отправьте /group_start чтобы начать сбор файлов\n"
        "2. Отправляйте HTML файлы (макс. {})\n"
        "3. Отправьте /group_finish чтобы объединить и обработать\n\n"
        "Команды:\n"
        "/status - статус системы\n"
        "/tasks - мои задачи\n"
        "/retry <id> - повторить задачу\n"
        "/check <id> - проверить задачу\n"
        "/group_start - начать сбор файлов\n"
        "/group_finish - завершить сбор и объединить\n"
        "/group_cancel - отменить сбор файлов\n"
        "/group_status - статус сбора файлов".format(config.MAX_GROUP_FILES)
    )

@dp.message(Command("group_start"))
async def cmd_group_start(message: Message):
    """Начинает сбор файлов для объединения"""
    user_id = message.from_user.id
    chat_id = message.chat.id

    # Проверяем, есть ли активная сессия
    if user_id in user_group_sessions:
        session = user_group_sessions[user_id]
        if session.is_active:
            await message.answer(
                f"⚠️ У вас уже есть активная сессия сбора файлов!\n"
                f"📊 Файлов собрано: {session.files_count}/{session.max_files}\n"
                f"📋 ID группы: <code>{session.group_id}</code>\n\n"
                f"Используйте /group_status чтобы посмотреть статус\n"
                f"Или /group_cancel чтобы отменить сбор"
            )
            return

    # Создаем новую групповую задачу
    group_id = str(uuid.uuid4())
    parent_task = await create_group_task(user_id, chat_id, group_id)

    # Создаем сессию
    session = GroupSession(
        group_id=group_id,
        user_id=user_id,
        chat_id=chat_id,
        task_id=parent_task.task_id
    )

    user_group_sessions[user_id] = session

    await message.answer(
        f"🔄 Начинаю сбор HTML файлов для объединения!\n\n"
        f"📋 ID группы: <code>{group_id}</code>\n"
        f"📋 ID задачи: <code>{parent_task.task_id}</code>\n\n"
        f"Теперь отправляйте мне HTML файлы.\n"
        f"Максимальное количество файлов: {session.max_files}\n\n"
        f"Когда все файлы будут отправлены, используйте команду:\n"
        f"/group_finish - чтобы объединить и отправить на обработку\n"
        f"/group_cancel - чтобы отменить сбор файлов\n"
        f"/group_status - чтобы посмотреть текущий статус"
    )

@dp.message(Command("group_finish"))
async def cmd_group_finish(message: Message):
    """Завершает сбор файлов и запускает объединение"""
    user_id = message.from_user.id

    if user_id not in user_group_sessions:
        await message.answer("❌ У вас нет активной сессии сбора файлов!")
        return

    session = user_group_sessions[user_id]

    if not session.is_active:
        await message.answer("❌ Эта сессия уже завершена!")
        return

    # Получаем количество файлов в группе
    files_count = await state_manager.get_group_files_count(session.group_id)

    if files_count == 0:
        await message.answer("❌ В группе нет файлов для объединения!")

        # Закрываем сессию
        session.is_active = False

        # Удаляем задачу
        task_data = await state_manager.get_task(session.task_id)
        if task_data:
            task = ProcessingTask.from_dict(task_data)
            task.status = TaskStatus.FAILED
            task.error_message = "Нет файлов для объединения"
            await state_manager.save_task(task)

        return

    # Помечаем сессию как неактивную
    session.is_active = False

    # Получаем родительскую задачу
    task_data = await state_manager.get_task(session.task_id)
    if not task_data:
        await message.answer("❌ Родительская задача не найдена!")
        return

    parent_task = ProcessingTask.from_dict(task_data)

    await message.answer(
        f"✅ Завершаю сбор файлов!\n\n"
        f"📊 Файлов для объединения: {files_count}\n"
        f"📋 ID группы: <code>{session.group_id}</code>\n"
        f"📋 ID задачи: <code>{parent_task.task_id}</code>\n\n"
        f"🔄 Начинаю объединение файлов..."
    )

    # Запускаем обработку группы файлов
    await process_group_files(session.group_id, parent_task)

@dp.message(Command("group_cancel"))
async def cmd_group_cancel(message: Message):
    """Отменяет сбор файлов"""
    user_id = message.from_user.id

    if user_id not in user_group_sessions:
        await message.answer("❌ У вас нет активной сессии сбора файлов!")
        return

    session = user_group_sessions[user_id]

    if not session.is_active:
        await message.answer("❌ Эта сессия уже завершена!")
        return

    # Получаем количество файлов в группе
    files_count = await state_manager.get_group_files_count(session.group_id)

    # Удаляем файлы группы
    await state_manager.delete_group_files(session.group_id)

    # Обновляем задачу
    task_data = await state_manager.get_task(session.task_id)
    if task_data:
        task = ProcessingTask.from_dict(task_data)
        task.status = TaskStatus.FAILED
        task.error_message = "Сбор файлов отменен пользователем"
        await state_manager.save_task(task)

    # Удаляем сессию
    del user_group_sessions[user_id]

    await message.answer(
        f"❌ Сбор файлов отменен!\n\n"
        f"📊 Удалено файлов: {files_count}\n"
        f"📋 ID группы: <code>{session.group_id}</code>"
    )

@dp.message(Command("group_status"))
async def cmd_group_status(message: Message):
    """Показывает статус сбора файлов"""
    user_id = message.from_user.id

    if user_id not in user_group_sessions:
        await message.answer("❌ У вас нет активной сессии сбора файлов!")
        return

    session = user_group_sessions[user_id]

    # Получаем количество файлов в группе
    files_count = await state_manager.get_group_files_count(session.group_id)

    # Получаем список файлов
    group_files = await state_manager.get_group_files(session.group_id)

    status_text = (
        f"📊 Статус сбора файлов:\n\n"
        f"📋 ID группы: <code>{session.group_id}</code>\n"
        f"📋 ID задачи: <code>{session.task_id}</code>\n"
        f"📊 Собрано файлов: {files_count}/{session.max_files}\n"
        f"🔄 Статус: {'Активен' if session.is_active else 'Завершен'}\n"
        f"⏱️ Создано: {session.created_at.strftime('%H:%M:%S')}\n"
        f"🕐 Последняя активность: {session.last_activity.strftime('%H:%M:%S')}\n\n"
    )

    if group_files:
        status_text += "📋 Список файлов:\n"
        for i, file_info in enumerate(group_files[:10], 1):
            status_text += f"{i}. {file_info['file_name']}\n"

        if len(group_files) > 10:
            status_text += f"... и еще {len(group_files) - 10} файлов\n"

    if session.is_active:
        status_text += (
            f"\nℹ️ Команды:\n"
            f"/group_finish - завершить сбор и объединить\n"
            f"/group_cancel - отменить сбор файлов"
        )

    await message.answer(status_text)

@dp.message(Command("status"))
async def cmd_status(message: Message):
    """Статус системы"""
    status_text = "📊 Статус системы:\n\n"

    if minio_client:
        try:
            buckets = list(minio_client.list_buckets())
            status_text += f"✅ MinIO: подключен\n"
            for bucket in buckets:
                objects = list(minio_client.list_objects(bucket.name))
                status_text += f"   📂 {bucket.name}: {len(objects)} файлов\n"
        except Exception as e:
            status_text += f"❌ MinIO ошибка: {str(e)[:50]}\n"
    else:
        status_text += "⚠️ MinIO: не подключен\n"

    if kafka_producer:
        status_text += f"✅ Kafka Producer: подключен\n"
    else:
        status_text += "⚠️ Kafka Producer: не подключен\n"

    # Активные сессии
    if user_group_sessions:
        status_text += f"\n🔄 Активных сессий сбора файлов: {len(user_group_sessions)}"
        for user_id, session in list(user_group_sessions.items())[:3]:
            files_count = await state_manager.get_group_files_count(session.group_id)
            status_text += f"\n   👤 {user_id}: {files_count} файлов"

    pending_tasks = await state_manager.get_pending_tasks()
    if pending_tasks:
        status_text += f"\n\n🔄 Незавершенных задач: {len(pending_tasks)}"
        for task_id in pending_tasks[:3]:
            status_text += f"\n   • {task_id[:8]}..."

    await message.answer(status_text)

@dp.message(Command("tasks"))
async def cmd_tasks(message: Message):
    """Показать задачи пользователя"""
    user_id = message.from_user.id

    cursor = await state_manager.db.execute('''
        SELECT * FROM tasks
        WHERE user_id = ?
        ORDER BY created_at DESC
        LIMIT 10
    ''', (user_id,))

    rows = await cursor.fetchall()

    if not rows:
        await message.answer("📭 У вас пока нет задач")
        return

    tasks_text = "📋 Ваши последние задачи:\n\n"

    for i, row in enumerate(rows, 1):
        columns = [description[0] for description in cursor.description]
        task = dict(zip(columns, row))

        task_id_short = task['task_id'][:8]
        status_icon = {
            'completed': '✅',
            'failed': '❌',
            'pending': '⏳',
            'processing': '🔄',
            'timeout': '⏰',
            'collecting': '📥'
        }.get(task['status'], '❓')

        tasks_text += f"{i}. {status_icon} {task['file_name']}\n"
        tasks_text += f"   ID: {task_id_short}... | Статус: {task['status']}"

        if task.get('is_group_task'):
            tasks_text += " | 📁 Групповая"

        tasks_text += "\n"

        if task['created_at']:
            created = datetime.fromisoformat(task['created_at'])
            tasks_text += f"   Создано: {created.strftime('%H:%M:%S')}\n"

        tasks_text += "\n"

    await message.answer(tasks_text)

@dp.message(Command("check"))
async def cmd_check(message: Message):
    """Проверить статус задачи"""
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Укажите ID задачи: /check <task_id>")
        return

    task_id = args[1]
    task_data = await state_manager.get_task(task_id)

    if not task_data:
        await message.answer(f"❌ Задача {task_id} не найдена")
        return

    task = ProcessingTask.from_dict(task_data)

    status_info = {
        TaskStatus.PENDING: "⏳ Ожидает",
        TaskStatus.PROCESSING: "🔄 Обрабатывается",
        TaskStatus.COMPLETED: "✅ Завершена",
        TaskStatus.FAILED: "❌ Ошибка",
        TaskStatus.TIMEOUT: "⏰ Таймаут",
        TaskStatus.COLLECTING: "📥 Сбор файлов"
    }

    status_text = status_info.get(task.status, "❓ Неизвестно")

    response = (
        f"🔍 <b>Статус задачи</b>\n\n"
        f"📋 <b>ID:</b> <code>{task.task_id}</code>\n"
        f"📄 <b>Файл:</b> {task.file_name}\n"
        f"📊 <b>Статус:</b> {status_text}\n"
        f"👤 <b>Пользователь:</b> {task.user_id}\n"
        f"🕐 <b>Создано:</b> {task.created_at.strftime('%H:%M:%S') if task.created_at else 'N/A'}\n"
    )

    if task.is_group_task:
        response += f"📁 <b>Тип:</b> Групповая задача\n"
        if task.group_task_id:
            response += f"📋 <b>ID группы:</b> <code>{task.group_task_id}</code>\n"

    if task.error_message:
        response += f"\n❌ <b>Ошибка:</b> {task.error_message[:200]}"

    if task.processed_minio_path:
        response += f"\n📁 <b>Результат:</b> {task.processed_minio_path}"

    await message.answer(response)

@dp.message(Command("retry"))
async def cmd_retry(message: Message):
    """Повторить выполнение задачи"""
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Укажите ID задачи: /retry <task_id>")
        return

    task_id = args[1]
    task_data = await state_manager.get_task(task_id)

    if not task_data:
        await message.answer("❌ Задача не найдена")
        return

    task = ProcessingTask.from_dict(task_data)

    if task.user_id != message.from_user.id:
        await message.answer("❌ Это не ваша задача")
        return

    try:
        processing_msg = await message.answer(f"🔄 Повторная отправка задачи {task_id[:8]}...")

        task.status = TaskStatus.PENDING
        task.processing_message_id = processing_msg.message_id
        task.error_message = None

        await state_manager.save_task(task)
        active_tasks[task_id] = task

        if kafka_producer:
            await send_to_kafka_input(task)
            await processing_msg.edit_text(f"✅ Задача повторно отправлена в Kafka!")
        else:
            await processing_msg.edit_text(f"❌ Kafka недоступен")

    except Exception as e:
        logger.error(f"❌ Ошибка при повторной отправке: {e}")
        await message.answer(f"❌ Ошибка: {str(e)[:200]}")

@dp.message(Command("debug_db"))
async def cmd_debug_db(message: Message):
    """Отладка БД"""
    cursor = await state_manager.db.execute('''
        SELECT task_id, file_name, user_id, chat_id, status, created_at, is_group_task
        FROM tasks
        ORDER BY created_at DESC
        LIMIT 20
    ''')

    rows = await cursor.fetchall()

    response = "📋 <b>Последние 20 задач в БД:</b>\n\n"
    for i, row in enumerate(rows, 1):
        response += f"{i}. <code>{row[0]}</code>\n"
        response += f"   📄 {row[1]}\n"
        response += f"   👤 {row[2]} (чат: {row[3]})\n"
        response += f"   📊 {row[4]}\n"
        response += f"   🕐 {row[5]}\n"
        if row[6]:
            response += f"   📁 Групповая задача\n"
        response += "\n"

    await message.answer(response[:4000])

@dp.message(F.document)
async def handle_document(message: Message):
    """Обработка документов"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    file_name = message.document.file_name

    logger.info(f"📥 [handle_document] Получен файл: {file_name} от user_id={user_id}, chat_id={chat_id}")

    # Проверяем, активна ли сессия сбора файлов
    if user_id in user_group_sessions and user_group_sessions[user_id].is_active:
        # Если активна сессия, обрабатываем как групповой файл
        await handle_group_document(message, file_name)
        return

    # Обычная обработка файла
    if not (file_name.endswith('.json') or file_name.endswith('.html')):
        await message.answer("❌ Отправьте только JSON (.json) или HTML (.html) файлы")
        return

    if message.document.file_size > config.MAX_FILE_SIZE:
        await message.answer(f"❌ Файл слишком большой. Максимальный размер: {config.MAX_FILE_SIZE // 209715200} MB")
        return

    task_id = str(uuid.uuid4())
    task = ProcessingTask(
        task_id=task_id,
        user_id=user_id,
        chat_id=chat_id,
        file_name=file_name,
        status=TaskStatus.PENDING
    )

    await state_manager.save_task(task)
    active_tasks[task_id] = task

    processing_msg = await message.answer(
        f"⏳ Загружаю файл: {file_name}\n"
        f"📋 ID задачи: <code>{task_id}</code>"
    )

    task.processing_message_id = processing_msg.message_id
    await state_manager.save_task(task)

    try:
        file_info = await bot.get_file(message.document.file_id)
        downloaded_file = await bot.download_file(file_info.file_path)
        file_content = downloaded_file.read()

        content_type = 'application/json' if file_name.endswith('.json') else 'text/html'

        await processing_msg.edit_text(
            f"📤 Загружаю в MinIO: {file_name}\n"
            f"📋 ID задачи: <code>{task_id}</code>"
        )

        minio_path = await upload_to_minio(
            file_content=file_content,
            file_name=file_name,
            bucket=config.MINIO_INPUT_BUCKET,
            content_type=content_type
        )

        task.original_minio_path = minio_path
        task.status = TaskStatus.PROCESSING
        task.started_at = datetime.now()
        await state_manager.save_task(task)

        await processing_msg.edit_text(
            f"🔗 Файл сохранен: {minio_path.split('/')[-1]}\n"
            f"📤 Отправляю в Kafka...\n"
            f"📋 ID задачи: <code>{task_id}</code>"
        )

        if kafka_producer:
            await send_to_kafka_input(task)

            await processing_msg.edit_text(
                f"✅ Задача отправлена в Kafka!\n\n"
                f"📄 Файл: {file_name}\n"
                f"📁 MinIO: {minio_path}\n"
                f"📋 ID задачи: <code>{task_id}</code>\n"
                f"⏳ Ожидаю обработки (макс. {config.PROCESSING_TIMEOUT} сек)..."
            )

            asyncio.create_task(check_processing_timeout(task_id))

        else:
            await processing_msg.edit_text(
                f"⚠️ Kafka недоступен, запускаю эмуляцию обработки...\n"
                f"📋 ID задачи: <code>{task_id}</code>"
            )

            await emulate_processing(task, file_content)

    except Exception as e:
        logger.error(f"❌ Ошибка обработки файла: {e}")
        task.status = TaskStatus.FAILED
        task.error_message = str(e)
        await state_manager.save_task(task)

        await processing_msg.edit_text(
            f"❌ Ошибка обработки файла:\n"
            f"{str(e)[:200]}\n\n"
            f"📋 ID задачи: <code>{task_id}</code>"
        )

async def handle_group_document(message: Message, file_name: str):
    """Обрабатывает документ как часть групповой задачи"""
    user_id = message.from_user.id
    chat_id = message.chat.id

    if user_id not in user_group_sessions:
        await message.answer("❌ У вас нет активной сессии сбора файлов!")
        return

    session = user_group_sessions[user_id]

    if not session.is_active:
        await message.answer("❌ Эта сессия уже завершена!")
        return

    # Проверяем тип файла
    if not file_name.endswith('.html'):
        await message.answer("❌ Для объединения принимаются только HTML файлы!")
        return

    if message.document.file_size > config.MAX_FILE_SIZE:
        await message.answer(f"❌ Файл слишком большой. Максимальный размер: {config.MAX_FILE_SIZE // 209715200} MB")
        return

    # Проверяем лимит файлов
    current_count = await state_manager.get_group_files_count(session.group_id)
    if current_count >= session.max_files:
        await message.answer(
            f"❌ Достигнут лимит файлов! ({session.max_files})\n\n"
            f"Используйте /group_finish чтобы завершить сбор и объединить файлы"
        )
        return

    try:
        # Создаем отдельную задачу для файла
        task_id = str(uuid.uuid4())
        task = ProcessingTask(
            task_id=task_id,
            user_id=user_id,
            chat_id=chat_id,
            file_name=file_name,
            status=TaskStatus.PENDING,
            is_group_task=True,
            group_task_id=session.group_id,
            parent_task_id=session.task_id
        )

        await state_manager.save_task(task)

        # Загружаем файл
        file_info = await bot.get_file(message.document.file_id)
        downloaded_file = await bot.download_file(file_info.file_path)
        file_content = downloaded_file.read()

        # Проверяем, что файл не пустой
        if len(file_content) == 0:
            await message.answer(f"⚠️ Файл {file_name} пустой. Он не будет добавлен в группу.")
            return

        # Загружаем в MinIO
        minio_path = await upload_to_minio(
            file_content=file_content,
            file_name=file_name,
            bucket=config.MINIO_INPUT_BUCKET,
            content_type='text/html'
        )

        # Обновляем задачу
        task.original_minio_path = minio_path
        task.status = TaskStatus.COMPLETED  # Файл загружен успешно
        await state_manager.save_task(task)

        # Сохраняем в группу
        await state_manager.save_group_file(
            group_id=session.group_id,
            task_id=task_id,
            file_name=file_name,
            minio_path=minio_path,
            order_index=current_count + 1
        )

        # Обновляем сессию
        session.files_count = current_count + 1
        session.last_activity = datetime.now()

        await message.answer(
            f"✅ Файл добавлен в группу!\n\n"
            f"📄 Файл: {file_name}\n"
            f"📊 В группе: {session.files_count}/{session.max_files} файлов\n"
            f"📋 ID группы: <code>{session.group_id}</code>\n\n"
            f"Отправляйте следующий HTML файл или используйте /group_finish"
        )

    except Exception as e:
        logger.error(f"❌ Ошибка обработки группового файла: {e}")
        logger.exception(e)
        await message.answer(
            f"❌ Ошибка добавления файла в группу:\n"
            f"{str(e)[:200]}"
        )

async def check_processing_timeout(task_id: str):
    """Проверяет таймаут обработки задачи"""
    await asyncio.sleep(config.PROCESSING_TIMEOUT)

    if task_id in active_tasks:
        task = active_tasks[task_id]
        if task.status in [TaskStatus.PENDING, TaskStatus.PROCESSING]:
            task.status = TaskStatus.TIMEOUT
            task.error_message = f"Таймаут обработки ({config.PROCESSING_TIMEOUT} сек)"
            await state_manager.save_task(task)

            await send_direct_message(
                task.chat_id,
                f"❌ Таймаут обработки: {task.file_name}"
            )

            logger.warning(f"⚠️ Таймаут обработки для задачи {task_id}")

async def emulate_processing(task: ProcessingTask, original_content: bytes):
    """Эмуляция обработки файла (если Kafka недоступен)"""
    try:
        await asyncio.sleep(3)

        if task.file_name.endswith('.json'):
            data = json.loads(original_content.decode('utf-8'))
            processed_data = {
                "metadata": {
                    "original_filename": task.file_name,
                    "task_id": task.task_id,
                    "processed_at": datetime.now().isoformat(),
                    "processing_type": "emulation",
                },
                "original_data": data,
                "statistics": {
                    "original_size": len(original_content),
                    "emulated_processing": True
                }
            }
            processed_content = json.dumps(processed_data, indent=2, ensure_ascii=False).encode('utf-8')
        else:
            processed_content = f"<!-- Emulated processing -->\n{original_content.decode('utf-8')}".encode('utf-8')

        output_name = f"processed_{task.file_name}"
        output_path = await upload_to_minio(
            file_content=processed_content,
            file_name=output_name,
            bucket=config.MINIO_OUTPUT_BUCKET,
            content_type='application/json' if task.file_name.endswith('.json') else 'text/html'
        )

        task.processed_minio_path = output_path
        task.status = TaskStatus.COMPLETED
        task.completed_at = datetime.now()
        task.kafka_response_received = True

        await state_manager.save_task(task)

        await send_processed_file_to_user(task)

    except Exception as e:
        logger.error(f"❌ Ошибка эмуляции обработки: {e}")
        task.status = TaskStatus.FAILED
        task.error_message = str(e)
        await state_manager.save_task(task)

        await send_direct_message(
            task.chat_id,
            f"❌ Ошибка эмуляции обработки: {task.file_name}"
        )

async def recover_pending_tasks():
    """Восстанавливает незавершенные задачи при перезапуске"""
    logger.info("🔄 Восстановление незавершенных задач...")

    pending_tasks = await state_manager.get_pending_tasks()

    for task_id in pending_tasks:
        try:
            task_data = await state_manager.get_task(task_id)
            if not task_data:
                continue

            task = ProcessingTask.from_dict(task_data)

            time_since_created = (datetime.now() - task.created_at).total_seconds()
            if time_since_created > config.PROCESSING_TIMEOUT:
                task.status = TaskStatus.TIMEOUT
                task.error_message = f"Задача устарела при перезапуске"
                await state_manager.save_task(task)
                logger.warning(f"⚠️ Задача {task_id} устарела")
                continue

            active_tasks[task_id] = task

            if task.kafka_message_sent and not task.kafka_response_received:
                logger.info(f"🔄 Восстанавливаю задачу: {task_id}")

                await send_direct_message(
                    task.chat_id,
                    f"🔄 Восстановление задачи после перезапуска...\n"
                    f"📋 ID: {task_id[:8]}..."
                )

                remaining_time = config.PROCESSING_TIMEOUT - time_since_created
                if remaining_time > 0:
                    asyncio.create_task(check_processing_timeout_with_delay(task_id, remaining_time))

        except Exception as e:
            logger.error(f"❌ Ошибка восстановления задачи {task_id}: {e}")

async def check_processing_timeout_with_delay(task_id: str, delay: float):
    """Проверяет таймаут с заданной задержкой"""
    await asyncio.sleep(delay)
    await check_processing_timeout(task_id)


async def cleanup_old_sessions():
    """Очистка устаревших сессий"""
    while True:
        try:
            current_time = datetime.now()
            users_to_remove = []

            for user_id, session in list(user_group_sessions.items()):
                # Если сессия активна и прошло больше 1 часа с последней активности
                if session.is_active and (current_time - session.last_activity).total_seconds() > 3600:
                    logger.warning(f"⚠️ Очистка устаревшей сессии пользователя {user_id}")

                    # Отправляем уведомление пользователю
                    await send_direct_message(
                        session.chat_id,
                        f"⚠️ Ваша сессия сбора файлов была автоматически отменена из-за неактивности.\n"
                        f"📋 ID группы: <code>{session.group_id}</code>"
                    )

                    # Удаляем файлы группы
                    await state_manager.delete_group_files(session.group_id)

                    # Обновляем задачу
                    task_data = await state_manager.get_task(session.task_id)
                    if task_data:
                        task = ProcessingTask.from_dict(task_data)
                        task.status = TaskStatus.FAILED
                        task.error_message = "Сессия отменена по таймауту неактивности"
                        await state_manager.save_task(task)

                    users_to_remove.append(user_id)

            # Удаляем устаревшие сессии
            for user_id in users_to_remove:
                if user_id in user_group_sessions:
                    del user_group_sessions[user_id]

            await asyncio.sleep(300)  # Проверяем каждые 5 минут

        except Exception as e:
            logger.error(f"❌ Ошибка в cleanup_old_sessions: {e}")
            await asyncio.sleep(60)

async def main():
    """Основная функция"""
    logger.info("🚀 Запуск бота с поддержкой объединения файлов...")

    set_main_loop(asyncio.get_event_loop())
    await state_manager.init()
    await recover_pending_tasks()

    try:
        bot_info = await bot.get_me()
        logger.info(f"✅ Бот: @{bot_info.username} ({bot_info.first_name})")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к боту: {e}")
        return

    # Запускаем Kafka Consumer
    if config.KAFKA_BOOTSTRAP_SERVERS:
        logger.info("✅ Запускаю Kafka Consumer как фоновую задачу...")
        consumer_task = asyncio.create_task(start_kafka_consumer())
        logger.info(f"✅ Kafka Consumer запущен как фоновая задача")

    # Запускаем очистку устаревших сессий
    cleanup_task = asyncio.create_task(cleanup_old_sessions())
    logger.info("✅ Очистка устаревших сессий запущена")

    logger.info("✅ Бот готов к работе!")
    logger.info(f"📊 Активных задач: {len(active_tasks)}")

    try:
        await dp.start_polling(bot)
    finally:
        logger.info("🛑 Останавливаю бота...")

        # Отменяем фоновые задачи
        if 'consumer_task' in locals():
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                logger.info("✅ Kafka Consumer остановлен")

        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            logger.info("✅ Очистка сессий остановлена")

        await state_manager.close()
        if kafka_producer:
            kafka_producer.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")