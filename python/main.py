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

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import Message, BufferedInputFile
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from minio import Minio
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

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
    MAX_FILE_SIZE: int = field(default_factory=lambda: int(os.getenv('MAX_FILE_SIZE', '209715200')))  # 10 MB
    STATE_DB_PATH: str = field(default_factory=lambda: os.getenv('STATE_DB_PATH', 'bot_state.db'))

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

class StateManager:
    """Управление состоянием для устойчивости к перезапускам"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db = None

    async def init(self):
        """Инициализация базы данных"""
        self.db = await aiosqlite.connect(self.db_path)

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

        await self.db.commit()
        logger.info(f"✅ База данных инициализирована: {self.db_path}")

    async def save_task(self, task: 'ProcessingTask'):
        """Сохраняет задачу в БД"""
        await self.db.execute('''
            INSERT OR REPLACE INTO tasks
            (task_id, user_id, chat_id, processing_message_id, file_name,
             original_minio_path, processed_minio_path, status, created_at,
             started_at, completed_at, error_message, kafka_message_sent, kafka_response_received)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            task.task_id, task.user_id, task.chat_id, task.processing_message_id,
            task.file_name, task.original_minio_path, task.processed_minio_path,
            task.status.value, task.created_at, task.started_at, task.completed_at,
            task.error_message, task.kafka_message_sent, task.kafka_response_received
        ))
        await self.db.commit()

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
        if not isinstance(task_id, str):
            raise TypeError(f"task_id must be str, got {type(task_id)}")

        query = """
        UPDATE tasks
        SET kafka_response_received = 1
        WHERE task_id = ?
        """

        async with self.db.execute(query, (task_id,)):
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
            WHERE status IN (?, ?)
            AND kafka_response_received = 0
            AND created_at > datetime('now', '-1 hour')
        ''', (TaskStatus.PENDING.value, TaskStatus.PROCESSING.value))

        rows = await cursor.fetchall()
        return [row[0] for row in rows]

    async def get_task(self, task_id: str) -> Optional[Dict]:
        """Получает задачу по ID"""
        cursor = await self.db.execute('''
            SELECT * FROM tasks WHERE task_id = ?
        ''', (task_id,))

        row = await cursor.fetchone()
        if row:
            # Преобразуем sqlite3.Row в словарь
            columns = [description[0] for description in cursor.description]
            return dict(zip(columns, row))
        return None

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
            'kafka_response_received': self.kafka_response_received
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'ProcessingTask':
        """Создает из словаря"""
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
            kafka_message_sent=bool(data['kafka_message_sent']),
            kafka_response_received=bool(data['kafka_response_received'])
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

# Глобальная ссылка на основной event loop
main_loop = None

def set_main_loop(loop):
    """Устанавливает основной event loop"""
    global main_loop
    main_loop = loop

async def run_in_main_loop(coro):
    """Запускает корутину в основном event loop"""
    if main_loop and main_loop != asyncio.get_event_loop():
        # Создаем future в основном loop
        future = asyncio.run_coroutine_threadsafe(coro, main_loop)
        try:
            return future.result(timeout=30)
        except Exception as e:
            logger.error(f"❌ Ошибка выполнения в основном loop: {e}")
            return None
    else:
        # Уже в основном loop
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

        # Попытки с экспоненциальной задержкой
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
                    time.sleep(2 ** attempt)  # Экспоненциальная задержка
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
        logger.info(f"📤 [send_direct_message] Текст: {text[:100]}...")

        # Создаем корутину для отправки
        async def send_coro():
            try:
                logger.info(f"📤 [send_coro] Пытаюсь отправить сообщение...")
                result = await bot.send_message(
                    chat_id=chat_id,
                    text=text[:1000],
                    parse_mode=ParseMode.HTML  # Убедитесь, что используете HTML
                )
                logger.info(f"📤 [send_coro] Сообщение отправлено успешно!")
                return result
            except Exception as e:
                logger.error(f"❌ [send_coro] Ошибка: {e}")
                raise

        # Запускаем в основном loop
        result = await run_in_main_loop(send_coro())

        if result:
            logger.info(f"✅ [send_direct_message] Сообщение отправлено в chat_id={chat_id}")
            return True
        else:
            logger.error(f"❌ [send_direct_message] Не удалось отправить сообщение в chat_id={chat_id}")
            # Пробуем HTTP как запасной вариант
            logger.info(f"📤 [send_direct_message] Пробую отправить через HTTP API...")
            return send_telegram_message_sync(chat_id, text)

    except Exception as e:
        logger.error(f"❌ [send_direct_message] Критическая ошибка отправки в chat_id={chat_id}: {e}")
        # Пробуем HTTP как последний шанс
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
                # Используем asyncio.sleep для неблокирующего ожидания
                await asyncio.sleep(0.1)

                # Получаем сообщения с таймаутом
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

                            # Если ключ пустой, берем из тела
                            if not task_id:
                                task_id = response_data.get('task_id')

                            if not task_id:
                                logger.error(f"❌ [CONSUMER] Не удалось извлечь task_id!")
                                consumer.commit()
                                continue

                            logger.info(f"📥 [CONSUMER] Задача: {task_id}, статус: {response_data.get('status')}")

                            # Создаем задачу для обработки
                            asyncio.create_task(handle_kafka_response(task_id, response_data))

                            consumer.commit()

                        except Exception as e:
                            logger.error(f"❌ [CONSUMER] Ошибка обработки сообщения: {e}")
                            logger.exception(e)

            except Exception as e:
                logger.error(f"❌ [CONSUMER] Ошибка в цикле: {e}")
                logger.exception(e)
                await asyncio.sleep(1)  # Пауза при ошибке

    except NoBrokersAvailable:
        logger.error("❌ Не удалось подключиться к Kafka брокерам")
    except Exception as e:
        logger.error(f"❌ Не удалось запустить Kafka Consumer: {e}")
        logger.exception(e)
    finally:
        if consumer:
            consumer.close()


async def send_processed_file_to_user(task: ProcessingTask):
    """Отправляет обработанный файл пользователю"""
    try:
        logger.info(f"📤 Отправка файла для задачи {task.task_id}")

        if not task.processed_minio_path:
            logger.error(f"❌ Нет пути к обработанному файлу")
            await send_direct_message(task.chat_id, f"❌ Файл не найден: {task.file_name}")
            return

        # Скачиваем файл
        file_content = await download_from_minio(task.processed_minio_path)

        # Получаем расширение файла из пути
        file_ext = os.path.splitext(task.processed_minio_path)[-1] or '.csv'
        output_filename = f"processed_{task.task_id[:8]}{file_ext}"

        file_obj = BufferedInputFile(file_content, filename=output_filename)

        # Очень короткое описание
        caption = f"✅ {task.file_name}"

        # Создаем корутину для отправки файла
        async def send_file_coro():
            try:
                await bot.send_document(
                    chat_id=task.chat_id,
                    document=file_obj,
                    caption=caption[:1024]
                )
                logger.info(f"✅ Файл отправлен пользователю {task.user_id}")
            except Exception as e:
                logger.error(f"❌ Ошибка отправки файла: {e}")

        # Запускаем в основном loop
        if main_loop and main_loop != asyncio.get_event_loop():
            asyncio.run_coroutine_threadsafe(send_file_coro(), main_loop)
        else:
            await send_file_coro()

    except Exception as e:
        logger.error(f"❌ Ошибка отправки файла: {e}")

        # Отправляем уведомление об ошибке
        await send_direct_message(
            task.chat_id,
            f"❌ Ошибка отправки файла: {task.file_name}"
        )

async def handle_kafka_response(task_id: str, response_data: Dict[str, Any]):
    """
    Обрабатывает ответ из Kafka OUTPUT topic.
    """
    try:
        logger.info(f"🔧 [handle_kafka_response] ======== НАЧАЛО ОБРАБОТКИ ========")
        logger.info(f"🔧 [handle_kafka_response] Вызвана с task_id: {task_id}")

        # ПРОСТАЯ ПРОВЕРКА - функция ли вообще вызывается?
        if not task_id:
            logger.error(f"❌ [handle_kafka_response] Task ID пустой!")
            return

        logger.info(f"🔧 [handle_kafka_response] Ищу задачу в БД...")

        # Простой поиск задачи
        task_data = await state_manager.get_task(task_id)

        if not task_data:
            logger.error(f"❌ [handle_kafka_response] Задача {task_id} не найдена в БД!")

            # Проверим, есть ли такая задача по части ID
            if len(task_id) >= 8:
                short_id = task_id[:8]
                cursor = await state_manager.db.execute('''
                    SELECT task_id FROM tasks WHERE task_id LIKE ? LIMIT 1
                ''', (f'{short_id}%',))
                row = await cursor.fetchone()
                if row:
                    logger.info(f"🔧 [handle_kafka_response] Нашлась задача по частичному ID: {row[0]}")
                    # Получаем полные данные
                    task_data = await state_manager.get_task(row[0])
                    task_id = row[0]  # Обновляем ID
                else:
                    logger.error(f"❌ [handle_kafka_response] Задача не найдена даже по частичному ID!")
                    return
            else:
                return

        # Преобразуем в ProcessingTask
        task = ProcessingTask.from_dict(task_data)
        logger.info(f"✅ [handle_kafka_response] Задача найдена: {task.file_name} для пользователя {task.user_id}")

        # ПРОСТАЯ ОБРАБОТКА СТАТУСА
        status = response_data.get('status', '').lower()
        logger.info(f"🔧 [handle_kafka_response] Статус обработки: {status}")

        if status == 'success':
            # Обновляем задачу
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now()
            task.kafka_response_received = True

            # Сохраняем путь к файлу
            output_path = response_data.get('output', {}).get('file_path')
            if output_path:
                task.processed_minio_path = output_path
                logger.info(f"📁 [handle_kafka_response] Путь к файлу: {output_path}")

            await state_manager.save_task(task)

            # Отправляем уведомление
            message = f"✅ Обработка завершена: {task.file_name}"
            logger.info(f"📤 [handle_kafka_response] Отправляю сообщение: {message}")

            # ПРОСТАЯ ОТПРАВКА
            try:
                await bot.send_message(
                    chat_id=task.chat_id,
                    text=message,
                    parse_mode=ParseMode.HTML
                )
                logger.info(f"✅ [handle_kafka_response] Сообщение отправлено!")
            except Exception as send_error:
                logger.error(f"❌ [handle_kafka_response] Ошибка отправки: {send_error}")

            # Если нужно отправить файл
            if response_data.get('notifications', {}).get('should_send_file', True) and task.processed_minio_path:
                logger.info(f"📤 [handle_kafka_response] Отправляю файл...")
                await send_processed_file_to_user(task)

        else:
            logger.warning(f"⚠️ [handle_kafka_response] Неуспешный статус: {status}")
            task.status = TaskStatus.FAILED
            task.error_message = f"Статус: {status}"
            task.kafka_response_received = True
            await state_manager.save_task(task)

            # Отправляем уведомление об ошибке
            error_msg = f"❌ Ошибка обработки: {task.file_name}"
            await bot.send_message(chat_id=task.chat_id, text=error_msg)

        # Помечаем ответ полученным
        await state_manager.mark_kafka_response_received(task.task_id)
        logger.info(f"✅ [handle_kafka_response] Ответ помечен как полученный")

        # Удаляем из активных задач
        if task.task_id in active_tasks:
            del active_tasks[task.task_id]

        logger.info(f"🎉 [handle_kafka_response] ОБРАБОТКА ЗАВЕРШЕНА УСПЕШНО!")

    except Exception as e:
        logger.error(f"❌ [handle_kafka_response] КРИТИЧЕСКАЯ ОШИБКА: {e}")
        logger.exception(e)

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "📁 Бот для обработки файлов с Kafka\n\n"
        "Отправьте мне JSON или HTML файл, и я:\n"
        "1. 📤 Сохраню в MinIO\n"
        "2. 🔄 Отправлю задачу в Kafka\n"
        "3. ⏳ Дождусь обработки\n"
        "4. 📥 Отправлю результат\n\n"
        "Команды:\n"
        "/status - статус системы\n"
        "/tasks - мои задачи\n"
        "/retry <id> - повторить задачу\n"
        "/check <id> - проверить задачу"
    )

@dp.message(Command("status"))
async def cmd_status(message: Message):
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

    pending_tasks = await state_manager.get_pending_tasks()
    if pending_tasks:
        status_text += f"\n🔄 Незавершенных задач: {len(pending_tasks)}"
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
        # Получаем названия колонок
        columns = [description[0] for description in cursor.description]
        task = dict(zip(columns, row))

        task_id_short = task['task_id'][:8]
        status_icon = {
            'completed': '✅',
            'failed': '❌',
            'pending': '⏳',
            'processing': '🔄',
            'timeout': '⏰'
        }.get(task['status'], '❓')

        tasks_text += f"{i}. {status_icon} {task['file_name']}\n"
        tasks_text += f"   ID: {task_id_short}... | Статус: {task['status']}\n"

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
        TaskStatus.TIMEOUT: "⏰ Таймаут"
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
        SELECT task_id, file_name, user_id, chat_id, status, created_at
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
        response += f"   🕐 {row[5]}\n\n"

    await message.answer(response[:4000])

@dp.message(F.document)
async def handle_document(message: Message):
    """Обработка документов"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    file_name = message.document.file_name

    logger.info(f"📥 [handle_document] Получен файл: {file_name} от user_id={user_id}, chat_id={chat_id}")


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


async def main():
     """Основная функция"""
     logger.info("🚀 Запуск бота с Kafka и восстановлением состояния...")

     # Устанавливаем основной event loop
     set_main_loop(asyncio.get_event_loop())

     await state_manager.init()

     await recover_pending_tasks()

     try:
         bot_info = await bot.get_me()
         logger.info(f"✅ Бот: @{bot_info.username} ({bot_info.first_name})")
     except Exception as e:
         logger.error(f"❌ Ошибка подключения к боту: {e}")
         return

     # ✅ ВАЖНО: Запускаем Kafka Consumer как фоновую задачу
     if config.KAFKA_BOOTSTRAP_SERVERS:
         logger.info("✅ Запускаю Kafka Consumer как фоновую задачу...")
         consumer_task = asyncio.create_task(start_kafka_consumer())
         logger.info(f"✅ Kafka Consumer запущен как фоновая задача")

     logger.info("✅ Бот готов к работе!")
     logger.info(f"📊 Активных задач: {len(active_tasks)}")

     try:
         await dp.start_polling(bot)
     finally:
         # Закрываем соединения
         logger.info("🛑 Останавливаю бота...")
         await state_manager.close()
         if kafka_producer:
             kafka_producer.close()
         # Отменяем задачу consumer
         if 'consumer_task' in locals():
             consumer_task.cancel()
             try:
                 await consumer_task
             except asyncio.CancelledError:
                 logger.info("✅ Kafka Consumer остановлен")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")