import os
import asyncio
import json
import uuid
import logging
import pickle
import hashlib
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field, asdict
from io import BytesIO
from pathlib import Path
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

    KAFKA_BOOTSTRAP_SERVERS: str = field(default_factory=lambda: os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'))
    KAFKA_INPUT_TOPIC: str = field(default_factory=lambda: os.getenv('KAFKA_INPUT_TOPIC', 'file-processing-input'))
    KAFKA_OUTPUT_TOPIC: str = field(default_factory=lambda: os.getenv('KAFKA_OUTPUT_TOPIC', 'file-processing-output'))
    KAFKA_CONSUMER_GROUP: str = field(default_factory=lambda: os.getenv('KAFKA_CONSUMER_GROUP', 'telegram-bot-group'))

    PROCESSING_TIMEOUT: int = field(default_factory=lambda: int(os.getenv('PROCESSING_TIMEOUT', '300')))  # 5 минут
    MAX_FILE_SIZE: int = field(default_factory=lambda: int(os.getenv('MAX_FILE_SIZE', '10485760')))  # 10 MB
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
        """Отмечает, что ответ Kafka получен"""
        await self.db.execute(
            "UPDATE tasks SET kafka_response_received = 1 WHERE task_id = ?",
            (task_id,)
        )
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
            return dict(row)
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
    bot = Bot(token=config.TELEGRAM_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    logger.info("✅ Бот инициализирован")
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
        max_in_flight_requests_per_connection=1,
        idempotent=True
    )
    logger.info(f"✅ Kafka Producer подключен к {config.KAFKA_BOOTSTRAP_SERVERS}")
except Exception as e:
    logger.error(f"⚠️ Ошибка Kafka Producer: {e}")
    kafka_producer = None

active_tasks = {}  # task_id -> ProcessingTask


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

        "event_id": str(uuid.uuid4()),  # Уникальный ID события
        "event_type": "file_uploaded",  # Тип события
        "event_timestamp": datetime.now().isoformat(),  # Время события

        "task": {
            "task_id": task.task_id,  # Уникальный ID задачи
            "user_id": task.user_id,  # ID пользователя Telegram
            "chat_id": task.chat_id,  # ID чата
            "source": "telegram_bot",  # Источник запроса
        },

        "file": {
            "original_name": task.file_name,  # Оригинальное имя файла
            "file_size": None,  # Будет заполнено если известно
            "file_type": "json" if task.file_name.endswith('.json') else "html",
            "encoding": "utf-8",  # Кодировка файла
        },

        "storage": {
            "type": "minio",  # Тип хранилища
            "bucket": config.MINIO_INPUT_BUCKET,  # Бакет MinIO
            "object_path": task.original_minio_path,  # Полный путь в MinIO
            "access_url": f"http://{config.MINIO_ENDPOINT}/{task.original_minio_path}",  # URL для доступа
        },

        "processing": {
            "required_operations": ["validate", "transform"],  # Требуемые операции
            "priority": "normal",  # Приоритет обработки
            "timeout_seconds": config.PROCESSING_TIMEOUT,  # Таймаут обработки
            "expected_format": "json" if task.file_name.endswith('.json') else "html",
        },

        "recovery": {
            "retry_count": 0,  # Счетчик попыток
            "last_attempt": None,  # Последняя попытка
            "original_message_id": task.processing_message_id,  # ID сообщения в Telegram
            "bot_token_hash": hashlib.md5(config.TELEGRAM_BOT_TOKEN.encode()).hexdigest()[:8],  # Хеш токена для идентификации
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

        logger.info(f"📤 Отправлено в Kafka INPUT topic:")
        logger.info(json.dumps(kafka_message, indent=2, ensure_ascii=False))

        return True

    except Exception as e:
        logger.error(f"❌ Ошибка отправки в Kafka: {e}")
        raise

async def start_kafka_consumer():
    """Запускает Kafka Consumer для получения ответов"""
    if not config.KAFKA_BOOTSTRAP_SERVERS:
        logger.warning("⚠️ Kafka bootstrap servers не указаны, consumer не запущен")
        return

    consumer = None
    try:
        consumer = KafkaConsumer(
            config.KAFKA_OUTPUT_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS.split(','),
            group_id=config.KAFKA_CONSUMER_GROUP,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',  # Важно: читаем с начала при перезапуске
            enable_auto_commit=False,  # Ручное подтверждение
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

                msg_pack = consumer.poll(timeout_ms=1000)

                for tp, messages in msg_pack.items():
                    for message in messages:
                        try:
                            task_id = message.key
                            response_data = message.value

                            logger.info(f"📥 Получен ответ из Kafka для задачи: {task_id}")

                            await handle_kafka_response(task_id, response_data)

                            consumer.commit()

                        except Exception as e:
                            logger.error(f"❌ Ошибка обработки сообщения Kafka: {e}")

            except Exception as e:
                logger.error(f"❌ Ошибка в Kafka Consumer: {e}")

    except NoBrokersAvailable:
        logger.error("❌ Не удалось подключиться к Kafka брокерам")
    except Exception as e:
        logger.error(f"❌ Не удалось запустить Kafka Consumer: {e}")
    finally:
        if consumer:
            consumer.close()

async def handle_kafka_response(task_id: str, response_data: Dict[str, Any]):
    """
    Обрабатывает ответ из Kafka OUTPUT topic.

    ⚠️ ВАЖНО: Это тот JSON, который микросервис отправляет в ответ!
    Пример ожидаемого ответа:
    """
    try:
        logger.info(f"🔧 Обработка ответа Kafka для task_id={task_id}")
        logger.info(f"📥 Получен ответ из Kafka OUTPUT topic:")
        logger.info(json.dumps(response_data, indent=2, ensure_ascii=False))

        required_fields = ['task_id', 'status', 'event_timestamp']
        for field in required_fields:
            if field not in response_data:
                logger.error(f"❌ В ответе Kafka отсутствует поле: {field}")
                return

        task_data = await state_manager.get_task(task_id)
        if not task_data:
            logger.error(f"❌ Задача {task_id} не найдена в БД")
            return

        task = ProcessingTask.from_dict(task_data)

        if response_data['status'] == "success":
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now()
            task.kafka_response_received = True

            if 'output' in response_data and 'file_path' in response_data['output']:
                task.processed_minio_path = response_data['output']['file_path']

                await send_processed_file_to_user(task)
            else:

                await update_processing_message(
                    task.chat_id,
                    task.processing_message_id,
                    f"✅ Обработка завершена успешно!\n\n"
                    f"📋 Результаты:\n{json.dumps(response_data.get('results', {}), indent=2, ensure_ascii=False)[:500]}"
                )

        elif response_data['status'] == "error":
            task.status = TaskStatus.FAILED
            task.error_message = response_data.get('error_message', 'Unknown error')
            task.kafka_response_received = True

            await update_processing_message(
                task.chat_id,
                task.processing_message_id,
                f"❌ Ошибка обработки:\n{task.error_message}"
            )
        else:
            logger.warning(f"⚠️ Неизвестный статус в ответе Kafka: {response_data['status']}")
            return

        await state_manager.save_task(task)
        await state_manager.mark_kafka_response_received(task_id)

        if task_id in active_tasks and task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
            del active_tasks[task_id]

    except Exception as e:
        logger.error(f"❌ Ошибка в handle_kafka_response: {e}")


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
        "/cancel <id> - отменить задачу"
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
        for task_id in pending_tasks[:3]:  # Показываем первые 3
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
        task = dict(row)
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

@dp.message(F.document)
async def handle_document(message: Message):
    """Обработка документов"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    file_name = message.document.file_name

    if not (file_name.endswith('.json') or file_name.endswith('.html')):
        await message.answer("❌ Отправьте только JSON (.json) или HTML (.html) файлы")
        return

    if message.document.file_size > config.MAX_FILE_SIZE:
        await message.answer(f"❌ Файл слишком большой. Максимальный размер: {config.MAX_FILE_SIZE // 1048576} MB")
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

async def send_processed_file_to_user(task: ProcessingTask):
    """Отправляет обработанный файл пользователю"""
    try:

        file_content = await download_from_minio(task.processed_minio_path)

        file_obj = BufferedInputFile(file_content, filename=f"processed_{task.file_name}")

        await bot.send_document(
            chat_id=task.chat_id,
            document=file_obj,
            caption=(
                f"✅ Обработка завершена!\n\n"
                f"📄 Файл: {task.file_name}\n"
                f"📋 ID задачи: <code>{task.task_id}</code>\n"
                f"⏱️ Время обработки: {(task.completed_at - task.started_at).total_seconds():.1f} сек\n"
                f"📁 Результат: {task.processed_minio_path}"
            )
        )

        await update_processing_message(
            task.chat_id,
            task.processing_message_id,
            f"✅ Обработка завершена!\nФайл отправлен в чат."
        )

        logger.info(f"✅ Файл отправлен пользователю {task.user_id}: {task.file_name}")

    except Exception as e:
        logger.error(f"❌ Ошибка отправки файла пользователю: {e}")

        await update_processing_message(
            task.chat_id,
            task.processing_message_id,
            f"❌ Ошибка отправки файла:\n{str(e)[:200]}"
        )

async def update_processing_message(chat_id: int, message_id: Optional[int], text: str):
    """Обновляет сообщение о статусе обработки"""
    if message_id:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text
            )
        except Exception as e:
            logger.error(f"❌ Ошибка обновления сообщения: {e}")

async def check_processing_timeout(task_id: str):
    """Проверяет таймаут обработки задачи"""
    await asyncio.sleep(config.PROCESSING_TIMEOUT)

    if task_id in active_tasks:
        task = active_tasks[task_id]
        if task.status in [TaskStatus.PENDING, TaskStatus.PROCESSING]:
            task.status = TaskStatus.TIMEOUT
            task.error_message = f"Таймаут обработки ({config.PROCESSING_TIMEOUT} сек)"
            await state_manager.save_task(task)

            await update_processing_message(
                task.chat_id,
                task.processing_message_id,
                f"❌ Таймаут обработки!\nЗадача не была обработана за {config.PROCESSING_TIMEOUT} секунд.\n"
                f"Используйте /retry {task_id} для повторной отправки."
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
                    "note": "Это эмуляция обработки. В реальной системе здесь будет результат микросервиса."
                },
                "original_data": data,
                "statistics": {
                    "original_size": len(original_content),
                    "emulated_processing": True
                }
            }
            processed_content = json.dumps(processed_data, indent=2, ensure_ascii=False).encode('utf-8')
        else:
            processed_content = f"<!-- Emulated processing for task {task.task_id} -->\n{original_content.decode('utf-8')}".encode('utf-8')

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

        await update_processing_message(
            task.chat_id,
            task.processing_message_id,
            f"❌ Ошибка эмуляции обработки:\n{str(e)[:200]}"
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

                await update_processing_message(
                    task.chat_id,
                    task.processing_message_id,
                    f"🔄 Восстановление задачи после перезапуска...\n"
                    f"📋 ID: <code>{task_id}</code>\n"
                    f"⏳ Ожидание ответа от микросервиса..."
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

    await state_manager.init()

    await recover_pending_tasks()

    try:
        bot_info = await bot.get_me()
        logger.info(f"✅ Бот: @{bot_info.username} ({bot_info.first_name})")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к боту: {e}")
        return

    if config.KAFKA_BOOTSTRAP_SERVERS and kafka_producer:
        import threading
        consumer_thread = threading.Thread(
            target=lambda: asyncio.run(start_kafka_consumer()),
            daemon=True
        )
        consumer_thread.start()
        logger.info("✅ Kafka Consumer запущен в отдельном потоке")

    logger.info("✅ Бот готов к работе!")
    logger.info(f"📊 Активных задач: {len(active_tasks)}")

    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
    finally:

        asyncio.run(state_manager.close())
        if kafka_producer:
            kafka_producer.close()