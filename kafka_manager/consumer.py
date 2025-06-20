import json
import time
from abc import ABC, abstractmethod
from aiokafka import AIOKafkaConsumer
from config import settings
from label_studio_sdk import LabelStudio
from redis_tasks_manager import remove_task_from_redis


class BaseKafkaConsumer(ABC):
    def __init__(self):
        self.consumer = AIOKafkaConsumer(
            settings.OUTPUT_TOPIC,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=False,
            group_id="my-group-id"
        )
        self.label_studio_client = LabelStudio(
            base_url=settings.LABEL_STUDIO_URL,
            api_key=settings.LABEL_STUDIO_API_KEY
        )

    async def start(self):
        await self.consumer.start()
        print(f"🚀 Started consuming messages from {settings.OUTPUT_TOPIC}")
        try:
            await self.consume_loop()
        except Exception as e:
            print(f"❌ Ошибка в процессе получения сообщений: {e}")
        finally:
            await self.consumer.stop()
            print("🔚 Консьюмер остановлен")

    async def handle_message(self, msg):
        try:
            payload = msg.value
            print(f"📨 Получено сообщение: task_id={payload.get('task_id')}")
            response = payload.get("result")
            task_id = payload.get("task_id")

            task_remover = await remove_task_from_redis(task_id)
            print(task_remover)

            self.label_studio_client.predictions.create(
                task=task_id, result=response, model_version="Yolo"
            )
            await self.consumer.commit()
        except Exception as e:
            print(f"🚫 Ошибка при обработке сообщения: {e}")

    @abstractmethod
    async def consume_loop(self):
        ...


class TimedKafkaConsumer(BaseKafkaConsumer):
    def __init__(self, time_expire: int = 5):
        super().__init__()
        self.time_expire = time_expire

    async def consume_loop(self):
        start_time = time.time()
        async for msg in self.consumer:
            if time.time() - start_time >= self.time_expire:
                print("⏰ Время ожидания истекло.")
                break
            await self.handle_message(msg)


class UntimedKafkaConsumer(BaseKafkaConsumer):
    async def consume_loop(self):
        async for msg in self.consumer:
            await self.handle_message(msg)



async def consume_with_fallback(time_expire: int = 5):
    timed = TimedKafkaConsumer(time_expire=time_expire)
    await timed.start()

    # После его завершения — запускаем Untimed
    untimed = UntimedKafkaConsumer()
    await untimed.start()
