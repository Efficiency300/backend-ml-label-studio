import asyncio
import json
import time
from redis.asyncio import Redis
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "input-topic"
OUTPUT_TOPIC = "output-topic"

class KafkaManager:

    r = Redis(host='localhost', port=6379, decode_responses=True)
    _lock = asyncio.Lock()
    _cleanup_interval = 5
    _task_timeout = 30

    @classmethod
    async def send_message_to_kafka(cls, project_id: str, tasks: list[dict]):
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        await producer.start()
        try:
            timestamp = time.time()
            for task in tasks:
                task_id = task['id']
                message = list(task['data'].values())[0]
                print(message)
                payload = {
                    "source": f"http://localhost:8081уй{message}",
                    "task_id": task_id,
                    "project_id": project_id
                }
                await producer.send_and_wait(INPUT_TOPIC, json.dumps(payload).encode())
                redis_key = f"tasks:{project_id}"
                async with cls._lock:
                    await cls.r.hset(redis_key, task_id, json.dumps({
                        "payload": payload,
                        "timestamp": timestamp,
                        "received": 0,
                        "expected": 1
                    }))
            print(f"📤 Отправлены все задачи в Kafka для проекта: {project_id}")
        finally:
            await producer.stop()

    @classmethod
    async def handle_kafka_message(cls, message: dict) -> dict | None:
        """Обрабатывает входящие сообщения из Kafka и обновляет Redis."""
        task_id = message.get("task_id")
        project_id = message.get("project_id")
        if not task_id or not project_id:
            return None
        redis_key = f"tasks:{project_id}"
        task_data_raw = await cls.r.hget(redis_key, str(task_id))
        if not task_data_raw:
            return None  # Задача уже удалена или истекла
        task_data = json.loads(task_data_raw)
        task_data["received"] += 1
        task_data["timestamp"] = time.time()
        await cls.r.hset(redis_key, task_id, json.dumps(task_data))
        if task_data["received"] >= task_data["expected"]:
            await cls.r.hdel(redis_key, task_id)
            print(f"✅ Задача {task_id} завершена и удалена из Redis")
            return {"status": "completed", "task_id": task_id, "message": message}
        return {"status": "pending", "task_id": task_id, "message": message}

    @classmethod
    async def cleaner(cls):
        """Периодически очищает просроченные задачи из Redis."""
        while True:
            await asyncio.sleep(cls._cleanup_interval)
            keys = await cls.r.keys("tasks:*")
            now = time.time()
            for key in keys:
                task_entries = await cls.r.hgetall(key)
                if not task_entries:
                    continue
                to_delete = []
                for task_id, data_str in task_entries.items():
                    try:
                        data = json.loads(data_str)
                        ts = float(data.get("timestamp", 0))
                        received = int(data.get("received", 0))
                        expected = int(data.get("expected", 1))
                        if received >= expected or now - ts > cls._task_timeout:
                            to_delete.append(task_id)
                    except Exception as e:
                        print(f"⚠️ Ошибка при разборе данных задачи: {e}")
                if to_delete:
                    await cls.r.hdel(key, *to_delete)
                    for task_id in to_delete:
                        print(f"🧹 Очищена задача {task_id} из {key}")

    @classmethod
    async def kafka_response_listener(cls):
        """Слушает ответы из Kafka и обрабатывает их."""
        consumer = AIOKafkaConsumer(
            OUTPUT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="result-consumer",
            enable_auto_commit=True,
            auto_offset_reset='latest'
        )
        await consumer.start()
        print("👂 Прослушивание ответов из Kafka...")
        try:
            async for msg in consumer:
                try:
                    message = json.loads(msg.value.decode())
                    result = await cls.handle_kafka_message(message)
                    if result:
                        return result
                except Exception as e:
                    return {"status": "failed", "message": str(e)}
        finally:
            await consumer.stop()

    @classmethod
    async def process_with_timeout(cls,project_id: str, tasks: list[dict]):
        """Обрабатывает задачи с учетом тайм-аутов."""
        send_task = asyncio.create_task(cls.send_message_to_kafka(project_id, tasks))
        listener_task = asyncio.create_task(cls.kafka_response_listener())
        cleaner_task = asyncio.create_task(cls.cleaner())

        try:
            # Ожидание ответа в течение 5 секунд
            done, pending = await asyncio.wait([listener_task], timeout=5)
            if listener_task in done:
                result = listener_task.result()
                if result.get("status") == "completed":
                    return result
            else:
                print("⏳ Нет ответа за 5 секунд, возвращается стандартный ответ")
                return {"results": "res", "status": "ok"}

            # Продолжение ожидания до 30 секунд
            done, pending = await asyncio.wait([listener_task], timeout=25)  # Дополнительно 25 секунд
            if listener_task in done:
                result = listener_task.result()
                if result.get("status") == "completed":
                    return result
            else:
                print("⌛ Нет ответа за 30 секунд, возвращается стандартный ответ")
                return {"results": "res", "status": "ok"}
        finally:
            for task in [send_task, listener_task, cleaner_task]:
                if not task.done():
                    task.cancel()

# Пример использования
