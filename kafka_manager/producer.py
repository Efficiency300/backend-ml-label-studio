from aiokafka import AIOKafkaProducer
from redis_tasks_manager import r
from config import settings
import json


async def send_tasks(project_id: str, tasks: list[dict]) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
    )
    await producer.start()
    try:
        for task in tasks:
            task_id = task.get("id")
            data_values = list(task.get("data", {}).values())
            message = data_values[0] if data_values else None

            if not message:
                print(f"⚠️ Пропущена задача {task_id}: отсутствуют или некорректные данные.")
                continue

            payload = {
                "source": f"http://localhost:8081{message}",
                "data": {
                    "task_id": task_id,
                    "project_id": project_id,
                }
            }

            print(f"➡️ Отправка задачи {task_id}: {payload['source']}")
            try:
                await producer.send(settings.INPUT_TOPIC, payload)
                await r.incr(f"sent_count:{project_id}")
                await r.rpush(f"payload:{payload['data']['task_id']}", json.dumps(payload))
            except Exception as e:
                print(f"❌ Ошибка при отправке задачи {task_id}: {e}")
                continue

        await producer.flush()
        print(f"✅ Все задачи отправлены в Kafka для проекта: {project_id}")
    finally:
        await producer.stop()
