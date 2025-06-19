from kafka import KafkaProducer
from config import settings
import json
import redis

r = redis.Redis(host='localhost', port=6379, db=0)
def send_tasks(project_id: str, tasks: list[dict]) -> None:
    producer = KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
        retries=3,
    )
    try:
        for task in tasks:
            task_id = task.get("id")
            data_values = list(task.get("data", {}).values())
            if not data_values:
                print(f"⚠️ Пропущена задача {task_id}: отсутствуют данные.")
                continue

            message = data_values[0]
            payload = {
                "source": f"http://localhost:8081{message}",
                "data": {
                    "task_id": task_id,
                    "project_id": project_id,
                }
            }

            print(f"➡️ Подготовка к отправке: {payload['source']}")
            producer.send(settings.INPUT_TOPIC, payload)

        producer.flush(timeout=30)
        print(f"✅ Все задачи отправлены в Kafka для проекта: {project_id}")
    finally:
        producer.close()
