from kafka import KafkaConsumer
from config import settings
from label_studio_sdk import LabelStudio
import json
import time


def consume(time_expire: int = 5):
    consumer = KafkaConsumer(
        settings.OUTPUT_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="my-group-id"
    )
    label_studio_client = LabelStudio(base_url=settings.LABEL_STUDIO_URL,api_key=settings.LABEL_STUDIO_API_KEY)

    start_time = time.time()

    try:
        print(f"Started consuming messages from {settings.OUTPUT_TOPIC}")
        for msg in consumer:
            if time.time() - start_time >= time_expire:
                print("⏰ Время ожидания истекло.")
                break

            try:
                payload = msg.value
                print(f"Получено сообщение")
                response = payload.get("result")
                task_id = payload.get("task_id")

                label_studio_client.predictions.create(task=task_id, result=response, model_version="Yolo")

            except Exception as e:
                print(f"Ошибка при обработке сообщения: {e}")
    except Exception as e:
        print(f"Ошибка в процессе получения сообщений: {e}")

    finally:
        consumer.close()
