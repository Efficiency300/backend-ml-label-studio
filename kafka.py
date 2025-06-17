import asyncio
import json
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from icecream import ic

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "input-topic"
OUTPUT_TOPIC = "output-topic"


class KafkaManager:
    @classmethod
    async def send_message_to_kafka(cls, project_id: str, tasks: list[dict]):
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        await producer.start()
        try:
            for task in tasks:
                task_id = task['id']
                message = list(task['data'].values())[0]
                print(f"Sending: {message}")
                payload = {
                    "source": f"http://localhost:8081{message}",
                    "task_id": task_id,
                    "project_id": project_id
                }
                await producer.send_and_wait(INPUT_TOPIC, json.dumps(payload).encode())
            print(f"📤 Отправлены все задачи в Kafka для проекта: {project_id}")
        finally:
            await producer.stop()

    @classmethod
    async def get_messages(cls):
        consumer = AIOKafkaConsumer(
            OUTPUT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',  # Use 'earliest' for testing
            enable_auto_commit=True,
            group_id="my-group-id"
        )

        await consumer.start()
        try:
            print(f"Started consuming messages from {OUTPUT_TOPIC}")
            async for msg in consumer:
                try:
                    payload = msg.value
                    print(f"📥 Получено сообщение: {payload}, Partition: {msg.partition}, Offset: {msg.offset}")
                    type = payload.get('type')
                    value = payload.get('value')
                    score = payload.get('score')

                    prediction_result = [{
                        "from_name": "boxes",  # name of the RectangleLabels
                        "to_name": "image",  # name of the Image
                        "type": type,  # type of the control
                        "value": value
                    }]
                    ic(prediction_result)
                    return prediction_result
                except Exception as e:
                    print(f"Ошибка при обработке сообщения: {e}")
        except Exception as e:
            print(f"Ошибка в процессе получения сообщений: {e}")
        finally:
            await consumer.stop()
            print("Consumer stopped")

