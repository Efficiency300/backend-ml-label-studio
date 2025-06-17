import asyncio
import json
from typing import Any
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from icecream import ic
from redis.asyncio import Redis
r = Redis(host='localhost', port=6379, decode_responses=True)





class KafkaManager:

    def __init__(
        self,
        kafka_bootstrap_servers: str = "localhost:9092",
        input_topic: str = "input-topic",
        output_topic: str = "output-topic",
    ):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = output_topic

    async def send_message_to_kafka(self, project_id: str, tasks: list[dict]) -> None:
        producer = AIOKafkaProducer(bootstrap_servers=self.kafka_bootstrap_servers)
        await producer.start()
        try:
            send_tasks = []

            for task in tasks:
                task_id = task.get("id")
                data_values = list(task.get("data", {}).values())

                if not data_values:
                    print(f"⚠️ Пропущена задача {task_id}: отсутствуют данные.")
                    continue

                message = data_values[0]
                payload = {
                    "source": f"http://localhost:8081{message}",
                    "task_id": task_id,
                    "project_id": project_id,
                }

                print(f"➡️ Подготовка к отправке: {payload['source']}")
                send_tasks.append(
                    producer.send_and_wait(self.input_topic, json.dumps(payload).encode("utf-8"))
                )

            # Отправляем все сообщения параллельно
            await asyncio.gather(*send_tasks)

            print(f"✅ Все задачи отправлены в Kafka для проекта: {project_id}")
        finally:
            await producer.stop()


    async def get_messages_from_kafka(self, time_expire: int = 5) -> list[dict[str, str | Any]] | dict[str, str] | None:
        consumer = AIOKafkaConsumer(
            self.output_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id="my-group-id"
        )

        await consumer.start()
        start_time = asyncio.get_event_loop().time()
        try:
            print(f"Started consuming messages from {self.output_topic}")
            async for msg in consumer:
                if asyncio.get_event_loop().time() - start_time > time_expire:
                    print("⏰ Время ожидания истекло.")
                    return {"status": "ok"}

                try:
                    payload = msg.value
                    print(f"📥 Получено сообщение: {payload}, Partition: {msg.partition}, Offset: {msg.offset}")
                    obj_type = payload.get('type')
                    value = payload.get('value')

                    prediction_result = [{
                        "from_name": "boxes",
                        "to_name": "image",
                        "type": obj_type,
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






