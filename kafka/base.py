# kafka/base.py
import asyncio
import json

from icecream import ic
from redis.asyncio import Redis
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from typing import Any


class KafkaBaseMixin:
    kafka_bootstrap_servers: str
    input_topic: str
    output_topic: str
    redis: Redis

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
                    "data": "123123"

                }

                print(f"➡️ Подготовка к отправке: {payload['source']}")

                # Сохраняем payload в Redis хеш по task_id
                await self.redis.hset(f"task:{task_id}", mapping=payload)

                # Увеличиваем счётчик задач по project_id
                await self.redis.incr(f"project:{project_id}:task_count")

                # Отправка в Kafka
                send_tasks.append(
                    producer.send_and_wait(self.input_topic, json.dumps(payload).encode("utf-8"))
                )

            await asyncio.gather(*send_tasks)
            print(f"✅ Все задачи отправлены в Kafka для проекта: {project_id}")
        finally:
            await producer.stop()


    async def get_messages_from_kafka(self, time_expire: int = 5) -> list[dict[str, str | Any]] | dict[str, str] | None:
        consumer = AIOKafkaConsumer(
            self.output_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
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
                    ic(f"📥 Получено сообщение: {payload}")
                    with open("output.txt", "w") as f:
                        f.write(json.dumps(payload))
                    return payload
                except Exception as e:
                    print(f"Ошибка при обработке сообщения: {e}")
        except Exception as e:
            print(f"Ошибка в процессе получения сообщений: {e}")
        finally:
            await consumer.stop()
            print("Consumer stopped")




    async  def expecting_manager(self):
        pass

