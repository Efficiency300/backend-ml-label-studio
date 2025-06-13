import asyncio

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "input-topic"
OUTPUT_TOPIC = "output-topic"

async def send_message_to_kafka(message: dict, project_id: str):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        await producer.send_and_wait(INPUT_TOPIC,json.dumps({"data": message, "correlation_id": project_id}).encode("utf-8"))
    finally:
        await producer.stop()
        print("Done sending message to kafka")








async def wait_until_ready(consumer, project_id: str, remaining_time: float) -> dict | None:
    if remaining_time <= 0:
        return None

    try:
        msg = await asyncio.wait_for(consumer.__anext__(), timeout=remaining_time)
    except asyncio.TimeoutError:
        return None

    message = json.loads(msg.value.decode("utf-8"))
    if message.get("project_id") == project_id:
        return message
    else:
        # Подождем немного и вызовем себя снова, уменьшая оставшееся время
        await asyncio.sleep(0.1)
        return await wait_until_ready(consumer, project_id, remaining_time - 0.1)

async def get_message_from_kafka(project_id: str, timeout_seconds: float) -> dict | None:
    consumer = AIOKafkaConsumer(
        OUTPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="my-topic",
        enable_auto_commit=True,
        auto_offset_reset='latest',
    )
    await consumer.start()
    try:
        result = await wait_until_ready(consumer, project_id, timeout_seconds)
        return result
    finally:
        await consumer.stop()
