import uvicorn
from fastapi import FastAPI
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json

app = FastAPI()

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "input-topic"
OUTPUT_TOPIC = "output-topic"



async def consume_input_and_produce_output():
    consumer = AIOKafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="my-topic",
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await consumer.start()
    await producer.start()
    try:
        async for msg in consumer:
            message = json.loads(msg.value.decode("utf-8"))
            data = message.get("data", {})
            correlation_id = message.get("correlation_id")
            # Модифицируем сообщение
            modified_message = {**data, "modified": True, "correlation_id": correlation_id}
            # Отправляем модифицированное сообщение в output-topic
            await producer.send_and_wait(OUTPUT_TOPIC, json.dumps(modified_message).encode("utf-8"))
    finally:
        await consumer.stop()
        await producer.stop()


@app.on_event("startup")
async def startup_event():
    # Запускаем консумер для input-topic и продюсер для output-topic в фоновом режиме
    asyncio.create_task(consume_input_and_produce_output())

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)