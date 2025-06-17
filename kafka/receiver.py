# kafka/receiver.py
from kafka.base import KafkaBaseMixin
from redis.asyncio import Redis

class KafkaReceiver(KafkaBaseMixin):
    def __init__(self, kafka_bootstrap_servers="localhost:9092", output_topic="output-topic", redis_host: str = "localhost"):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.output_topic = output_topic
        self.input_topic = ""
        self.redis = Redis(host=redis_host, port=6379, decode_responses=True)