# kafka/sender.py
from kafka.base import KafkaBaseMixin
from redis.asyncio import Redis

class KafkaSender(KafkaBaseMixin):
    def __init__(self, kafka_bootstrap_servers="localhost:9092" , input_topic="input-topic", redis_host: str = "localhost"):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = ""
        self.redis = Redis(host=redis_host, port=6379, decode_responses=True)