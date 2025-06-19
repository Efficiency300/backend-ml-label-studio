from dotenv import load_dotenv
import os

# Загружаем переменные из .env файла
load_dotenv()

class Settings:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    INPUT_TOPIC = os.getenv("INPUT_TOPIC")
    OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC")
    LABEL_STUDIO_URL = os.getenv("LABEL_STUDIO_URL")
    LABEL_STUDIO_API_KEY = os.getenv("LABEL_STUDIO_API_KEY")

settings = Settings()

