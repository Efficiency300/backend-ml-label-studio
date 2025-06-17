import asyncio

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from icecream import ic
import logging
from kafka import KafkaManager
from schema import ServerData
from label_studio_sdk.client import LabelStudio

app = FastAPI()
logger = logging.getLogger(__name__)
ls = LabelStudio(base_url='http://localhost:8080', api_key='6c15b147fbed94c1dc5904c8ef357d1cb7fed6e0')


# НАЧАЛЬНАЯ ВАЛИДАЦИЯ НА РАБОТАСПОСОБНОСТЬ
# ----------------------------------------------------------
@app.get("/")
@app.post("/setup")
@app.get("/health")
async def health_check():
    return JSONResponse(content={"model_class": "Yolo", "status": "UP"})


# ----------------------------------------------------------

# PREDICTION ЭНДПОИНТ
# ----------------------------------------------------------
@app.post("/predict")
async def predict(request: Request):
    data = ServerData.from_payload(await request.json())

    result = await KafkaManager.send_message_to_kafka(data.project_id, data.tasks)
    await asyncio.sleep(5)
    nige = await KafkaManager.get_messages()

    ls.predictions.create(task=1, result=nige, model_version="Yolo", score=0.95)

