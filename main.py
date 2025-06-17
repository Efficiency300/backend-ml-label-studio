from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from kafka.sender import KafkaSender
from kafka.receiver import KafkaReceiver
from schema import ServerData
from label_studio_sdk.client import LabelStudio

app = FastAPI()
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
    sender = KafkaSender()
    receiver = KafkaReceiver()
    await sender.send_message_to_kafka(data.project_id, data.tasks)
    result = await receiver.get_messages_from_kafka(time_expire=5)

    ls.predictions.create(task=3, result=result, model_version="Yolo", score=0.95)

