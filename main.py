from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from icecream import ic
import logging
from kafka import  KafkaManager
from schema import ServerData

app = FastAPI()
logger = logging.getLogger(__name__)
# НАЧАЛЬНАЯ ВАЛИДАЦИЯ НА РАБОТАСПОСОБНОСТЬ
# ----------------------------------------------------------
@app.get("/")
@app.post("/setup")
@app.get("/health")
async def health_check(request: Request):
    ic(await request.body())
    return JSONResponse(content={"model_class": "Yolo", "status": "UP"})




# ----------------------------------------------------------

# PREDICTION ЭНДПОИНТ
# ----------------------------------------------------------
@app.post("/predict")
async def predict(request: Request):
    data = ServerData.from_payload(await request.json())


    result = await KafkaManager.process_with_timeout(data.project_id , data.tasks)
    ic(result)
    return JSONResponse(content={"results": "res", "status": "ok"})



