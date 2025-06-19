from fastapi import FastAPI, Request , BackgroundTasks
from kafka_manager.consumer import consume
from kafka_manager.producer import send_tasks
from fastapi.responses import JSONResponse
from schema import ServerData
from icecream import ic
app = FastAPI()



@app.get("/")
@app.post("/setup")
@app.get("/health")
async def health_check():
    return JSONResponse(content={"model_class": "Yolo", "status": "UP"})


@app.post("/predict")
async def predict(request: Request, bt: BackgroundTasks):
    data = ServerData.from_payload(await request.json())
    send_tasks(data.project_id, data.tasks)
    bt.add_task(consume, time_expire=10)
    return JSONResponse({"result": "ok"})




