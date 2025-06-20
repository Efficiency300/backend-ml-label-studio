from fastapi import FastAPI, Request , BackgroundTasks
from kafka_manager.producer import send_tasks
from kafka_manager.consumer import consume_with_fallback
from fastapi.responses import JSONResponse
from schema import ServerData
from calary_manager.celery_worker import run_external_project  # импорт Celery-задачи
app = FastAPI()

@app.get("/")
@app.post("/setup")
@app.get("/health")
async def health_check():
    return JSONResponse(content={"model_class": "Yolo", "status": "UP"})

@app.post("/predict")
async def predict(request: Request, bt: BackgroundTasks):
    data = ServerData.from_payload(await request.json())
    await send_tasks(data.project_id, data.tasks)

    run_external_project.delay()  # <-- Важно: это просто отправляет задачу воркеру

    bt.add_task(consume_with_fallback, 1)
    return JSONResponse({"result": "ok"})
