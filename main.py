from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from yolo.model import YOLO
from response import ModelResponse
from label_studio_sdk._extensions.label_studio_tools.core.utils.io import get_local_path
from utils import  is_preload_needed


import logging

app = FastAPI()
MODEL_CLASS = YOLO
logger = logging.getLogger(__name__)
# НАЧАЛЬНАЯ ВАЛИДАЦИЯ НА РАБОТАСПОСОБНОСТЬ
# ----------------------------------------------------------
@app.get("/")
def health_check():
    return JSONResponse(content={"model_class": MODEL_CLASS.__name__, "status": "UP"})

@app.get("/health")
def health_check2():
    return JSONResponse(content={"model_class": MODEL_CLASS.__name__, "status": "UP"})

@app.post("/setup")
async def setup(request: Request):
    data = await request.json()
    project_id = data.get('project').split('.', 1)[0]
    label_config = data.get('schema')
    extra_params = data.get('extra_params')

    model = MODEL_CLASS(project_id=project_id, label_config=label_config)
    if extra_params:
        model.set_extra_params(extra_params)

    model_version = model.get('model_version')
    return JSONResponse(content={'model_version': model_version})


# ----------------------------------------------------------

# PREDICTION ЭНДПОИНТ
# ----------------------------------------------------------
@app.post("/predict")
async def predict(request: Request):
    data = await request.json()
    tasks = data.get('tasks')
    if not tasks or not isinstance(tasks, list):
        raise HTTPException(status_code=400, detail="No valid 'tasks' field provided in request")

    project = str(data.get('project', '1'))
    project_id = project.split('.', 1)[0] if project else None
    label_config = data.get('label_config')
    params = data.get('params', {})
    context = params.pop('context', {})

    # Validate tasks and extract image or video paths
    for task in tasks:
        task_data = task.get('data', {})
        image_path = task_data.get('image')
        video_path = task_data.get('video')

        if not image_path and not video_path:
            raise HTTPException(
                status_code=400,
                detail=f"No 'image' or 'video' field in task {task.get('id', 'unknown')}"
            )

        # Resolve path for image or video
        path = image_path or video_path
        if not is_preload_needed(path):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid path in task {task.get('id', 'unknown')}: {path}"
            )

        try:
            if image_path:
                task['data']['image'] = get_local_path(url=image_path, task_id=task.get('id'))
            elif video_path:
                task['data']['video'] = get_local_path(url=video_path, task_id=task.get('id'))
        except Exception as e:
            logger.error(f"Failed to resolve path {path} for task {task.get('id', 'unknown')}: {str(e)}")
            raise HTTPException(
                status_code=400,
                detail=f"Failed to resolve path for task {task.get('id', 'unknown')}: {str(e)}"
            )

    # Initialize YOLO model
    model = MODEL_CLASS(project_id=project_id, label_config=label_config)
    logger.info(f"Running prediction for {len(tasks)} tasks, project_id: {project_id}")

    # Perform prediction
    response = model.predict(tasks, context=context, **params)
    if not response:
        logger.warning(f"No predictions for tasks")
        return JSONResponse(content={'results': []})

    # Process response
    if isinstance(response, ModelResponse):
        if not response.has_model_version():
            mv = model.model_version
            if mv:
                response.set_version(str(mv))
        else:
            response.update_predictions_version()
        response = response.model_dump()

    res = response.get("predictions", []) if isinstance(response, dict) else response
    logger.info(f"Prediction completed for {len(res)} tasks")

    return JSONResponse(content={'results': res})




# ----------------------------------------------------------
TRAIN_EVENTS = (
    'ANNOTATION_CREATED',
    'ANNOTATION_UPDATED',
    'ANNOTATION_DELETED',
    'START_TRAINING'
)

@app.post("/webhook")
async def webhook(request: Request):
    data = await request.json()
    event = data.pop('action')

    if event not in TRAIN_EVENTS:
        return JSONResponse(content={'status': 'Unknown event'})

    project_id = str(data['project']['id'])
    label_config = data['project']['label_config']
    model = MODEL_CLASS(project_id, label_config=label_config)
    logger.debug(f"Received event: {event}, project_id: {project_id}")
    try:
        result = model.fit(event, data)
        logger.debug(f"Training result: {result}")
        return JSONResponse(content={'result': result, 'status': 'ok'}, status_code=201)
    except Exception as e:
        logger.error(f"Training failed: {str(e)}")
        return JSONResponse(content={'error': str(e), 'status': 'error'}, status_code=201)


# ----------------------------------------------------------