from icecream import ic
from label_studio_sdk.client import LabelStudio
from label_studio_sdk.label_interface.objects import PredictionValue

ls = LabelStudio(base_url='http://localhost:8080', api_key='6c15b147fbed94c1dc5904c8ef357d1cb7fed6e0')
project = ls.projects.get(id=2)
li = project.get_label_interface()

tasks = ls.tasks.list(project=project.id, include='id')



for task in tasks:
    prediction_result = [{
        "from_name": "boxes",        # name of the RectangleLabels
        "to_name": "image",          # name of the Image
        "type": "rectanglelabels",   # type of the control
        "value": {
            "x": 10, "y": 10,        # position in %
            "width": 30, "height": 20,
            "rotation": 0,
            "rectanglelabels": ["car"]
        }
    }]

    cock = ls.predictions.create(task=task.id, result=prediction_result, model_version="Yolo", score=0.95)
    ic(f"{cock}")
