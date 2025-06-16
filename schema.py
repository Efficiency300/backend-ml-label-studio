from pydantic import BaseModel
from typing import Dict, List, Any
from fastapi import HTTPException


class ServerData(BaseModel):
    tasks: List[Dict[str, Any]]
    project_: str
    project_id: str
    label_config: str
    path: list

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "ServerData":
        tasks = payload.get('tasks')
        if not tasks or not isinstance(tasks, list):
            raise HTTPException(status_code=400, detail="No valid 'tasks' field provided in request")

        project = str(payload.get('project', '1'))
        project_id = project.split('.', 1)[0] if project else None
        label_config = payload.get('label_config')

        source = []
        for task in tasks:
            task_data = task.get('data', {})
            image_path = task_data.get('image')
            video_path = task_data.get('video')

            path = image_path or video_path
            source.append(path)

        return cls(
            tasks=tasks,
            project_=project,
            project_id=project_id,
            label_config=label_config,
            path=source,
        )
