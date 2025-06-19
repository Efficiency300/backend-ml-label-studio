from pydantic import BaseModel
from typing import Dict, List, Any
from fastapi import HTTPException


class ServerData(BaseModel):
    tasks: List[Dict[str, Any]]
    project: str
    project_id: str
    label_config: str
    # parameters: List = None
    draft: List
    path: list

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "ServerData":
        tasks = payload.get('tasks')
        if not tasks or not isinstance(tasks, list):
            raise HTTPException(status_code=400, detail="No valid 'tasks' field provided in request")

        project = str(payload.get('project', '1'))
        project_id = project.split('.', 1)[0] if project else None
        label_config = payload.get('label_config')
        # parameters = payload.get("params")
        # context = parameters.get("context", {}) or None
        # params_result = context.get("result", [])
        draft_result = sum(
            [task.get("drafts", []) for task in tasks if isinstance(task.get("drafts"), list)],
            []
        )

        source = []
        for task in tasks:
            task_data = task.get('data', {})
            image_path = task_data.get('image')
            video_path = task_data.get('video')

            path = image_path or video_path
            source.append(path)

        return cls(
            tasks=tasks,
            project=project,
            project_id=project_id,
            label_config=label_config,
            # parameters=params_result,
            draft=draft_result,
            path=source,
        )
