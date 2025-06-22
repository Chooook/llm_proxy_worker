from pydantic import BaseModel


class HandlerConfig(BaseModel):
    name: str
    task_type: str
    import_path: str
    version: str
    available_workers: int = 0
    description: str = ''
