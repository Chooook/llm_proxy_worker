from pydantic import BaseModel


class HandlerConfig(BaseModel):
    name: str
    task_type: str
    import_path: str
