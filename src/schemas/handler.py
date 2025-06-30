from pydantic import BaseModel, computed_field


class HandlerConfig(BaseModel):
    name: str
    task_type: str
    import_path: str
    version: str
    description: str = ''

    @computed_field(return_type=str)
    @property
    def handler_id(self):
        return f'{self.task_type}:{self.version}'
