from pydantic import BaseModel, computed_field


class HandlerConfig(BaseModel):
    name: str
    task_type: str
    version: str
    description: str = ''

    interface_func_module: str
    interface_func_name: str
    interface_endpoint_url: str = ''

    db_loader_script_path: str = ''
    service_launcher_script_path: str = ''

    git_repo: str = ''
    git_branch: str = 'master'

    @computed_field(return_type=str)
    @property
    def handler_id(self):
        return f'{self.task_type}:{self.version}'
