from pydantic import BaseModel, computed_field


class HandlerConfig(BaseModel):
    name: str
    task_type: str
    version: str
    description: str = ''

    source_dir_name: str = ''
    # OR  # TODO add check local dir or git repo
    git_repo: str = ''
    git_branch: str = 'master'

    interface_func_module: str
    interface_func_name: str

    knowledge_base_loader: str = ''
    service_launcher_script_path: str = ''
    wait_for_service_launch_seconds: int = 0

    disabled: bool = False

    @computed_field(return_type=str)
    @property
    def handler_id(self):
        return f'{self.task_type}:{self.version}'
