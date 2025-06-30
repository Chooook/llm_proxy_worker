from enum import Enum

from pydantic import BaseModel, computed_field

from schemas.answer import Answer
from schemas.feedback import TaskFeedback, TaskFeedbackType


class TaskStatus(str, Enum):
    PENDING = 'pending'  # no handlers available
    QUEUED = 'queued'  # waiting for free handler
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'


class Task(BaseModel):
    task_id: str = ''
    prompt: str
    status: TaskStatus = TaskStatus.PENDING
    handler_id: str = ''
    user_id: str = ''
    short_task_id: str = ''
    queued_at: str = ''
    finished_at: str = ''
    context: str = ''
    retries: int = 0
    result: Answer = Answer(text='')
    error: Answer = Answer(text='')
    start_position: int = 0
    current_position: int = 0
    feedback: TaskFeedback = TaskFeedbackType.NEUTRAL
    worker_processing_time: float = 0

    @computed_field(return_type=str)
    @property
    def task_type(self):
        return self.handler_id.split(':')[0]

    @computed_field(return_type=str)
    @property
    def task_type_version(self):
        return self.handler_id.split(':')[1]
