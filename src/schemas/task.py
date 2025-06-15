from enum import Enum

from pydantic import BaseModel

from schemas.answer import Answer
from schemas.feedback import TaskFeedbackType, TaskFeedback


class TaskType(str, Enum):
    DUMMY = 'dummy'
    GENERATE_WITH_LOCAL = 'generate_local'
    # SEARCH_IN_KNOWLEDGE_BASE = 'search'
    GENERATE_WITH_PM = 'generate_pm'
    GENERATE_WITH_SPC = 'generate_spc'
    # GENERATE_WITH_OAPSO = 'generate_oapso'


class Task(BaseModel):
    task_id: str = ''
    prompt: str
    status: str = ''
    task_type: TaskType = ''
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
