from enum import Enum
from typing import Optional

from pydantic import BaseModel


class TaskFeedbackType(str, Enum):
    LIKE = 'like'
    DISLIKE = 'dislike'
    NEUTRAL = 'neutral'


class FeedbackItem(BaseModel):
    text: str
    contact: Optional[str] = None


class TaskFeedback(BaseModel):
    feedback: TaskFeedbackType = TaskFeedbackType.NEUTRAL
