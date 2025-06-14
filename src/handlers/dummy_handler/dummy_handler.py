from random import shuffle

from schemas.answer import Answer
from schemas.task import Task


def handle_task_dummy(task: Task) -> Answer:
    """Handle task dummy"""
    answer_text = list(task.prompt)
    shuffle(answer_text)
    return Answer(text=''.join(answer_text))
