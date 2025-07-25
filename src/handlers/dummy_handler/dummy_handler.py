from random import shuffle


def handle_task_dummy(task):
    """Handle task dummy"""
    answer_text = list(task['prompt'])
    shuffle(answer_text)
    return ''.join(answer_text)
