from random import shuffle


def _dummy_handler(task: dict) -> str:
    """Handle task dummy"""
    prompt = list(task['prompt'])
    shuffle(prompt)
    return str(prompt)
